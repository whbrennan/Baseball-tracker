"""
Stats pages: saved real pages, parsed the way the scraper parses live ones
(Chromium reads the tables; the site's own scripts are turned off so the
saved DOM is what gets parsed).

Expected values are the real 2026 season lines on the day the pages were saved.
"""

import pytest

import scraper as S
from conftest import load_fixture
from fake_sheets import FakeSpreadsheet, records

ETSU_URL = "https://etsubucs.com/sports/baseball/stats/2026"
MARYLAND_URL = "https://umterps.com/sports/baseball/stats/2026"

FETTERMAN = {"PlayerID": "P001", "Name": "Tyler Fetterman", "School": "East Tennessee State University",
             "Division": "D1", "Type": "Hitter", "Jersey": "24", "Stats_URL": ETSU_URL}
MENDEZ = {"PlayerID": "P002", "Name": "David Mendez", "School": "University of Maryland",
          "Division": "D1", "Type": "Hitter", "Jersey": "9", "Stats_URL": MARYLAND_URL}


def tables_of(page, name):
    page.set_content(load_fixture(name))
    return S.read_tables(page)


# ── classic layout: every table on the page ───────────────────────────────────

def test_classic_layout_batting(page):
    table, hdrs = S.find_table(tables_of(page, "etsu_stats"), "batting")
    assert table is not None and "SB-ATT" in hdrs
    out = S.map_row(S.player_from_table(table, hdrs, "24"), S.BATTING_MAP)
    assert {k: out[k] for k in ("G", "GS", "AB", "H", "2B", "HR", "RBI", "BB", "SO")} == {
        "G": "38", "GS": "31", "AB": "118", "H": "31", "2B": "12", "HR": "3",
        "RBI": "28", "BB": "21", "SO": "31"}
    assert (out["SB"], out["CS"]) == ("0", "1")  # 0-1: caught once
    assert (out["AVG"], out["OBP"], out["SLG"], out["OPS"]) == (".263", ".378", ".441", ".819")


def test_classic_layout_defense(page):
    table, hdrs = S.find_table(tables_of(page, "etsu_stats"), "defense")
    out = S.map_row(S.player_from_table(table, hdrs, "24"), S.DEFENSE_MAP)
    assert {k: out[k] for k in S.DEFENSE_COLS} == {
        "TC": "2", "PO": "1", "A": "0", "E": "1", "FLD%": ".500",
        "DP": "0", "SBA": "0", "CSB": "0", "PB": "0", "CI": "0"}


def test_classic_layout_jersey_not_listed(page):
    table, hdrs = S.find_table(tables_of(page, "etsu_stats"), "batting")
    assert S.player_from_table(table, hdrs, "999") is None


# ── dropdown layout: one table shown, the rest in embedded data ───────────────

def test_dropdown_layout_batting_table_with_name_column(page):
    table, hdrs = S.find_table(tables_of(page, "maryland_stats"), "batting")
    out = S.map_row(S.player_from_table(table, hdrs, "9"), S.BATTING_MAP)
    assert (out["G"], out["GS"], out["AB"], out["H"]) == ("54", "54", "215", "68")
    # Issue #51's example: SB-ATT 12-14, caught stealing 2.
    assert (out["SB"], out["CS"]) == ("12", "2")


def test_dropdown_layout_has_no_defense_table(page):
    assert S.find_table(tables_of(page, "maryland_stats"), "defense") == (None, [])


def test_dropdown_layout_defense_from_embedded_data(page):
    page.set_content(load_fixture("maryland_stats"))
    payload = S._parse_payload(page.eval_on_selector_all("script", "els => els.map(e => e.textContent || '')"))
    raw = S.embedded_stats_from_payload(payload, "defense", "9")
    assert {k: v for k, v in S.map_row(raw, S.DEFENSE_MAP).items() if k in S.DEFENSE_COLS} == {
        "TC": "219", "PO": "81", "A": "131", "E": "7", "FLD%": ".968",
        "DP": "28", "SBA": "0", "CSB": "0", "PB": "0", "CI": "0"}


def test_embedded_data_jersey_missing_vs_no_data():
    payload = S._parse_payload(S._SCRIPT_RE.findall(load_fixture("maryland_stats")))
    assert S.embedded_stats_from_payload(payload, "defense", "999") == {}
    assert S.embedded_stats_from_payload(None, "defense", "9") is None
    assert S.embedded_stats_from_payload(payload, "batting", "9") is None  # not an embedded list


def test_embedded_pitching_record_shape():
    payload = S._parse_payload(S._SCRIPT_RE.findall(load_fixture("maryland_stats")))
    pitchers = []
    for obj in payload:
        if isinstance(obj, dict) and "individualStats" in obj:
            container = payload[obj["individualStats"]]
            pitchers = [payload[i] for i in payload[container["individualPitchingStats"]]]
            break
    jersey = payload[pitchers[0]["playerUniform"]]
    raw = S.embedded_stats_from_payload(payload, "pitching", jersey)
    out = S.map_row(raw, S.PITCHING_MAP)
    assert out["IP"] and S.innings_to_outs(out["IP"]) is not None
    assert out["ERA"] and out["G"] != ""


# ── scrape(): the whole page flow, with the network replaced by the fixture ───

def serve(page, url, fixture):
    html = load_fixture(fixture)
    page.route(url, lambda route: route.fulfill(status=200, content_type="text/html", body=html))


def test_scrape_classic_page(page, no_sleep):
    serve(page, ETSU_URL, "etsu_stats")
    stats, note = S.scrape(page, FETTERMAN, "batting")
    assert note == ""
    assert (stats["AB"], stats["SB"], stats["CS"]) == ("118", "0", "1")


def test_scrape_falls_back_to_embedded_defense(page, no_sleep):
    serve(page, MARYLAND_URL, "maryland_stats")
    stats, note = S.scrape(page, MENDEZ, "defense")
    assert note == ""
    assert (stats["TC"], stats["PO"], stats["A"]) == ("219", "81", "131")


def test_scrape_missing_jersey_returns_zeros_with_note(page, no_sleep):
    serve(page, ETSU_URL, "etsu_stats")
    stats, note = S.scrape(page, dict(FETTERMAN, Jersey="999"), "batting")
    assert "not listed" in note
    assert stats["AB"] == "0" and stats["CS"] == "0"


def test_scrape_without_jersey_raises(page):
    with pytest.raises(S.ScrapeError):
        S.scrape(page, dict(FETTERMAN, Jersey=""), "batting")


def test_scrape_page_without_stats_raises(page, no_sleep, monkeypatch):
    page.route(ETSU_URL, lambda route: route.fulfill(status=200, content_type="text/html",
                                                     body="<html><body>Down for maintenance</body></html>"))
    # Don't sit through the scraper's 15s waits for embedded data that will never
    # appear, and keep its raw-HTML fallback (which goes to the live site) offline.
    monkeypatch.setattr(page, "wait_for_function", lambda *a, **k: None)
    monkeypatch.setattr(S.requests, "get", lambda *a, **k: (_ for _ in ()).throw(OSError("offline")))
    with pytest.raises(S.ScrapeError, match="No pitching table"):
        S.scrape(page, dict(FETTERMAN, Type="Pitcher"), "pitching")


# ── process_stat(): page -> sheet, against an in-memory spreadsheet ───────────

BATTING_HEADER = S.BASE_FIELDS + S.BATTING_COLS
HISTORY_HEADER = S.HISTORY_PREFIX_FIELDS + S.BATTING_COLS


def old_batting_row(last_updated, **stats):
    """A Batting row as the pre-#51 scraper wrote it (CS = attempts)."""
    base = {"Last_Updated": last_updated, "PlayerID": "P002", "Name": "David Mendez",
            "School": "University of Maryland", "Division": "D1",
            "G": "54", "GS": "54", "AB": "215", "R": "56", "H": "68", "2B": "13", "3B": "2",
            "HR": "11", "RBI": "50", "BB": "25", "SO": "31", "HBP": "8", "SAC": "2", "SF": "4",
            "SB": "12", "CS": "14", "AVG": ".316", "OBP": ".401", "SLG": ".549", "OPS": ".950",
            "ISO": "0.233", "BABIP": "0.322", "BB_pct": "9.8", "K_pct": "12.2"}
    base.update(stats)
    return [base[h] for h in BATTING_HEADER]


def sheet_with(batting_rows=(), history_rows=()):
    return FakeSpreadsheet({
        "Batting": [BATTING_HEADER, *batting_rows],
        "Batting_History": [HISTORY_HEADER, *history_rows],
    })


def test_process_stat_corrects_cs_and_snapshots_once(page, no_sleep):
    serve(page, MARYLAND_URL, "maryland_stats")
    this_season = S.datetime.now(S.EASTERN).strftime("%Y-%m-%d 10:00")
    sheet = sheet_with([old_batting_row(this_season)])

    S.process_stat(sheet, page, MENDEZ, "batting")
    current = records(sheet.worksheet("Batting"))
    history = records(sheet.worksheet("Batting_History"))
    assert len(current) == 1
    assert (current[0]["SB"], current[0]["CS"]) == ("12", "2")
    assert len(history) == 1 and history[0]["CS"] == "2"

    # Same page again: nothing changed, so no second snapshot.
    S.process_stat(sheet, page, MENDEZ, "batting")
    assert len(records(sheet.worksheet("Batting_History"))) == 1


def test_process_stat_adds_new_player(page, no_sleep):
    serve(page, ETSU_URL, "etsu_stats")
    sheet = sheet_with()
    S.process_stat(sheet, page, FETTERMAN, "batting")
    row = records(sheet.worksheet("Batting"))[0]
    assert (row["PlayerID"], row["AB"], row["CS"]) == ("P001", "118", "1")
    assert len(records(sheet.worksheet("Batting_History"))) == 1


def test_process_stat_refuses_stats_that_went_down(page, no_sleep):
    serve(page, MARYLAND_URL, "maryland_stats")
    this_season = S.datetime.now(S.EASTERN).strftime("%Y-%m-%d 10:00")
    before = old_batting_row(this_season, AB="300", H="90")
    sheet = sheet_with([before])
    with pytest.raises(S.ScrapeError, match="went down"):
        S.process_stat(sheet, page, MENDEZ, "batting")
    assert sheet.worksheet("Batting").get_all_values()[1] == before
    assert records(sheet.worksheet("Batting_History")) == []
