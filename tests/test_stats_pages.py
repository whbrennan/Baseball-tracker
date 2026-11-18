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
    payload = S._parse_payload(S.scripts_in_html(load_fixture("maryland_stats")))
    assert S.embedded_stats_from_payload(payload, "defense", "999") == {}
    assert S.embedded_stats_from_payload(None, "defense", "9") is None
    assert S.embedded_stats_from_payload(payload, "batting", "9") is None  # not an embedded list


def test_embedded_pitching_record_shape():
    payload = S._parse_payload(S.scripts_in_html(load_fixture("maryland_stats")))
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


# ── load_stats_page(): the whole page flow, network replaced by fixtures ──────

def serve(page, url, fixture=None, body=None, hits=None):
    html = body if body is not None else load_fixture(fixture)

    def handle(route):
        if hits is not None:
            hits.append(route.request.url)
        route.fulfill(status=200, content_type="text/html", body=html)
    page.route(url, handle)


@pytest.fixture
def quick(monkeypatch, no_sleep):
    """Short waits, and the raw-HTML fallback (which goes to the live site) kept offline."""
    monkeypatch.setattr(S, "READY_TIMEOUT_MS", 300)
    monkeypatch.setattr(S.requests, "get", lambda *a, **k: (_ for _ in ()).throw(OSError("offline")))


def test_classic_page_loads_once_for_batting_and_defense(page, quick):
    hits = []
    serve(page, ETSU_URL, "etsu_stats", hits=hits)
    data = S.load_stats_page(page, ETSU_URL, ["batting", "defense"])
    assert len(hits) == 1
    bat, _ = S.player_stats(data, "batting", "24")
    fld, _ = S.player_stats(data, "defense", "24")
    assert (bat["AB"], bat["SB"], bat["CS"]) == ("118", "0", "1")
    assert (fld["TC"], fld["PO"]) == ("2", "1")


def test_dropdown_layout_defense_from_embedded_data_same_load(page, quick):
    hits = []
    serve(page, MARYLAND_URL, "maryland_stats", hits=hits)
    data = S.load_stats_page(page, MARYLAND_URL, ["batting", "defense"])
    assert len(hits) == 1
    fld, note = S.player_stats(data, "defense", "9")
    assert note == "" and (fld["TC"], fld["PO"], fld["A"]) == ("219", "81", "131")


def test_player_missing_from_page_gets_zeros_with_note(page, quick):
    serve(page, ETSU_URL, "etsu_stats")
    data = S.load_stats_page(page, ETSU_URL, ["batting"])
    stats, note = S.player_stats(data, "batting", "999")
    assert "not listed" in note and stats["AB"] == "0" and stats["CS"] == "0"


def test_no_jersey_raises():
    with pytest.raises(S.ScrapeError, match="No jersey"):
        S.player_stats(S.StatsPage("u"), "batting", "")


def test_page_without_stats_is_retried_then_reported(page, quick):
    hits = []
    serve(page, ETSU_URL, body="<html><body>Down for maintenance</body></html>", hits=hits)
    data = S.load_stats_page(page, ETSU_URL, ["pitching"])
    assert len(hits) == S.LOAD_ATTEMPTS
    with pytest.raises(S.ScrapeError, match="No pitching table"):
        S.player_stats(data, "pitching", "24")


def test_page_that_never_loads_raises(page, quick):
    page.route(ETSU_URL, lambda route: route.abort())
    with pytest.raises(S.ScrapeError, match="Could not load"):
        S.load_stats_page(page, ETSU_URL, ["batting"])


# A minimal dropdown-layout page: the Batting table is shown; choosing Fielding
# from the s-select-box swaps in the fielding table. No embedded data.
DROPDOWN_PAGE = """<html><body>
<button class="s-select-box" onclick="document.getElementById('opts').style.display='block'">Batting</button>
<ul id="opts" style="display:none">
  <li class="s-select__option-item" onclick="pick('p')">Pitching</li>
  <li class="s-select__option-item" onclick="pick('f')">Fielding</li>
</ul>
<div id="t"><table><tr><th>#</th><th>AVG</th><th>AB</th></tr><tr><td>7</td><td>.300</td><td>10</td></tr></table></div>
<script>
function pick(k) { setTimeout(() => {
  document.getElementById('t').innerHTML = k === 'f'
    ? '<table><tr><th>#</th><th>C</th><th>PO</th><th>A</th><th>E</th><th>FLD%</th></tr>' +
      '<tr><td>7</td><td>12</td><td>9</td><td>3</td><td>0</td><td>1.000</td></tr></table>'
    : '<table><tr><th>#</th><th>IP</th><th>ERA</th></tr><tr><td>7</td><td>3.1</td><td>2.70</td></tr></table>';
}, 200); }
</script></body></html>"""


@pytest.fixture
def js_page(browser):
    context = browser.new_context()  # the dropdown needs the page's own script
    context.route("**/*", lambda route: route.abort())
    page = context.new_page()
    yield page
    context.close()


def test_dropdown_is_used_when_no_table_or_embedded_data(js_page, quick):
    url = "https://dropdown.example/sports/baseball/stats/2026"
    serve(js_page, url, body=DROPDOWN_PAGE)
    data = S.load_stats_page(js_page, url, ["batting", "defense"])
    bat, _ = S.player_stats(data, "batting", "7")
    fld, _ = S.player_stats(data, "defense", "7")
    assert bat["AB"] == "10"
    assert (fld["TC"], fld["PO"], fld["A"], fld["FLD%"]) == ("12", "9", "3", "1.000")


def test_embedded_override_never_clicks_the_dropdown(js_page, quick):
    url = "https://dropdown.example/sports/baseball/stats/2026"
    serve(js_page, url, body=DROPDOWN_PAGE)
    data = S.load_stats_page(js_page, url, ["defense"], layout="embedded")
    assert data.missing(["defense"]) == ["defense"]


# ── scrape_players(): pages -> in-memory spreadsheet ──────────────────────────

BATTING_HEADER = S.BASE_FIELDS + S.BATTING_COLS
HISTORY_HEADER = S.HISTORY_PREFIX_FIELDS + S.BATTING_COLS
DEFENSE_HEADER = S.BASE_FIELDS + S.DEFENSE_COLS
DEF_HISTORY_HEADER = S.HISTORY_PREFIX_FIELDS + S.DEFENSE_COLS
CURLESS = {"PlayerID": "P099", "Name": "Tristan Curless", "School": "East Tennessee State University",
           "Division": "D1", "Type": "Hitter", "Jersey": "32", "Stats_URL": ETSU_URL}


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


def sheet_with(batting_rows=()):
    return FakeSpreadsheet({
        "Batting": [BATTING_HEADER, *batting_rows],
        "Batting_History": [HISTORY_HEADER],
        "Defense": [DEFENSE_HEADER],
        "Defense_History": [DEF_HISTORY_HEADER],
        "Scrape_Log": [["Time", "PlayerID", "Name", "School", "Status", "Notes"]],
    })


def this_season():
    return S.datetime.now(S.EASTERN).strftime("%Y-%m-%d 10:00")


def test_scrape_players_end_to_end(page, quick):
    hits = []
    serve(page, ETSU_URL, "etsu_stats", hits=hits)
    serve(page, MARYLAND_URL, "maryland_stats", hits=hits)
    sheet = sheet_with([old_batting_row(this_season())])

    ok, failed = S.scrape_players(sheet, page, [FETTERMAN, MENDEZ, CURLESS], schools=[])
    assert (ok, failed) == (["Tyler Fetterman", "David Mendez", "Tristan Curless"], [])
    # One load per stats page, even with two ETSU hitters and two stat types each.
    assert sorted(hits) == [ETSU_URL, MARYLAND_URL]

    batting = {r["PlayerID"]: r for r in records(sheet.worksheet("Batting"))}
    assert (batting["P002"]["SB"], batting["P002"]["CS"]) == ("12", "2")   # updated in place
    assert batting["P001"]["AB"] == "118" and batting["P099"]["AB"] == "207"  # added
    assert {r["Season"] for r in batting.values()} == {str(S.SEASON)}
    assert len(records(sheet.worksheet("Batting_History"))) == 3
    assert len(records(sheet.worksheet("Defense"))) == 3
    assert [r["Status"] for r in records(sheet.worksheet("Scrape_Log"))] == ["SUCCESS"] * 3


def test_scrape_players_batches_sheet_calls(page, quick):
    serve(page, ETSU_URL, "etsu_stats")
    serve(page, MARYLAND_URL, "maryland_stats")
    sheet = sheet_with([old_batting_row(this_season())])
    S.scrape_players(sheet, page, [FETTERMAN, MENDEZ, CURLESS], schools=[])

    batting, history = sheet.worksheet("Batting"), sheet.worksheet("Batting_History")
    # Season column added on first run (one extra read + one write), then one
    # read, one batch_update for existing rows and one append_rows for new ones.
    assert batting.calls["get_all_values"] == 2
    assert batting.calls["batch_update"] == 1 and batting.calls["append_rows"] == 1
    assert batting.calls["append_row"] == 0 and batting.calls["get_all_records"] == 0
    assert history.calls["append_rows"] == 1 and history.calls["append_row"] == 0
    assert sheet.worksheet("Scrape_Log").calls["append_rows"] == 1

    # Second run: Season column exists, so a single read per current tab.
    batting.calls.clear()
    S.scrape_players(sheet, page, [FETTERMAN, MENDEZ, CURLESS], schools=[])
    assert batting.calls["get_all_values"] == 1
    # Nothing changed, so no new snapshots.
    assert len(records(history)) == 3


def test_scrape_players_refuses_stats_that_went_down(page, quick):
    serve(page, MARYLAND_URL, "maryland_stats")
    before = old_batting_row(this_season(), AB="300", H="90")
    sheet = sheet_with([before])
    ok, failed = S.scrape_players(sheet, page, [MENDEZ], schools=[])
    assert ok == [] and "went down" in failed[0]
    assert records(sheet.worksheet("Batting"))[0]["AB"] == "300"
    assert records(sheet.worksheet("Batting_History")) == []
    assert records(sheet.worksheet("Scrape_Log"))[0]["Status"] == "ERROR"


def test_scrape_players_reports_a_page_that_never_loads(page, quick):
    page.route(ETSU_URL, lambda route: route.abort())
    sheet = sheet_with()
    ok, failed = S.scrape_players(sheet, page, [FETTERMAN, CURLESS], schools=[])
    assert ok == [] and len(failed) == 2 and "Could not load" in failed[0]


def test_group_by_page_uses_the_season_url():
    a = dict(FETTERMAN, Stats_URL="https://x.com/sports/baseball/stats/2025")
    b = dict(CURLESS, Stats_URL="https://x.com/sports/baseball/stats")
    groups = S.group_by_page([a, b])
    assert list(groups) == [f"https://x.com/sports/baseball/stats/{S.SEASON}"]
