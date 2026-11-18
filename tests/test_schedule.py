"""scrape_schedule.py: helpers, saved schedule pages, and the sheet merge/write."""

import pytest
from bs4 import BeautifulSoup

import scrape_schedule as SS
from conftest import ROOT, load_fixture
from fake_sheets import FakeWorksheet, records

SEASON = SS.SEASON


# ── helpers ───────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("raw, expected", [
    ("Feb 13, 2026 (Friday)", "2026-02-13"),
    ("May 20 2026", "2026-05-20"),
    ("February 13, 2026", "2026-02-13"),
    ("2/13/2026", "2026-02-13"),
    ("2026-02-13", "2026-02-13"),
    ("Feb 13", f"{SEASON}-02-13"),  # no year: from the season (#21)
    ("TBA", None),
    ("", None),
])
def test_parse_date(raw, expected):
    assert SS.parse_date(raw) == expected


@pytest.mark.parametrize("raw, expected", [
    ("vs. Marist", ("Marist", "H")),
    ("vs Marist", ("Marist", "H")),
    ("at Virginia Tech", ("Virginia Tech", "A")),
    ("At Virginia Tech", ("Virginia Tech", "A")),
    ("Marist", ("Marist", "H")),
    ("at Duke W, 5-3", ("Duke", "A")),
    ("", (None, "H")),
])
def test_parse_opponent(raw, expected):
    assert SS.parse_opponent(raw) == expected


@pytest.mark.parametrize("raw, expected", [
    ("W, 6-5", "W, 6-5"),
    ("W 6-5", "W, 6-5"),
    ("l 2-11", "L, 2-11"),
    ("T, 3-3", "T, 3-3"),
    ("Postponed", "Postponed"),
    ("", ""),
])
def test_normalise_result(raw, expected):
    assert SS.normalise_result(raw) == expected


@pytest.mark.parametrize("raw, expected", [
    ("4:00 p.m.", "4:00PM"),
    ("11 A.M.", "11AM"),
    ("TBA", "TBA"),
    ("2 pm", "2pm"),
])
def test_normalise_time(raw, expected):
    assert SS.normalise_time(raw) == expected


@pytest.mark.parametrize("raw, season, expected", [
    # Scraping the 2027 schedule in November 2026: spring dates are 2027.
    ("Feb 14", 2027, "2027-02-14"),
    ("Jan 3", 2027, "2027-01-03"),
    # Fall ball before the 2027 season is autumn 2026.
    ("Oct 12", 2027, "2026-10-12"),
    ("Dec 31", 2027, "2026-12-31"),
    ("9/20", 2027, "2026-09-20"),
    # A year on the page always wins.
    ("Feb 14, 2026", 2027, "2026-02-14"),
    # Leap day only exists in leap seasons.
    ("Feb 29", 2028, "2028-02-29"),
    ("Feb 29", 2027, None),
])
def test_parse_date_around_new_year(raw, season, expected):
    assert SS.parse_date(raw, season=season) == expected


@pytest.mark.parametrize("date, season", [
    ("2026-02-13", 2026), ("2026-06-20", 2026), ("2026-09-01", 2027),
    ("2026-12-31", 2027), ("2027-01-01", 2027),
])
def test_game_season(date, season):
    assert SS.game_season(date) == season


def test_extract_time():
    assert SS.extract_time("Feb 13 (Fri) 4:00 p.m.") == "4:00PM"
    assert SS.extract_time("Feb 13") is None


# ── saved pages ───────────────────────────────────────────────────────────────

def test_table_layout_etsu():
    soup = BeautifulSoup(load_fixture("etsu_schedule"), "html.parser")
    games = SS.parse_table_layout(soup, "East Tennessee State", "D1", "304")
    assert len(games) == 53
    first = games[0]
    assert {k: first[k] for k in ("DATE", "TIME", "OPPONENT", "LOCATION", "RESULT", "ESPN_TEAM_ID")} == {
        "DATE": "2026-02-13", "TIME": "2pm", "OPPONENT": "Marist",
        "LOCATION": "Johnson City, Tenn. / Thomas Stadium", "RESULT": "W, 6-5", "ESPN_TEAM_ID": "304"}
    assert sum(g["RESULT"].startswith("W") for g in games) == 30
    assert sum(g["RESULT"].startswith("L") for g in games) == 23
    assert sum(g["TV"] == "ESPN+" for g in games) > 0
    assert all(g["DATE"].startswith("2026-") for g in games)


def test_table_layout_etsu_home_away_column():
    # ETSU gives home/away in its own "At" column (#55).
    soup = BeautifulSoup(load_fixture("etsu_schedule"), "html.parser")
    games = SS.parse_table_layout(soup, "East Tennessee State", "D1", "304")
    counts = {ha: sum(g["HOME_AWAY"] == ha for g in games) for ha in "HAN"}
    assert counts == {"H": 26, "A": 26, "N": 1}


def test_json_layout_gw():
    html = (ROOT / "gw_sample.txt").read_text(encoding="utf-8")
    games = SS.parse_json_layout(BeautifulSoup(html, "html.parser"), "George Washington", "D1", "45")
    assert len(games) == 55
    first = games[0]
    assert {k: first[k] for k in ("DATE", "TIME", "OPPONENT", "HOME_AWAY", "LOCATION", "TV", "RESULT")} == {
        "DATE": "2026-02-13", "TIME": "4:00PM", "OPPONENT": "Western Carolina", "HOME_AWAY": "A",
        "LOCATION": "Cullowhee, NC", "TV": "ESPN+", "RESULT": "L, 5-7"}
    assert sum(g["HOME_AWAY"] == "A" for g in games) == 24
    assert sum(g["HOME_AWAY"] == "N" for g in games) == 1  # A-10 tournament, Tysons
    assert sum(g["TV"] == "ESPN+" for g in games) == 14


def test_json_layout_without_data_returns_nothing():
    soup = BeautifulSoup("<html><script>var x = 1;</script></html>", "html.parser")
    assert SS.parse_json_layout(soup, "GW", "D1", "") == []


def test_scrape_school_picks_layout(monkeypatch):
    soup = BeautifulSoup(load_fixture("etsu_schedule"), "html.parser")
    seen = []
    monkeypatch.setattr(SS, "fetch_soup", lambda url: seen.append(url) or soup)
    games = SS.scrape_school({"school": "East Tennessee State", "division": "D1",
                              "schedule_url": "https://etsubucs.com/sports/baseball/schedule/2026",
                              "espn_team_id": "304"})
    assert seen == [f"https://etsubucs.com/sports/baseball/schedule/{SEASON}?print=true"]
    assert len(games) == 53
    assert {g["SEASON"] for g in games} == {"2026"}


def test_scrape_school_drops_games_from_another_season(monkeypatch):
    soup = BeautifulSoup(load_fixture("etsu_schedule"), "html.parser")
    monkeypatch.setattr(SS, "fetch_soup", lambda url: soup)
    monkeypatch.setattr(SS, "SEASON", 2027)
    games = SS.scrape_school({"school": "East Tennessee State",
                              "schedule_url": "https://etsubucs.com/sports/baseball/schedule/{season}"})
    assert games == []


# ── merge with previous rows, and the sheet write ─────────────────────────────

def game(school, date, opp="X"):
    return SS._build_game(school, "D1", date, "1PM", opp, "H", "", "", "", "")


def test_merge_keeps_previous_rows_when_school_scrape_empty():
    previous = [game("A", "2026-03-01"), game("B", "2026-03-01")]
    new = {"A": [game("A", "2026-03-02")], "B": []}
    merged, kept = SS.merge_with_previous(["A", "B"], new, previous)
    assert [g["SCHOOL"] for g in merged] == ["A", "B"]
    assert merged[0]["DATE"] == "2026-03-02"
    assert "B" in kept and "A" not in kept


def test_merge_keeps_previous_rows_when_far_fewer_games():
    previous = [game("A", f"2026-03-{d:02d}") for d in range(1, 21)]
    new = {"A": [game("A", "2026-03-01")] * 4}
    merged, kept = SS.merge_with_previous(["A"], new, previous, season=2026)
    assert len(merged) == 20 and "A" in kept


def test_merge_never_keeps_last_seasons_rows():
    previous = [game("A", f"2026-03-{d:02d}") for d in range(1, 21)]
    # New season, school's page not up yet: no rows, rather than last spring's.
    merged, kept = SS.merge_with_previous(["A"], {"A": []}, previous, season=2027)
    assert merged == [] and not kept
    # New season, a short early schedule: taken as is.
    new = {"A": [game("A", "2027-02-14")] * 4}
    merged, kept = SS.merge_with_previous(["A"], new, previous, season=2027)
    assert len(merged) == 4 and not kept


def test_merge_previous_rows_without_season_column_use_their_date():
    previous = [game("A", "2026-03-01")]
    del previous[0]["SEASON"]
    merged, kept = SS.merge_with_previous(["A"], {}, previous, season=2026)
    assert merged[0]["SEASON"] == "2026" and "A" in kept


def test_merge_refreshes_division_and_espn_id_on_kept_rows():
    stale = dict(game("GW", "2026-03-01"), DIVISION="D2", ESPN_TEAM_ID="45")
    config = {"GW": {"school": "GW", "division": "D1", "espn_team_id": "2638"}}
    merged, _ = SS.merge_with_previous(["GW"], {}, [stale], config, season=2026)
    assert (merged[0]["DIVISION"], merged[0]["ESPN_TEAM_ID"]) == ("D1", "2638")


def test_merge_drops_schools_no_longer_configured():
    merged, _ = SS.merge_with_previous(["A"], {"A": [game("A", "2026-03-01")]},
                                       [game("Gone", "2026-03-01")])
    assert [g["SCHOOL"] for g in merged] == ["A"]


def test_write_overwrites_then_clears_leftover_rows():
    old = [SS.SHEET_COLUMNS] + [[f"old{i}"] * len(SS.SHEET_COLUMNS) for i in range(5)]
    ws = FakeWorksheet("Schedule", old, row_count=6)
    previous, row_count = SS.read_existing_games(ws)
    assert len(previous) == 5 and row_count == 6

    SS.write_to_sheet(ws, [game("A", "2026-03-01"), game("A", "2026-03-02")], row_count)
    rows = records(ws)
    assert [r["DATE"] for r in rows] == ["2026-03-01", "2026-03-02"]


def test_write_grows_sheet_when_needed():
    ws = FakeWorksheet("Schedule", [SS.SHEET_COLUMNS], row_count=2)
    SS.write_to_sheet(ws, [game("A", f"2026-03-{d:02d}") for d in range(1, 11)], 1)
    assert len(records(ws)) == 10 and ws.row_count >= 11
