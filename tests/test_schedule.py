"""scrape_schedule.py: helpers, saved schedule pages, and the sheet merge/write."""

import pytest
from bs4 import BeautifulSoup

import scrape_schedule as SS
from conftest import ROOT, load_fixture
from fake_sheets import FakeWorksheet, records

YEAR = SS.CURRENT_YEAR


# ── helpers ───────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("raw, expected", [
    ("Feb 13, 2026 (Friday)", "2026-02-13"),
    ("May 20 2026", "2026-05-20"),
    ("February 13, 2026", "2026-02-13"),
    ("2/13/2026", "2026-02-13"),
    ("2026-02-13", "2026-02-13"),
    ("Feb 13", f"{YEAR}-02-13"),  # no year: current calendar year (see #21)
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


@pytest.mark.xfail(strict=True, reason=(
    "Known bug: ETSU's print page gives home/away in its own 'At' column "
    "(26 Home, 26 Away, 1 Neutral), which parse_table_layout ignores, so every game is 'H'."))
def test_table_layout_etsu_home_away():
    soup = BeautifulSoup(load_fixture("etsu_schedule"), "html.parser")
    games = SS.parse_table_layout(soup, "East Tennessee State", "D1", "304")
    assert sum(g["HOME_AWAY"] == "A" for g in games) == 26


def test_json_layout_gw():
    html = (ROOT / "gw_sample.txt").read_text(encoding="utf-8")
    games = SS.parse_json_layout(BeautifulSoup(html, "html.parser"), "George Washington", "D1", "45")
    assert len(games) == 55
    first = games[0]
    assert {k: first[k] for k in ("DATE", "TIME", "OPPONENT", "HOME_AWAY", "LOCATION", "TV", "RESULT")} == {
        "DATE": "2026-02-13", "TIME": "4:00PM", "OPPONENT": "Western Carolina", "HOME_AWAY": "A",
        "LOCATION": "Cullowhee, NC", "TV": "ESPN+", "RESULT": "L, 5-7"}
    assert sum(g["HOME_AWAY"] == "A" for g in games) == 24
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
    assert seen == ["https://etsubucs.com/sports/baseball/schedule/2026?print=true"]
    assert len(games) == 53


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


def test_merge_keeps_previous_rows_when_far_fewer_games_same_season():
    previous = [game("A", f"2026-03-{d:02d}") for d in range(1, 21)]
    new = {"A": [game("A", "2026-03-01")] * 4}
    merged, kept = SS.merge_with_previous(["A"], new, previous)
    assert len(merged) == 20 and "A" in kept


def test_merge_accepts_short_schedule_in_a_new_season():
    previous = [game("A", f"2026-03-{d:02d}") for d in range(1, 21)]
    new = {"A": [game("A", "2027-02-14")] * 4}
    merged, kept = SS.merge_with_previous(["A"], new, previous)
    assert len(merged) == 4 and not kept


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
