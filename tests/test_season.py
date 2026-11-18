"""season.py and the scraper's Season column."""

import json

import pytest

import check_season as C
import scraper as S
import season
from conftest import ROOT
from fake_sheets import FakeWorksheet, records


def test_season_file_is_the_setting():
    assert season.SEASON == json.loads((ROOT / "season.json").read_text())["season"]


def test_env_overrides_season(monkeypatch):
    monkeypatch.setenv("SEASON", "2031")
    assert season.load_season() == 2031


@pytest.mark.parametrize("url, expected", [
    ("https://etsubucs.com/sports/baseball/stats/2026", "https://etsubucs.com/sports/baseball/stats/2027"),
    ("https://a.com/sports/baseball/schedule/{season}", "https://a.com/sports/baseball/schedule/2027"),
    # No year in the path: add it (these sites serve /stats/<year> too).
    ("https://marymountsaints.com/sports/baseball/stats", "https://marymountsaints.com/sports/baseball/stats/2027"),
    ("https://a.com/sports/baseball/schedule/", "https://a.com/sports/baseball/schedule/2027"),
    # Query strings survive.
    ("https://radfordathletics.com/sports/baseball/stats/2026?path=baseball",
     "https://radfordathletics.com/sports/baseball/stats/2027?path=baseball"),
    ("https://hardingsports.com/sports/baseball/stats?path=base",
     "https://hardingsports.com/sports/baseball/stats/2027?path=base"),
    # Anything else is left alone.
    ("https://a.com/sports/baseball/roster", "https://a.com/sports/baseball/roster"),
    ("", ""),
])
def test_season_url(url, expected):
    assert season.season_url(url, 2027) == expected


def test_every_school_url_is_a_season_template():
    schools = json.loads((ROOT / "schools.json").read_text())
    assert all("{season}" in s["schedule_url"] for s in schools)


# ── Season column on the stats tabs ───────────────────────────────────────────

HEADER = S.HISTORY_PREFIX_FIELDS + S.BATTING_COLS


def hist_row(last_updated):
    row = dict.fromkeys(HEADER, "0")
    row.update({"Last_Updated": last_updated, "PlayerID": "P1"})
    return [row[h] for h in HEADER]


def test_ensure_season_column_adds_and_backfills_once():
    # Trailing blank header cells, as on the real Batting_History tab.
    ws = FakeWorksheet("Batting_History",
                       [HEADER + ["", ""], hist_row("2026-02-24 04:21"), hist_row("2027-03-01 10:00")],
                       col_count=len(HEADER))
    assert S.ensure_season_column(ws) is True
    values = ws.get_all_values()
    assert values[0][len(HEADER)] == "Season"
    assert [r["Season"] for r in records(ws)] == ["2026", "2027"]
    assert S.ensure_season_column(ws) is False


def test_new_rows_carry_the_season():
    payload = S.build_history_payload(
        {"PlayerID": "P1", "Name": "N", "School": "S", "Division": "D1"}, {}, S.BATTING_COLS)
    assert payload["Season"] == str(S.SEASON)
    row = S.build_row(HEADER + ["Season"], payload, S.BATTING_COLS, history=True)
    assert row[-1] == str(S.SEASON)


@pytest.mark.parametrize("row, expected", [
    ({"Season": "2027", "Last_Updated": "2026-12-01 10:00"}, 2027),
    ({"Season": "", "Last_Updated": "2026-05-01 10:00"}, 2026),
    ({}, None),
])
def test_row_season(row, expected):
    assert S.row_season(row) == expected


def test_backwards_guard_follows_the_season_setting(monkeypatch):
    previous = {"Season": "2026", "Last_Updated": "2026-11-17 10:00", "G": 54, "AB": 215}
    zeros = {"G": "0", "AB": "0"}
    monkeypatch.setattr(S, "SEASON", 2026)
    assert S.stats_went_backwards(previous, zeros, "batting")
    # After the switch to 2027, last season's totals may reset to zero.
    monkeypatch.setattr(S, "SEASON", 2027)
    assert S.stats_went_backwards(previous, zeros, "batting") == []


# ── preseason URL check ───────────────────────────────────────────────────────


def test_check_targets_build_every_url_for_the_season():
    schools = [{"school": "ETSU", "schedule_url": "https://etsubucs.com/sports/baseball/schedule/{season}"}]
    players = [{"School": "ETSU", "Stats_URL": "https://etsubucs.com/sports/baseball/stats/2026"},
               {"School": "ETSU", "Stats_URL": "https://etsubucs.com/sports/baseball/stats/2026"},
               {"School": "Harding", "Stats_URL": "https://hardingsports.com/sports/baseball/stats?path=base"}]
    assert C.targets(2027, schools, players) == [
        ("schedule: ETSU", "https://etsubucs.com/sports/baseball/schedule/2027?print=true"),
        ("stats: ETSU", "https://etsubucs.com/sports/baseball/stats/2027"),
        ("stats: Harding", "https://hardingsports.com/sports/baseball/stats/2027?path=base"),
    ]


@pytest.mark.parametrize("final, status, title, verdict", [
    ("https://a.com/sports/baseball/stats/2027", 200, "2027 Baseball Statistics", "OK"),
    ("https://a.com/sports/baseball/schedule", 200, "Baseball Schedule", "NOT PUBLISHED"),
    ("https://a.com/sports/baseball/stats/2027", 200, "Baseball Cumulative Statistics", "WRONG SEASON"),
    ("https://a.com/sports/baseball/stats/2027", 403, "", "ERROR"),
    ("https://b.com/sports/baseball/stats/2027", 200, "2027 Baseball Statistics", "MOVED"),
    ("https://a.com/sports/baseball/stats/2027", None, "", "ERROR"),
])
def test_check_classify(final, status, title, verdict):
    assert C.classify(2027, "https://a.com/sports/baseball/stats/2027", final, status, title)[0] == verdict
