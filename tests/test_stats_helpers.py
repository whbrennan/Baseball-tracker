"""Pure helpers in scraper.py: no network, browser or sheet."""

import pytest

import scraper as S


# ── combined cells and header alignment ───────────────────────────────────────

@pytest.mark.parametrize("value, expected", [
    ("38-31", ("38", "31")),
    ("54 - 54", ("54", "54")),
    ("12-14", ("12", "14")),
    ("7", ("", "")),
    ("", ("", "")),
])
def test_split_combined(value, expected):
    assert S.split_combined(value) == expected


def test_align_headers_drops_player_when_name_cell_is_a_th():
    hdrs = ["#", "PLAYER", "AVG", "AB"]
    assert S.align_headers(hdrs, ["9", ".316", "215"]) == ["#", "AVG", "AB"]


def test_align_headers_keeps_player_when_name_is_a_td():
    hdrs = ["#", "PLAYER", "AVG", "AB"]
    cells = ["9", "Mendez, David", ".316", "215"]
    assert S.align_headers(hdrs, cells) == hdrs


# ── caught stealing (#51) ─────────────────────────────────────────────────────

def batting_raw(**overrides):
    raw = {"GP-GS": "54-54", "AB": "215", "R": "56", "H": "68", "2B": "13", "3B": "2",
           "HR": "11", "RBI": "50", "BB": "25", "SO": "31", "HBP": "8", "SH": "2",
           "SF": "4", "SB-ATT": "12-14", "AVG": ".316", "OB%": ".401", "SLG%": ".549",
           "OPS": ".950"}
    raw.update(overrides)
    return raw


def test_cs_is_attempts_minus_steals():
    out = S.map_row(batting_raw(), S.BATTING_MAP)
    assert (out["SB"], out["CS"]) == ("12", "2")
    assert "SB_ATT" not in out


@pytest.mark.parametrize("sb_att, sb, cs", [
    ("0-0", "0", "0"),
    ("0-1", "0", "1"),
    ("3 - 3", "3", "0"),
    ("", "", ""),
    ("-", "", ""),
])
def test_cs_edge_cases(sb_att, sb, cs):
    out = S.map_row(batting_raw(**{"SB-ATT": sb_att}), S.BATTING_MAP)
    assert (out["SB"], out["CS"]) == (sb, cs)


def test_zero_stats_has_cs_not_attempts():
    out = S.zero_stats(S.BATTING_MAP)
    assert out["SB"] == "0" and out["CS"] == "0"
    assert "SB_ATT" not in out
    assert set(S.BATTING_COLS) - set(out) == {"ISO", "BABIP", "BB_pct", "K_pct"}


# ── batting derived stats ─────────────────────────────────────────────────────

def test_batting_derived_stats():
    out = S.map_row(batting_raw(), S.BATTING_MAP)
    # PA = 215 + 25 + 8 + 4 + 2 = 254
    assert out["BB_pct"] == "9.8"
    assert out["K_pct"] == "12.2"
    assert out["ISO"] == "0.233"
    # (68 - 11) / (215 - 31 - 11 + 4)
    assert out["BABIP"] == "0.322"


def test_ops_falls_back_to_obp_plus_slg():
    out = S.map_row(batting_raw(OPS=""), S.BATTING_MAP)
    assert out["OPS"] == "0.950"


# ── innings pitched ───────────────────────────────────────────────────────────

@pytest.mark.parametrize("ip, outs", [
    ("0.0", 0), ("1.0", 3), ("7.1", 22), ("7.2", 23), ("45.2", 137),
    ("12", 36), (".1", 1), ("", None), ("--", None), (None, None),
])
def test_innings_to_outs(ip, outs):
    assert S.innings_to_outs(ip) == outs


def pitching_raw(**overrides):
    raw = {"APP-GS": "10-3", "W-L": "2-1", "SV": "0", "IP": "7.2", "H": "6", "R": "4",
           "ER": "3", "BB": "13", "SO": "5", "HBP": "1", "ERA": "3.52", "WHIP": "2.48"}
    raw.update(overrides)
    return raw


def test_pitching_rates_use_thirds_of_an_inning():
    out = S.map_row(pitching_raw(), S.PITCHING_MAP)
    # 7.2 IP is 7 2/3 innings, not 7.2.
    assert out["K_per_9"] == "5.87"
    assert out["BB_per_9"] == "15.26"
    assert out["K_BB"] == "0.38"
    # BF estimate = 23 outs + 6 H + 13 BB + 1 HBP = 43
    assert out["K_pct"] == "11.6"
    assert out["BB_pct"] == "30.2"


def test_pitching_rates_skipped_without_innings():
    out = S.map_row(pitching_raw(IP="0.0", SO="0", BB="1"), S.PITCHING_MAP)
    assert "K_per_9" not in out


def test_pitching_combined_columns():
    out = S.map_row(pitching_raw(), S.PITCHING_MAP)
    assert (out["G"], out["GS"], out["W"], out["L"]) == ("10", "3", "2", "1")


# ── change detection and safety checks ────────────────────────────────────────

def test_stats_changed_ignores_number_formatting():
    previous = {"AB": 215, "AVG": 0.316, "CS": ""}
    assert not S.stats_changed(previous, {"AB": "215", "AVG": ".316", "CS": ""}, ["AB", "AVG", "CS"])


def test_stats_changed_detects_cs_correction():
    previous = {"SB": 12, "CS": 14}
    assert S.stats_changed(previous, {"SB": "12", "CS": "2"}, ["SB", "CS"])


def test_stats_changed_when_no_previous_row():
    assert S.stats_changed(None, {}, ["AB"])


def this_season():
    return S.datetime.now(S.EASTERN).strftime("%Y-%m-%d 10:00")


def test_stats_went_backwards_flags_drop_this_season():
    previous = {"Last_Updated": this_season(), "G": 54, "AB": 215, "H": 68, "BB": 25, "SO": 31}
    drops = S.stats_went_backwards(previous, {"G": "0", "AB": "0", "H": "0", "BB": "0", "SO": "0"}, "batting")
    assert [d[0] for d in drops] == ["G", "AB", "H", "BB", "SO"]


def test_stats_went_backwards_allows_new_season():
    previous = {"Last_Updated": "2001-05-01 10:00", "G": 54, "AB": 215}
    assert S.stats_went_backwards(previous, {"G": "0", "AB": "0"}, "batting") == []


def test_cs_drop_is_not_a_backwards_stat():
    # The #51 fix lowers CS once; that must not be refused as a bad scrape.
    previous = {"Last_Updated": this_season(), "G": 54, "AB": 215, "H": 68, "BB": 25, "SO": 31, "CS": 14}
    new = {"G": "54", "AB": "215", "H": "68", "BB": "25", "SO": "31", "CS": "2"}
    assert S.stats_went_backwards(previous, new, "batting") == []


def test_innings_compare_correctly_for_backwards_check():
    previous = {"Last_Updated": this_season(), "G": 5, "IP": "7.2", "SO": 5, "BB": 13}
    assert S.stats_went_backwards(previous, {"G": "5", "IP": "7.1", "SO": "5", "BB": "13"}, "pitching")
    assert not S.stats_went_backwards(previous, {"G": "6", "IP": "8.0", "SO": "5", "BB": "13"}, "pitching")
