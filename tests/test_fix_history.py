"""fix_history.py: planning on sheet values, and applying to an in-memory sheet."""

import pytest

import fix_history as F
import scraper as S
from fake_sheets import FakeWorksheet, records

BAT_HEADER = S.HISTORY_PREFIX_FIELDS + S.BATTING_COLS
PIT_HEADER = S.HISTORY_PREFIX_FIELDS + S.PITCHING_COLS
FIXED = [("2026-11-17 17:41", "2026-11-17 17:48")]


def bat(utc, pid, sb, cs, ab="215", name="P"):
    row = dict.fromkeys(BAT_HEADER, "0")
    row.update({"Last_Updated": "x", "Scraped_At_UTC": utc, "Business_Date_ET": utc[:10],
                "PlayerID": pid, "Name": name, "AB": ab, "SB": sb, "CS": cs})
    return [row[h] for h in BAT_HEADER]


def pit(utc, pid, ip, k9, so="5", bb="13", h="6", hbp="1"):
    fresh = S.map_row({"IP": ip, "SO": so, "BB": bb, "H": h, "HBP": hbp}, {k: k for k in "IP SO BB H HBP".split()})
    row = dict.fromkeys(PIT_HEADER, "0")
    row.update({k: fresh.get(k, "") for k in F.RATE_COLS})
    row.update({"Last_Updated": "x", "Scraped_At_UTC": utc, "Business_Date_ET": utc[:10],
                "PlayerID": pid, "Name": "P", "IP": ip, "SO": so, "BB": bb, "H": h, "HBP": hbp,
                "K_per_9": k9})
    return [row[h] for h in PIT_HEADER]


def test_parse_window():
    assert F.parse_window("2026-11-17 17:41..2026-11-17 17:48") == FIXED[0]
    with pytest.raises(ValueError):
        F.parse_window("yesterday..today")


def test_batting_flip_flop_is_corrected_and_deduplicated():
    values = [BAT_HEADER,
              bat("2026-11-16 15:00", "P2", "12", "14"),   # old code
              bat("2026-11-17 17:43", "P2", "12", "2"),    # fixed run
              bat("2026-11-17 23:13", "P2", "12", "14"),   # old code again
              bat("2026-11-18 01:00", "P2", "13", "15", ab="219")]  # old code, real change
    changes, notes = F.plan_batting(values, FIXED)
    assert [(i, new) for i, _, _, new in changes] == [(0, "2"), (2, "2"), (3, "2")]
    assert not notes
    assert F.plan_duplicates(values, S.BATTING_COLS, changes) == [1, 2]


def test_batting_no_steals_unchanged_and_impossible_rows_reported():
    values = [BAT_HEADER,
              bat("2026-03-01 15:00", "P1", "0", "1"),
              bat("2026-03-02 15:00", "P3", "5", "2")]   # CS < SB outside a fixed run
    changes, notes = F.plan_batting(values, FIXED)
    assert changes == [] and len(notes) == 1


def test_existing_duplicates_are_kept():
    values = [BAT_HEADER,
              bat("2026-03-01 15:00", "P1", "0", "0"),
              bat("2026-03-01 17:00", "P1", "0", "0")]
    assert F.plan_duplicates(values, S.BATTING_COLS, []) == []


def test_pitching_rates_recomputed_only_where_wrong():
    values = [PIT_HEADER,
              pit("2026-03-01 15:00", "P9", "7.2", "6.25"),   # old: 5 / 7.2 * 9
              pit("2026-03-01 15:00", "P8", "7.0", "6.43")]   # whole innings: right already
    changes = F.plan_pitching(values)
    assert ("K_per_9", "6.25", "5.87") in [(c, old, new) for i, c, old, new in changes if i == 0]
    assert not [c for c in changes if c[0] == 1]


def test_pitching_blank_rates_stay_blank():
    row = pit("2026-02-24 09:00", "P9", "7.2", "6.25")
    row[PIT_HEADER.index("BB_pct")] = ""
    changes = F.plan_pitching([PIT_HEADER, row])
    assert "BB_pct" not in [c for _, c, _, _ in changes]


def test_apply_writes_cells_and_deletes_rows():
    values = [BAT_HEADER,
              bat("2026-11-16 15:00", "P2", "12", "14"),
              bat("2026-11-17 17:43", "P2", "12", "2"),
              bat("2026-11-17 23:13", "P2", "12", "14")]
    ws = FakeWorksheet("Batting_History", values)
    changes, _ = F.plan_batting(values, FIXED)
    deletes = F.plan_duplicates(values, S.BATTING_COLS, changes)
    F.apply_changes(ws, values, S.BATTING_COLS, changes, deletes)
    rows = records(ws)
    assert [(r["Scraped_At_UTC"], r["CS"]) for r in rows] == [("2026-11-16 15:00", "2")]

    # Running again finds nothing to do.
    again = ws.get_all_values()
    changes, _ = F.plan_batting(again, FIXED)
    assert changes == [] and F.plan_duplicates(again, S.BATTING_COLS, changes) == []


def test_fixed_run_windows_reads_runs_and_code(monkeypatch):
    import base64

    class Resp:
        def __init__(self, data):
            self.data = data
        def json(self):
            return self.data
        def raise_for_status(self):
            pass

    old = base64.b64encode(b'"SB-ATT" : ("SB", "CS"),').decode()
    new = base64.b64encode(b'"SB-ATT" : ("SB", "SB_ATT"),').decode()

    def fake_get(url, params=None, **_):
        if url.endswith("/runs"):
            return Resp({"total_count": 2, "workflow_runs": [
                {"head_sha": "new", "run_started_at": "2026-11-17T17:41:59Z", "updated_at": "2026-11-17T17:47:02Z"},
                {"head_sha": "old", "run_started_at": "2026-11-17T23:10:00Z", "updated_at": "2026-11-17T23:17:00Z"}]})
        return Resp({"content": new if params["ref"] == "new" else old})

    monkeypatch.setattr(F.requests, "get", fake_get)
    assert F.fixed_run_windows("o/r", "t") == [("2026-11-17 17:41", "2026-11-17 17:48")]
