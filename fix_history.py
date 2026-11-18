"""
fix_history.py
One-off correction of history rows written before two scraper fixes.

1. Batting_History CS (issue #51). Rows written by the old scraper hold
   stolen-base attempts in CS; they become CS - SB. Rows written by the fixed
   scraper are left alone. Which rows those are is decided per scraper run: the
   GitHub API lists the stats workflow's runs and this script checks whether the
   scraper.py each run used already had the fix. A row whose Scraped_At_UTC
   falls inside such a run is already correct.

2. Pitching_History rates. K_per_9, BB_per_9, K_BB, xFIP, BB_pct and K_pct are
   recomputed from each row's IP, H, BB, SO and HBP with innings counted in
   thirds ("7.2" = 7 2/3). Recomputing is safe to repeat, so every row is checked.
   Blank cells (rows written before a column existed) stay blank.

3. Duplicate snapshots. Until the fix reached main, scheduled runs on the old
   code and test runs on the new code alternated, each writing a snapshot that
   differed only in CS (or in the pitching rates). Once corrected, those rows
   repeat the player's previous snapshot and are deleted. Rows that were
   already identical to the previous snapshot before this correction are kept.

Dry run by default: prints every change and writes nothing. Pass --apply to write.

Usage:
    python fix_history.py [--apply] [--fixed-window "2026-11-17 17:42..2026-11-17 17:47"]

Environment: GOOGLE_CREDENTIALS, SPREADSHEET_ID, and in Actions GITHUB_TOKEN and
GITHUB_REPOSITORY (to find the fixed runs). --fixed-window (UTC, repeatable)
adds or replaces windows by hand.
"""

import argparse
import base64
import os
import sys
from datetime import datetime, timedelta

import requests

import scraper as S

STATS_WORKFLOW = "scrape.yml"
# A string only the fixed scraper.py contains (BATTING_MAP's SB-ATT target).
FIX_MARKER = '("SB", "SB_ATT")'
# The fix was first committed on this day; earlier runs can't have it.
FIX_FIRST_DAY = "2026-11-17"
RATE_COLS = ["K_per_9", "BB_per_9", "K_BB", "xFIP", "BB_pct", "K_pct"]
MINUTE = "%Y-%m-%d %H:%M"


# ── which runs already had the fix ────────────────────────────────────────────

def _github(path, token, **params):
    resp = requests.get(
        f"https://api.github.com{path}", params=params, timeout=30,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"})
    resp.raise_for_status()
    return resp


def fixed_run_windows(repo, token):
    """[(start, end)] in UTC minutes for stats runs whose code had the CS fix."""
    runs, page = [], 1
    while True:
        data = _github(f"/repos/{repo}/actions/workflows/{STATS_WORKFLOW}/runs", token,
                       created=f">={FIX_FIRST_DAY}", per_page=100, page=page).json()
        runs += data.get("workflow_runs", [])
        if len(runs) >= data.get("total_count", 0) or not data.get("workflow_runs"):
            break
        page += 1

    has_fix, windows = {}, []
    for run in runs:
        sha = run["head_sha"]
        if sha not in has_fix:
            source = _github(f"/repos/{repo}/contents/scraper.py", token, ref=sha).json()
            code = base64.b64decode(source.get("content", "")).decode("utf-8", "replace")
            has_fix[sha] = FIX_MARKER in code
        if has_fix[sha] and run.get("run_started_at"):
            windows.append((_minute(run["run_started_at"]),
                            _minute(run["updated_at"], round_up=True)))
    return sorted(windows)


def _minute(iso, round_up=False):
    dt = datetime.strptime(iso[:19], "%Y-%m-%dT%H:%M:%S")
    if round_up and dt.second:
        dt += timedelta(minutes=1)
    return dt.strftime(MINUTE)


def parse_window(text):
    start, _, end = text.partition("..")
    for value in (start, end):
        datetime.strptime(value.strip(), MINUTE)
    return start.strip(), end.strip()


def in_windows(scraped_at_utc, windows):
    return any(start <= scraped_at_utc <= end for start, end in windows)


# ── planning (pure: values in, changes out) ───────────────────────────────────

def _number(value):
    return S._to_number(value)


def _same(a, b):
    """Equal as stats: numbers compared to 3 decimals, anything else as text."""
    x, y = _number(a), _number(b)
    if x is not None and y is not None:
        return abs(x - y) < 0.0005
    return str(a or "").strip() == str(b or "").strip()


def _row_utc(row):
    scraped = str(row.get("Scraped_At_UTC", "")).strip()
    return scraped if S.looks_like_datetime(scraped) else S.to_scraped_at_utc(row.get("Last_Updated", ""))


def _table(values, target_cols):
    headers = S.get_effective_headers([h.strip() for h in values[0]], target_cols, history=True)
    rows = []
    for raw in values[1:]:
        padded = list(raw) + [""] * (len(headers) - len(raw))
        rows.append({h: padded[i] for i, h in enumerate(headers) if h})
    return headers, rows


def plan_batting(values, fixed_windows):
    """Return (cell_changes, notes). cell_changes: [(row_index, col_name, old, new)], 0-based data rows."""
    headers, rows = _table(values, S.BATTING_COLS)
    changes, notes = [], []
    for i, row in enumerate(rows):
        sb, cs = _number(row.get("SB")), _number(row.get("CS"))
        if sb is None or cs is None or sb == 0:
            continue  # CS == attempts == caught when there are no steals
        if in_windows(_row_utc(row), fixed_windows):
            continue
        if cs < sb:
            notes.append(f"row {i + 2}: {row.get('Name')} SB {row.get('SB')} CS {row.get('CS')} "
                         "outside a fixed run but CS < SB; left alone")
            continue
        changes.append((i, "CS", row.get("CS"), str(int(cs - sb))))
    return changes, notes


def plan_pitching(values):
    headers, rows = _table(values, S.PITCHING_COLS)
    changes = []
    for i, row in enumerate(rows):
        raw = {"IP": row.get("IP", ""), "H": row.get("H", ""), "BB": row.get("BB", ""),
               "SO": row.get("SO", ""), "HBP": row.get("HBP", "")}
        fresh = S.map_row(raw, {k: k for k in raw})
        for col in RATE_COLS:
            # Only correct values that are there; blanks are rows written before
            # the column existed and stay blank.
            if col in fresh and str(row.get(col, "")).strip() and not _same(row[col], fresh[col]):
                changes.append((i, col, row[col], fresh[col]))
    return changes


def plan_duplicates(values, target_cols, changes):
    """0-based data rows that only became a repeat of the player's previous snapshot
    because of `changes`."""
    _, rows = _table(values, target_cols)
    after = [dict(r) for r in rows]
    for i, col, _, new in changes:
        after[i][col] = new

    def same_stats(a, b):
        return all(_same(a.get(c), b.get(c)) for c in target_cols)

    original_prev, kept_prev, delete = {}, {}, []
    for i, (before_row, after_row) in enumerate(zip(rows, after)):
        pid = str(before_row.get("PlayerID", "")).strip()
        if not pid:
            continue
        was_repeat = pid in original_prev and same_stats(original_prev[pid], before_row)
        now_repeat = pid in kept_prev and same_stats(kept_prev[pid], after_row)
        original_prev[pid] = before_row
        if now_repeat and not was_repeat:
            delete.append(i)
        else:
            kept_prev[pid] = after_row
    return delete


# ── apply ─────────────────────────────────────────────────────────────────────

def apply_changes(ws, values, target_cols, changes, delete_rows):
    from gspread.utils import rowcol_to_a1
    headers = S.get_effective_headers([h.strip() for h in values[0]], target_cols, history=True)
    col_of = {h: i + 1 for i, h in enumerate(headers) if h}
    updates = [{"range": rowcol_to_a1(i + 2, col_of[col]), "values": [[new]]}
               for i, col, _, new in changes]
    if updates:
        ws.batch_update(updates)
    # Delete bottom-up so earlier row numbers stay valid.
    for i in sorted(delete_rows, reverse=True):
        ws.delete_rows(i + 2)


def report(tab, values, target_cols, changes, delete_rows, notes=(), limit=40):
    _, rows = _table(values, target_cols)
    print(f"\n{tab}: {len(rows)} rows, {len(changes)} cell changes on "
          f"{len({c[0] for c in changes})} rows, {len(delete_rows)} duplicate rows to delete")
    for i, col, old, new in changes[:limit]:
        r = rows[i]
        print(f"  row {i + 2:>5} {r.get('Last_Updated')} {r.get('Name')}: {col} {old} -> {new}")
    if len(changes) > limit:
        print(f"  ... and {len(changes) - limit} more")
    for i in delete_rows:
        r = rows[i]
        print(f"  delete row {i + 2:>5} {r.get('Last_Updated')} {r.get('Name')}")
    for note in notes:
        print(f"  note: {note}")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--apply", action="store_true", help="write the corrections")
    parser.add_argument("--fixed-window", action="append", default=[], type=parse_window,
                        metavar="START..END", help="UTC minutes of a run that already had the CS fix")
    args = parser.parse_args(argv)

    windows = list(args.fixed_window)
    repo, token = os.environ.get("GITHUB_REPOSITORY"), os.environ.get("GITHUB_TOKEN")
    if repo and token:
        windows += fixed_run_windows(repo, token)
    elif not windows:
        sys.exit("No GITHUB_TOKEN/GITHUB_REPOSITORY to find fixed runs, and no --fixed-window given.")
    print("Runs that already had the CS fix (UTC):")
    for start, end in windows:
        print(f"  {start} .. {end}")

    sheet = S.connect()
    jobs = [("Batting_History", S.BATTING_COLS), ("Pitching_History", S.PITCHING_COLS)]
    for tab, cols in jobs:
        ws = sheet.worksheet(tab)
        values = ws.get_all_values()
        notes = []
        if tab == "Batting_History":
            changes, notes = plan_batting(values, windows)
        else:
            changes = plan_pitching(values)
        delete_rows = plan_duplicates(values, cols, changes)
        report(tab, values, cols, changes, delete_rows, notes)
        if args.apply and (changes or delete_rows):
            apply_changes(ws, values, cols, changes, delete_rows)
            print(f"  written to {tab}")

    if not args.apply:
        print("\nDry run: nothing written. Run with --apply to make these changes.")


if __name__ == "__main__":
    main()
