"""
fix_players_sheet.py
Checks the Players tab for typos and stray whitespace, and optionally fixes them.

- Trims leading/trailing spaces in every cell.
- Division must be one of D1, D2, D3, NJCAA; Type must be Hitter or Pitcher.
  Values that differ only by case or a small misspelling (e.g. "NJCCA",
  "Picther") are corrected; anything else is reported for a person to fix.
- Reports missing jersey numbers and stats URLs, and duplicate PlayerIDs.

Dry run by default. Pass --apply to write the corrections.
Exits 1 if problems remain that it can't fix automatically.

Usage: python fix_players_sheet.py [--apply]
"""

import difflib
import json
import os
import sys

import gspread
from google.oauth2.service_account import Credentials

PLAYERS_TAB = "Players"
ALLOWED = {
    "Division": ["D1", "D2", "D3", "NJCAA"],
    "Type": ["Hitter", "Pitcher"],
}
REQUIRED = ["PlayerID", "Name", "School", "Division", "Type", "Jersey", "Stats_URL"]


def correct_value(value, allowed):
    """Return the allowed value this one was meant to be, or None if unclear."""
    by_lower = {a.lower(): a for a in allowed}
    if value.lower() in by_lower:
        return by_lower[value.lower()]
    match = difflib.get_close_matches(value.lower(), list(by_lower), n=1, cutoff=0.75)
    return by_lower[match[0]] if match else None


def check_rows(values):
    """Return (fixes, problems).

    fixes: list of (row_number, column_number, header, old, new), 1-based.
    problems: list of human-readable strings needing a person.
    """
    if not values:
        return [], ["Players tab is empty"]
    headers = [h.strip() for h in values[0]]
    col = {h: i for i, h in enumerate(headers)}
    problems = [f"missing column: {h}" for h in REQUIRED if h not in col]
    fixes, seen_ids = [], {}

    for r, row in enumerate(values[1:], start=2):
        if not any(str(c).strip() for c in row):
            continue
        cells = list(row) + [""] * (len(headers) - len(row))
        fixed = {}
        for c, raw in enumerate(cells[:len(headers)]):
            value = str(raw)
            new = value.strip()
            header = headers[c]
            if header in ALLOWED and new:
                corrected = correct_value(new, ALLOWED[header])
                if corrected is None:
                    problems.append(f"row {r} {header}: '{new}' is not one of {ALLOWED[header]}")
                else:
                    new = corrected
            if new != value:
                fixes.append((r, c + 1, header, value, new))
            fixed[header] = new

        pid = fixed.get("PlayerID", "")
        label = f"row {r} ({fixed.get('Name') or pid or '?'})"
        for h in ("PlayerID", "Name", "School", "Jersey", "Stats_URL"):
            if h in col and not fixed.get(h):
                problems.append(f"{label}: {h} is empty")
        if pid:
            if pid in seen_ids:
                problems.append(f"{label}: PlayerID {pid} also used on row {seen_ids[pid]}")
            seen_ids[pid] = r

    return fixes, problems


def connect():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_CREDENTIALS"]),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds).open_by_key(os.environ["SPREADSHEET_ID"])


def main(apply=False, sheet=None):
    ws = (sheet or connect()).worksheet(PLAYERS_TAB)
    fixes, problems = check_rows(ws.get_all_values())

    for r, c, header, old, new in fixes:
        print(f"{'FIX ' if apply else 'WOULD FIX'} row {r} {header}: {old!r} -> {new!r}")
    for p in problems:
        print(f"NEEDS A PERSON: {p}")

    if fixes and apply:
        ws.batch_update([
            {"range": gspread.utils.rowcol_to_a1(r, c), "values": [[new]]}
            for r, c, _h, _old, new in fixes
        ])
        print(f"\nApplied {len(fixes)} fix(es) to {PLAYERS_TAB}.")
    elif fixes:
        print(f"\n{len(fixes)} fix(es) found. Re-run with --apply to write them.")
    else:
        print(f"\nNo automatic fixes needed in {PLAYERS_TAB}.")

    return bool(problems)


if __name__ == "__main__":
    if main(apply="--apply" in sys.argv):
        sys.exit(1)
