"""
backup_sheet.py
Exports every tab of the Google Sheet to CSV files, for the nightly backup.

    python backup_sheet.py OUTPUT_DIR

Each tab becomes OUTPUT_DIR/<tab name>.csv with the cell values exactly as the
sheet shows them. CSVs for tabs that no longer exist are removed, so the folder
always mirrors the sheet. Nothing is changed unless every tab was read: a
failed read leaves the previous backup untouched and exits 1.

Reads with the service account and the read-only Sheets scope (not the public
CSV export, which blanks cells whose type doesn't match the rest of the
column). Environment: GOOGLE_CREDENTIALS, SPREADSHEET_ID.
"""

import csv
import io
import json
import os
import pathlib
import re
import sys

import gspread
from google.oauth2.service_account import Credentials
from gspread.http_client import BackOffHTTPClient

READ_ONLY = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


def connect():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_CREDENTIALS"]), scopes=READ_ONLY)
    return gspread.authorize(creds, http_client=BackOffHTTPClient).open_by_key(os.environ["SPREADSHEET_ID"])


def file_name(tab):
    """A safe file name for a tab: letters, digits, spaces, _ - . ( ) kept."""
    name = re.sub(r"[^\w .()-]", "_", tab).strip(" .") or "tab"
    return f"{name}.csv"


def to_csv(values):
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    writer.writerows(values)
    return out.getvalue()


def export(spreadsheet):
    """{file name: CSV text} for every tab. Raises if any tab can't be read."""
    files = {}
    for ws in spreadsheet.worksheets():
        name = file_name(ws.title)
        if name in files:
            raise ValueError(f"two tabs map to the same file name: {name}")
        files[name] = to_csv(ws.get_all_values())
    if not files:
        raise ValueError("the spreadsheet has no tabs")
    return files


def write_backup(files, out_dir):
    """Write the CSVs and remove ones for tabs that are gone. Returns (written, removed)."""
    out = pathlib.Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    removed = []
    for old in out.glob("*.csv"):
        if old.name not in files:
            old.unlink()
            removed.append(old.name)
    for name, text in files.items():
        (out / name).write_text(text, encoding="utf-8")
    return sorted(files), sorted(removed)


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) != 1:
        sys.exit("usage: python backup_sheet.py OUTPUT_DIR")
    try:
        files = export(connect())
    except Exception as e:
        print(f"Backup failed, previous backup left as it was: {e}")
        return 1
    written, removed = write_backup(files, argv[0])
    for name in written:
        rows = files[name].count("\n")
        print(f"  {name}: {rows} rows")
    for name in removed:
        print(f"  removed {name} (tab no longer exists)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
