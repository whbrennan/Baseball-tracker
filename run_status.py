"""
run_status.py
Shared end-of-run reporting for the scrapers.

- Decides whether a run failed badly enough to exit non-zero, so the GitHub
  Actions run turns red and GitHub emails the repo owner.
- Appends a one-row summary to the Run_Log tab of the spreadsheet.
- Writes the same summary to the Actions job summary page when running in CI.

Reporting must never break a scrape, so every write here is best-effort.
"""

import os
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread

RUN_LOG_TAB = "Run_Log"
RUN_LOG_HEADERS = ["Finished_ET", "Script", "Status", "OK", "Failed", "Failed_Items", "Notes"]

# A run "fails" when more than this share of its items failed. A single flaky
# school site shouldn't turn the run red; a broken parser or site redesign
# affecting several players should.
MAX_FAILED_FRACTION = 0.25


def run_failed(ok_count, failed_count, max_fraction=MAX_FAILED_FRACTION):
    """True when nothing succeeded, or failures exceed max_fraction of items."""
    total = ok_count + failed_count
    if total == 0 or ok_count == 0:
        return True
    return failed_count / total > max_fraction


def _get_or_create_log(spreadsheet):
    try:
        return spreadsheet.worksheet(RUN_LOG_TAB)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=RUN_LOG_TAB, rows=1000, cols=len(RUN_LOG_HEADERS))
        ws.append_row(RUN_LOG_HEADERS)
        return ws


def _write_step_summary(lines):
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    try:
        with open(path, "a") as f:
            f.write("\n".join(lines) + "\n")
    except OSError as e:
        print(f"    Warning: could not write job summary: {e}")


def record_run(spreadsheet, script, ok_items, failed_items, notes=""):
    """Print, log and return whether the run failed.

    ok_items / failed_items are lists of short names (players or schools);
    failed_items entries may include a reason after a colon.
    """
    failed = run_failed(len(ok_items), len(failed_items))
    status = "FAILED" if failed else ("PARTIAL" if failed_items else "OK")
    finished = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M")

    print(f"\n{script}: {status} -- {len(ok_items)} ok, {len(failed_items)} failed")
    for item in failed_items:
        print(f"    failed: {item}")
    if notes:
        print(f"    notes: {notes}")

    summary = [
        f"### {script}: {status}",
        "",
        f"{len(ok_items)} ok, {len(failed_items)} failed ({finished} ET)",
    ]
    if failed_items:
        summary += [""] + [f"- {item}" for item in failed_items]
    if notes:
        summary += ["", notes]
    _write_step_summary(summary)

    if spreadsheet is not None:
        try:
            _get_or_create_log(spreadsheet).append_row([
                finished, script, status, len(ok_items), len(failed_items),
                "; ".join(failed_items)[:45000], notes[:45000],
            ])
        except Exception as e:  # reporting must not fail the run
            print(f"    Warning: could not write {RUN_LOG_TAB}: {e}")

    return failed
