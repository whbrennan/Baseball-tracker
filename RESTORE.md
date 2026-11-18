# Restoring the sheet from a backup

The **Back Up Sheet** workflow saves every tab of the Google Sheet as a CSV
file on the `data-backup` branch, in `backups/`. It runs nightly during the
season (February to June) and weekly otherwise, and commits only when
something changed, so that branch's history is also a day-by-day record of
the data.

## Find the version you want

- Latest: https://github.com/whbrennan/Baseball-tracker/tree/data-backup/backups
- Earlier: open a file there and click **History**; each commit is one backup
  (named "Sheet backup YYYY-MM-DD"). Open the commit you want, then the file,
  then **Raw**, and save it (right-click > Save as) as a `.csv` file.

## Put a tab back

1. Pause the scrapers so they don't write while you restore: in the Actions
   tab, open **Baseball Stats Scraper** and **Scrape Schedules** and choose
   **Disable workflow** (from the "..." menu).
2. In the Google Sheet, open the tab to restore.
3. **File > Import > Upload**, choose the saved CSV.
4. Import location: **Replace current sheet**. Separator: **Comma**. Untick
   **Convert text to numbers, dates, and formulas** so values come back exactly
   as saved (for example `.316` and `2026-02-24`).
5. Check a few rows, then re-enable both workflows.

To restore only some rows (for example history rows a bad run changed),
import the CSV into a new tab with **Insert new sheet(s)**, copy the rows you
need, and paste them over the damaged ones.

## Notes

- The backup holds what the sheet already shows publicly; no credentials are
  in it.
- Google's own version history (**File > Version history**) is still there
  for undoing a recent edit in place.
