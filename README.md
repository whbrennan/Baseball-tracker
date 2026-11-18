# Baseball-tracker
College baseball stats for 18 players

## Tests

    pip install -r requirements-dev.txt
    python -m playwright install --with-deps chromium
    python -m pytest

Tests use saved pages in `tests/fixtures/` and an in-memory sheet, so they
need no credentials or network. They run on every push (`.github/workflows/tests.yml`).
Refresh the saved pages with `python tests/capture_fixtures.py`.

## Backups

Every tab of the sheet is saved nightly in season (weekly otherwise) to the
`data-backup` branch by the **Back Up Sheet** workflow. See
[RESTORE.md](RESTORE.md) to put data back.
