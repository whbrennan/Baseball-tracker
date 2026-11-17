import gzip
import os
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent
FIXTURES = pathlib.Path(__file__).resolve().parent / "fixtures"

# The scrapers are top-level scripts; make them importable, and never let a
# test pick up real credentials from the environment.
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
for var in ("GOOGLE_CREDENTIALS", "SPREADSHEET_ID", "PUSHOVER_USER_KEY", "PUSHOVER_API_TOKEN"):
    os.environ.pop(var, None)


def load_fixture(name):
    """Return a saved page's HTML. Names are files in tests/fixtures without .html.gz."""
    return gzip.decompress((FIXTURES / f"{name}.html.gz").read_bytes()).decode("utf-8")


@pytest.fixture(scope="session")
def browser():
    """Headless Chromium, or skip the test if it isn't installed."""
    sync_api = pytest.importorskip("playwright.sync_api")
    with sync_api.sync_playwright() as pw:
        try:
            browser = pw.chromium.launch(headless=True)
        except Exception as e:  # browser not downloaded
            pytest.skip(f"Chromium not available: {e}")
        yield browser
        browser.close()


@pytest.fixture
def page(browser):
    """A page with the site's own scripts disabled, so a saved page renders as saved.

    All network requests are refused (images, fonts, analytics in the saved
    pages), so tests stay offline. A test serves a page with page.route(),
    which takes precedence over this.
    """
    context = browser.new_context(java_script_enabled=False)
    context.route("**/*", lambda route: route.abort())
    page = context.new_page()
    yield page
    context.close()


@pytest.fixture
def no_sleep(monkeypatch):
    """Skip the scrapers' fixed waits."""
    import time
    monkeypatch.setattr(time, "sleep", lambda *_: None)
