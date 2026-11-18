"""
Refresh the saved stats/schedule pages used by the tests.

    python tests/capture_fixtures.py            # all fixtures
    python tests/capture_fixtures.py etsu_stats # one fixture

Each page is loaded in Chromium the same way scraper.py loads it, and the
rendered DOM is saved gzipped under tests/fixtures/. Schedule print pages are
fetched with requests, as scrape_schedule.py does. After refreshing, re-check
the expected values in the tests: they are real season numbers.
"""

import gzip
import pathlib
import sys
import time

import requests

FIXTURES = pathlib.Path(__file__).parent / "fixtures"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

# name -> (kind, url)
PAGES = {
    # Classic Sidearm stats page: every table rendered at once.
    "etsu_stats": ("browser", "https://etsubucs.com/sports/baseball/stats/2026"),
    # Dropdown-layout Sidearm page: one table visible, the rest only in the
    # embedded Nuxt data (individualStats).
    "maryland_stats": ("browser", "https://umterps.com/sports/baseball/stats/2026"),
    # Schedule print pages.
    "etsu_schedule": ("requests", "https://etsubucs.com/sports/baseball/schedule/2026?print=true"),
}


def save(name, html):
    path = FIXTURES / f"{name}.html.gz"
    path.write_bytes(gzip.compress(html.encode("utf-8"), mtime=0))
    print(f"saved {path} ({len(html):,} chars)")


def main(names):
    FIXTURES.mkdir(exist_ok=True)
    browser_pages = [n for n in names if PAGES[n][0] == "browser"]
    for name in names:
        kind, url = PAGES[name]
        if kind == "requests":
            resp = requests.get(url, headers={"User-Agent": UA}, timeout=30)
            resp.raise_for_status()
            save(name, resp.text)
    if browser_pages:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_context(user_agent=UA).new_page()
            for name in browser_pages:
                page.goto(PAGES[name][1], wait_until="domcontentloaded", timeout=60000)
                time.sleep(5)
                save(name, page.content())
            browser.close()


if __name__ == "__main__":
    main(sys.argv[1:] or list(PAGES))
