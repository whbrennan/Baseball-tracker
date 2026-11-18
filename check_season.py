"""
check_season.py
Preseason check: does every schedule and stats URL work for a season?

Builds each school's schedule URL (schools.json) and each player's stats URL
(Players tab) for the season, opens it in Chromium the way the scrapers do,
and reports one line per URL:

  OK            page loaded and shows the season in its title or heading
  NOT PUBLISHED the site redirected away (sites do this until a season's page exists)
  MOVED         the same page now lives on another website (a warning: the
                scrapers follow the redirect, but the URL should be updated)
  WRONG SEASON  page loaded but doesn't mention the season
  ERROR         HTTP error, timeout or bot wall

Exits 1 if any page is NOT PUBLISHED, WRONG SEASON or ERROR; MOVED pages are
listed as warnings. Writes nothing to the sheet.

Usage:
    python check_season.py [--season 2027] [--players-csv FILE_OR_URL]

The Players tab is read with GOOGLE_CREDENTIALS/SPREADSHEET_ID when set,
otherwise from --players-csv (for example the sheet's public CSV export).
"""

import argparse
import csv
import io
import json
import os
import sys

from urllib.parse import urlparse

import requests

from season import SEASON, season_url

# Verdicts that are reported but don't fail the check.
WARNINGS = ("MOVED",)

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")


def load_players(players_csv=None):
    if players_csv:
        if players_csv.startswith("http"):
            text = requests.get(players_csv, timeout=30).text
        else:
            with open(players_csv, encoding="utf-8") as f:
                text = f.read()
        return list(csv.DictReader(io.StringIO(text)))
    import scraper
    return scraper.get_players(scraper.connect())


def targets(season, schools, players):
    """[(label, url)] for every schedule and stats page to check."""
    out = []
    for s in schools:
        if s.get("schedule_url"):
            base = season_url(s["schedule_url"], season)
            sep = "&" if "?" in base else "?"
            out.append((f"schedule: {s['school']}", f"{base}{sep}print=true"))
    seen = set()
    for p in players:
        url = season_url(p.get("Stats_URL", ""), season)
        if url and url not in seen:
            seen.add(url)
            out.append((f"stats: {p.get('School', '?')}", url))
    return out


def classify(season, requested_url, final_url, status, title_text):
    """Return (verdict, detail) for one loaded page."""
    if status is None or status >= 400:
        return "ERROR", f"HTTP {status}" if status else "no response"
    want, got = urlparse(requested_url), urlparse(final_url)
    if got.path.rstrip("/") != want.path.rstrip("/"):
        return "NOT PUBLISHED", f"redirects to {final_url}"
    if got.hostname != want.hostname:
        return "MOVED", f"now at {got.scheme}://{got.hostname} -- update the URL"
    if str(season) not in title_text:
        return "WRONG SEASON", (title_text.strip()[:80] or "no title")
    return "OK", title_text.strip()[:80]


def check(season, items):
    from playwright.sync_api import sync_playwright
    results = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_context(user_agent=UA).new_page()
        for label, url in items:
            try:
                resp = page.goto(url, wait_until="domcontentloaded", timeout=45000)
                heading = page.eval_on_selector_all("h1, h2", "els => els.slice(0, 3).map(e => e.innerText).join(' ')")
                verdict = classify(season, url, page.url, resp.status if resp else None,
                                   f"{page.title()} {heading}")
            except Exception as e:
                verdict = ("ERROR", str(e).splitlines()[0][:80])
            results.append((label, url) + verdict)
            print(f"{verdict[0]:<13} {label:<45} {url}  {verdict[1]}", flush=True)
        browser.close()
    return results


def write_summary(season, results):
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    lines = [f"### Season {season} URL check", "", "| Result | Page | URL | Detail |", "|---|---|---|---|"]
    lines += [f"| {v} | {label} | {url} | {d} |" for label, url, v, d in results]
    with open(path, "a") as f:
        f.write("\n".join(lines) + "\n")


def main(argv=None):
    parser = argparse.ArgumentParser(description="Check every schedule and stats URL for a season.")
    parser.add_argument("--season", type=int, default=SEASON)
    parser.add_argument("--players-csv", help="Players tab as CSV (file or URL) instead of the sheet")
    args = parser.parse_args(argv)

    with open("schools.json", encoding="utf-8") as f:
        schools = json.load(f)
    items = targets(args.season, schools, load_players(args.players_csv))
    print(f"Checking {len(items)} pages for the {args.season} season\n")
    results = check(args.season, items)
    write_summary(args.season, results)
    warn = [r for r in results if r[2] in WARNINGS]
    bad = [r for r in results if r[2] not in ("OK",) + WARNINGS]
    print(f"\n{len(results) - len(bad) - len(warn)} OK, {len(warn)} warning(s), {len(bad)} need attention")
    for label, url, verdict, detail in warn:
        print(f"  warning: {label}: {detail}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
