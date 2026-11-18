"""
sites.py
Per-site settings shared by the scrapers.

- The browser user agent. The stats scraper builds it from the Chromium that
  Playwright actually runs; BROWSER_UA is the default for plain HTTP requests.
- Layout overrides from schools.json. Layouts are detected automatically;
  an override is only for a site that detection gets wrong:
    "stats_layout":    "dropdown" (use the site's Batting/Pitching/Fielding
                        dropdown before its embedded data) or "embedded"
                        (never click the dropdown)
    "schedule_layout": "table" or "json"
  A school's stats page is matched to its schools.json entry by website
  (the host of its schedule_url).
"""

import json
import pathlib
from urllib.parse import urlparse

SCHOOLS_FILE = pathlib.Path(__file__).resolve().parent / "schools.json"

# Matches the Chromium bundled with the pinned Playwright (1.63 -> 153).
CHROME_MAJOR = 153
_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/{major}.0.0.0 Safari/537.36")
BROWSER_UA = _UA.format(major=CHROME_MAJOR)


def user_agent(browser_version=None):
    """A desktop Chrome user agent for the given Chromium version ("153.0.8010.12")."""
    major = str(browser_version or "").split(".")[0]
    return _UA.format(major=major if major.isdigit() else CHROME_MAJOR)


def host(url):
    name = urlparse(str(url or "")).hostname or ""
    return name[4:] if name.startswith("www.") else name


def load_schools(path=SCHOOLS_FILE):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def stats_layout(url, schools=None):
    """The stats_layout override for the school whose site serves url, or None."""
    for school in schools if schools is not None else load_schools():
        if host(school.get("schedule_url")) == host(url):
            return school.get("stats_layout")
    return None
