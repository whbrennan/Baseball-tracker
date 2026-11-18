"""
season.py
The tracked season, and the rules that turn it into URLs and dates.

The season is the spring year (2027 = the spring 2027 season). It is set once
in season.json. Fall games (September-December) belong to the following
spring's season: October 2026 fall ball is part of the 2027 season.
"""

import json
import os
import pathlib
import re

SEASON_FILE = pathlib.Path(__file__).resolve().parent / "season.json"
FALL_MONTHS = range(9, 13)


def load_season():
    """The configured season. The SEASON environment variable overrides season.json."""
    override = os.environ.get("SEASON", "").strip()
    if override:
        return int(override)
    return int(json.loads(SEASON_FILE.read_text())["season"])


SEASON = load_season()


def season_for_date(year, month):
    """The season a game on this date belongs to (fall dates count toward next spring)."""
    return year + 1 if month in FALL_MONTHS else year


def year_for_month(month, season=None):
    """The calendar year of a month within a season, for dates printed without a year."""
    season = SEASON if season is None else season
    return season - 1 if month in FALL_MONTHS else season


# /stats/2026, /schedule/2026, or a bare /stats or /schedule at the end of the path.
_SEASON_SEGMENT = re.compile(r"(/(?:stats|schedule))(?:/\d{4})?(?=/?$)")


def season_url(url, season=None):
    """Point a Sidearm stats or schedule URL at the given season.

    "{season}" in the URL is replaced. Otherwise a year after /stats or
    /schedule is replaced, or added when there is none, keeping any query
    string: .../stats/2026?path=baseball -> .../stats/2027?path=baseball.
    Other URLs are returned unchanged.
    """
    season = SEASON if season is None else season
    url = str(url or "").strip()
    if "{season}" in url:
        return url.replace("{season}", str(season))
    base, sep, query = url.partition("?")
    new_base, found = _SEASON_SEGMENT.subn(lambda m: f"{m.group(1)}/{season}", base.rstrip("/"), count=1)
    return new_base + sep + query if found else url
