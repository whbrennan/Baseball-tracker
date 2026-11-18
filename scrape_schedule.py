"""
scrape_schedule.py
Scrapes baseball schedules for all schools in schools.json and writes
results to the Schedule sheet in the project Google Spreadsheet.

The season comes from season.json (see season.py): each school's schedule_url
contains {season}, and dates printed without a year get it from the season.

Handles two page layouts, detected automatically:
  - TABLE layout : standard Sidearm ?print=true pages (most schools)
  - JSON layout  : Nuxt/Vue embedded JSON blob (GW and similar)
A school's "schedule_layout" in schools.json ("table" or "json") overrides
detection.
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timezone

import gspread
import requests
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
from gspread.http_client import BackOffHTTPClient
from gspread.utils import rowcol_to_a1

from run_status import record_run
from season import SEASON, season_for_date, season_url, year_for_month
from sites import BROWSER_UA

# ── Config ────────────────────────────────────────────────────────────────────

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
SHEET_NAME     = "Schedule"
SCHOOLS_FILE   = "schools.json"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

HEADERS = {"User-Agent": BROWSER_UA}

SHEET_COLUMNS = [
    "SCHOOL", "DIVISION", "DATE", "TIME", "OPPONENT",
    "HOME_AWAY", "LOCATION", "TV", "RESULT", "ESPN_TEAM_ID", "LAST_UPDATED", "SEASON",
]

# A separate home/away column, and what its values mean. Wins over "vs."/"at".
HOME_AWAY_HEADERS = {"AT", "H/A", "HOME/AWAY", "HOME / AWAY"}
HOME_AWAY_VALUES = {"HOME": "H", "H": "H", "AWAY": "A", "A": "A", "NEUTRAL": "N", "N": "N"}

# If a school's scrape returns fewer than this share of the games it had last
# time (same season), assume the parse broke and keep the previous rows.
MIN_KEEP_FRACTION = 0.5


# ── Google Sheets auth ────────────────────────────────────────────────────────

def get_sheet():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    if not creds_json:
        sys.exit("ERROR: GOOGLE_CREDENTIALS environment variable not set.")
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    # BackOffHTTPClient retries quota (429), timeout and server errors with backoff.
    client = gspread.authorize(creds, http_client=BackOffHTTPClient)
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    try:
        sheet = spreadsheet.worksheet(SHEET_NAME)
    except gspread.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=SHEET_NAME, rows=2000, cols=len(SHEET_COLUMNS))
    return sheet


# ── HTML fetch ────────────────────────────────────────────────────────────────

def fetch_soup(url, retries=3, delay=5):
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser")
        except requests.RequestException as e:
            print(f"  [warn] attempt {attempt+1}/{retries} failed for {url}: {e}")
            if attempt < retries - 1:
                time.sleep(delay)
    print(f"  [error] all retries failed for {url}")
    return None


# ── JSON layout parser (GW / Nuxt embedded data) ─────────────────────────────

def parse_json_layout(soup, school, division, espn_team_id):
    """
    Sidearm sites built on Nuxt/Vue embed all schedule data as a flat reference
    array in a <script> tag. Each entry is either a primitive or a dict whose
    values are indices into the same array. We locate the games list, resolve
    each game one level deep, and extract the fields we need.
    """
    raw = _extract_nuxt_array(soup)
    if raw is None:
        print("  [warn] could not find/parse Nuxt JSON blob")
        return []

    def res(val):
        """Resolve one level: if val is an int index return raw[val], else val."""
        if isinstance(val, int) and 0 <= val < len(raw):
            return raw[val]
        return val

    def res_dict(val):
        """Resolve a value and, if the result is a dict, resolve its values too."""
        obj = res(val)
        if isinstance(obj, dict):
            return {k: res(v) for k, v in obj.items()}
        return obj

    # Navigate: raw[456] = {'schedules-baseball,': 457}
    #           raw[457] = { ..., 'games': 470 }
    #           raw[470] = [471, 531, 565, ...]  ← list of game indices
    schedule_ptr = None
    for i, entry in enumerate(raw):
        if isinstance(entry, dict) and "games" in entry and "school_name" in entry:
            schedule_ptr = i
            break
    if schedule_ptr is None:
        print("  [warn] could not find schedule node in Nuxt JSON")
        return []

    games_idx  = res(raw[schedule_ptr].get("games"))
    game_list  = raw[games_idx] if isinstance(games_idx, int) else games_idx
    if not isinstance(game_list, list):
        print("  [warn] games node is not a list")
        return []

    games = []
    for gi in game_list:
        try:
            game_obj = res(gi)
            if not isinstance(game_obj, dict):
                continue

            # ── Date ──
            date_raw = res(game_obj.get("date", ""))
            try:
                dt       = datetime.fromisoformat(str(date_raw))
                date_fmt = dt.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                continue

            # ── Time ──
            game_time = normalise_time(str(res(game_obj.get("time", "TBA")) or "TBA"))

            # ── Home / Away ──
            loc_ind   = str(res(game_obj.get("location_indicator", "H"))).upper()
            home_away = loc_ind if loc_ind in ("A", "N") else "H"

            # ── Location ──
            location  = str(res(game_obj.get("location", "")) or "")

            # ── Opponent ──
            opp_obj   = res_dict(game_obj.get("opponent"))
            opponent  = str(res(opp_obj.get("title", "Unknown")) if isinstance(opp_obj, dict) else "Unknown")

            # ── TV ──
            media_obj = res_dict(game_obj.get("media"))
            tv        = ""
            if isinstance(media_obj, dict):
                tv_val = res(media_obj.get("tv"))
                if tv_val and tv_val is not None and str(tv_val).lower() not in ("none", "false", ""):
                    tv = str(tv_val)
                else:
                    # Check for tv_image (ESPN+ logo present = ESPN+ game)
                    tv_img = res(media_obj.get("tv_image"))
                    if tv_img:
                        tv = "ESPN+"

            # ── Result ──
            result    = ""
            result_obj = res_dict(game_obj.get("result"))
            if isinstance(result_obj, dict):
                status     = res(result_obj.get("status", ""))
                team_score = res(result_obj.get("team_score"))
                opp_score  = res(result_obj.get("opponent_score"))
                if status and str(status).upper() in ("W", "L", "T") and team_score is not None:
                    result = f"{str(status).upper()}, {team_score}-{opp_score}"

            games.append(_build_game(school, division, date_fmt, game_time, opponent,
                                     home_away, location, tv, result, espn_team_id))
        except Exception as e:
            print(f"  [warn] JSON game parse error: {e}")
            continue

    return games


def _extract_nuxt_array(soup):
    """Find the Nuxt flat reference array embedded in a <script> tag."""
    for script in soup.find_all("script"):
        txt = script.string or ""
        if "ShallowReactive" not in txt:
            continue
        start = txt.find("[[")
        if start == -1:
            start = txt.find("[{")
        if start == -1:
            continue
        try:
            return json.loads(txt[start:])
        except Exception:
            continue
    return None


# ── TABLE layout parser (standard Sidearm ?print=true) ───────────────────────

def parse_table_layout(soup, school, division, espn_team_id):
    games  = []
    tables = soup.find_all("table")
    if not tables:
        return games

    for table in tables:
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        col_map = {}
        for i, th in enumerate(rows[0].find_all(["th", "td"])):
            txt = th.get_text(strip=True).upper()
            if "DATE" in txt:
                col_map["date"] = i
            elif "OPP" in txt:
                col_map["opp"] = i
            elif any(x in txt for x in ("LOC", "SITE")):
                col_map["loc"] = i
            elif any(x in txt for x in ("RESULT", "SCORE", "W/L")):
                col_map["result"] = i
            elif "TIME" in txt:
                col_map["time"] = i
            elif txt in HOME_AWAY_HEADERS:
                col_map["ha"] = i
            elif any(x in txt for x in ("TV", "NETWORK")):
                col_map["tv"] = i

        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            try:
                game = _extract_from_table_row(cells, col_map, school, division, espn_team_id)
                if game:
                    games.append(game)
            except Exception as e:
                print(f"  [warn] table row parse error: {e}")

    return games


def _extract_from_table_row(cells, col_map, school, division, espn_team_id):
    def cell_text(idx):
        return cells[idx].get_text(" ", strip=True) if 0 <= idx < len(cells) else ""

    date_idx = col_map.get("date", 0)
    date_fmt = parse_date(cell_text(date_idx))
    if not date_fmt:
        return None

    opp_idx             = col_map.get("opp", 1)
    opp_raw             = cell_text(opp_idx)
    opponent, home_away = parse_opponent(opp_raw)
    if not opponent:
        return None
    if "ha" in col_map:
        home_away = HOME_AWAY_VALUES.get(cell_text(col_map["ha"]).strip().upper(), home_away)

    loc_idx  = col_map.get("loc", 2)
    location = cell_text(loc_idx)

    res_idx  = col_map.get("result", 3)
    result   = normalise_result(cell_text(res_idx))

    time_idx  = col_map.get("time", -1)
    game_time = cell_text(time_idx) if time_idx >= 0 else ""
    if not game_time:
        game_time = (extract_time(cell_text(date_idx)) or
                     extract_time(opp_raw) or "TBA")
    game_time = normalise_time(game_time)

    tv = ""
    if "tv" in col_map:
        tv = cell_text(col_map["tv"])
    if not tv:
        for idx in [opp_idx, date_idx]:
            if 0 <= idx < len(cells):
                img = cells[idx].find("img")
                if img and re.search(r"espn|accn", img.get("alt", "") + img.get("src", ""), re.I):
                    tv = img.get("alt", "ESPN+").strip()
                    break

    return _build_game(school, division, date_fmt, game_time, opponent,
                       home_away, location, tv, result, espn_team_id)


# ── Shared helpers ────────────────────────────────────────────────────────────

def _build_game(school, division, date, time_, opponent, home_away,
                location, tv, result, espn_team_id):
    return {
        "SCHOOL":       school,
        "DIVISION":     division,
        "DATE":         date,
        "TIME":         time_,
        "OPPONENT":     opponent,
        "HOME_AWAY":    home_away,
        "LOCATION":     location,
        "TV":           tv,
        "RESULT":       result,
        "ESPN_TEAM_ID": str(espn_team_id) if espn_team_id else "",
        "LAST_UPDATED": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "SEASON":       str(game_season(date)),
    }


def game_season(date):
    """Season of a YYYY-MM-DD date: fall (Sep-Dec) games count toward next spring."""
    return season_for_date(int(date[:4]), int(date[5:7]))


def row_season(game):
    """Season of a Schedule row: its SEASON cell, else worked out from its date."""
    value = str(game.get("SEASON", "")).strip()
    if value.isdigit():
        return int(value)
    date = str(game.get("DATE", ""))
    return game_season(date) if re.match(r"\d{4}-\d{2}-\d{2}$", date) else None


def parse_date(raw, season=None):
    """Return YYYY-MM-DD, or None.

    A year printed on the page wins. Without one, the year comes from the
    season: Sep-Dec dates are the fall before it, Jan-Aug dates are in it.
    """
    raw = re.sub(r"\([A-Za-z]+\)", "", str(raw)).strip()
    raw = re.sub(r"[,]+", "", raw).strip()
    for fmt in ("%b %d %Y", "%B %d %Y", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    for fmt in ("%b %d", "%B %d", "%m/%d"):
        try:
            # Parse inside a leap year so Feb 29 is accepted, then set the real year.
            dt = datetime.strptime(f"{raw} 2000", f"{fmt} %Y")
        except ValueError:
            continue
        try:
            return dt.replace(year=year_for_month(dt.month, season)).strftime("%Y-%m-%d")
        except ValueError:  # Feb 29 in a non-leap season
            return None
    return None


def extract_time(raw):
    m = re.search(r"(\d{1,2}:\d{2}\s*[ap]\.?m\.?)", raw, re.IGNORECASE)
    return normalise_time(m.group(1)) if m else None


def normalise_time(raw):
    return (str(raw).strip()
            .replace("a.m.", "AM").replace("p.m.", "PM")
            .replace("A.M.", "AM").replace("P.M.", "PM")
            .replace(" ", ""))


def parse_opponent(raw):
    raw = raw.strip()
    m   = re.match(r"^(vs\.?|at)\s+(.+)$", raw, re.IGNORECASE)
    if m:
        ha   = "H" if m.group(1).lower().startswith("vs") else "A"
        name = m.group(2).strip()
    else:
        ha, name = "H", raw
    name = re.sub(r"\s+[WL],\s*\d+[-\u2013]\d+.*$", "", name).strip()
    return (name or None, ha)


def normalise_result(raw):
    if not raw:
        return ""
    raw = raw.strip()
    if re.match(r"^[WLT],", raw):
        return raw
    m = re.match(r"^([WLT])\s*[,\s]*(\d+[-\u2013]\d+)", raw, re.IGNORECASE)
    return f"{m.group(1).upper()}, {m.group(2)}" if m else raw


# ── Per-school scrape ─────────────────────────────────────────────────────────

def scrape_school(school_cfg):
    school       = school_cfg["school"]
    division     = school_cfg.get("division", "D1")
    espn_team_id = school_cfg.get("espn_team_id", "")
    base_url     = school_cfg.get("schedule_url", "")

    if not base_url:
        print(f"  [skip] {school}: no schedule_url")
        return []

    base_url = season_url(base_url)
    sep = "&" if "?" in base_url else "?"
    url = f"{base_url}{sep}print=true"

    print(f"  Fetching {school} → {url}")
    soup = fetch_soup(url)
    if not soup:
        return []

    override = school_cfg.get("schedule_layout")
    if override == "json":
        games  = parse_json_layout(soup, school, division, espn_team_id)
        layout = "json"
    elif override == "table":
        games  = parse_table_layout(soup, school, division, espn_team_id)
        layout = "table"
    else:
        tables_with_data = [t for t in soup.find_all("table") if len(t.find_all("tr")) >= 2]
        if tables_with_data:
            games  = parse_table_layout(soup, school, division, espn_team_id)
            layout = "table"
        else:
            games  = parse_json_layout(soup, school, division, espn_team_id)
            layout = "json (auto)"

    other = [g for g in games if g["SEASON"] != str(SEASON)]
    if other:
        print(f"    skipped {len(other)} games outside the {SEASON} season")
        games = [g for g in games if g["SEASON"] == str(SEASON)]
    print(f"    [{layout}] {len(games)} games")
    return games


# ── Write to Google Sheets ────────────────────────────────────────────────────

def read_existing_games(sheet):
    """Return (games as dicts, number of sheet rows in use incl. header)."""
    values = sheet.get_all_values()
    if not values:
        return [], 0
    headers = [h.strip().upper() for h in values[0]]
    games = []
    for row in values[1:]:
        if not any(cell.strip() for cell in row):
            continue
        games.append({h: (row[i] if i < len(row) else "") for i, h in enumerate(headers) if h})
    return games, len(values)


def merge_with_previous(school_names, new_by_school, previous_games, config=None, season=None):
    """Combine this run's games with the sheet's existing rows, per school.

    A school keeps its previous rows when this run found no games for it, or
    far fewer than before, since that almost always means the site was down or
    its layout changed rather than games vanishing. Only previous rows from
    the current season are kept, so last season's schedule is never carried
    into a new one. Kept rows get DIVISION and ESPN_TEAM_ID from config
    (school -> schools.json entry). Schools no longer in schools.json are dropped.

    Returns (games, kept) where kept maps school -> reason previous rows were kept.
    """
    season = SEASON if season is None else season
    config = config or {}
    previous_by_school = {}
    for game in previous_games:
        if row_season(game) == season:
            previous_by_school.setdefault(game.get("SCHOOL", ""), []).append(game)

    merged, kept = [], {}
    for school in school_names:
        new = new_by_school.get(school, [])
        previous = previous_by_school.get(school, [])
        if previous and (not new or len(new) < len(previous) * MIN_KEEP_FRACTION):
            kept[school] = (f"no games scraped; kept {len(previous)} previous rows" if not new else
                            f"only {len(new)} games scraped vs {len(previous)} before; kept previous rows")
            merged.extend(_refresh_school_fields(g, config.get(school), season) for g in previous)
        else:
            merged.extend(new)
    return merged, kept


def _refresh_school_fields(game, cfg, season):
    game = dict(game)
    game["SEASON"] = str(season)
    if cfg:
        game["DIVISION"] = cfg.get("division", game.get("DIVISION", ""))
        espn = cfg.get("espn_team_id", "")
        game["ESPN_TEAM_ID"] = str(espn) if espn else ""
    return game


def write_to_sheet(sheet, all_games, previous_row_count):
    """Overwrite the sheet in place, then clear any leftover rows below.

    Writing over the existing data (instead of clearing first) means a reader
    never sees an empty schedule, and a failed write leaves the old data intact.
    """
    print(f"\nWriting {len(all_games)} total games to '{SHEET_NAME}'...")
    rows = [SHEET_COLUMNS] + [[g.get(c, "") for c in SHEET_COLUMNS] for g in all_games]
    if len(rows) > sheet.row_count:
        sheet.add_rows(len(rows) - sheet.row_count)
    if len(SHEET_COLUMNS) > sheet.col_count:
        sheet.add_cols(len(SHEET_COLUMNS) - sheet.col_count)
    sheet.update(values=rows, range_name="A1", value_input_option="RAW")
    if previous_row_count > len(rows):
        start = rowcol_to_a1(len(rows) + 1, 1)
        end = rowcol_to_a1(previous_row_count, max(len(SHEET_COLUMNS), sheet.col_count))
        sheet.batch_clear([f"{start}:{end}"])
    print(f"Done. {len(all_games)} rows written.")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    """Scrape every school. Returns True if the run failed overall."""
    if not os.path.exists(SCHOOLS_FILE):
        sys.exit(f"ERROR: {SCHOOLS_FILE} not found.")

    with open(SCHOOLS_FILE) as f:
        schools = json.load(f)

    print(f"Scraping {len(schools)} schools for the {SEASON} season...\n")

    new_by_school, errors = {}, {}
    for school_cfg in schools:
        school = school_cfg.get("school", "?")
        try:
            new_by_school[school] = scrape_school(school_cfg)
        except Exception as e:
            print(f"  [error] {school}: {e}")
            errors[school] = str(e)
        time.sleep(1)

    sheet = get_sheet()
    previous_games, previous_row_count = read_existing_games(sheet)
    school_names = [s.get("school", "?") for s in schools]
    config = {s.get("school", "?"): s for s in schools}
    all_games, kept = merge_with_previous(school_names, new_by_school, previous_games, config)

    ok_schools, failed_schools = [], []
    for school in school_names:
        if school in errors or school in kept or not new_by_school.get(school):
            reason = errors.get(school) or kept.get(school) or "no games scraped"
            failed_schools.append(f"{school}: {reason}")
        else:
            ok_schools.append(school)

    if all_games:
        write_to_sheet(sheet, all_games, previous_row_count)
    else:
        print("WARNING: no games scraped and none on the sheet — sheet not updated.")

    return record_run(sheet.spreadsheet, "Schedule scraper", ok_schools, failed_schools)


if __name__ == "__main__":
    if main():
        sys.exit(1)