"""
scrape_schedule.py
Scrapes baseball schedules for all schools in schools.json and writes
results to the Schedule sheet in the project Google Spreadsheet.

Handles two page layouts:
  - TABLE layout : standard Sidearm ?print=true pages (most schools)
  - JSON layout  : Nuxt/Vue embedded JSON blob (GW and similar)
"""

import json
import os
import re
import sys
import time
from datetime import datetime

import gspread
import requests
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

# ── Config ────────────────────────────────────────────────────────────────────

SPREADSHEET_ID = "1vZ9retMliQq99hw2twZ-KtlvLKLOStkcHq73CAx94gs"
SHEET_NAME     = "Schedule"
SCHOOLS_FILE   = "schools.json"
CURRENT_YEAR   = datetime.now().year

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

SHEET_COLUMNS = [
    "SCHOOL", "DIVISION", "DATE", "TIME", "OPPONENT",
    "HOME_AWAY", "LOCATION", "TV", "RESULT", "ESPN_TEAM_ID", "LAST_UPDATED",
]

# Schools whose pages embed schedule data as a Nuxt JSON blob
JSON_LAYOUT_SCHOOLS = {"George Washington"}


# ── Google Sheets auth ────────────────────────────────────────────────────────

def get_sheet():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    if not creds_json:
        sys.exit("ERROR: GOOGLE_CREDENTIALS environment variable not set.")
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    client = gspread.authorize(creds)
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
            loc_ind   = res(game_obj.get("location_indicator", "H"))
            home_away = "H" if str(loc_ind).upper() != "A" else "A"

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
        "LAST_UPDATED": datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def parse_date(raw):
    raw = re.sub(r"\([A-Za-z]+\)", "", str(raw)).strip()
    raw = re.sub(r"[,]+", "", raw).strip()
    for fmt in ("%b %d %Y", "%b %d", "%B %d %Y", "%B %d",
                "%m/%d/%Y", "%m/%d", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(raw, fmt)
            if dt.year == 1900:
                dt = dt.replace(year=CURRENT_YEAR)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
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

    sep = "&" if "?" in base_url else "?"
    url = f"{base_url}{sep}print=true"

    print(f"  Fetching {school} → {url}")
    soup = fetch_soup(url)
    if not soup:
        return []

    if school in JSON_LAYOUT_SCHOOLS:
        games  = parse_json_layout(soup, school, division, espn_team_id)
        layout = "json"
    else:
        tables_with_data = [t for t in soup.find_all("table") if len(t.find_all("tr")) >= 2]
        if tables_with_data:
            games  = parse_table_layout(soup, school, division, espn_team_id)
            layout = "table"
        else:
            games  = parse_json_layout(soup, school, division, espn_team_id)
            layout = "json (auto)"

    print(f"    [{layout}] {len(games)} games")
    return games


# ── Write to Google Sheets ────────────────────────────────────────────────────

def write_to_sheet(sheet, all_games):
    print(f"\nWriting {len(all_games)} total games to '{SHEET_NAME}'...")
    rows = [SHEET_COLUMNS] + [[g.get(c, "") for c in SHEET_COLUMNS] for g in all_games]
    sheet.clear()
    sheet.update(rows, value_input_option="RAW")
    print(f"Done. {len(all_games)} rows written.")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not os.path.exists(SCHOOLS_FILE):
        sys.exit(f"ERROR: {SCHOOLS_FILE} not found.")

    with open(SCHOOLS_FILE) as f:
        schools = json.load(f)

    print(f"Scraping {len(schools)} schools (year={CURRENT_YEAR})...\n")

    all_games = []
    for school_cfg in schools:
        try:
            games = scrape_school(school_cfg)
            all_games.extend(games)
        except Exception as e:
            print(f"  [error] {school_cfg.get('school', '?')}: {e}")
        time.sleep(1)

    if not all_games:
        print("WARNING: no games scraped — sheet not updated.")
        return

    sheet = get_sheet()
    write_to_sheet(sheet, all_games)


if __name__ == "__main__":
    main()