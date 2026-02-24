"""
scrape_schedule.py
Scrapes game schedules from Sidearm Sports JSON API (used by virtually
all schools in schools.json) and writes to a 'Schedule' tab in Google Sheets.

Credentials read from environment variable GOOGLE_CREDENTIALS (full JSON).

Usage:
    python3 scrape_schedule.py             # scrape all schools
    python3 scrape_schedule.py --today     # only today's games
    python3 scrape_schedule.py --school "Maryland"  # one school only
    python3 scrape_schedule.py --debug     # print raw API response for first school
"""

import json, os, re, sys, time, argparse
from datetime import datetime, date
from pathlib import Path
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
import gspread

# ── CONFIG ────────────────────────────────────────────────────────────────────
SPREADSHEET_ID = '1vZ9retMliQq99hw2twZ-KtlvLKLOStkcHq73CAx94gs'
SHEET_NAME     = 'Schedule'
SCHOOLS_FILE   = 'schools.json'
REQUEST_DELAY  = 1.5
CURRENT_YEAR   = datetime.now().year

TV_LOGOS = {
    'espn+': 'ESPN+', 'espnu': 'ESPNU', 'espn2': 'ESPN2', 'espn': 'ESPN',
    'sec network': 'SEC Network', 'acc network': 'ACC Network',
    'big ten network': 'Big Ten Network', 'mlb network': 'MLB Network',
    'fs1': 'FS1', 'fs2': 'FS2', 'stadium': 'Stadium',
    'flobaseball': 'FloBaseball', 'youtube': 'YouTube', 'live stream': 'Stream',
}

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/html, */*',
}

# ── GOOGLE SHEETS AUTH ────────────────────────────────────────────────────────
def get_sheet():
    raw = os.environ.get('GOOGLE_CREDENTIALS', '').strip()
    if not raw:
        print("ERROR: GOOGLE_CREDENTIALS not set.")
        sys.exit(1)
    try:
        info = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"ERROR: GOOGLE_CREDENTIALS is not valid JSON — {e}")
        sys.exit(1)
    if 'private_key' in info:
        info['private_key'] = info['private_key'].replace('\\n', '\n')
    scopes = ['https://spreadsheets.google.com/feeds',
              'https://www.googleapis.com/auth/drive']
    creds  = Credentials.from_service_account_info(info, scopes=scopes)
    client = gspread.authorize(creds)
    wb     = client.open_by_key(SPREADSHEET_ID)
    try:
        ws = wb.worksheet(SHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = wb.add_worksheet(title=SHEET_NAME, rows=2000, cols=15)
        print(f"  Created new sheet tab: {SHEET_NAME}")
    return ws

# ── LOAD SCHOOLS ──────────────────────────────────────────────────────────────
def load_schools():
    p = Path(SCHOOLS_FILE)
    if not p.exists():
        print(f"ERROR: {SCHOOLS_FILE} not found.")
        sys.exit(1)
    return json.loads(p.read_text())

# ── HELPERS ───────────────────────────────────────────────────────────────────
def base_url(schedule_url):
    """Extract https://school.com from a full schedule URL."""
    p = urlparse(schedule_url)
    return f"{p.scheme}://{p.netloc}"

def normalize_tv(raw):
    if not raw: return ''
    low = raw.lower()
    for key, label in TV_LOGOS.items():
        if key in low: return label
    return raw.strip()

def parse_iso_date(s):
    """Return YYYY-MM-DD from an ISO-ish string, or ''."""
    if not s: return ''
    m = re.search(r'(\d{4})-(\d{2})-(\d{2})', str(s))
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else ''

def format_time(s):
    """Return a clean time string like '6:00 PM' from various formats."""
    if not s: return 'TBD'
    s = str(s).strip()
    # Already formatted
    if re.search(r'\d{1,2}:\d{2}\s*(AM|PM)', s, re.I):
        return s.upper()
    # HH:MM:SS
    m = re.match(r'(\d{1,2}):(\d{2})', s)
    if m:
        h, mn = int(m.group(1)), m.group(2)
        ap = 'AM' if h < 12 else 'PM'
        h12 = h % 12 or 12
        return f"{h12}:{mn} {ap}"
    return s or 'TBD'

# ── SIDEARM JSON API ──────────────────────────────────────────────────────────
# Sidearm exposes schedule data via two known endpoints.
# We try both and use whichever returns data.

def try_sidearm_json_api(school_base, year=CURRENT_YEAR):
    """
    Try Sidearm's schedule JSON API endpoints.
    Returns list of raw game dicts or None if both fail.
    """
    endpoints = [
        f"{school_base}/services/schedule_stats_handler.ashx"
        f"?sport_id=MBA&span=year&year={year}",

        f"{school_base}/services/schedule_stats_handler.ashx"
        f"?sport_id=SB&span=year&year={year}",   # fallback sport code

        f"{school_base}/data/json/schedule_baseball_{year}.json",

        f"{school_base}/services/responsive-calendar.ashx"
        f"?sport_id=MBA&year={year}",
    ]
    for url in endpoints:
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            if r.status_code == 200 and r.text.strip().startswith(('{','[')):
                data = r.json()
                # Unwrap common wrapper keys
                for key in ('data', 'schedule', 'games', 'events', 'items'):
                    if isinstance(data, dict) and key in data:
                        data = data[key]
                        break
                if isinstance(data, list) and len(data) > 0:
                    print(f"    → Sidearm JSON API hit: {url.split('?')[0].split('/')[-1]}")
                    return data
        except Exception:
            pass
    return None


def try_sidearm_html_embedded(soup):
    """
    Sidearm sometimes embeds schedule JSON inside a <script> tag.
    Look for patterns like: var schedule = [...] or window.__data = {...}
    """
    for script in soup.find_all('script'):
        text = script.string or ''
        # Look for JSON arrays/objects assigned to variables
        for pattern in [
            r'var\s+(?:schedule|scheduleData|games|events)\s*=\s*(\[.*?\]);',
            r'window\.__(?:schedule|data|state)\s*=\s*(\{.*?\});',
            r'"games"\s*:\s*(\[.*?\])',
            r'"schedule"\s*:\s*(\[.*?\])',
        ]:
            m = re.search(pattern, text, re.DOTALL)
            if m:
                try:
                    data = json.loads(m.group(1))
                    if isinstance(data, list) and len(data) > 0:
                        print(f"    → Found embedded JSON in <script> tag")
                        return data
                    if isinstance(data, dict):
                        for key in ('games','schedule','events','data'):
                            if key in data and isinstance(data[key], list):
                                return data[key]
                except Exception:
                    pass
    return None


def try_sidearm_print_page(schedule_url):
    """
    Sidearm print pages render static HTML with no JS required.
    Try appending ?print=true or /print to the URL.
    """
    for url in [schedule_url + '?print=true', schedule_url + '/print']:
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, 'html.parser')
                # Look for table rows with date/opponent data
                rows = soup.select('tr.schedule_game, tr[class*="game"], .schedule-item')
                if rows:
                    print(f"    → Using print page: {url}")
                    return soup, rows
        except Exception:
            pass
    return None, None


def parse_sidearm_game(game, school):
    """
    Parse a single game dict from Sidearm JSON API into our standard format.
    Sidearm field names vary slightly — we check multiple possible keys.
    """
    def get(d, *keys):
        for k in keys:
            if k in d and d[k] not in (None, '', 'null'):
                return str(d[k]).strip()
        return ''

    # Date — try multiple field names
    raw_date = get(game, 'date', 'game_date', 'Date', 'event_date',
                   'start_date', 'StartDate')
    game_date = parse_iso_date(raw_date) or raw_date[:10] if raw_date else ''

    # Time
    raw_time = get(game, 'time', 'game_time', 'Time', 'start_time', 'StartTime')
    game_time = format_time(raw_time)

    # Opponent
    opponent = get(game, 'opponent', 'Opponent', 'opponent_name',
                   'OpponentName', 'away_team', 'home_team', 'team_name')
    # Strip leading "vs." / "at " for cleanliness
    opponent = re.sub(r'^(vs\.?\s*|at\s+)', '', opponent, flags=re.I).strip()

    # Home/Away
    loc_fields = get(game, 'location', 'home_away', 'HomeAway',
                     'game_type', 'GameType', 'is_home')
    raw_opp    = get(game, 'opponent', 'Opponent', 'opponent_name', '')
    if loc_fields:
        lf = loc_fields.lower()
        if any(x in lf for x in ['away', 'false', '0', 'at ']):
            home_away = 'Away'
        elif any(x in lf for x in ['neutral', 'tourney', 'tournament']):
            home_away = 'Neutral'
        else:
            home_away = 'Home'
    elif raw_opp.lower().startswith(('at ', '@ ')):
        home_away = 'Away'
    else:
        home_away = 'Home'

    # Venue
    location = get(game, 'location_name', 'venue', 'Venue', 'facility',
                   'stadium', 'location', 'city')

    # TV / Broadcast
    tv_raw = get(game, 'broadcast', 'tv', 'TV', 'network',
                 'broadcast_network', 'coverage')
    tv = normalize_tv(tv_raw)

    # Result / Score
    result = get(game, 'result', 'Result', 'score', 'Score',
                 'game_result', 'final_score')

    return {
        'school':    school,
        'date':      game_date,
        'time':      game_time,
        'opponent':  opponent,
        'home_away': home_away,
        'location':  location,
        'tv':        tv,
        'result':    result,
    }


def parse_sidearm_print_rows(rows, school):
    """Parse static HTML rows from a Sidearm print page."""
    games = []
    MONTH_MAP = {'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,
                 'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}
    for row in rows:
        cells = [c.get_text(strip=True) for c in row.find_all(['td','th'])]
        if len(cells) < 2: continue
        text  = ' '.join(cells)
        # Find date
        game_date = ''
        m = re.search(r'(\d{4})-(\d{2})-(\d{2})', text)
        if m:
            game_date = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        else:
            m = re.search(r'(\d{1,2})/(\d{1,2})/(\d{2,4})', text)
            if m:
                yr = int(m.group(3)); yr = yr+2000 if yr < 100 else yr
                game_date = f"{yr}-{int(m.group(1)):02d}-{int(m.group(2)):02d}"
            else:
                m = re.search(r'([A-Za-z]{3})\.?\s+(\d{1,2})', text)
                if m and m.group(1).lower()[:3] in MONTH_MAP:
                    mo = MONTH_MAP[m.group(1).lower()[:3]]
                    game_date = f"{CURRENT_YEAR}-{mo:02d}-{int(m.group(2)):02d}"
        if not game_date:
            continue
        opp_m = re.search(r'(?:vs\.?|at)\s+([A-Z][A-Za-z &.\'-]+)', text)
        opponent = opp_m.group(1).strip() if opp_m else (cells[1] if len(cells) > 1 else '')
        home_away = 'Away' if re.search(r'\bat\b', text[:30], re.I) else 'Home'
        time_m = re.search(r'(\d{1,2}:\d{2}\s*(?:AM|PM))', text, re.I)
        game_time = time_m.group(1).upper() if time_m else 'TBD'
        if opponent:
            games.append({'school': school, 'date': game_date, 'time': game_time,
                          'opponent': opponent, 'home_away': home_away,
                          'location': '', 'tv': '', 'result': ''})
    return games


# ── MAIN SCRAPE FUNCTION ──────────────────────────────────────────────────────
def scrape_school(entry, debug=False):
    name         = entry['school']
    schedule_url = entry.get('schedule_url', '')
    if not schedule_url:
        print(f"  ⚠️  No schedule_url for {name}, skipping")
        return []

    school_base = base_url(schedule_url)
    games       = []

    # ── Strategy 1: Sidearm JSON API ─────────────────────────────────────────
    raw_games = try_sidearm_json_api(school_base, CURRENT_YEAR)
    if raw_games:
        if debug:
            print(f"\n    DEBUG — raw API response (first game):")
            print(json.dumps(raw_games[0], indent=2)[:800])
        games = [g for g in
                 [parse_sidearm_game(g, name) for g in raw_games]
                 if g['date'] and g['opponent']]

    # ── Strategy 2: Embedded JSON in page <script> tags ──────────────────────
    if not games:
        try:
            r = requests.get(schedule_url, headers=HEADERS, timeout=15)
            soup = BeautifulSoup(r.text, 'html.parser')
            if debug:
                # Save raw HTML for inspection
                Path(f"debug_{name.replace(' ','_')}.html").write_text(r.text)
                print(f"    DEBUG — saved raw HTML to debug_{name.replace(' ','_')}.html")
            embedded = try_sidearm_html_embedded(soup)
            if embedded:
                games = [g for g in
                         [parse_sidearm_game(g, name) for g in embedded]
                         if g['date'] and g['opponent']]
        except Exception as e:
            print(f"    ⚠️  Fetch error: {e}")

    # ── Strategy 3: Sidearm print page ───────────────────────────────────────
    if not games:
        print_soup, print_rows = try_sidearm_print_page(schedule_url)
        if print_rows:
            games = parse_sidearm_print_rows(print_rows, name)

    # ── Strategy 4: Alternate year endpoint ──────────────────────────────────
    if not games and str(CURRENT_YEAR) in schedule_url:
        alt_url = schedule_url.replace(str(CURRENT_YEAR), str(CURRENT_YEAR - 1))
        raw_games = try_sidearm_json_api(base_url(alt_url), CURRENT_YEAR - 1)
        if raw_games:
            games = [g for g in
                     [parse_sidearm_game(g, name) for g in raw_games]
                     if g['date'] and g['opponent']]

    if games:
        print(f"    ✓  Found {len(games)} games")
    else:
        print(f"    ⚠️  No games found — run with --debug to inspect raw response")

    return games


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--today',  action='store_true',
                        help='Only write today\'s games')
    parser.add_argument('--school', type=str, default='',
                        help='Scrape one school by name')
    parser.add_argument('--debug',  action='store_true',
                        help='Print raw API response and save HTML for first school')
    args = parser.parse_args()

    schools = load_schools()
    if args.school:
        schools = [s for s in schools
                   if args.school.lower() in s['school'].lower()]
        if not schools:
            print(f"No school matching '{args.school}'")
            sys.exit(1)

    today_str  = date.today().isoformat()
    all_games  = []
    first      = True

    for entry in schools:
        name = entry['school']
        div  = entry.get('division', '')
        espn = entry.get('espn_team_id', '')
        print(f"\n🔍 Scraping: {name}")

        games = scrape_school(entry, debug=(args.debug and first))
        first = False

        for g in games:
            g['division']     = div
            g['espn_team_id'] = espn
            g['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M')

        if args.today:
            games = [g for g in games if g.get('date') == today_str]
            print(f"    → {len(games)} game(s) today")

        all_games.extend(games)
        time.sleep(REQUEST_DELAY)

    if not all_games:
        print("\nNo games to write.")
        return

    print(f"\n📝 Writing {len(all_games)} rows to Google Sheets…")
    ws   = get_sheet()
    COLS = ['school','division','date','time','opponent','home_away',
            'location','tv','result','espn_team_id','last_updated']

    existing        = ws.get_all_records()
    scraped_schools = {e['school'] for e in schools}
    kept            = [r for r in existing
                       if r.get('SCHOOL','') not in scraped_schools]

    rows = [[c.upper() for c in COLS]]
    for r in kept:
        rows.append([r.get(c.upper(), '') for c in COLS])
    for g in all_games:
        rows.append([g.get(c, '') for c in COLS])

    ws.clear()
    ws.update('A1', rows)
    print(f"✅ Done — {len(all_games)} games written.")

if __name__ == '__main__':
    main()