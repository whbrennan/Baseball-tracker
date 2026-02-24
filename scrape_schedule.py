"""
scrape_schedule.py
Scrapes Sidearm Sports schedule pages via their print URL (static HTML)
and writes results to a 'Schedule' tab in Google Sheets.

Usage:
    python3 scrape_schedule.py                        # all schools
    python3 scrape_schedule.py --today                # today only
    python3 scrape_schedule.py --school "Maryland"    # one school
    python3 scrape_schedule.py --debug                # save HTML for all schools
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

MONTH_MAP = {
    'january':1,'february':2,'march':3,'april':4,'may':5,'june':6,
    'july':7,'august':8,'september':9,'october':10,'november':11,'december':12,
    'jan':1,'feb':2,'mar':3,'apr':4,'jun':6,'jul':7,'aug':8,
    'sep':9,'oct':10,'nov':11,'dec':12
}

TV_MAP = {
    'espn+':'ESPN+','espnu':'ESPNU','espn2':'ESPN2','espn':'ESPN',
    'sec network':'SEC Network','acc network':'ACC Network',
    'big ten network':'Big Ten Network','mlb network':'MLB Network',
    'fs1':'FS1','fs2':'FS2','stadium':'Stadium',
    'flobaseball':'FloBaseball','youtube':'YouTube','live stream':'Stream',
}

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,*/*',
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

# ── DATE / TIME HELPERS ───────────────────────────────────────────────────────
def parse_date(text):
    """
    Try every known date format. Returns YYYY-MM-DD or ''.
    Handles: 2026-02-14, 2/14/26, 2/14/2026, Feb. 14, February 14, Feb 14 2026
    """
    text = text.strip()

    # ISO: 2026-02-14
    m = re.search(r'(\d{4})-(\d{2})-(\d{2})', text)
    if m: return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    # mm/dd/yyyy or mm/dd/yy
    m = re.search(r'\b(\d{1,2})/(\d{1,2})/(\d{2,4})\b', text)
    if m:
        yr = int(m.group(3))
        yr = yr + 2000 if yr < 100 else yr
        return f"{yr}-{int(m.group(1)):02d}-{int(m.group(2)):02d}"

    # Month name: "Feb. 14", "February 14", "Feb 14, 2026"
    m = re.search(
        r'\b(january|february|march|april|may|june|july|august|'
        r'september|october|november|december|'
        r'jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec)\.?\s+'
        r'(\d{1,2})(?:st|nd|rd|th)?(?:,?\s*(\d{4}))?',
        text, re.IGNORECASE
    )
    if m:
        mo  = MONTH_MAP[m.group(1).lower()]
        day = int(m.group(2))
        yr  = int(m.group(3)) if m.group(3) else CURRENT_YEAR
        return f"{yr}-{mo:02d}-{day:02d}"

    return ''


def parse_time(text):
    """Extract and normalize a time like '6:00 PM' from any string."""
    m = re.search(r'(\d{1,2}):(\d{2})\s*(a\.?m\.?|p\.?m\.?)?', text, re.I)
    if m:
        h, mn = int(m.group(1)), m.group(2)
        ap = re.sub(r'\.', '', m.group(3) or '').upper()
        if not ap:
            ap = 'PM' if 1 <= h <= 7 else 'AM'
        h12 = h % 12 or 12
        return f"{h12}:{mn} {ap}"
    # Time without colon: "6 PM"
    m = re.search(r'\b(\d{1,2})\s*(am|pm)\b', text, re.I)
    if m:
        return f"{m.group(1)}:00 {m.group(2).upper()}"
    return 'TBD'


def normalize_tv(text):
    if not text: return ''
    low = text.lower()
    for key, label in TV_MAP.items():
        if key in low: return label
    return text.strip()


# ── SIDEARM PRINT PAGE PARSER ─────────────────────────────────────────────────
def fetch_print_page(schedule_url):
    """Fetch ?print=true version of a Sidearm schedule page."""
    # Try both print URL patterns
    for url in [schedule_url.rstrip('/') + '?print=true',
                schedule_url.rstrip('/') + '/print']:
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            if r.status_code == 200 and len(r.text) > 500:
                return url, r.text
        except Exception as e:
            print(f"    ⚠️  Fetch error ({url}): {e}")
    return None, None


def parse_print_page(html, school_name, debug=False):
    """
    Parse Sidearm's ?print=true static HTML schedule.

    Sidearm print pages render a table where each <tr> is one game.
    Column order varies by school but typically:
      Date | Opponent | Location/H-A | Time | Result | TV

    Strategy:
    1. Find the schedule table by looking for rows containing date-like text
    2. Auto-detect which column index holds date, opponent, time, etc.
    3. Parse every data row
    """
    soup = BeautifulSoup(html, 'html.parser')

    if debug:
        # Save full HTML for manual inspection in Codespaces
        fname = f"debug_{school_name.replace(' ','_').replace('&','and')}.html"
        Path(fname).write_text(html)
        print(f"    📄 Saved {fname} ({len(html):,} chars)")

    games = []

    # ── Find all tables and pick the one most likely to be the schedule ───────
    best_table = None
    best_score = 0
    for tbl in soup.find_all('table'):
        text = tbl.get_text(' ')
        # Score the table by how many date-like strings it contains
        score = len(re.findall(
            r'\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*'
            r'\.?\s+\d{1,2}|\d{1,2}/\d{1,2}/\d{2,4}',
            text, re.I
        ))
        if score > best_score:
            best_score = score
            best_table = tbl

    if not best_table:
        # No tables — try <li> or <div> based layouts
        return parse_list_layout(soup, school_name)

    rows = best_table.find_all('tr')
    if not rows:
        return []

    # ── Auto-detect column positions from header row ──────────────────────────
    col_date = col_opp = col_time = col_loc = col_result = col_tv = None
    header_row = rows[0]
    headers    = [th.get_text(strip=True).lower()
                  for th in header_row.find_all(['th', 'td'])]

    for i, h in enumerate(headers):
        if any(x in h for x in ['date','day']):
            col_date = i
        elif any(x in h for x in ['opponent','team','vs','game']):
            col_opp = i
        elif any(x in h for x in ['time','start']):
            col_time = i
        elif any(x in h for x in ['location','site','venue','city','place']):
            col_loc = i
        elif any(x in h for x in ['result','score','w/l','record']):
            col_result = i
        elif any(x in h for x in ['tv','broadcast','network','coverage']):
            col_tv = i

    if debug:
        print(f"    Column map — date:{col_date} opp:{col_opp} "
              f"time:{col_time} loc:{col_loc} result:{col_result} tv:{col_tv}")
        print(f"    Headers: {headers}")

    # ── Parse each data row ───────────────────────────────────────────────────
    for row in rows[1:]:
        cells = row.find_all(['td', 'th'])
        if not cells:
            continue
        texts = [c.get_text(' ', strip=True) for c in cells]
        full  = ' '.join(texts)

        # Skip header/section rows (no useful data)
        if len(texts) < 2:
            continue
        if all(t == '' for t in texts):
            continue

        # ── Extract date ──────────────────────────────────────────────────────
        game_date = ''
        if col_date is not None and col_date < len(texts):
            game_date = parse_date(texts[col_date])
        if not game_date:
            # Scan all cells for a date
            for t in texts:
                game_date = parse_date(t)
                if game_date:
                    break
        if not game_date:
            continue   # Can't use a row with no date

        # ── Extract opponent ──────────────────────────────────────────────────
        opponent = ''
        if col_opp is not None and col_opp < len(texts):
            opponent = texts[col_opp]
        if not opponent:
            # Pick the longest non-date, non-time cell as opponent
            candidates = []
            for t in texts:
                if not parse_date(t) and not re.match(r'^\d{1,2}:\d{2}', t):
                    candidates.append(t)
            if candidates:
                opponent = max(candidates, key=len)

        # Clean up "vs. Team" / "at Team" prefixes
        opponent = re.sub(r'^(vs\.?\s*|at\s+@?\s*)', '', opponent,
                          flags=re.I).strip()
        # Remove score that may be appended: "Opponent Name W 7-3"
        opponent = re.sub(r'\s+[WLT]\s+\d+[-–]\d+.*$', '', opponent).strip()

        if not opponent or len(opponent) < 2:
            continue

        # ── Home / Away ───────────────────────────────────────────────────────
        raw_loc = texts[col_loc] if col_loc is not None and col_loc < len(texts) else ''
        home_away = 'Home'
        if re.search(r'\baway\b|^@\s', raw_loc + full[:60], re.I):
            home_away = 'Away'
        elif re.search(r'\bneutral\b|\btournament\b|\btourney\b|\bclassic\b',
                       raw_loc + full[:60], re.I):
            home_away = 'Neutral'
        # Also check original opponent text before cleaning
        orig_opp = texts[col_opp] if col_opp is not None and col_opp < len(texts) else full
        if re.search(r'^at\s|^@\s', orig_opp, re.I):
            home_away = 'Away'

        # ── Time ─────────────────────────────────────────────────────────────
        game_time = 'TBD'
        if col_time is not None and col_time < len(texts):
            game_time = parse_time(texts[col_time]) if texts[col_time] else 'TBD'
        if game_time == 'TBD':
            # Scan all cells
            for t in texts:
                pt = parse_time(t)
                if pt != 'TBD':
                    game_time = pt
                    break

        # ── TV ────────────────────────────────────────────────────────────────
        tv = ''
        if col_tv is not None and col_tv < len(texts):
            tv = normalize_tv(texts[col_tv])
        if not tv:
            tv_m = re.search(
                r'\b(espn\+?2?u?|sec network|acc network|fs[12]|'
                r'big ten network|mlb network|stadium|flobaseball)\b',
                full, re.I
            )
            if tv_m:
                tv = normalize_tv(tv_m.group(1))

        # ── Result ────────────────────────────────────────────────────────────
        result = ''
        if col_result is not None and col_result < len(texts):
            result = texts[col_result]
        if not result:
            r_m = re.search(r'\b([WL])\s+(\d+[-–]\d+)', full)
            if r_m:
                result = f"{r_m.group(1)} {r_m.group(2)}"

        games.append({
            'school':    school_name,
            'date':      game_date,
            'time':      game_time,
            'opponent':  opponent,
            'home_away': home_away,
            'location':  raw_loc,
            'tv':        tv,
            'result':    result,
        })

    return games


def parse_list_layout(soup, school_name):
    """
    Fallback for schools that use <li> or <div> based layouts
    instead of a <table> on their print page.
    """
    games = []
    containers = (
        soup.select('li.schedule-item, li[class*="game"], '
                    'div.schedule-item, div[class*="game-item"], '
                    'article[class*="game"]')
    )
    for item in containers:
        text      = item.get_text(' ', strip=True)
        game_date = parse_date(text)
        if not game_date:
            continue
        opp_m    = re.search(r'(?:vs\.?|at)\s+([A-Z][A-Za-z &.\'-]+)', text)
        opponent = opp_m.group(1).strip() if opp_m else ''
        if not opponent:
            continue
        games.append({
            'school':    school_name,
            'date':      game_date,
            'time':      parse_time(text),
            'opponent':  opponent,
            'home_away': 'Away' if re.search(r'\bat\b', text[:40], re.I) else 'Home',
            'location':  '',
            'tv':        '',
            'result':    '',
        })
    return games


# ── MAIN SCRAPE FUNCTION ──────────────────────────────────────────────────────
def scrape_school(entry, debug=False):
    name         = entry['school']
    schedule_url = entry.get('schedule_url', '')
    if not schedule_url:
        print(f"  ⚠️  No schedule_url for {name}, skipping")
        return []

    url, html = fetch_print_page(schedule_url)
    if not html:
        print(f"    ⚠️  Could not fetch print page")
        return []

    print(f"    → Print page: {url}")
    games = parse_print_page(html, name, debug=debug)

    if games:
        print(f"    ✓  Found {len(games)} games")
    else:
        print(f"    ⚠️  No games parsed — run with --debug to save HTML for inspection")

    return games


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--today',  action='store_true',
                        help='Only write today\'s games')
    parser.add_argument('--school', type=str, default='',
                        help='Scrape one school by name')
    parser.add_argument('--debug',  action='store_true',
                        help='Save raw HTML files for inspection')
    args = parser.parse_args()

    schools = load_schools()
    if args.school:
        schools = [s for s in schools
                   if args.school.lower() in s['school'].lower()]
        if not schools:
            print(f"No school matching '{args.school}'")
            sys.exit(1)

    today_str = date.today().isoformat()
    all_games = []

    for entry in schools:
        name = entry['school']
        div  = entry.get('division', '')
        espn = entry.get('espn_team_id', '')
        print(f"\n🔍 Scraping: {name}")

        games = scrape_school(entry, debug=args.debug)

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
                       if r.get('SCHOOL', '') not in scraped_schools]

    rows = [[c.upper() for c in COLS]]
    for r in kept:
        rows.append([r.get(c.upper(), '') for c in COLS])
    for g in all_games:
        rows.append([g.get(c, '') for c in COLS])

    ws.clear()
    ws.update(values=rows, range_name='A1')   # fixed argument order warning
    print(f"✅ Done — {len(all_games)} games written.")


if __name__ == '__main__':
    main()