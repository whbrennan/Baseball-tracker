"""
scrape_schedule.py
Scrapes game schedules from college baseball program pages and writes
to a 'Schedule' tab in the existing Google Sheet.

Credentials are read from environment variables (set via GitHub Secrets):
    GOOGLE_CLIENT_EMAIL
    GOOGLE_PRIVATE_KEY
    GOOGLE_PROJECT_ID   (optional)

Usage:
    python3 scrape_schedule.py             # scrape all schools
    python3 scrape_schedule.py --today     # only write today's games
    python3 scrape_schedule.py --school "Texas"  # one school only
"""

import json, os, re, sys, time, argparse
from datetime import datetime, date
from pathlib import Path
import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials

# ── CONFIG ────────────────────────────────────────────────────────────────────
SPREADSHEET_ID = '1vZ9retMliQq99hw2twZ-KtlvLKLOStkcHq73CAx94gs'
SHEET_NAME     = 'Schedule'
SCHOOLS_FILE   = 'schools.json'
REQUEST_DELAY  = 1.5
CURRENT_YEAR   = datetime.now().year

TV_LOGOS = {
    'espn+': 'ESPN+', 'espnu': 'ESPNU', 'espn2': 'ESPN2', 'espn': 'ESPN',
    'sec network': 'SEC Network', 'acc network': 'ACC Network',
    'big ten network': 'Big Ten Network', 'pac-12': 'Pac-12 Network',
    'mlb network': 'MLB Network', 'fs1': 'FS1', 'fs2': 'FS2',
    'stadium': 'Stadium', 'flobaseball': 'FloBaseball',
    'youtube': 'YouTube', 'live stream': 'Stream',
}

# ── GOOGLE SHEETS AUTH (from env vars / GitHub Secrets) ───────────────────────
def get_sheet():
    raw = os.environ.get('GOOGLE_CREDENTIALS', '').strip()

    if not raw:
        print("ERROR: GOOGLE_CREDENTIALS environment variable is not set.")
        print("  Locally: export GOOGLE_CREDENTIALS=$(cat your-service-account.json)")
        print("  In CI:   set GOOGLE_CREDENTIALS as a GitHub Secret containing the full JSON.")
        sys.exit(1)

    try:
        info = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"ERROR: GOOGLE_CREDENTIALS is not valid JSON — {e}")
        print("  Make sure the secret contains the full contents of your service account JSON file.")
        sys.exit(1)

    # GitHub Secrets sometimes escape newlines in private_key — fix that
    if 'private_key' in info:
        info['private_key'] = info['private_key'].replace('\\n', '\n')

    scopes = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
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
        starter = [
            {
                "school":       "Example University",
                "division":     "D1",
                "schedule_url": "https://exampleathletics.com/sports/baseball/schedule",
                "espn_team_id": "",
                "notes":        "Replace with real schools"
            }
        ]
        p.write_text(json.dumps(starter, indent=2))
        print(f"Created starter {SCHOOLS_FILE} — edit it with your schools then re-run.")
        sys.exit(0)
    return json.loads(p.read_text())

# ── HTTP HELPER ───────────────────────────────────────────────────────────────
HEADERS = {'User-Agent': 'Mozilla/5.0 (compatible; CollegeBaseballTracker/1.0)'}

def fetch(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        return BeautifulSoup(r.text, 'html.parser')
    except Exception as e:
        print(f"    ⚠️  Fetch failed: {url} — {e}")
        return None

# ── DATE / TIME PARSERS ───────────────────────────────────────────────────────
MONTH_MAP = {
    'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,
    'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12
}

def parse_date(raw):
    raw = raw.strip().lower()
    m = re.search(r'(\d{4})-(\d{2})-(\d{2})', raw)
    if m: return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = re.search(r'(\d{1,2})/(\d{1,2})/(\d{2,4})', raw)
    if m:
        yr = int(m.group(3)); yr = yr+2000 if yr<100 else yr
        return f"{yr}-{int(m.group(1)):02d}-{int(m.group(2)):02d}"
    m = re.search(r'([a-z]{3})\.?\s+(\d{1,2})', raw)
    if m and m.group(1)[:3] in MONTH_MAP:
        mo = MONTH_MAP[m.group(1)[:3]]
        return f"{CURRENT_YEAR}-{mo:02d}-{int(m.group(2)):02d}"
    return ''

def parse_time(raw):
    raw = raw.strip()
    m = re.search(r'(\d{1,2}):(\d{2})\s*(am|pm)?', raw, re.I)
    if m:
        h, mn, ap = int(m.group(1)), m.group(2), (m.group(3) or '').upper()
        if not ap: ap = 'PM' if 1 <= h <= 7 else 'AM'
        return f"{h}:{mn} {ap}"
    m = re.search(r'(\d{1,2})\s*(am|pm)', raw, re.I)
    if m: return f"{m.group(1)}:00 {m.group(2).upper()}"
    return raw or 'TBD'

def normalize_tv(raw):
    low = raw.lower()
    for key, label in TV_LOGOS.items():
        if key in low: return label
    return raw.strip()

# ── SCRAPERS ──────────────────────────────────────────────────────────────────
def scrape_sidearm(soup, school):
    games = []
    containers = (
        soup.select('li.schedule-item') or
        soup.select('div.schedule-item') or
        soup.select('li[class*="s-game"]') or
        soup.select('tr.schedule_game') or
        soup.select('div[class*="game-item"]')
    )
    for item in containers:
        g = {'school': school, 'raw_source': 'sidearm'}
        d   = item.select_one('[class*="date"], [class*="schedule-date"], time')
        opp = item.select_one('[class*="opponent"], [class*="team-name"], [class*="schedule-opponent"]')
        loc = item.select_one('[class*="location"], [class*="home-away"]')
        t   = item.select_one('[class*="time"], [class*="schedule-time"]')
        ven = item.select_one('[class*="location-text"], [class*="facility"]')
        tv  = item.select_one('[class*="network"], [class*="broadcast"], [class*="tv"]')
        res = item.select_one('[class*="result"], [class*="score"]')

        g['date']      = parse_date(d.get_text()) if d else ''
        g['opponent']  = opp.get_text(strip=True) if opp else ''
        loc_text       = loc.get_text(strip=True).lower() if loc else ''
        g['home_away'] = 'Away' if '@' in loc_text or 'away' in loc_text else \
                         'Neutral' if 'neutral' in loc_text else 'Home'
        g['time']      = parse_time(t.get_text()) if t else 'TBD'
        g['location']  = ven.get_text(strip=True) if ven else ''
        g['tv']        = normalize_tv(tv.get_text()) if tv else ''
        g['result']    = res.get_text(strip=True) if res else ''

        if g['date'] and g['opponent']:
            games.append(g)
    return games

def scrape_generic(soup, school):
    games = []
    rows = soup.select('table.schedule tr, table#schedule-table tr, table[class*="sched"] tr')
    for row in rows:
        cells = row.find_all(['td','th'])
        if len(cells) < 3: continue
        texts = [c.get_text(strip=True) for c in cells]
        g = {'school':school,'raw_source':'generic-table','date':'','time':'TBD',
             'opponent':'','home_away':'Home','location':'','tv':'','result':''}
        for i, t in enumerate(texts):
            d = parse_date(t)
            if d:
                g['date'] = d
                rest = [x for x in texts[i+1:] if x]
                if rest:     g['opponent'] = rest[0]
                if len(rest)>1: g['time']  = parse_time(rest[1])
                if len(rest)>2: g['location'] = rest[2]
                break
        if g['date'] and g['opponent']:
            if g['opponent'].startswith(('at ','@ ')):
                g['home_away'] = 'Away'
                g['opponent']  = re.sub(r'^(at |@ )', '', g['opponent'])
            games.append(g)

    if not games:
        for item in soup.select('ul.schedule li, div.game, article.game'):
            text = item.get_text(' ', strip=True)
            d = parse_date(text)
            if not d: continue
            g = {'school':school,'raw_source':'generic-list','date':d,
                 'time':parse_time(text),'opponent':'',
                 'home_away':'Away' if ' at ' in text.lower() else 'Home',
                 'location':'','tv':'','result':''}
            m = re.search(r'(?:vs\.?|at)\s+([A-Z][A-Za-z &.\'-]+)', text)
            if m: g['opponent'] = m.group(1).strip()
            if g['opponent']: games.append(g)
    return games

def detect_and_scrape(soup, school, url):
    if not soup: return []
    html = str(soup)
    if any(x in html for x in ['sidearm','schedule-item','s-game-schedule','sidearmstats']):
        print(f"    → Detected: Sidearm Sports")
        games = scrape_sidearm(soup, school)
    else:
        print(f"    → Using: generic scraper")
        games = scrape_generic(soup, school)
    if not games:
        print(f"    ⚠️  No games found — may need custom scraper. URL: {url}")
    else:
        print(f"    ✓  Found {len(games)} games")
    return games

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--today',  action='store_true')
    parser.add_argument('--school', type=str, default='')
    args = parser.parse_args()

    schools = load_schools()
    if args.school:
        schools = [s for s in schools if args.school.lower() in s['school'].lower()]
        if not schools:
            print(f"No school matching '{args.school}' in {SCHOOLS_FILE}")
            sys.exit(1)

    today_str  = date.today().isoformat()
    all_games  = []

    for entry in schools:
        name    = entry['school']
        url     = entry.get('schedule_url','')
        div     = entry.get('division','')
        espn_id = entry.get('espn_team_id','')

        if not url:
            print(f"⚠️  {name}: no schedule_url, skipping")
            continue

        print(f"\n🔍 Scraping: {name}")
        games = detect_and_scrape(fetch(url), name, url)

        for g in games:
            g['division']     = div
            g['espn_team_id'] = espn_id
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

    existing       = ws.get_all_records()
    scraped_schools = {e['school'] for e in schools}
    kept           = [r for r in existing if r.get('SCHOOL','') not in scraped_schools]

    rows = [[c.upper() for c in COLS]]
    for r in kept:
        rows.append([r.get(c.upper(),'') for c in COLS])
    for g in all_games:
        rows.append([g.get(c,'') for c in COLS])

    ws.clear()
    ws.update('A1', rows)
    print(f"✅ Done — {len(all_games)} games written, {len(kept)} existing rows preserved.")

if __name__ == '__main__':
    main()