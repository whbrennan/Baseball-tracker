# Baseball Tracker Project Context

## Repo
GitHub: whbrennan/Baseball-tracker
Main file: index.html
Spreadsheet ID: 1vZ9retMliQq99hw2twZ-KtlvLKLOStkcHq73CAx94gs

## What's built and working
- index.html: full stats tracker with Batting/Pitching/Defense/Leaderboard/Compare/Scatter/History tabs
- Inactive roster section (G=0 players shown at bottom, excluded from all tables)
- Debug console (only visible at ?debug=1)
- scrape_schedule.py: scrapes 16/17 schools via Sidearm print pages, 748 games written to Schedule sheet
- .github/workflows/scrape_schedule.yml: runs every 3 hours
- schools.json: 17 schools configured with ESPN IDs for D1 schools
- GOOGLE_CREDENTIALS secret: full JSON key in GitHub Secrets

## Sheets in Google Spreadsheet
- Batting, Pitching, Defense: player stats
- Batting_History, Pitching_History: trend data
- Schedule: game schedule data (columns: SCHOOL, DIVISION, DATE, TIME, OPPONENT, HOME_AWAY, LOCATION, TV, RESULT, ESPN_TEAM_ID, LAST_UPDATED)

## Outstanding issues
1. George Washington schedule not parsing — print page uses div/list layout, not table
   - URL: https://gwsports.com/sports/baseball/schedule?print=true
   - Has dates (Feb/Mar visible) but no <table> tag
   - Need to fix parse_list_layout() in scrape_schedule.py

## Next step after GW fix
- Add Today's Games tab to index.html
  - Reads Schedule sheet via Google Sheets CSV
  - Shows: player name, school, opponent, time, home/away, TV info
  - Live scores via ESPN API for D1 schools (espn_team_id in schools.json)
  - ESPN scoreboard endpoint: https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/scoreboard

## Schools and ESPN IDs
ETSU: 2193, Maryland: 120, Monmouth: 2405, Iona: 314, William & Mary: 2729, GW: 45
D2/D3/NJCAA have no ESPN IDs

## Key design decisions
- Credentials: single GOOGLE_CREDENTIALS secret (full JSON)
- Scraper uses ?print=true Sidearm pages (static HTML, no JS rendering needed)
- Debug panel in index.html: only shows at ?debug=1
- Inactive roster: collapsible section, table/card toggle, orange styling
