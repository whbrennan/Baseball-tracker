import os
import json
import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright
from datetime import datetime

# ── CONFIG ───────────────────────────────────────────────────────────────────
SPREADSHEET_ID    = os.environ.get("SPREADSHEET_ID")
GOOGLE_CREDS_JSON = os.environ.get("GOOGLE_CREDENTIALS")

# Map Sidearm column names → our Google Sheet column names
BATTING_MAP = {
    "GP-GS"  : ("G", "GS"),   # split combined field e.g. "7-7"
    "AB"     : "AB",
    "R"      : "R",
    "H"      : "H",
    "2B"     : "2B",
    "3B"     : "3B",
    "HR"     : "HR",
    "RBI"    : "RBI",
    "BB"     : "BB",
    "SO"     : "SO",
    "HBP"    : "HBP",
    "SH"     : "SAC",          # Sidearm calls it SH
    "SF"     : "SF",
    "SB-ATT" : ("SB", "CS"),   # split combined field e.g. "4-4"
    "AVG"    : "AVG",
    "OB%"    : "OBP",          # Sidearm calls it OB%
    "SLG%"   : "SLG",          # Sidearm calls it SLG%
    "OPS"    : "OPS",
}

PITCHING_MAP = {
    "APP-GS" : ("G", "GS"),    # split combined field
    "W-L"    : ("W", "L"),     # split combined field
    "SV"     : "SV",
    "IP"     : "IP",
    "H"      : "H",
    "R"      : "R",
    "ER"     : "ER",
    "BB"     : "BB",
    "SO"     : "SO",
    "HBP"    : "HBP",
    "ERA"    : "ERA",
    "WHIP"   : "WHIP",
}

# Defense table has no GP-GS column — G/GS omitted intentionally
DEFENSE_MAP = {
    "C"    : "TC",             # Sidearm calls total chances "C"
    "PO"   : "PO",
    "A"    : "A",
    "E"    : "E",
    "FLD%" : "FLD%",
    "DP"   : "DP",
}

# ── GOOGLE SHEETS ─────────────────────────────────────────────────────────────
def connect():
    creds_dict = json.loads(GOOGLE_CREDS_JSON)
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds  = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open_by_key(SPREADSHEET_ID)

def get_players(sheet):
    return sheet.worksheet("Players").get_all_records()

# ── STAT PARSING ──────────────────────────────────────────────────────────────
def split_combined(value, sep="-"):
    """Split a combined stat like '7-7' or '4-4' into two values."""
    parts = value.split(sep, 1)
    if len(parts) > 1:
        return parts[0].strip(), parts[1].strip()
    return "", ""

def align_headers(hdrs, cells):
    """
    Sidearm includes a 'Player' column in the header row that has
    no corresponding data cell (it renders as a link/image only).
    If headers outnumber cells by 1, drop the 'Player' header to realign.
    """
    if "Player" in hdrs and len(hdrs) == len(cells) + 1:
        hdrs = [h for h in hdrs if h != "Player"]
    return hdrs

def map_row(raw, col_map):
    """
    Convert a raw {sidearm_header: value} dict into our sheet column names,
    handling combined fields like GP-GS → G, GS.
    """
    out = {}
    for src_col, dest in col_map.items():
        val = raw.get(src_col, "")
        if isinstance(dest, tuple):          # combined field — split it
            v1, v2 = split_combined(val)
            out[dest[0]] = v1
            out[dest[1]] = v2
        else:
            out[dest] = val

    # Compute derived stats if missing
    if not out.get("OPS"):
        try:
            out["OPS"] = f"{float(out.get('OBP','0')) + float(out.get('SLG','0')):.3f}"
        except Exception:
            pass
    try:
        ip = float(out.get("IP", 0))
        bb = float(out.get("BB", 0))
        so = float(out.get("SO", 0))
        if ip > 0:
            out["K_per_9"]  = f"{(so / ip * 9):.2f}"
            out["BB_per_9"] = f"{(bb / ip * 9):.2f}"
    except Exception:
        pass
    return out

# ── SCRAPING ──────────────────────────────────────────────────────────────────
def find_table(page, stat_type):
    """
    Return the correct stats table based on stat_type by checking headers.
    """
    for table in page.query_selector_all("table"):
        rows = table.query_selector_all("tr")
        if len(rows) < 2:
            continue
        hdrs = [c.inner_text().strip()
                for c in rows[0].query_selector_all("th, td")]
        if stat_type == "batting"  and "AVG"  in hdrs and "AB"   in hdrs: return table, hdrs
        if stat_type == "pitching" and "ERA"  in hdrs and "IP"   in hdrs: return table, hdrs
        if stat_type == "defense"  and "FLD%" in hdrs and "PO"   in hdrs: return table, hdrs
    return None, []

def zero_stats(col_map):
    """Return a dict of all zeros for every destination column in a map."""
    out = {}
    for dest in col_map.values():
        if isinstance(dest, tuple):
            out[dest[0]] = "0"
            out[dest[1]] = "0"
        else:
            out[dest] = "0"
    return out

def scrape(page, player, stat_type):
    """
    Load the stats page and find the player's row by jersey number.
    Returns a mapped dict of {our_column: value}, or all zeros if
    the player or table isn't found (i.e. hasn't appeared yet).
    """
    url    = player["Stats_URL"]
    jersey = str(player.get("Jersey", "")).strip()

    if not jersey:
        print(f"    ⚠ No jersey number for {player['Name']} — cannot match row")
        return zero_stats({"batting": BATTING_MAP,
                           "pitching": PITCHING_MAP,
                           "defense": DEFENSE_MAP}[stat_type])

    import time
    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    time.sleep(5)  # let JS finish rendering after DOM loads

    col_map = {"batting": BATTING_MAP,
               "pitching": PITCHING_MAP,
               "defense": DEFENSE_MAP}[stat_type]

    table, hdrs = find_table(page, stat_type)
    if not table:
        print(f"    ⚠ No {stat_type} table found — writing zeros")
        return zero_stats(col_map)

    for row in table.query_selector_all("tr")[1:]:
        cells = [c.inner_text().strip() for c in row.query_selector_all("td")]
        if not cells:
            continue

        aligned_hdrs = align_headers(hdrs, cells)
        row_jersey   = cells[0].strip()

        if row_jersey == jersey:
            raw = {aligned_hdrs[i]: cells[i]
                   for i in range(min(len(aligned_hdrs), len(cells)))}
            return map_row(raw, col_map)

    print(f"    ⚠ Jersey #{jersey} not found — writing zeros")
    return zero_stats(col_map)

# ── SHEET WRITING ──────────────────────────────────────────────────────────────
def write_stats(sheet, tab, player, mapped, target_cols):
    ws       = sheet.worksheet(tab)
    existing = ws.get_all_records()
    pid      = player["PlayerID"]
    now      = datetime.now().strftime("%Y-%m-%d %H:%M")

    new_row = [now, pid, player["Name"], player["School"], player["Division"]]
    new_row += [str(mapped.get(col, "")) for col in target_cols]

    for i, row in enumerate(existing):
        if row.get("PlayerID") == pid:
            ws.update(values=[new_row], range_name=f"A{i + 2}")  # fixed arg order
            print(f"    ✓ {tab} updated")
            return
    ws.append_row(new_row)
    print(f"    ✓ {tab} added")

def log(sheet, player, status, notes=""):
    sheet.worksheet("Scrape_Log").append_row([
        datetime.now().strftime("%Y-%m-%d %H:%M"),
        player["PlayerID"], player["Name"],
        player["School"], status, notes
    ])

# ── COLUMN LISTS (must match Google Sheet headers exactly) ────────────────────
BATTING_COLS  = ["G","GS","AB","R","H","2B","3B","HR","RBI","BB","SO",
                 "HBP","SAC","SF","SB","CS","AVG","OBP","SLG","OPS"]
PITCHING_COLS = ["G","GS","W","L","SV","IP","H","R","ER","BB","SO",
                 "HBP","ERA","WHIP","K_per_9","BB_per_9","K_BB","xFIP"]
DEFENSE_COLS  = ["TC","PO","A","E","FLD%","DP"]

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main(test_player_id=None):
    print("Connecting to Google Sheets...")
    sheet   = connect()
    players = get_players(sheet)

    if test_player_id:
        players = [p for p in players if p["PlayerID"] == test_player_id]
        if not players:
            print(f"Player ID '{test_player_id}' not found.")
            return

    print(f"Processing {len(players)} player(s)...\n")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ))
        page = context.new_page()

        for player in players:
            print(f"→ {player['Name']} ({player['School']}) Jersey #{player.get('Jersey','')}")
            try:
                if player["Type"] == "Hitter":
                    for stat_type, tab, cols in [
                        ("batting", "Batting", BATTING_COLS),
                        ("defense", "Defense", DEFENSE_COLS),
                    ]:
                        s = scrape(page, player, stat_type)
                        write_stats(sheet, tab, player, s, cols)
                else:
                    s = scrape(page, player, "pitching")
                    write_stats(sheet, "Pitching", player, s, PITCHING_COLS)

                log(sheet, player, "SUCCESS")

            except Exception as e:
                print(f"    ✗ ERROR: {e}")
                log(sheet, player, "ERROR", str(e))

        browser.close()

    print("\n✓ Done! Check your Google Sheet.")

# ── ENTRY POINT ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Change "P001" to any PlayerID, or use     main() to run all 18
    main()