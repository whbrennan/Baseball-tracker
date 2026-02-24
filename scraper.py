import os
import json
import time
import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright
from datetime import datetime

# ── CONFIG ───────────────────────────────────────────────────────────────────
SPREADSHEET_ID      = os.environ.get("SPREADSHEET_ID")
GOOGLE_CREDS_JSON   = os.environ.get("GOOGLE_CREDENTIALS")
PUSHOVER_USER       = os.environ.get("PUSHOVER_USER_KEY")
PUSHOVER_TOKEN      = os.environ.get("PUSHOVER_API_TOKEN")

BATTING_MAP = {
    "GP-GS"  : ("G", "GS"),
    "AB"     : "AB",   "R"   : "R",    "H"   : "H",
    "2B"     : "2B",   "3B"  : "3B",   "HR"  : "HR",
    "RBI"    : "RBI",  "BB"  : "BB",   "SO"  : "SO",
    "HBP"    : "HBP",  "SH"  : "SAC",  "SF"  : "SF",
    "SB-ATT" : ("SB", "CS"),
    "AVG"    : "AVG",  "OB%" : "OBP",  "SLG%": "SLG",  "OPS": "OPS",
}

PITCHING_MAP = {
    "APP-GS" : ("G", "GS"),
    "W-L"    : ("W", "L"),
    "SV"     : "SV",   "IP"  : "IP",   "H"   : "H",
    "R"      : "R",    "ER"  : "ER",   "BB"  : "BB",
    "SO"     : "SO",   "HBP" : "HBP",
    "ERA"    : "ERA",  "WHIP": "WHIP",
}

DEFENSE_MAP = {
    "C"    : "TC",  "PO"   : "PO",  "A" : "A",
    "E"    : "E",   "FLD%" : "FLD%","DP" : "DP",
}

# ── COLUMN LISTS ──────────────────────────────────────────────────────────────
BATTING_COLS  = ["G","GS","AB","R","H","2B","3B","HR","RBI","BB","SO",
                 "HBP","SAC","SF","SB","CS","AVG","OBP","SLG","OPS",
                 "ISO","BABIP","BB_pct","K_pct"]
PITCHING_COLS = ["G","GS","W","L","SV","IP","H","R","ER","BB","SO",
                 "HBP","ERA","WHIP","K_per_9","BB_per_9","K_BB","xFIP",
                 "BB_pct","K_pct"]
DEFENSE_COLS  = ["TC","PO","A","E","FLD%","DP"]

# ── GOOGLE SHEETS ─────────────────────────────────────────────────────────────
def connect():
    creds_dict = json.loads(GOOGLE_CREDS_JSON)
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds  = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds).open_by_key(SPREADSHEET_ID)

def get_players(sheet):
    return sheet.worksheet("Players").get_all_records()

# ── STAT PARSING ──────────────────────────────────────────────────────────────
def split_combined(value, sep="-"):
    parts = value.split(sep, 1)
    if len(parts) > 1:
        return parts[0].strip(), parts[1].strip()
    return "", ""

def align_headers(hdrs, cells):
    if "Player" in hdrs and len(hdrs) == len(cells) + 1:
        hdrs = [h for h in hdrs if h != "Player"]
    return hdrs

def map_row(raw, col_map):
    out = {}
    for src, dest in col_map.items():
        val = raw.get(src, "")
        if isinstance(dest, tuple):
            v1, v2 = split_combined(val)
            out[dest[0]] = v1
            out[dest[1]] = v2
        else:
            out[dest] = val

    # ── Pitching computed ─────────────────────────────────────────────────────
    try:
        ip  = float(out.get("IP",  0))
        bb  = float(out.get("BB",  0))
        so  = float(out.get("SO",  0))
        hbp = float(out.get("HBP", 0))
        h_p = float(out.get("H",   0))
        if ip > 0:
            out["K_per_9"]  = f"{so  / ip * 9:.2f}"
            out["BB_per_9"] = f"{bb  / ip * 9:.2f}"
            out["K_BB"]     = f"{so  / bb:.2f}" if bb > 0 else "—"
            xfip = ((13 * ip / 9) + (3 * (bb + hbp)) - (2 * so)) / ip + 3.10
            out["xFIP"]     = f"{xfip:.2f}"
            bf = ip * 3 + h_p + bb + hbp
            if bf > 0:
                out["BB_pct"] = f"{bb / bf * 100:.1f}"
                out["K_pct"]  = f"{so / bf * 100:.1f}"
    except Exception:
        pass

    # ── Batting computed ──────────────────────────────────────────────────────
    try:
        ab  = float(out.get("AB",  0))
        bb  = float(out.get("BB",  0))
        so  = float(out.get("SO",  0))
        hbp = float(out.get("HBP", 0))
        sac = float(out.get("SAC", 0))
        sf  = float(out.get("SF",  0))
        h   = float(out.get("H",   0))
        hr  = float(out.get("HR",  0))
        slg = float(out.get("SLG", 0))
        avg = float(out.get("AVG", 0))
        if ab > 0:
            pa = ab + bb + hbp + sf + sac
            if pa > 0:
                out["BB_pct"] = f"{bb / pa * 100:.1f}"
                out["K_pct"]  = f"{so / pa * 100:.1f}"
            if slg > 0:
                out["ISO"]    = f"{slg - avg:.3f}"
            denom = ab - so - hr + sf
            if denom > 0 and h >= hr:
                out["BABIP"] = f"{(h - hr) / denom:.3f}"
    except Exception:
        pass

    # ── OPS fallback ──────────────────────────────────────────────────────────
    if not out.get("OPS"):
        try:
            out["OPS"] = f"{float(out.get('OBP','0')) + float(out.get('SLG','0')):.3f}"
        except Exception:
            pass

    return out

def zero_stats(col_map):
    out = {}
    for dest in col_map.values():
        if isinstance(dest, tuple):
            out[dest[0]] = "0"
            out[dest[1]] = "0"
        else:
            out[dest] = "0"
    return out

# ── SCRAPING ──────────────────────────────────────────────────────────────────
def find_table(page, stat_type):
    for table in page.query_selector_all("table"):
        rows = table.query_selector_all("tr")
        if len(rows) < 2:
            continue
        hdrs = [c.inner_text().strip() for c in rows[0].query_selector_all("th, td")]
        if stat_type == "batting"  and "AVG"  in hdrs and "AB"  in hdrs: return table, hdrs
        if stat_type == "pitching" and "ERA"  in hdrs and "IP"  in hdrs: return table, hdrs
        if stat_type == "defense"  and "FLD%" in hdrs and "PO"  in hdrs: return table, hdrs
    return None, []

def scrape(page, player, stat_type):
    jersey = str(player.get("Jersey", "")).strip()
    if not jersey:
        print(f"    ⚠ No jersey number — writing zeros")
        return zero_stats({"batting":BATTING_MAP,"pitching":PITCHING_MAP,"defense":DEFENSE_MAP}[stat_type])

    page.goto(player["Stats_URL"], wait_until="domcontentloaded", timeout=60000)
    time.sleep(5)

    col_map = {"batting":BATTING_MAP,"pitching":PITCHING_MAP,"defense":DEFENSE_MAP}[stat_type]
    table, hdrs = find_table(page, stat_type)
    if not table:
        print(f"    ⚠ No {stat_type} table found — writing zeros")
        return zero_stats(col_map)

    for row in table.query_selector_all("tr")[1:]:
        cells = [c.inner_text().strip() for c in row.query_selector_all("td")]
        if not cells:
            continue
        aligned = align_headers(hdrs, cells)
        if cells[0].strip() == jersey:
            raw = {aligned[i]: cells[i] for i in range(min(len(aligned), len(cells)))}
            return map_row(raw, col_map)

    print(f"    ⚠ Jersey #{jersey} not found — writing zeros")
    return zero_stats(col_map)

# ── SHEET WRITING ──────────────────────────────────────────────────────────────
def write_stats(sheet, tab, player, mapped, target_cols):
    """Update current stats row. Returns previous row for comparison."""
    ws  = sheet.worksheet(tab)
    pid = player["PlayerID"]
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    row = [now, pid, player["Name"], player["School"], player["Division"]]
    row += [str(mapped.get(col, "")) for col in target_cols]
    existing = ws.get_all_records()
    for i, r in enumerate(existing):
        if r.get("PlayerID") == pid:
            ws.update(values=[row], range_name=f"A{i+2}")
            print(f"    ✓ {tab} updated")
            return r          # return old row for comparison
    ws.append_row(row)
    print(f"    ✓ {tab} added")
    return None               # no previous row

def write_history(sheet, tab, player, mapped, target_cols):
    ws  = sheet.worksheet(tab)
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    row = [now, player["PlayerID"], player["Name"], player["School"], player["Division"]]
    row += [str(mapped.get(col, "")) for col in target_cols]
    ws.append_row(row)
    print(f"    ✓ {tab} snapshot saved")

def log(sheet, player, status, notes=""):
    sheet.worksheet("Scrape_Log").append_row([
        datetime.now().strftime("%Y-%m-%d %H:%M"),
        player["PlayerID"], player["Name"], player["School"], status, notes
    ])

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
            "Chrome/120.0.0.0 Safari/537.36"))
        page = context.new_page()

        for player in players:
            print(f"→ {player['Name']} ({player['School']}) Jersey #{player.get('Jersey','')}")
            try:
                if player["Type"] == "Hitter":
                    for st, tab, hist, cols in [
                        ("batting", "Batting", "Batting_History", BATTING_COLS),
                        ("defense", "Defense", "Defense_History", DEFENSE_COLS),
                    ]:
                        s        = scrape(page, player, st)
                        old_row  = write_stats(sheet, tab, player, s, cols)
                        write_history(sheet, hist, player, s, cols)
                        if st == "batting":
                            # First appearance
                            if old_row and is_zero_row(old_row, BATTING_COLS) and not is_zero_row(s, BATTING_COLS):
                                push(
                                    f"⚾ {player['Name']} has arrived!",
                                    f"{player['School']} ({player['Division']})\n"
                                    f"AVG: {s.get('AVG','—')}  OPS: {s.get('OPS','—')}  H: {s.get('H','—')}",
                                    priority=1
                                )
                            # Threshold check
                            check_thresholds(player, s, old_row, "batting")
                else:
                    s       = scrape(page, player, "pitching")
                    old_row = write_stats(sheet, "Pitching", player, s, PITCHING_COLS)
                    write_history(sheet, "Pitching_History", player, s, PITCHING_COLS)
                    # First appearance
                    if old_row and is_zero_row(old_row, PITCHING_COLS) and not is_zero_row(s, PITCHING_COLS):
                        push(
                            f"🥎 {player['Name']} has arrived!",
                            f"{player['School']} ({player['Division']})\n"
                            f"ERA: {s.get('ERA','—')}  IP: {s.get('IP','—')}  K: {s.get('SO','—')}",
                            priority=1
                        )
                    # Threshold check
                    check_thresholds(player, s, old_row, "pitching")
                log(sheet, player, "SUCCESS")
            except Exception as e:
                print(f"    ✗ ERROR: {e}")
                log(sheet, player, "ERROR", str(e))

        browser.close()
    print("\n✓ Done! Check your Google Sheet.")

if __name__ == "__main__":
    main()