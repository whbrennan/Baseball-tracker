import os
import json
import time
import requests
import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright
from datetime import datetime
from zoneinfo import ZoneInfo

# ── CONFIG ────────────────────────────────────────────────────────────────────
SPREADSHEET_ID    = os.environ.get("SPREADSHEET_ID")
GOOGLE_CREDS_JSON = os.environ.get("GOOGLE_CREDENTIALS")
PUSHOVER_USER     = os.environ.get("PUSHOVER_USER_KEY")
PUSHOVER_TOKEN    = os.environ.get("PUSHOVER_API_TOKEN")

UTC = ZoneInfo("UTC")
EASTERN = ZoneInfo("America/New_York")

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
    "C"    : "TC",
    "PO"   : "PO",
    "A"    : "A",
    "E"    : "E",
    "FLD%" : "FLD%",
    "DP"   : "DP",
    "SBA"  : "SBA",
    "CSB"  : "CSB",
    "PB"   : "PB",
    "CI"   : "CI",
}

# ── COLUMN LISTS ──────────────────────────────────────────────────────────────
BATTING_COLS  = ["G","GS","AB","R","H","2B","3B","HR","RBI","BB","SO",
                 "HBP","SAC","SF","SB","CS","AVG","OBP","SLG","OPS",
                 "ISO","BABIP","BB_pct","K_pct"]
PITCHING_COLS = ["G","GS","W","L","SV","IP","H","R","ER","BB","SO",
                 "HBP","ERA","WHIP","K_per_9","BB_per_9","K_BB","xFIP",
                 "BB_pct","K_pct"]
DEFENSE_COLS  = ["TC","PO","A","E","FLD%","DP","SBA","CSB","PB","CI"]

# ── THRESHOLD ALERTS ──────────────────────────────────────────────────────────
BATTING_THRESHOLDS = {
    "HR" : (">=", 1),
    "RBI": (">=", 3),
    "AVG": (">=", 0.400),
    "OPS": (">=", 1.000),
}

PITCHING_THRESHOLDS = {
    "SO" : (">=", 10),
    "ERA": ("<=", 1.00),
    "IP" : (">=", 7.0),
}

# ── NOTIFICATIONS ─────────────────────────────────────────────────────────────
def push(title, message, priority=0):
    """Send a Pushover notification. Silently skips if credentials are missing."""
    if not PUSHOVER_USER or not PUSHOVER_TOKEN:
        print(f"    [Pushover skipped] {title}: {message}")
        return
    try:
        requests.post(
            "https://api.pushover.net/1/messages.json",
            data={
                "token"   : PUSHOVER_TOKEN,
                "user"    : PUSHOVER_USER,
                "title"   : title,
                "message" : message,
                "priority": priority,
            },
            timeout=10,
        )
    except Exception as e:
        print(f"    Warning: Pushover error: {e}")


def _threshold_triggered(new_val, old_val, op, cutoff):
    """Return True if new_val newly crosses cutoff (old_val did not)."""
    try:
        nv = float(new_val) if new_val not in ("", "-", None) else None
        ov = float(old_val) if old_val not in ("", "-", None) else None
        if nv is None:
            return False
        meets_now  = (nv >= cutoff) if op == ">=" else (nv <= cutoff)
        met_before = ((ov >= cutoff) if op == ">=" else (ov <= cutoff)) if ov is not None else False
        return meets_now and not met_before
    except (ValueError, TypeError):
        return False


def check_thresholds(player, new_stats, old_row, stat_type):
    """Fire a Pushover alert when a player newly crosses a notable threshold."""
    if old_row is None:
        return
    thresholds = BATTING_THRESHOLDS if stat_type == "batting" else PITCHING_THRESHOLDS
    for stat, (op, cutoff) in thresholds.items():
        new_val = new_stats.get(stat, "")
        old_val = old_row.get(stat, "")
        if _threshold_triggered(new_val, old_val, op, cutoff):
            push(
                f"ALERT: {player['Name']} -- {stat} alert",
                f"{player['School']} ({player['Division']})\n"
                f"{stat}: {new_val} (was {old_val or '--'})",
                priority=0,
            )

# ── GOOGLE SHEETS ─────────────────────────────────────────────────────────────
def connect():
    creds_dict = json.loads(GOOGLE_CREDS_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
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
    if "PLAYER" in hdrs and len(hdrs) == len(cells) + 1:
        hdrs = [h for h in hdrs if h != "PLAYER"]
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
            out["K_BB"]     = f"{so  / bb:.2f}" if bb > 0 else "--"
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


def is_zero_row(mapped, cols):
    """Return True if every tracked stat is zero or empty."""
    return all(not mapped.get(c) or str(mapped.get(c)) in ("0", "", "--") for c in cols)


def current_timestamps():
    now_utc = datetime.now(UTC)
    now_et = now_utc.astimezone(EASTERN)
    return {
        "last_updated_et": now_et.strftime("%Y-%m-%d %H:%M"),
        "scraped_at_utc": now_utc.strftime("%Y-%m-%d %H:%M"),
        "business_date_et": now_et.strftime("%Y-%m-%d"),
    }


BASE_FIELDS = ["Last_Updated", "PlayerID", "Name", "School", "Division"]
HISTORY_PREFIX_FIELDS = ["Last_Updated", "Scraped_At_UTC", "Business_Date_ET", "PlayerID", "Name", "School", "Division"]

HEADER_ALIASES = {
    "lastupdated": "Last_Updated",
    "lastupdatedet": "Last_Updated",
    "lastupdatedeastern": "Last_Updated",
    "scrapedatutc": "Scraped_At_UTC",
    "scrapedat": "Scraped_At_UTC",
    "scrapedtimeutc": "Scraped_At_UTC",
    "businessdateet": "Business_Date_ET",
    "businessdate": "Business_Date_ET",
    "gamedateet": "Business_Date_ET",
    "playerid": "PlayerID",
    "player": "PlayerID",
    "playername": "Name",
    "schoolname": "School",
}


def normalize_header(text):
    return "".join(ch.lower() for ch in str(text or "") if ch.isalnum())


def canonicalize_header(header):
    text = str(header or "").strip()
    if not text:
        return ""
    normalized = normalize_header(text)
    return HEADER_ALIASES.get(normalized, text)


def get_headers(ws):
    return [h.strip() for h in ws.row_values(1)]


def get_expected_headers(target_cols, history=False):
    return (HISTORY_PREFIX_FIELDS if history else BASE_FIELDS) + target_cols


def get_effective_headers(headers, target_cols, history=False):
    effective = [canonicalize_header(header) for header in headers]
    expected = get_expected_headers(target_cols, history=history)

    # Fallback for sheets where inserted columns have blank or unrecognized labels.
    if len(effective) >= len(expected):
        for index, expected_header in enumerate(expected):
            if not effective[index]:
                effective[index] = expected_header

    return effective


def build_row(headers, payload, target_cols, history=False):
    effective_headers = get_effective_headers(headers, target_cols, history=history)
    return [str(payload.get(header, "")) for header in effective_headers]


def looks_like_player_id(value):
    text = str(value or "").strip()
    return bool(text) and " " not in text and ":" not in text and text.count("-") <= 1


def looks_like_datetime(value):
    text = str(value or "").strip()
    if not text:
        return False
    try:
        datetime.strptime(text, "%Y-%m-%d %H:%M")
        return True
    except ValueError:
        return False


def looks_like_date(value):
    text = str(value or "").strip()
    if not text:
        return False
    try:
        datetime.strptime(text, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def to_scraped_at_utc(last_updated_et):
    text = str(last_updated_et or "").strip()
    if not text:
        return ""
    try:
        dt_et = datetime.strptime(text, "%Y-%m-%d %H:%M").replace(tzinfo=EASTERN)
        return dt_et.astimezone(UTC).strftime("%Y-%m-%d %H:%M")
    except ValueError:
        return ""


def build_current_payload(player, mapped, target_cols):
    ts = current_timestamps()
    payload = {
        "Last_Updated": ts["last_updated_et"],
        "PlayerID": player["PlayerID"],
        "Name": player["Name"],
        "School": player["School"],
        "Division": player["Division"],
    }
    for col in target_cols:
        payload[col] = str(mapped.get(col, ""))
    return payload


def build_history_payload(player, mapped, target_cols):
    ts = current_timestamps()
    payload = build_current_payload(player, mapped, target_cols)
    payload["Scraped_At_UTC"] = ts["scraped_at_utc"]
    payload["Business_Date_ET"] = ts["business_date_et"]
    return payload


def history_row_needs_repair(row_map):
    if not row_map:
        return False
    scraped_at = row_map.get("Scraped_At_UTC", "")
    player_id = row_map.get("PlayerID", "")
    business_date = row_map.get("Business_Date_ET", "")
    return (
        looks_like_player_id(scraped_at)
        and not looks_like_datetime(scraped_at)
        and not looks_like_player_id(player_id)
        and not looks_like_date(business_date)
    )


def repair_history_table(sheet, tab, target_cols):
    ws = sheet.worksheet(tab)
    headers = get_headers(ws)
    effective_headers = get_effective_headers(headers, target_cols, history=True)
    print(f"    {tab} headers: {headers}")
    print(f"    {tab} mapped headers: {effective_headers}")
    legacy_headers = BASE_FIELDS + target_cols
    all_values = ws.get_all_values()
    repairs = []

    for row_index, raw_row in enumerate(all_values[1:], start=2):
        padded_row = raw_row + [""] * max(0, len(effective_headers) - len(raw_row))
        row_map = {effective_headers[i]: padded_row[i] for i in range(len(effective_headers))}
        if not history_row_needs_repair(row_map):
            continue

        legacy_values = padded_row[:len(legacy_headers)]
        legacy_map = {
            legacy_headers[i]: legacy_values[i] if i < len(legacy_values) else ""
            for i in range(len(legacy_headers))
        }
        repaired_payload = {
            "Last_Updated": legacy_map.get("Last_Updated", ""),
            "Scraped_At_UTC": to_scraped_at_utc(legacy_map.get("Last_Updated", "")),
            "Business_Date_ET": str(legacy_map.get("Last_Updated", "")).split(" ")[0],
            "PlayerID": legacy_map.get("PlayerID", ""),
            "Name": legacy_map.get("Name", ""),
            "School": legacy_map.get("School", ""),
            "Division": legacy_map.get("Division", ""),
        }
        for col in target_cols:
            repaired_payload[col] = legacy_map.get(col, "")

        repaired_row = build_row(headers, repaired_payload, target_cols, history=True)
        repairs.append({
            "range": f"A{row_index}",
            "values": [repaired_row],
        })

    if repairs:
        ws.batch_update(repairs)
        print(f"    OK: {tab} repaired {len(repairs)} misaligned row(s)")
    return len(repairs)


def stats_changed(previous_row, mapped, target_cols):
    """Return True when any tracked stat differs from the previous row."""
    if previous_row is None:
        return True
    def normalize(value):
        text = str(value if value is not None else "").strip()
        if text in ("", "--"):
            return text
        try:
            num = float(text)
            if num.is_integer():
                return str(int(num))
            return f"{num:.6f}".rstrip("0").rstrip(".")
        except (ValueError, TypeError):
            return text
    for col in target_cols:
        if normalize(previous_row.get(col, "")) != normalize(mapped.get(col, "")):
            return True
    return False

# ── SCRAPING ──────────────────────────────────────────────────────────────────
def find_table(page, stat_type):
    for table in page.query_selector_all("table"):
        rows = table.query_selector_all("tr")
        if len(rows) < 2:
            continue
        hdrs = [c.inner_text().strip().upper() for c in rows[0].query_selector_all("th, td")]
        if stat_type == "batting"  and "AVG" in hdrs and "AB"  in hdrs: return table, hdrs
        if stat_type == "pitching" and "ERA" in hdrs and "IP"  in hdrs: return table, hdrs
        if stat_type == "defense"  and "FLD%" in hdrs and "PO" in hdrs: return table, hdrs
    return None, []


def scrape(page, player, stat_type):
    jersey = str(player.get("Jersey", "")).strip()
    if not jersey:
        print(f"    Warning: No jersey number -- writing zeros")
        return zero_stats({"batting": BATTING_MAP, "pitching": PITCHING_MAP, "defense": DEFENSE_MAP}[stat_type])

    page.goto(player["Stats_URL"], wait_until="domcontentloaded", timeout=60000)
    time.sleep(5)

    # ── Tab navigation for SIDEARM sites ─────────────────────────────────────
    if stat_type == "pitching":
        if stat_type == "pitching":
            if "gwsports.com" in player["Stats_URL"]:  # ← add this line
                try:
                    trigger = page.locator("button.s-select-box", has_text="Batting").first
                    trigger.click(timeout=5000)
                    time.sleep(1)
                    pitch_option = page.locator("li.s-select__option-item", has_text="Pitching").first
                    pitch_option.click(timeout=5000)
                    time.sleep(3)
                except Exception as e:
                    print(f"    Warning: Pitching tab click failed: {e}")

    if stat_type == "defense":
        if "gwsports.com" in player["Stats_URL"] or "ecupirates.com" in player["Stats_URL"]:
            try:
                trigger = page.locator("button.s-select-box", has_text="Batting").first
                trigger.click(timeout=5000)
                time.sleep(1)
                fielding = page.locator("li.s-select__option-item", has_text="Fielding").first
                fielding.click(timeout=5000)
                time.sleep(2)
            except Exception:
                pass

    col_map = {"batting": BATTING_MAP, "pitching": PITCHING_MAP, "defense": DEFENSE_MAP}[stat_type]
    table, hdrs = find_table(page, stat_type)
    if not table:
        print(f"    Warning: No {stat_type} table found -- writing zeros")
        return zero_stats(col_map)

    hdrs = [h.upper() for h in hdrs]

    for row in table.query_selector_all("tr")[1:]:
        cells = [c.inner_text().strip() for c in row.query_selector_all("td")]
        if not cells:
            continue
        aligned = align_headers(hdrs, cells)
        if cells[0].strip() == jersey:
            raw = {aligned[i]: cells[i] for i in range(min(len(aligned), len(cells)))}
            return map_row(raw, col_map)

    print(f"    Warning: Jersey #{jersey} not found -- writing zeros")
    return zero_stats(col_map)

# ── SHEET WRITING ─────────────────────────────────────────────────────────────
def write_stats(sheet, tab, player, mapped, target_cols):
    """Update current stats row. Returns previous row for comparison."""
    ws  = sheet.worksheet(tab)
    pid = player["PlayerID"]
    headers = get_headers(ws)
    row = build_row(headers, build_current_payload(player, mapped, target_cols), target_cols, history=False)
    existing = ws.get_all_records()
    for i, r in enumerate(existing):
        record = {canonicalize_header(k): v for k, v in r.items()}
        if record.get("PlayerID") == pid:
            ws.update(values=[row], range_name=f"A{i+2}")
            print(f"    OK: {tab} updated")
            return record
    ws.append_row(row)
    print(f"    OK: {tab} added")
    return None


def write_history(sheet, tab, player, mapped, target_cols, previous_row=None):
    ws  = sheet.worksheet(tab)
    if not stats_changed(previous_row, mapped, target_cols):
        print(f"    OK: {tab} unchanged -- snapshot skipped")
        return False
    headers = get_headers(ws)
    row = build_row(headers, build_history_payload(player, mapped, target_cols), target_cols, history=True)
    ws.append_row(row)
    print(f"    OK: {tab} snapshot saved")
    return True


def log(sheet, player, status, notes=""):
    ts = current_timestamps()
    sheet.worksheet("Scrape_Log").append_row([
        ts["last_updated_et"],
        player["PlayerID"], player["Name"], player["School"], status, notes,
    ])

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main(test_player_id=None):
    print("Connecting to Google Sheets...")
    sheet   = connect()
    repair_history_table(sheet, "Batting_History", BATTING_COLS)
    repair_history_table(sheet, "Pitching_History", PITCHING_COLS)
    repair_history_table(sheet, "Defense_History", DEFENSE_COLS)
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
            print(f"-> {player['Name']} ({player['School']}) Jersey #{player.get('Jersey','')}")
            try:
                if player["Type"] == "Hitter":
                    for st, tab, hist, cols in [
                        ("batting", "Batting",  "Batting_History",  BATTING_COLS),
                        ("defense", "Defense",  "Defense_History",  DEFENSE_COLS),
                    ]:
                        s       = scrape(page, player, st)
                        old_row = write_stats(sheet, tab, player, s, cols)
                        write_history(sheet, hist, player, s, cols, old_row)
                        if st == "batting":
                            if (old_row is None or is_zero_row(old_row, BATTING_COLS)) and not is_zero_row(s, BATTING_COLS):
                                push(
                                    f"Player {player['Name']} has arrived!",
                                    f"{player['School']} ({player['Division']})\n"
                                    f"AVG: {s.get('AVG','--')}  OPS: {s.get('OPS','--')}  H: {s.get('H','--')}",
                                    priority=1,
                                )
                            check_thresholds(player, s, old_row, "batting")
                else:
                    s       = scrape(page, player, "pitching")
                    old_row = write_stats(sheet, "Pitching", player, s, PITCHING_COLS)
                    write_history(sheet, "Pitching_History", player, s, PITCHING_COLS, old_row)
                    if (old_row is None or is_zero_row(old_row, PITCHING_COLS)) and not is_zero_row(s, PITCHING_COLS):
                        push(
                            f"Pitcher {player['Name']} has arrived!",
                            f"{player['School']} ({player['Division']})\n"
                            f"ERA: {s.get('ERA','--')}  IP: {s.get('IP','--')}  K: {s.get('SO','--')}",
                            priority=1,
                        )
                    check_thresholds(player, s, old_row, "pitching")
                log(sheet, player, "SUCCESS")
            except Exception as e:
                print(f"    ERROR: {e}")
                log(sheet, player, "ERROR", str(e))

        browser.close()
    print("\nDone! Check your Google Sheet.")


if __name__ == "__main__":
    test_player_id = os.environ.get("TEST_PLAYER_ID")
    if not test_player_id and len(os.sys.argv) > 1:
        test_player_id = os.sys.argv[1]
    main(test_player_id=test_player_id)
