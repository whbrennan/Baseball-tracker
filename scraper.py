import os
import re
import sys
import json
import time
import requests
import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright
from datetime import datetime
from zoneinfo import ZoneInfo

from run_status import record_run

# ── CONFIG ────────────────────────────────────────────────────────────────────
SPREADSHEET_ID    = os.environ.get("SPREADSHEET_ID")
GOOGLE_CREDS_JSON = os.environ.get("GOOGLE_CREDENTIALS")
PUSHOVER_USER     = os.environ.get("PUSHOVER_USER_KEY")
PUSHOVER_TOKEN    = os.environ.get("PUSHOVER_API_TOKEN")

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

UTC = ZoneInfo("UTC")
EASTERN = ZoneInfo("America/New_York")

BATTING_MAP = {
    "GP-GS"  : ("G", "GS"),
    "AB"     : "AB",   "R"   : "R",    "H"   : "H",
    "2B"     : "2B",   "3B"  : "3B",   "HR"  : "HR",
    "RBI"    : "RBI",  "BB"  : "BB",   "SO"  : "SO",
    "HBP"    : "HBP",  "SH"  : "SAC",  "SF"  : "SF",
    # Stolen bases are shown as steals-attempts ("12-14"). CS is derived from
    # them in map_row: caught stealing = attempts - steals.
    "SB-ATT" : ("SB", "SB_ATT"),
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

# ── FAILURE HANDLING ──────────────────────────────────────────────────────────
class ScrapeError(Exception):
    """A stats page couldn't be read reliably. Nothing is written for that stat type."""


# Season-cumulative stats that can only go up during a season. If a scrape
# returns lower values than the sheet already holds, the scrape is wrong
# (wrong jersey, page not loaded, wrong table) and must not overwrite good data.
COUNTING_STATS = {
    "batting" : ["G", "AB", "H", "BB", "SO"],
    "pitching": ["G", "IP", "SO", "BB"],
    "defense" : ["TC", "PO", "A"],
}


def _to_number(value):
    text = str(value if value is not None else "").strip()
    if text in ("", "-", "--"):
        return None
    try:
        return float(text)
    except ValueError:
        return None


def same_season(previous_row):
    """True if the existing sheet row was written during the current season.

    College seasons run Feb-June, so the calendar year of Last_Updated
    identifies the season. Across a season rollover stats legitimately reset.
    """
    last_updated = str((previous_row or {}).get("Last_Updated", "")).strip()
    return last_updated[:4] == str(datetime.now(EASTERN).year)


def stats_went_backwards(previous_row, mapped, stat_type):
    """Return [(stat, old, new)] for counting stats that decreased this season."""
    if not previous_row or not same_season(previous_row):
        return []
    drops = []
    for col in COUNTING_STATS[stat_type]:
        old = _to_number(previous_row.get(col))
        new = _to_number(mapped.get(col))
        if old is not None and new is not None and new < old:
            drops.append((col, previous_row.get(col), mapped.get(col)))
    return drops

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


def innings_to_outs(value):
    """Convert innings pitched as written ("45.2" = 45 and 2/3) to outs, or None."""
    text = str(value if value is not None else "").strip()
    if not text:
        return None
    whole, _, frac = text.partition(".")
    try:
        innings = int(whole or "0")
        outs = int(frac[:1] or "0")
    except ValueError:
        return None
    return innings * 3 + min(outs, 2)


def innings_value(value):
    """Innings pitched as a true number for rate stats ("45.2" -> 45.667)."""
    outs = innings_to_outs(value)
    return outs / 3 if outs is not None else 0.0


def caught_stealing(sb, attempts):
    """CS from steals and attempts as strings; "" when either is missing."""
    try:
        return str(max(int(attempts) - int(sb), 0))
    except (TypeError, ValueError):
        return ""


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

    if "SB_ATT" in out:
        out["CS"] = caught_stealing(out.get("SB"), out.pop("SB_ATT"))

    # ── Pitching computed ─────────────────────────────────────────────────────
    try:
        ip  = innings_value(out.get("IP", ""))
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
    if "SB_ATT" in out:
        out["CS"] = caught_stealing(out["SB"], out.pop("SB_ATT"))
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
KNOWN_DIVISIONS = {"D1", "D2", "D3", "NJCAA"}

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


def normalize_school_name(value):
    return " ".join(str(value or "").split()).strip().lower()


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
    return (
        bool(text)
        and " " not in text
        and ":" not in text
        and text.count("-") <= 1
        and any(ch.isdigit() for ch in text)
    )


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


def history_row_missing_metadata(row_map):
    if not row_map:
        return False
    last_updated = row_map.get("Last_Updated", "")
    player_id = row_map.get("PlayerID", "")
    scraped_at = row_map.get("Scraped_At_UTC", "")
    business_date = row_map.get("Business_Date_ET", "")
    return (
        looks_like_datetime(last_updated)
        and looks_like_player_id(player_id)
        and (not looks_like_datetime(scraped_at) or not looks_like_date(business_date))
    )


def history_row_missing_player_fields(row_map):
    if not row_map:
        return False
    return (
        looks_like_datetime(row_map.get("Scraped_At_UTC", ""))
        and looks_like_date(row_map.get("Business_Date_ET", ""))
        and not looks_like_player_id(row_map.get("PlayerID", ""))
        and str(row_map.get("Name", "")).strip() in KNOWN_DIVISIONS
    )


def build_player_lookup(players, stat_type):
    lookup = {}
    for player in players:
        player_type = str(player.get("Type", "")).strip()
        matches_type = (
            (stat_type == "pitching" and player_type == "Pitcher")
            or (stat_type in ("batting", "defense") and player_type == "Hitter")
        )
        if not matches_type:
            continue
        key = (
            normalize_school_name(player.get("School", "")),
            str(player.get("Division", "")).strip(),
        )
        if key in lookup:
            lookup[key] = None
        else:
            lookup[key] = player
    return lookup


def repair_history_table(sheet, tab, target_cols, players):
    ws = sheet.worksheet(tab)
    headers = get_headers(ws)
    effective_headers = get_effective_headers(headers, target_cols, history=True)
    print(f"    {tab} headers: {headers}")
    print(f"    {tab} mapped headers: {effective_headers}")
    legacy_headers = BASE_FIELDS + target_cols
    all_values = ws.get_all_values()
    all_records = ws.get_all_records()
    stat_type = "pitching" if tab == "Pitching_History" else "defense" if tab == "Defense_History" else "batting"
    player_lookup = build_player_lookup(players, stat_type)
    repairs = []

    for offset, raw_row in enumerate(all_values[1:]):
        row_index = offset + 2
        padded_row = raw_row + [""] * max(0, len(effective_headers) - len(raw_row))
        row_map = {effective_headers[i]: padded_row[i] for i in range(len(effective_headers))}
        record_map = {}
        if offset < len(all_records):
            record_map = {canonicalize_header(k): v for k, v in all_records[offset].items()}
        needs_shift_repair = history_row_needs_repair(row_map)
        needs_metadata_repair = history_row_missing_metadata(record_map)
        needs_player_field_repair = history_row_missing_player_fields(record_map)
        if not needs_shift_repair and not needs_metadata_repair and not needs_player_field_repair:
            continue

        if needs_shift_repair:
            legacy_values = padded_row[:len(legacy_headers)]
            source_map = {
                legacy_headers[i]: legacy_values[i] if i < len(legacy_values) else ""
                for i in range(len(legacy_headers))
            }
        elif needs_player_field_repair:
            lookup_key = (
                normalize_school_name(record_map.get("PlayerID", "")),
                str(record_map.get("Name", "")).strip(),
            )
            player = player_lookup.get(lookup_key)
            if not player:
                continue
            source_map = {
                "Last_Updated": record_map.get("Last_Updated", ""),
                "Scraped_At_UTC": record_map.get("Scraped_At_UTC", ""),
                "Business_Date_ET": record_map.get("Business_Date_ET", ""),
            }
            source_map["PlayerID"] = player.get("PlayerID", "")
            source_map["Name"] = player.get("Name", "")
            source_map["School"] = raw_row[3] if len(raw_row) > 3 else player.get("School", "")
            source_map["Division"] = raw_row[4] if len(raw_row) > 4 else player.get("Division", "")
            stat_values = raw_row[5:]
            for index, col in enumerate(target_cols):
                source_map[col] = stat_values[index] if index < len(stat_values) else ""
        else:
            source_map = record_map

        repaired_payload = {
            "Last_Updated": source_map.get("Last_Updated", ""),
            "Scraped_At_UTC": to_scraped_at_utc(source_map.get("Last_Updated", "")),
            "Business_Date_ET": str(source_map.get("Last_Updated", "")).split(" ")[0],
            "PlayerID": source_map.get("PlayerID", ""),
            "Name": source_map.get("Name", ""),
            "School": source_map.get("School", ""),
            "Division": source_map.get("Division", ""),
        }
        for col in target_cols:
            repaired_payload[col] = source_map.get(col, "")

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
# Every <table> on the page as a list of rows of cell text. The first row keeps
# th and td cells (the header); later rows keep only td cells, so a player-name
# <th> is dropped and align_headers() lines the headers back up.
_TABLES_JS = """els => els.map(t => [...t.querySelectorAll('tr')].map((r, i) =>
    [...r.querySelectorAll(i === 0 ? 'th, td' : 'td')].map(c => (c.innerText || '').trim())))"""


def read_tables(page):
    """Fetch-side: every table on the loaded page, for the parsers below."""
    return page.eval_on_selector_all("table", _TABLES_JS)


def find_table(tables, stat_type):
    """Return (rows, upper-cased headers) of the first table for stat_type, or (None, [])."""
    for rows in tables:
        if len(rows) < 2:
            continue
        hdrs = [h.strip().upper() for h in rows[0]]
        if stat_type == "batting"  and "AVG" in hdrs and "AB"  in hdrs: return rows, hdrs
        if stat_type == "pitching" and "ERA" in hdrs and "IP"  in hdrs: return rows, hdrs
        if stat_type == "defense"  and "FLD%" in hdrs and "PO" in hdrs: return rows, hdrs
    return None, []


def player_from_table(rows, hdrs, jersey):
    """Return the raw {header: cell} dict for jersey from a stats table, or None."""
    for cells in rows[1:]:
        if not cells:
            continue
        aligned = align_headers(hdrs, cells)
        if cells[0].strip() == jersey:
            return {aligned[i]: cells[i] for i in range(min(len(aligned), len(cells)))}
    return None


# Newer Sidearm stats pages show only one table at a time (chosen from a
# dropdown) but embed every player's season line in the page's Nuxt data.
# Overall stats live under "individualStats" (conference under
# "individualStatsConference"), one list per stat type.
EMBEDDED_LISTS = {
    "pitching": "individualPitchingStats",
    "defense" : "individualFieldingStats",
}


_SCRIPT_RE = re.compile(r"<script[^>]*>(.*?)</script>", re.S | re.I)
_HAS_PAYLOAD_JS = "() => [...document.scripts].some(s => (s.textContent || '').includes('ShallowReactive'))"


def _embedded_payload(page):
    """Return the page's Nuxt flat reference array, or None.

    Reads the live DOM first. On some first loads the site's own scripts are
    still rewriting the page and the data isn't visible there, so fall back to
    the server's original HTML, which always contains it.
    """
    for attempt in range(2):
        try:
            page.wait_for_function(_HAS_PAYLOAD_JS, timeout=15000)
        except Exception:
            pass
        payload = _parse_payload(
            page.eval_on_selector_all("script", "els => els.map(e => e.textContent || '')"))
        if payload is not None:
            return payload
        if attempt == 0:
            print("    Note: embedded stats not on page yet -- reloading once")
            try:
                page.reload(wait_until="domcontentloaded", timeout=60000)
            except Exception as e:
                print(f"    Warning: reload failed: {e}")
    try:
        cookies = {c["name"]: c["value"] for c in page.context.cookies(page.url)}
        resp = requests.get(page.url, headers={"User-Agent": BROWSER_UA}, cookies=cookies, timeout=30)
        resp.raise_for_status()
        return _parse_payload(_SCRIPT_RE.findall(resp.text))
    except Exception as e:
        print(f"    Warning: could not fetch raw page for embedded stats: {e}")
        return None


def _parse_payload(scripts):
    for txt in scripts:
        if "ShallowReactive" not in txt:
            continue
        start = txt.find("[[")
        if start == -1:
            start = txt.find("[{")
        if start == -1:
            continue
        try:
            return json.loads(txt[start:])
        except ValueError:
            continue
    return None


def _embedded_raw(stat_type, rec):
    """Convert an embedded player record to the stats-table header names map_row expects."""
    def g(key):
        return str(rec.get(key, "") or "")
    if stat_type == "defense":
        return {
            "C": g("totalChances"), "PO": g("putouts"), "A": g("assists"), "E": g("errors"),
            "FLD%": g("fieldingPercentage"), "DP": g("doublePlays"), "SBA": g("stolenBasesAgainst"),
            "CSB": g("caughtStealingBy"), "PB": g("passedBalls"), "CI": g("catchersInterference"),
        }
    return {
        "APP-GS": f"{g('appearances')}-{g('gamesStarted')}", "W-L": f"{g('wins')}-{g('losses')}",
        "SV": g("saves"), "IP": g("inningsPitched"), "H": g("hitsAllowed"), "R": g("runsAllowed"),
        "ER": g("earnedRunsAllowed"), "BB": g("walksAllowed"), "SO": g("strikeouts"),
        "HBP": g("hitBatters"), "ERA": g("earnedRunAverage"), "WHIP": g("whip"),
    }


def embedded_player_stats(page, stat_type, jersey):
    """Read one player's overall season line from the page's embedded data.

    Returns the raw stats dict, {} if the stats list exists but the jersey isn't
    in it, or None if the page has no embedded stats for this stat type.
    """
    if stat_type not in EMBEDDED_LISTS:
        return None
    return embedded_stats_from_payload(_embedded_payload(page), stat_type, jersey)


def embedded_stats_from_payload(raw, stat_type, jersey):
    """Parse-side of embedded_player_stats: same return values, from the payload array."""
    list_key = EMBEDDED_LISTS.get(stat_type)
    if raw is None or not list_key:
        return None

    def res(v):
        if isinstance(v, int):
            return raw[v] if 0 <= v < len(raw) else ""  # negative = undefined/null
        return v

    for obj in raw:
        if not (isinstance(obj, dict) and "individualStats" in obj):
            continue
        container = res(obj["individualStats"])
        players = res(container.get(list_key)) if isinstance(container, dict) else None
        if not isinstance(players, list):
            continue
        for ref in players:
            rec = res(ref)
            if not isinstance(rec, dict):
                continue
            rec = {k: res(v) for k, v in rec.items()}
            if str(rec.get("playerUniform", "")).strip() == jersey:
                return _embedded_raw(stat_type, rec)
        return {}
    return None


def scrape(page, player, stat_type):
    """Return (stats, note) for one stat type.

    Raises ScrapeError when the page can't be read (no jersey configured, no
    stats table found). A player missing from a table that loaded fine is
    normal before their first appearance, so that returns zeros with a note;
    main() refuses the zeros if the player already has stats this season.
    """
    jersey = str(player.get("Jersey", "")).strip()
    if not jersey:
        raise ScrapeError("No jersey number in the Players sheet")

    page.goto(player["Stats_URL"], wait_until="domcontentloaded", timeout=60000)
    time.sleep(5)

    # ── Tab navigation for SIDEARM sites ─────────────────────────────────────
    # Known dropdown-layout sites: try the dropdown first. Any other site (or a
    # failed click) falls back to the page's embedded data below.
    if stat_type == "pitching":
        if "gwsports.com" in player["Stats_URL"]:
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
    table, hdrs = find_table(read_tables(page), stat_type)
    if not table:
        embedded = embedded_player_stats(page, stat_type, jersey)
        if embedded is None:
            raise ScrapeError(f"No {stat_type} table or embedded stats found at {player['Stats_URL']}")
        if embedded:
            print(f"    Note: {stat_type} read from the page's embedded data")
            return map_row(embedded, col_map), ""
        note = f"Jersey #{jersey} not listed in the {stat_type} stats"
        print(f"    Note: {note} -- treating as no appearances yet")
        return zero_stats(col_map), note

    raw = player_from_table(table, hdrs, jersey)
    if raw is not None:
        return map_row(raw, col_map), ""

    note = f"Jersey #{jersey} not listed in the {stat_type} table"
    print(f"    Note: {note} -- treating as no appearances yet")
    return zero_stats(col_map), note

# ── SHEET WRITING ─────────────────────────────────────────────────────────────
def read_current_row(ws, pid):
    """Return (sheet_row_number, record) for a player's current row, or (None, None)."""
    for i, r in enumerate(ws.get_all_records()):
        record = {canonicalize_header(k): v for k, v in r.items()}
        if record.get("PlayerID") == pid:
            return i + 2, record
    return None, None


def write_stats(ws, row_number, player, mapped, target_cols):
    """Overwrite the player's current stats row, or append one if new."""
    headers = get_headers(ws)
    row = build_row(headers, build_current_payload(player, mapped, target_cols), target_cols, history=False)
    if row_number:
        ws.update(values=[row], range_name=f"A{row_number}")
        print(f"    OK: {ws.title} updated")
    else:
        ws.append_row(row)
        print(f"    OK: {ws.title} added")


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
    """Append to Scrape_Log. Best-effort: a logging failure mustn't stop the run."""
    ts = current_timestamps()
    try:
        sheet.worksheet("Scrape_Log").append_row([
            ts["last_updated_et"],
            player["PlayerID"], player["Name"], player["School"], status, notes,
        ])
    except Exception as e:
        print(f"    Warning: could not write Scrape_Log: {e}")

# ── PER-STAT PROCESSING ───────────────────────────────────────────────────────
STAT_JOBS = {
    # stat_type: (current tab, history tab, columns)
    "batting" : ("Batting",  "Batting_History",  BATTING_COLS),
    "defense" : ("Defense",  "Defense_History",  DEFENSE_COLS),
    "pitching": ("Pitching", "Pitching_History", PITCHING_COLS),
}


def stat_types_for(player):
    return ["batting", "defense"] if player["Type"] == "Hitter" else ["pitching"]


def process_stat(sheet, page, player, stat_type):
    """Scrape one stat type and write it, or raise without writing anything."""
    tab, hist, cols = STAT_JOBS[stat_type]
    stats, note = scrape(page, player, stat_type)

    ws = sheet.worksheet(tab)
    row_number, old_row = read_current_row(ws, player["PlayerID"])

    drops = stats_went_backwards(old_row, stats, stat_type)
    if drops:
        detail = ", ".join(f"{col} {old} -> {new}" for col, old, new in drops)
        reason = f" ({note})" if note else ""
        raise ScrapeError(
            f"{stat_type} stats went down this season: {detail}{reason}. "
            "Kept the existing row; check the jersey number and stats URL."
        )

    write_stats(ws, row_number, player, stats, cols)
    write_history(sheet, hist, player, stats, cols, old_row)

    if stat_type == "batting":
        if (old_row is None or is_zero_row(old_row, BATTING_COLS)) and not is_zero_row(stats, BATTING_COLS):
            push(
                f"Player {player['Name']} has arrived!",
                f"{player['School']} ({player['Division']})\n"
                f"AVG: {stats.get('AVG','--')}  OPS: {stats.get('OPS','--')}  H: {stats.get('H','--')}",
                priority=1,
            )
        check_thresholds(player, stats, old_row, "batting")
    elif stat_type == "pitching":
        if (old_row is None or is_zero_row(old_row, PITCHING_COLS)) and not is_zero_row(stats, PITCHING_COLS):
            push(
                f"Pitcher {player['Name']} has arrived!",
                f"{player['School']} ({player['Division']})\n"
                f"ERA: {stats.get('ERA','--')}  IP: {stats.get('IP','--')}  K: {stats.get('SO','--')}",
                priority=1,
            )
        check_thresholds(player, stats, old_row, "pitching")

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main(test_player_id=None):
    """Scrape every player. Returns True if the run failed overall."""
    print("Connecting to Google Sheets...")
    sheet   = connect()
    players = get_players(sheet)
    repair_history_table(sheet, "Batting_History", BATTING_COLS, players)
    repair_history_table(sheet, "Pitching_History", PITCHING_COLS, players)
    repair_history_table(sheet, "Defense_History", DEFENSE_COLS, players)
    if test_player_id:
        players = [p for p in players if p["PlayerID"] == test_player_id]
        if not players:
            print(f"Player ID '{test_player_id}' not found.")
            return True
    print(f"Processing {len(players)} player(s)...\n")

    ok_players, failed_players = [], []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(user_agent=BROWSER_UA)
        page = context.new_page()

        for player in players:
            print(f"-> {player['Name']} ({player['School']}) Jersey #{player.get('Jersey','')}")
            errors = []
            for stat_type in stat_types_for(player):
                try:
                    process_stat(sheet, page, player, stat_type)
                except Exception as e:
                    print(f"    ERROR ({stat_type}): {e}")
                    errors.append(f"{stat_type}: {e}")
            if errors:
                log(sheet, player, "ERROR", " | ".join(errors))
                failed_players.append(f"{player['Name']}: {' | '.join(errors)}")
            else:
                log(sheet, player, "SUCCESS")
                ok_players.append(player["Name"])

        browser.close()

    return record_run(sheet, "Stats scraper", ok_players, failed_players)


if __name__ == "__main__":
    test_player_id = os.environ.get("TEST_PLAYER_ID")
    if not test_player_id and len(sys.argv) > 1:
        test_player_id = sys.argv[1]
    if main(test_player_id=test_player_id):
        sys.exit(1)
