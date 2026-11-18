import os
import re
import sys
import json
import time
import requests
import gspread
from gspread.http_client import BackOffHTTPClient
from gspread.utils import rowcol_to_a1
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright
from datetime import datetime
from zoneinfo import ZoneInfo

from run_status import record_run
from season import SEASON, season_url
from sites import BROWSER_UA, load_schools, stats_layout, user_agent

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


def row_season(row):
    """Season of a stats row: its Season cell, else the year it was written.

    Rows written before the Season column existed are all from the season of
    their calendar year (stats pages are only scraped for the season they show).
    """
    value = str((row or {}).get("Season", "")).strip()
    if value[:4].isdigit():
        return int(value[:4])
    last_updated = str((row or {}).get("Last_Updated", "")).strip()
    return int(last_updated[:4]) if last_updated[:4].isdigit() else None


def same_season(previous_row):
    """True if the existing sheet row is from the configured season.

    Across a season rollover stats legitimately reset to zero.
    """
    return row_season(previous_row) == SEASON


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
        # Sheets only: the scripts open one spreadsheet by ID, which needs no Drive
        # access. Full Drive scope would let a leaked key touch any file shared
        # with the service account (#30).
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    # BackOffHTTPClient retries quota (429), timeout and server errors with backoff.
    return gspread.authorize(creds, http_client=BackOffHTTPClient).open_by_key(SPREADSHEET_ID)


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
        "Season": str(SEASON),
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


# Header cells that identify each stats table.
REQUIRED_HEADERS = {
    "batting" : ("AVG", "AB"),
    "pitching": ("ERA", "IP"),
    "defense" : ("FLD%", "PO"),
}


def find_table(tables, stat_type):
    """Return (rows, upper-cased headers) of the first table for stat_type, or (None, [])."""
    need = REQUIRED_HEADERS[stat_type]
    for rows in tables:
        if len(rows) < 2:
            continue
        hdrs = [h.strip().upper() for h in rows[0]]
        if all(h in hdrs for h in need):
            return rows, hdrs
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


def embedded_stats_from_payload(raw, stat_type, jersey):
    """One player's overall season line from the page's embedded data (the payload array).

    Returns the raw stats dict, {} if the stats list exists but the jersey isn't
    in it, or None if the page has no embedded stats for this stat type.
    """
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


def embedded_has_list(raw, stat_type):
    """True if the payload has an embedded stats list for stat_type."""
    return embedded_stats_from_payload(raw, stat_type, jersey=None) is not None


def player_stats(page_data, stat_type, jersey):
    """Return (stats, note) for one player and stat type from a loaded stats page.

    Raises ScrapeError when the player can't be read: no jersey configured, or
    the page had neither a table nor embedded data for this stat type. A player
    missing from data that loaded fine is normal before their first appearance,
    so that returns zeros with a note; the caller refuses the zeros if the
    player already has stats this season.
    """
    jersey = str(jersey or "").strip()
    if not jersey:
        raise ScrapeError("No jersey number in the Players sheet")
    col_map = {"batting": BATTING_MAP, "pitching": PITCHING_MAP, "defense": DEFENSE_MAP}[stat_type]

    table, hdrs = page_data.tables.get(stat_type) or (None, [])
    if table:
        raw = player_from_table(table, hdrs, jersey)
        if raw is not None:
            return map_row(raw, col_map), ""
        note = f"Jersey #{jersey} not listed in the {stat_type} table"
    else:
        embedded = embedded_stats_from_payload(page_data.payload, stat_type, jersey)
        if embedded is None:
            raise ScrapeError(f"No {stat_type} table or embedded stats found at {page_data.url}")
        if embedded:
            return map_row(embedded, col_map), ""
        note = f"Jersey #{jersey} not listed in the {stat_type} stats"
    print(f"    Note: {note} -- treating as no appearances yet")
    return zero_stats(col_map), note


# ── LOADING A STATS PAGE ──────────────────────────────────────────────────────
class StatsPage:
    """What one stats URL gave: the table found for each stat type, and the embedded data."""

    def __init__(self, url, tables=None, payload=None):
        self.url = url
        self.tables = tables or {}   # stat_type -> (rows, hdrs) or (None, [])
        self.payload = payload       # Nuxt payload array, or None

    def has(self, stat_type):
        return bool((self.tables.get(stat_type) or (None,))[0]) or \
            embedded_has_list(self.payload, stat_type)

    def missing(self, stat_types):
        return [st for st in stat_types if not self.has(st)]


# Resolves when the page shows a table for every stat type needed (or, when
# allowed, has its embedded data), so a fast page isn't held up and a slow one
# gets time to finish.
_READY_JS = """({need, payloadOk}) => {
  const heads = [...document.querySelectorAll('table')].map(t => {
    const first = t.querySelector('tr');
    return first ? [...first.querySelectorAll('th, td')].map(c => (c.innerText || '').trim().toUpperCase()) : [];
  });
  if (need.every(group => heads.some(h => group.every(x => h.includes(x))))) return true;
  return payloadOk && [...document.scripts].some(s => (s.textContent || '').includes('ShallowReactive'));
}"""
READY_TIMEOUT_MS = 20000
LOAD_ATTEMPTS = 3
RETRY_DELAYS = (5, 15)  # seconds before the 2nd and 3rd attempts

# Options in the Sidearm stats dropdown (button.s-select-box) for each stat type.
DROPDOWN_OPTIONS = {"pitching": "Pitching", "defense": "Fielding"}
_DROPDOWN_BOX = re.compile(r"\b(Batting|Pitching|Fielding)\b")


def _wait_for(page, groups, timeout, payload_ok=True):
    try:
        page.wait_for_function(_READY_JS, arg={"need": [list(g) for g in groups], "payloadOk": payload_ok},
                               timeout=timeout)
        return True
    except Exception:
        return False


def _dropdown_tables(page, stat_type):
    """Pick stat_type in the page's Batting/Pitching/Fielding dropdown and re-read the tables.

    Returns the tables, or None if the page has no such dropdown or the table
    didn't appear.
    """
    option = DROPDOWN_OPTIONS.get(stat_type)
    if not option:
        return None
    box = page.locator("button.s-select-box", has_text=_DROPDOWN_BOX).first
    try:
        if not box.count():
            return None
        box.click(timeout=5000)
        page.locator("li.s-select__option-item", has_text=option).first.click(timeout=5000)
    except Exception as e:
        print(f"    Note: {option} dropdown choice failed: {e}")
        return None
    if not _wait_for(page, [REQUIRED_HEADERS[stat_type]], timeout=10000, payload_ok=False):
        print(f"    Note: no {stat_type} table after choosing {option}")
        return None
    return read_tables(page)


def _raw_html_payload(page):
    """The embedded data from the server's original HTML, for pages whose own
    scripts have already rewritten it out of the live DOM."""
    try:
        cookies = {c["name"]: c["value"] for c in page.context.cookies(page.url)}
        resp = requests.get(page.url, headers={"User-Agent": BROWSER_UA}, cookies=cookies, timeout=30)
        resp.raise_for_status()
        return _parse_payload(_SCRIPT_RE.findall(resp.text))
    except Exception as e:
        print(f"    Note: could not fetch raw page for embedded stats: {e}")
        return None


def _load_once(page, url, stat_types, layout=None):
    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    _wait_for(page, [REQUIRED_HEADERS[st] for st in stat_types], READY_TIMEOUT_MS)
    tables = read_tables(page)
    payload = _parse_payload(page.eval_on_selector_all("script", "els => els.map(e => e.textContent || '')"))
    found = {st: find_table(tables, st) for st in stat_types}

    # Not on the page as a table: use the embedded data, else the dropdown.
    # A "dropdown" override tries the dropdown first; "embedded" never clicks.
    for st in stat_types:
        if found[st][0]:
            continue
        if layout != "dropdown" and embedded_has_list(payload, st):
            continue
        if layout != "embedded":
            chosen = _dropdown_tables(page, st)
            if chosen:
                found[st] = find_table(chosen, st)
                if found[st][0]:
                    print(f"    Note: {st} table read via the site's dropdown")
    data = StatsPage(url, found, payload)
    if payload is None and data.missing(stat_types):
        data.payload = _raw_html_payload(page)
    for st in stat_types:
        if not found[st][0] and embedded_has_list(data.payload, st):
            print(f"    Note: {st} read from the page's embedded data")
    return data


def load_stats_page(page, url, stat_types, layout=None):
    """Load a stats URL and collect what each stat type needs, retrying slow or broken loads.

    Always returns a StatsPage once a load succeeds; stat types it still lacks
    after the last attempt make player_stats() raise for those players. Raises
    ScrapeError only if the page never loaded at all.
    """
    data, error = None, None
    for attempt in range(LOAD_ATTEMPTS):
        if attempt:
            delay = RETRY_DELAYS[min(attempt - 1, len(RETRY_DELAYS) - 1)]
            print(f"    Retrying in {delay}s (attempt {attempt + 1} of {LOAD_ATTEMPTS})")
            time.sleep(delay)
        try:
            data = _load_once(page, url, stat_types, layout)
        except Exception as e:
            error = e
            print(f"    Load failed: {str(e).splitlines()[0]}")
            continue
        missing = data.missing(stat_types)
        if not missing:
            return data
        print(f"    No {', '.join(missing)} data found yet")
    if data is None:
        raise ScrapeError(f"Could not load {url}: {str(error).splitlines()[0] if error else 'unknown error'}")
    return data

SEASON_HEADER = "Season"


def ensure_season_column(ws, values=None):
    """Add a Season column after the last named header, filling existing rows.

    Existing rows get the year they were written (see row_season). Runs once
    per tab; afterwards the header is there and nothing is written. Pass the
    tab's values if already read, to save a read.
    """
    headers = [h.strip() for h in values[0]] if values else get_headers(ws)
    if SEASON_HEADER in headers:
        return False
    if values is None:
        values = ws.get_all_values()
    named = [i for i, h in enumerate(headers) if h]
    col = (named[-1] + 2) if named else 1
    if col > ws.col_count:
        ws.add_cols(col - ws.col_count)
    canon = [canonicalize_header(h) for h in (values[0] if values else [])]
    column = [[SEASON_HEADER]]
    for row in values[1:]:
        record = {canon[i]: row[i] for i in range(min(len(canon), len(row))) if canon[i]}
        column.append([str(row_season(record) or "")])
    ws.update(values=column, range_name=rowcol_to_a1(1, col), value_input_option="RAW")
    print(f"    OK: {ws.title} -- added {SEASON_HEADER} column, filled {len(column) - 1} rows")
    return True


# ── SHEET TABS: read once, write in one batch ─────────────────────────────────
class CurrentTab:
    """A current-stats tab (one row per player), read once per run.

    Rows are changed in memory and sent with one batch_update (existing
    players) and one append_rows (new players) by flush().
    """

    def __init__(self, ws, target_cols):
        self.ws, self.cols = ws, target_cols
        values = ws.get_all_values()
        if ensure_season_column(ws, values):
            values = ws.get_all_values()
        self.headers = [h.strip() for h in values[0]] if values else []
        canon = [canonicalize_header(h) for h in self.headers]
        self.rows = {}
        for number, raw in enumerate(values[1:], start=2):
            record = {canon[i]: raw[i] for i in range(min(len(canon), len(raw))) if canon[i]}
            pid = str(record.get("PlayerID", "")).strip()
            if pid and pid not in self.rows:
                self.rows[pid] = (number, record)
        self.updates, self.appends = {}, []

    def current(self, pid):
        """(sheet row number, record) for a player's row, or (None, None)."""
        return self.rows.get(pid, (None, None))

    def set(self, player, mapped):
        row = build_row(self.headers, build_current_payload(player, mapped, self.cols), self.cols)
        number, _ = self.current(player["PlayerID"])
        if number:
            self.updates[number] = row
        else:
            self.appends.append(row)

    def flush(self):
        if self.updates:
            self.ws.batch_update([{"range": f"A{n}", "values": [row]} for n, row in sorted(self.updates.items())])
        if self.appends:
            self.ws.append_rows(self.appends)
        print(f"    {self.ws.title}: {len(self.updates)} updated, {len(self.appends)} added")


class HistoryTab:
    """A history tab: only its header row is read; snapshots are appended in one call."""

    def __init__(self, ws, target_cols):
        self.ws, self.cols = ws, target_cols
        ensure_season_column(ws)
        self.headers = get_headers(ws)
        self.appends = []

    def add(self, player, mapped):
        self.appends.append(build_row(self.headers, build_history_payload(player, mapped, self.cols),
                                      self.cols, history=True))

    def flush(self):
        if self.appends:
            self.ws.append_rows(self.appends)
        print(f"    {self.ws.title}: {len(self.appends)} snapshots added")


# ── PER-STAT PROCESSING ───────────────────────────────────────────────────────
STAT_JOBS = {
    # stat_type: (current tab, history tab, columns)
    "batting" : ("Batting",  "Batting_History",  BATTING_COLS),
    "defense" : ("Defense",  "Defense_History",  DEFENSE_COLS),
    "pitching": ("Pitching", "Pitching_History", PITCHING_COLS),
}


def stat_types_for(player):
    return ["batting", "defense"] if player["Type"] == "Hitter" else ["pitching"]


def notify(player, stat_type, stats, old_row):
    """Pushover alerts for a first appearance or a newly crossed threshold."""
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


def process_stat(tabs, page_data, player, stat_type):
    """Read one stat type for a player and queue its writes, or raise without queueing."""
    current, history = tabs[stat_type]
    stats, note = player_stats(page_data, stat_type, player.get("Jersey", ""))
    _, old_row = current.current(player["PlayerID"])

    drops = stats_went_backwards(old_row, stats, stat_type)
    if drops:
        detail = ", ".join(f"{col} {old} -> {new}" for col, old, new in drops)
        reason = f" ({note})" if note else ""
        raise ScrapeError(
            f"{stat_type} stats went down this season: {detail}{reason}. "
            "Kept the existing row; check the jersey number and stats URL."
        )

    current.set(player, stats)
    if stats_changed(old_row, stats, current.cols):
        history.add(player, stats)
        print(f"    {stat_type}: updated, snapshot queued")
    else:
        print(f"    {stat_type}: updated, unchanged since last snapshot")
    notify(player, stat_type, stats, old_row)


def open_tabs(sheet, stat_types):
    """{stat_type: (CurrentTab, HistoryTab)} for the stat types this run needs."""
    tabs = {}
    for st in stat_types:
        tab, hist, cols = STAT_JOBS[st]
        tabs[st] = (CurrentTab(sheet.worksheet(tab), cols), HistoryTab(sheet.worksheet(hist), cols))
    return tabs


def group_by_page(players):
    """{stats URL for the season: [players]}, in Players-tab order, so each page loads once."""
    groups = {}
    for player in players:
        groups.setdefault(season_url(player.get("Stats_URL", "")), []).append(player)
    return groups


def scrape_players(sheet, page, players, schools=None):
    """Scrape every player and write the results. Returns (ok_names, failed_items)."""
    schools = load_schools() if schools is None else schools
    needed = [st for st in STAT_JOBS if any(st in stat_types_for(p) for p in players)]
    tabs = open_tabs(sheet, needed)
    errors = {p["PlayerID"]: [] for p in players}
    log_rows = []

    for url, group in group_by_page(players).items():
        stat_types = [st for st in STAT_JOBS if any(st in stat_types_for(p) for p in group)]
        names = ", ".join(p["Name"] for p in group)
        print(f"-> {url}  ({names})")
        try:
            page_data = load_stats_page(page, url, stat_types, stats_layout(url, schools))
            load_error = None
        except ScrapeError as e:
            page_data, load_error = None, e
        for player in group:
            print(f"  {player['Name']} ({player['School']}) Jersey #{player.get('Jersey', '')}")
            for st in stat_types_for(player):
                try:
                    if load_error:
                        raise load_error
                    process_stat(tabs, page_data, player, st)
                except Exception as e:
                    print(f"    ERROR ({st}): {e}")
                    errors[player["PlayerID"]].append(f"{st}: {e}")

    # Write everything. A tab that can't be written fails the players it held.
    print("\nWriting to the sheet...")
    for st, pair in tabs.items():
        for tab in pair:
            try:
                tab.flush()
            except Exception as e:
                print(f"    ERROR writing {tab.ws.title}: {e}")
                for p in players:
                    if st in stat_types_for(p) and not errors[p["PlayerID"]]:
                        errors[p["PlayerID"]].append(f"{st}: could not write {tab.ws.title}: {e}")

    ok, failed = [], []
    ts = current_timestamps()["last_updated_et"]
    for p in players:
        problems = errors[p["PlayerID"]]
        status, notes = ("ERROR", " | ".join(problems)) if problems else ("SUCCESS", "")
        log_rows.append([ts, p["PlayerID"], p["Name"], p["School"], status, notes])
        if problems:
            failed.append(f"{p['Name']}: {' | '.join(problems)}")
        else:
            ok.append(p["Name"])
    try:
        sheet.worksheet("Scrape_Log").append_rows(log_rows)
    except Exception as e:  # logging must not fail the run
        print(f"    Warning: could not write Scrape_Log: {e}")
    return ok, failed


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main(test_player_id=None):
    """Scrape every player. Returns True if the run failed overall."""
    print(f"Season {SEASON}. Connecting to Google Sheets...")
    sheet   = connect()
    players = get_players(sheet)
    if test_player_id:
        players = [p for p in players if p["PlayerID"] == test_player_id]
        if not players:
            print(f"Player ID '{test_player_id}' not found.")
            return True
    print(f"Processing {len(players)} player(s)...\n")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(user_agent=user_agent(browser.version))
        page = context.new_page()
        ok_players, failed_players = scrape_players(sheet, page, players)
        browser.close()

    return record_run(sheet, "Stats scraper", ok_players, failed_players)


if __name__ == "__main__":
    test_player_id = os.environ.get("TEST_PLAYER_ID")
    if not test_player_id and len(sys.argv) > 1:
        test_player_id = sys.argv[1]
    if main(test_player_id=test_player_id):
        sys.exit(1)
