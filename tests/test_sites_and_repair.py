"""sites.py settings, the schedule layout override, and the manual repair script."""

from bs4 import BeautifulSoup

import repair_history as R
import scrape_schedule as SS
import scraper as S
import sites
from conftest import ROOT
from fake_sheets import FakeSpreadsheet, records


def test_user_agent_follows_the_browser():
    assert "Chrome/153.0.0.0" in sites.user_agent("153.0.8010.12")
    assert "Chrome/160.0.0.0" in sites.user_agent("160.1.2.3")
    assert sites.user_agent(None) == sites.BROWSER_UA


def test_stats_layout_matched_by_website():
    schools = [{"school": "GW", "schedule_url": "https://gwsports.com/sports/baseball/schedule/{season}",
                "stats_layout": "dropdown"},
               {"school": "CNU", "schedule_url": "https://www.cnusports.com/sports/baseball/schedule/{season}"}]
    assert sites.stats_layout("https://gwsports.com/sports/baseball/stats/2026", schools) == "dropdown"
    assert sites.stats_layout("https://cnusports.com/sports/baseball/stats/2026", schools) is None
    assert sites.stats_layout("https://other.edu/stats", schools) is None


def test_schedule_layout_override(monkeypatch):
    html = (ROOT / "gw_sample.txt").read_text(encoding="utf-8")
    soup = BeautifulSoup(html, "html.parser")
    monkeypatch.setattr(SS, "fetch_soup", lambda url: soup)
    cfg = {"school": "George Washington", "schedule_url": "https://gwsports.com/sports/baseball/schedule/{season}"}
    assert len(SS.scrape_school(dict(cfg, schedule_layout="json"))) == 55
    assert SS.scrape_school(dict(cfg, schedule_layout="table")) == []


# ── repair_history.py (moved out of the regular scrape) ───────────────────────

HEADER = S.HISTORY_PREFIX_FIELDS + S.BATTING_COLS
PLAYERS = [{"PlayerID": "P001", "Name": "Tyler Fetterman", "School": "East Tennessee State University",
            "Division": "D1", "Type": "Hitter"}]


def shifted_row():
    """A row written with the old current-tab layout (no Scraped_At_UTC / Business_Date_ET)."""
    legacy = ["2026-02-24 04:21", "P001", "Tyler Fetterman", "East Tennessee State University", "D1"]
    return legacy + ["6", "6", "22"] + [""] * (len(S.BATTING_COLS) - 3)


def good_row():
    return ["2026-02-25 04:21", "2026-02-25 09:21", "2026-02-25", "P001", "Tyler Fetterman",
            "East Tennessee State University", "D1"] + ["7"] * len(S.BATTING_COLS)


def test_repair_dry_run_reports_without_writing():
    sheet = FakeSpreadsheet({"Batting_History": [HEADER, shifted_row(), good_row()]})
    before = sheet.worksheet("Batting_History").get_all_values()
    assert R.repair_history_table(sheet, "Batting_History", S.BATTING_COLS, PLAYERS) == 1
    assert sheet.worksheet("Batting_History").get_all_values() == before


def test_repair_apply_realigns_the_row():
    sheet = FakeSpreadsheet({"Batting_History": [HEADER, shifted_row(), good_row()]})
    R.repair_history_table(sheet, "Batting_History", S.BATTING_COLS, PLAYERS, apply=True)
    fixed = records(sheet.worksheet("Batting_History"))[0]
    assert (fixed["PlayerID"], fixed["Scraped_At_UTC"], fixed["Business_Date_ET"]) == \
        ("P001", "2026-02-24 09:21", "2026-02-24")
    assert (fixed["G"], fixed["GS"], fixed["AB"]) == ("6", "6", "22")
    assert R.repair_history_table(sheet, "Batting_History", S.BATTING_COLS, PLAYERS) == 0
