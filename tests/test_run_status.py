import pytest

import run_status as RS
from fake_sheets import FakeSpreadsheet, records


@pytest.mark.parametrize("ok, failed, expected", [
    (23, 0, False),
    (21, 2, False),   # 8.7%: a flaky site or two
    (3, 1, False),    # exactly 25% is still a pass
    (17, 6, True),    # 26%
    (0, 0, True),     # nothing ran
    (0, 3, True),
])
def test_run_failed(ok, failed, expected):
    assert RS.run_failed(ok, failed) is expected


def test_record_run_creates_and_appends_run_log(tmp_path, monkeypatch):
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    sheet = FakeSpreadsheet()

    assert RS.record_run(sheet, "Stats scraper", ["A", "B", "C"], ["D: no table"]) is False
    assert RS.record_run(sheet, "Stats scraper", [], ["D: no table"]) is True

    rows = records(sheet.worksheet(RS.RUN_LOG_TAB))
    assert [r["Status"] for r in rows] == ["PARTIAL", "FAILED"]
    assert rows[0]["OK"] == "3" and rows[0]["Failed_Items"] == "D: no table"
    assert "### Stats scraper: PARTIAL" in summary.read_text()


def test_record_run_survives_sheet_errors(monkeypatch):
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)

    class Broken:
        def worksheet(self, _):
            raise RuntimeError("quota")

    assert RS.record_run(Broken(), "Schedule scraper", ["A"], []) is False
