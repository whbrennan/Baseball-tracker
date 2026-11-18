"""backup_sheet.py: every tab exported exactly, and a failed read changes nothing."""

import pytest

import backup_sheet as B
from fake_sheets import FakeSpreadsheet


def sheet():
    return FakeSpreadsheet({
        "Batting": [["PlayerID", "AVG", "Name"], ["P002", ".316", "Mendez, David"]],
        "Run_Log": [["Finished_ET", "Notes"], ["2026-11-17 19:56", 'said "hi"\nline 2']],
        "Players": [],
    })


def test_export_every_tab_exactly():
    files = B.export(sheet())
    assert sorted(files) == ["Batting.csv", "Players.csv", "Run_Log.csv"]
    # Values kept as shown (".316" stays text); commas, quotes and newlines are quoted.
    assert files["Batting.csv"] == 'PlayerID,AVG,Name\nP002,.316,"Mendez, David"\n'
    assert files["Run_Log.csv"] == 'Finished_ET,Notes\n2026-11-17 19:56,"said ""hi""\nline 2"\n'
    assert files["Players.csv"] == ""


def test_write_backup_mirrors_the_sheet(tmp_path):
    (tmp_path / "Old_Tab.csv").write_text("x\n")
    written, removed = B.write_backup(B.export(sheet()), tmp_path)
    assert written == ["Batting.csv", "Players.csv", "Run_Log.csv"]
    assert removed == ["Old_Tab.csv"]
    assert sorted(p.name for p in tmp_path.iterdir()) == written


@pytest.mark.parametrize("tab, name", [
    ("Batting_History", "Batting_History.csv"),
    ("Stats (old)", "Stats (old).csv"),
    ("a/b:c", "a_b_c.csv"),
    ("..", "tab.csv"),
])
def test_file_name(tab, name):
    assert B.file_name(tab) == name


def test_failed_read_leaves_previous_backup(tmp_path, monkeypatch):
    (tmp_path / "Batting.csv").write_text("previous\n")

    class Broken:
        def worksheets(self):
            raise RuntimeError("quota")

    monkeypatch.setattr(B, "connect", lambda: Broken())
    assert B.main([str(tmp_path)]) == 1
    assert (tmp_path / "Batting.csv").read_text() == "previous\n"


def test_empty_spreadsheet_is_an_error():
    with pytest.raises(ValueError):
        B.export(FakeSpreadsheet({}))
