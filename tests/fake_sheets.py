"""
In-memory stand-ins for the parts of gspread's Spreadsheet and Worksheet API
the scrapers use, so tests can exercise sheet reads and writes without
credentials or network.

Cells are stored as strings, like a sheet written with RAW input.
get_all_records() converts numbers the way gspread does by default.
"""

import gspread
from gspread.utils import a1_to_rowcol, numericise_all


class FakeWorksheet:
    def __init__(self, title, rows=None, spreadsheet=None, row_count=1000, col_count=26):
        self.title = title
        self.spreadsheet = spreadsheet
        self._rows = [[str(c) for c in r] for r in (rows or [])]
        self.row_count = max(row_count, len(self._rows))
        self.col_count = max([col_count] + [len(r) for r in self._rows])

    # ── reads ────────────────────────────────────────────────────────────────
    def _trimmed(self):
        rows = [list(r) for r in self._rows]
        while rows and not any(c != "" for c in rows[-1]):
            rows.pop()
        width = max((len(r) for r in rows), default=0)
        return [r + [""] * (width - len(r)) for r in rows]

    def get_all_values(self):
        return self._trimmed()

    def row_values(self, row):
        values = self._rows[row - 1] if row <= len(self._rows) else []
        values = list(values)
        while values and values[-1] == "":
            values.pop()
        return values

    def get_all_records(self):
        values = self._trimmed()
        if not values:
            return []
        headers = values[0]
        return [dict(zip(headers, numericise_all(row))) for row in values[1:]]

    # ── writes ───────────────────────────────────────────────────────────────
    def _set(self, row, col, value):
        while len(self._rows) < row:
            self._rows.append([])
        line = self._rows[row - 1]
        while len(line) < col:
            line.append("")
        line[col - 1] = str(value)
        self.row_count = max(self.row_count, row)
        self.col_count = max(self.col_count, col)

    def update(self, values=None, range_name=None, value_input_option=None, **_):
        start = (range_name or "A1").split(":")[0]
        row0, col0 = a1_to_rowcol(start)
        for r, line in enumerate(values):
            for c, value in enumerate(line):
                self._set(row0 + r, col0 + c, value)

    def batch_update(self, data, **_):
        for item in data:
            self.update(values=item["values"], range_name=item["range"])

    def append_row(self, values, **_):
        last = len(self._trimmed())
        for c, value in enumerate(values):
            self._set(last + 1, c + 1, value)

    def batch_clear(self, ranges):
        for rng in ranges:
            start, _, end = rng.partition(":")
            r1, c1 = a1_to_rowcol(start)
            r2, c2 = a1_to_rowcol(end or start)
            for r in range(r1, min(r2, len(self._rows)) + 1):
                line = self._rows[r - 1]
                for c in range(c1, min(c2, len(line)) + 1):
                    line[c - 1] = ""

    def add_rows(self, n):
        self.row_count += n

    def add_cols(self, n):
        self.col_count += n


class FakeSpreadsheet:
    def __init__(self, tabs=None):
        self._tabs = {}
        for title, rows in (tabs or {}).items():
            self._tabs[title] = FakeWorksheet(title, rows, spreadsheet=self)

    def worksheet(self, title):
        try:
            return self._tabs[title]
        except KeyError:
            raise gspread.WorksheetNotFound(title) from None

    def add_worksheet(self, title, rows=1000, cols=26, **_):
        ws = FakeWorksheet(title, spreadsheet=self, row_count=rows, col_count=cols)
        self._tabs[title] = ws
        return ws


def records(ws):
    """The sheet's data rows as dicts of strings (header -> cell)."""
    values = ws.get_all_values()
    return [dict(zip(values[0], row)) for row in values[1:]] if values else []

