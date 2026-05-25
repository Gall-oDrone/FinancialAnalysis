"""Tests for PgConn DataFrame construction from cursor rows."""

import pandas as pd

from storage.postgres.pgConn import PgConn


class TestDataframeFromCursor:
    def test_pad_columns_when_row_wider_than_description(self):
        conn = PgConn.__new__(PgConn)
        class Desc:
            def __init__(self, name):
                self.name = name

            def __getitem__(self, index):
                return self.name if index == 0 else None

        cursor = type(
            "C",
            (),
            {"description": [Desc("a"), Desc("b")]},
        )()
        data = [(1, 2, 3)]
        df = conn._dataframe_from_cursor(cursor, data)
        assert list(df.columns) == ["a", "b", "_extra_2"]
        assert df.iloc[0].tolist() == [1, 2, 3]

    def test_renames_reference_to_ref(self):
        conn = PgConn.__new__(PgConn)
        class Desc:
            def __init__(self, name):
                self.name = name

            def __getitem__(self, index):
                return self.name if index == 0 else None

        cursor = type("C", (), {"description": [Desc("reference"), Desc("book")]})()
        df = conn._dataframe_from_cursor(cursor, [("ref1", "btc")])
        assert "ref" in df.columns
        assert "reference" not in df.columns
