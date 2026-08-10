"""Shared helpers for hqdata tests."""

import re

import pandas as pd

from hqdata.sources.base import BaseSource

# Column contracts derived from BaseSource, so tests stay in sync with the interface.
CALENDAR_COLUMNS = set(BaseSource._empty_calendar().columns)
STOCK_LIST_COLUMNS = set(BaseSource._empty_stock_list().columns)
STOCK_DAILY_BAR_COLUMNS = set(BaseSource._empty_stock_daily_bar().columns)
STOCK_SNAPSHOT_COLUMNS = set(BaseSource._empty_stock_snapshot().columns)

DATE_PATTERN = r"^\d{8}$"
ETS_PATTERN = r"^\d{8}T\d{9}$"


def make_calendar(*open_days: str, closed: tuple = ()) -> pd.DataFrame:
    """Build a calendar DataFrame: open_days get is_open=Y, closed days get N."""
    days = sorted([*open_days, *closed])
    return pd.DataFrame(
        {"date": days, "is_open": ["N" if d in closed else "Y" for d in days]}
    )


def assert_has_columns(df: pd.DataFrame, expected: set) -> None:
    missing = set(expected) - set(df.columns)
    assert not missing, f"Missing columns: {sorted(missing)}"


def assert_daily_bar_sanity(df: pd.DataFrame) -> None:
    """Common invariants for a non-empty daily bar DataFrame."""
    assert (df["high"] >= df["low"]).all(), "high < low found"
    assert (df["high"] >= df["close"]).all(), "high < close found"
    assert (df["low"] <= df["close"]).all(), "low > close found"
    assert (df["volume"] > 0).all(), "non-positive volume found"
    assert (df["turnover"] > 0).all(), "non-positive turnover found"
    assert df["date"].str.match(DATE_PATTERN).all(), "date not in YYYYMMDD format"


class IntegrationTestMixin:
    """Integration tests shared by every source adapter's TestXxxIntegration class.

    Subclasses provide the `setup` autouse fixture that sets `self.source` and
    `self.trade_date`. Override `_DAILY_BAR_KWARGS` for adapters whose
    get_stock_daily_bar requires extra kwargs when called directly (bypassing
    the api.py layer that would normally inject them).
    """

    _DAILY_BAR_KWARGS: dict = {}

    # -- get_calendar ---------------------------------------------------

    def test_get_calendar(self):
        """Full range: every day present, is_open flags well-formed, subsets consistent."""
        df = self.source.get_calendar("20260101", "20260401")
        assert not df.empty, "get_calendar returned empty DataFrame"
        assert_has_columns(df, {"date", "is_open"})
        assert df["date"].str.match(DATE_PATTERN).all(), "date not in YYYYMMDD format"
        assert (
            df["is_open"].isin(["Y", "N"]).all()
        ), "is_open should only contain Y or N"
        assert len(df) == 91, f"Expected 91 days, got {len(df)}"
        assert df["date"].iloc[0] == "20260101"
        assert df["date"].iloc[-1] == "20260401"

        open_df = self.source.get_calendar("20260101", "20260401", is_open=True)
        closed_df = self.source.get_calendar("20260101", "20260401", is_open=False)
        assert (open_df["is_open"] == "Y").all()
        assert (closed_df["is_open"] == "N").all()
        assert len(open_df) == 57, f"Expected 57 trading days, got {len(open_df)}"
        assert len(open_df) + len(closed_df) == len(df)

    # -- get_stock_list -------------------------------------------------

    def test_get_stock_list_by_symbol(self):
        """Single symbol returns one row; comma-separated returns each requested symbol."""
        df = self.source.get_stock_list(trade_date=self.trade_date, symbol="000001.SZ")
        assert len(df) == 1, f"Expected single stock, got {len(df)} rows"
        assert df.iloc[0]["symbol"] == "000001.SZ"

        df = self.source.get_stock_list(
            trade_date=self.trade_date, symbol="000001.SZ,600000.SH"
        )
        assert set(df["symbol"]) == {"000001.SZ", "600000.SH"}

    def test_get_stock_list_by_exchange(self):
        """Single exchange filters strictly; multiple exchanges include each of them."""
        df = self.source.get_stock_list(trade_date=self.trade_date, exchange="SSE")
        assert not df.empty, "empty DataFrame for exchange=SSE"
        assert (df["exchange"] == "SSE").all(), "Expected all stocks to be from SSE"

        df = self.source.get_stock_list(trade_date=self.trade_date, exchange="SSE,SZE")
        assert set(df["exchange"]) == {"SSE", "SZE"}

    def test_get_stock_list_by_board(self):
        """Single board filters strictly; multiple boards include each of them."""
        df = self.source.get_stock_list(trade_date=self.trade_date, board="MB")
        assert not df.empty, "empty DataFrame for board=MB"
        assert (df["board"] == "MB").all(), "Expected all stocks to be from MB"

        df = self.source.get_stock_list(trade_date=self.trade_date, board="MB,GEM,STAR")
        assert set(df["board"]) == {"MB", "GEM", "STAR"}

    def test_get_stock_list_combined_filters(self):
        """Multiple filters combine with AND semantics."""
        df = self.source.get_stock_list(
            trade_date=self.trade_date, board="MB", exchange="SSE"
        )
        assert not df.empty, "empty DataFrame for board=MB,exchange=SSE"
        assert (df["board"] == "MB").all()
        assert (df["exchange"] == "SSE").all()

        df = self.source.get_stock_list(
            trade_date=self.trade_date, symbol="000001.SZ", board="MB", exchange="SZE"
        )
        assert list(df["symbol"]) == ["000001.SZ"]

    # -- get_stock_snapshot -----------------------------------------------

    def test_get_stock_snapshot(self):
        df = self.source.get_stock_snapshot("000001.SZ,600000.SH")
        assert not df.empty, "get_stock_snapshot returned empty DataFrame"
        assert_has_columns(df, STOCK_SNAPSHOT_COLUMNS)
        assert set(df["symbol"]) == {"000001.SZ", "600000.SH"}
        assert (df["volume"] > 0).all(), "volume should be > 0"
        ts_pattern = re.compile(ETS_PATTERN)
        for col in ("ets", "lts"):
            assert (
                df[col].apply(lambda x: bool(ts_pattern.match(x))).all()
            ), f"{col} format should be YYYYMMDDTHHMMSSsss"

    # -- get_stock_daily_bar ----------------------------------------------

    def test_get_stock_daily_bar(self):
        """Well-formed bars for one symbol per market, and for a multi-symbol query."""
        for symbol in ("000001.SZ", "600000.SH"):
            df = self.source.get_stock_daily_bar(
                symbol, "20260101", "20260401", **self._DAILY_BAR_KWARGS
            )
            assert not df.empty, f"{symbol} returned empty DataFrame"
            assert_has_columns(df, STOCK_DAILY_BAR_COLUMNS)
            assert_daily_bar_sanity(df)

        df = self.source.get_stock_daily_bar(
            "000001.SZ,600000.SH", "20260101", "20260401", **self._DAILY_BAR_KWARGS
        )
        assert set(df["symbol"]) == {"000001.SZ", "600000.SH"}
        assert_daily_bar_sanity(df)
