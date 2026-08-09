"""Shared helpers for hqdata tests."""

import pandas as pd

from hqdata.sources.base import BaseSource

# Column contracts derived from BaseSource, so tests stay in sync with the interface.
CALENDAR_COLUMNS = set(BaseSource._empty_calendar().columns)
STOCK_LIST_COLUMNS = set(BaseSource._empty_stock_list().columns)
STOCK_MINUTE_BAR_COLUMNS = set(BaseSource._empty_stock_minute_bar().columns)
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


def assert_minute_bar_sanity(df: pd.DataFrame) -> None:
    """Common invariants for a non-empty minute bar DataFrame."""
    assert (df["high"] >= df["low"]).all(), "high < low found"
    assert df["date"].str.match(DATE_PATTERN).all(), "date not in YYYYMMDD format"
    assert (
        df["ets"].str.match(ETS_PATTERN).all()
    ), "ets not in YYYYMMDDTHHMMSSsss format"
