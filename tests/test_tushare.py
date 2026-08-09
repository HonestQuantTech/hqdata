"""Tests for tushare source"""

import os
import re
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import hqdata.config  # noqa: F401  加载 .env
from hqdata.sources.tushare import TushareSource
from tests.helpers import (
    DATE_PATTERN,
    ETS_PATTERN,
    STOCK_DAILY_BAR_COLUMNS,
    STOCK_LIST_COLUMNS,
    STOCK_SNAPSHOT_COLUMNS,
    assert_daily_bar_sanity,
    assert_has_columns,
)


def make_source_with_stock_basic(stock_basic_df: pd.DataFrame) -> TushareSource:
    """Build an uninitialized TushareSource whose pro.stock_basic returns the given frame."""
    source = object.__new__(TushareSource)
    source.pro = MagicMock()
    source.pro.stock_basic.return_value = stock_basic_df
    return source


class TestTushareSource:
    """Unit tests for TushareSource."""

    @patch.dict(os.environ, {}, clear=True)
    def test_init_missing_token_raises(self):
        with pytest.raises(ValueError, match="TUSHARE_TOKEN"):
            TushareSource(token=None)

    def test_get_stock_list_filters_historical_universe(self):
        """Universe on trade_date: listed on/before, not yet delisted (boundary exclusive)."""
        source = make_source_with_stock_basic(
            pd.DataFrame(
                {
                    "ts_code": ["000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ"],
                    "name": ["A", "B", "C", "D"],
                    "industry": ["银行"] * 4,
                    "market": ["主板"] * 4,
                    "exchange": ["SZSE"] * 4,
                    "curr_type": ["CNY"] * 4,
                    # A: listed earlier, never delisted → in
                    # B: lists exactly on trade_date → in
                    # C: delists exactly on trade_date → out (boundary exclusive)
                    # D: lists after trade_date → out
                    "list_date": ["20190101", "20200102", "20180101", "20200103"],
                    "delist_date": [None, "20200103", "20200102", None],
                    "is_hs": ["N", "H", "S", "N"],
                }
            )
        )

        with patch.object(TushareSource._rate_limiter, "acquire", return_value=None):
            df = source.get_stock_list(trade_date="20200102")

        assert set(df["symbol"]) == {"000001.SZ", "000002.SZ"}
        assert (df["date"] == "20200102").all()
        source.pro.stock_basic.assert_called_once_with(
            ts_code=None,
            exchange=None,
            market=None,
            list_status="L,D",
            fields=source._STOCK_LIST_FIELDS,
        )


class TestTushareIntegration:
    """Integration tests using real Tushare API data."""

    @pytest.fixture(autouse=True)
    def setup(self):
        token = os.getenv("TUSHARE_TOKEN")
        if not token:
            pytest.skip("TUSHARE_TOKEN not set")
        self.source = TushareSource(token=token)
        today = date.today()
        start = (today - timedelta(days=30)).strftime("%Y%m%d")
        end = today.strftime("%Y%m%d")
        calendar = self.source.get_calendar(start, end, is_open=True)
        if calendar.empty:
            pytest.skip("No recent trading day available from Tushare")
        self.trade_date = calendar["date"].iloc[-1]

    # -- get_calendar -------------------------------------------------------

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

    # -- get_stock_list -----------------------------------------------------

    def test_get_stock_list(self):
        df = self.source.get_stock_list(trade_date=self.trade_date)
        assert not df.empty
        assert_has_columns(df, STOCK_LIST_COLUMNS)
        assert df["symbol"].is_unique
        assert (
            df["symbol"].str.match(r"^\d{6}\.(SH|SZ|BJ)$").all()
        ), "symbol format should be xxxxxx.SH/SZ/BJ"
        assert df["date"].str.match(DATE_PATTERN).all(), "date not in YYYYMMDD format"
        assert df["is_hs"].isin(["Y", "N"]).all(), "is_hs should only contain Y or N"

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

    # -- get_stock_snapshot -------------------------------------------------

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

    # -- get_stock_daily_bar --------------------------------------------------

    def test_get_stock_daily_bar(self):
        """Well-formed bars for one symbol per market, and for a multi-symbol query."""
        for symbol in ("000001.SZ", "600000.SH"):
            df = self.source.get_stock_daily_bar(
                symbol, "20260101", "20260401", trading_days=57
            )
            assert not df.empty, f"{symbol} returned empty DataFrame"
            assert_has_columns(df, STOCK_DAILY_BAR_COLUMNS)
            assert_daily_bar_sanity(df)

        df = self.source.get_stock_daily_bar(
            "000001.SZ,600000.SH", "20260101", "20260401", trading_days=57
        )
        assert set(df["symbol"]) == {"000001.SZ", "600000.SH"}
        assert_daily_bar_sanity(df)
