"""Tests for ricequant source"""

import os
import re
from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pytest

import hqdata.config  # noqa: F401  加载 .env
from hqdata.sources.ricequant import RicequantSource
from tests.helpers import (
    DATE_PATTERN,
    ETS_PATTERN,
    STOCK_DAILY_BAR_COLUMNS,
    STOCK_LIST_COLUMNS,
    STOCK_MINUTE_BAR_COLUMNS,
    STOCK_SNAPSHOT_COLUMNS,
    assert_daily_bar_sanity,
    assert_has_columns,
    assert_minute_bar_sanity,
)


class TestRicequantSource:
    """Unit tests for RicequantSource."""

    @patch.dict(os.environ, {}, clear=True)
    def test_init_missing_credentials_raises(self):
        """Raises ValueError when neither license_key nor username/password are provided."""
        with pytest.raises(ValueError, match="RQDATA"):
            RicequantSource(username=None, password=None, license_key=None)

    def test_get_stock_list_keeps_future_delisted_on_snapshot_date(self):
        """Historical stock universe should include stocks delisted after trade_date."""
        mock_df = pd.DataFrame(
            {
                "order_book_id": ["002231.XSHE", "000001.XSHE"],
                "symbol": ["*ST奥维", "平安银行"],
                # 002231 is already marked Delisted today, but its de_listed_date
                # (2026-03-27) is after the snapshot date — it must stay in.
                "status": ["Delisted", "Active"],
                "listed_date": ["2008-05-12", "1991-04-03"],
                "de_listed_date": ["2026-03-27", "0000-00-00"],
                "exchange": ["XSHE", "XSHE"],
                "board_type": ["MainBoard", "MainBoard"],
                "industry_name": ["制造业", "货币金融服务"],
            }
        )
        mock_rq = SimpleNamespace(
            all_instruments=lambda type, date: mock_df,
            id_convert=lambda value, to=None: (
                ["002231.SZ", "000001.SZ"] if isinstance(value, list) else value
            ),
        )

        source = RicequantSource.__new__(RicequantSource)
        with (
            patch("hqdata.sources.ricequant._get_rqdatac", return_value=mock_rq),
            patch.object(RicequantSource, "_get_hs_connect_stocks", return_value=set()),
        ):
            df = source.get_stock_list(trade_date="20260105")

        df = df.set_index("symbol")
        assert set(df.index) == {"002231.SZ", "000001.SZ"}
        assert df.loc["002231.SZ", "list_date"] == "20080512"
        assert df.loc["002231.SZ", "delist_date"] == "20260327"
        assert df.loc["000001.SZ", "delist_date"] == ""


class TestRicequantIntegration:
    """Integration tests using real Ricequant API data."""

    @pytest.fixture(autouse=True)
    def setup(self):
        license_key = os.getenv("RQDATA_LICENSE_KEY")
        username = os.getenv("RQDATA_USERNAME")
        password = os.getenv("RQDATA_PASSWORD")
        if not license_key and not (username and password):
            pytest.skip("RQDATA_LICENSE_KEY or RQDATA_USERNAME/RQDATA_PASSWORD not set")
        if license_key:
            self.source = RicequantSource(license_key=license_key)
        else:
            self.source = RicequantSource(username=username, password=password)
        today = date.today()
        start = (today - timedelta(days=30)).strftime("%Y%m%d")
        end = today.strftime("%Y%m%d")
        calendar = self.source.get_calendar(start, end, is_open=True)
        if calendar.empty:
            pytest.skip("No recent trading day available from Ricequant")
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
        assert (
            df["list_date"].str.match(DATE_PATTERN).all()
        ), "list_date should be in YYYYMMDD format"
        non_empty_delist = df.loc[df["delist_date"] != "", "delist_date"]
        assert non_empty_delist.str.match(
            DATE_PATTERN
        ).all(), "non-empty delist_date should be in YYYYMMDD format"
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

    # -- get_stock_minute_bar -------------------------------------------------

    def test_get_stock_minute_bar(self):
        """Well-formed bars for one symbol per market, and for a multi-symbol query."""
        for symbol in ("000001.SZ", "600000.SH"):
            df = self.source.get_stock_minute_bar(symbol, "1m", "20260401", "20260407")
            assert not df.empty, f"{symbol} returned empty DataFrame"
            assert_has_columns(df, STOCK_MINUTE_BAR_COLUMNS)
            assert_minute_bar_sanity(df)

        df = self.source.get_stock_minute_bar(
            "000001.SZ,600000.SH", "1m", "20260401", "20260407"
        )
        assert set(df["symbol"]) == {"000001.SZ", "600000.SH"}

    # -- get_stock_daily_bar --------------------------------------------------

    def test_get_stock_daily_bar(self):
        """Well-formed bars for one symbol per market, and for a multi-symbol query."""
        for symbol in ("000001.SZ", "600000.SH"):
            df = self.source.get_stock_daily_bar(symbol, "20260101", "20260401")
            assert not df.empty, f"{symbol} returned empty DataFrame"
            assert_has_columns(df, STOCK_DAILY_BAR_COLUMNS)
            assert_daily_bar_sanity(df)

        df = self.source.get_stock_daily_bar(
            "000001.SZ,600000.SH", "20260101", "20260401"
        )
        assert set(df["symbol"]) == {"000001.SZ", "600000.SH"}
        assert_daily_bar_sanity(df)
