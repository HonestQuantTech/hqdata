"""Tests for tushare source"""

import os
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import hqdata.config  # noqa: F401  加载 .env
from hqdata.sources.tushare import TushareSource
from tests.helpers import (
    DATE_PATTERN,
    IntegrationTestMixin,
    STOCK_LIST_COLUMNS,
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


class TestTushareIntegration(IntegrationTestMixin):
    """Integration tests using real Tushare API data."""

    # get_stock_daily_bar bypasses the api.py layer here, so trading_days
    # (normally injected by api.py) must be passed explicitly.
    _DAILY_BAR_KWARGS = {"trading_days": 57}

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
