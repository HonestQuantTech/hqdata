"""Tests for ricequant source"""

import os
from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pytest

import hqdata.config  # noqa: F401  加载 .env
from hqdata.sources.ricequant import RicequantSource
from tests.helpers import (
    DATE_PATTERN,
    IntegrationTestMixin,
    STOCK_LIST_COLUMNS,
    assert_has_columns,
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
            }
        )
        mock_rq = SimpleNamespace(
            all_instruments=lambda type, date: mock_df,
            id_convert=lambda value, to=None: (
                ["002231.SZ", "000001.SZ"] if isinstance(value, list) else value
            ),
        )

        source = RicequantSource.__new__(RicequantSource)
        with patch("hqdata.sources.ricequant._get_rqdatac", return_value=mock_rq):
            df = source.get_stock_list(trade_date="20260105")

        df = df.set_index("symbol")
        assert set(df.index) == {"002231.SZ", "000001.SZ"}
        assert df.loc["002231.SZ", "list_date"] == "20080512"
        assert df.loc["002231.SZ", "delist_date"] == "20260327"
        assert df.loc["000001.SZ", "delist_date"] == ""


class TestRicequantIntegration(IntegrationTestMixin):
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
