"""Tests for akshare source"""

from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pytest

import hqdata.config  # noqa: F401  loads .env
from hqdata.sources.akshare import AkshareSource
from tests.helpers import (
    DATE_PATTERN,
    CalendarIntegrationMixin,
    StockListIntegrationMixin,
    STOCK_LIST_COLUMNS,
    assert_has_columns,
)


def make_mock_akshare(
    sh_main=None,
    sh_star=None,
    sz_list=None,
    bj_list=None,
    sh_delist=None,
    sz_delist=None,
    trade_dates=None,
) -> SimpleNamespace:
    """Build a stand-in for the akshare module exposing only what AkshareSource calls."""
    sh_main = (
        sh_main
        if sh_main is not None
        else pd.DataFrame(columns=["证券代码", "证券简称", "上市日期"])
    )
    sh_star = sh_star if sh_star is not None else sh_main.iloc[0:0]
    sz_list = (
        sz_list
        if sz_list is not None
        else pd.DataFrame(columns=["板块", "A股代码", "A股简称", "A股上市日期"])
    )
    bj_list = (
        bj_list
        if bj_list is not None
        else pd.DataFrame(columns=["证券代码", "证券简称", "上市日期"])
    )
    sh_delist = (
        sh_delist
        if sh_delist is not None
        else pd.DataFrame(columns=["公司代码", "公司简称", "上市日期", "暂停上市日期"])
    )
    sz_delist = (
        sz_delist
        if sz_delist is not None
        else pd.DataFrame(columns=["证券代码", "证券简称", "上市日期", "终止上市日期"])
    )
    trade_dates = (
        trade_dates if trade_dates is not None else pd.DataFrame(columns=["trade_date"])
    )

    def _stock_info_sh_name_code(symbol="主板A股"):
        return sh_main if symbol == "主板A股" else sh_star

    return SimpleNamespace(
        tool_trade_date_hist_sina=lambda: trade_dates,
        stock_info_sh_name_code=_stock_info_sh_name_code,
        stock_info_sz_name_code=lambda symbol="A股列表": sz_list,
        stock_info_bj_name_code=lambda: bj_list,
        stock_info_sh_delist=lambda symbol="全部": sh_delist,
        stock_info_sz_delist=lambda symbol="终止上市公司": sz_delist,
    )


class TestAkshareSource:
    """Unit tests for AkshareSource."""

    def test_init_raises_if_akshare_not_installed(self):
        with patch(
            "hqdata.sources.akshare._get_akshare",
            side_effect=ImportError("akshare is not installed"),
        ):
            with pytest.raises(ImportError):
                AkshareSource()

    # -- get_calendar ---------------------------------------------------

    def test_get_calendar_marks_trading_days(self):
        mock_ak = make_mock_akshare(
            trade_dates=pd.DataFrame(
                {"trade_date": [date(2026, 1, 5), date(2026, 1, 6)]}
            )
        )
        source = AkshareSource.__new__(AkshareSource)
        with patch("hqdata.sources.akshare._get_akshare", return_value=mock_ak):
            df = source.get_calendar("20260105", "20260107")

        assert list(df["date"]) == ["20260105", "20260106", "20260107"]
        assert list(df["is_open"]) == ["Y", "Y", "N"]

    # -- get_stock_list -------------------------------------------------

    def test_get_stock_list_filters_historical_universe(self):
        """Universe on trade_date: listed on/before, not yet delisted (boundary exclusive);
        B-shares excluded; board derived from code prefix for delisted rows."""
        mock_ak = make_mock_akshare(
            sh_main=pd.DataFrame(
                {"证券代码": ["600001"], "证券简称": ["A"], "上市日期": ["2018-01-01"]}
            ),
            sh_star=pd.DataFrame(
                {"证券代码": ["688001"], "证券简称": ["B"], "上市日期": ["2020-01-01"]}
            ),
            sz_list=pd.DataFrame(
                {
                    "板块": ["主板", "创业板"],
                    "A股代码": ["000001", "300001"],
                    "A股简称": ["C", "D"],
                    "A股上市日期": ["2019-01-01", "2021-01-01"],
                }
            ),
            bj_list=pd.DataFrame(
                {"证券代码": ["920001"], "证券简称": ["E"], "上市日期": ["2022-01-01"]}
            ),
            sh_delist=pd.DataFrame(
                {
                    # 600002: delists exactly on trade_date -> excluded (boundary exclusive)
                    # 900001: B-share -> excluded regardless of dates
                    "公司代码": ["600002", "900001"],
                    "公司简称": ["F", "FB"],
                    "上市日期": ["2010-01-01", "2010-01-01"],
                    "暂停上市日期": ["2020-01-01", "2020-01-01"],
                }
            ),
            sz_delist=pd.DataFrame(
                {
                    # 300002: GEM prefix, still active on trade_date -> included
                    # 200001: B-share -> excluded regardless of dates
                    "证券代码": ["300002", "200001"],
                    "证券简称": ["G", "GB"],
                    "上市日期": ["2015-01-01", "2015-01-01"],
                    "终止上市日期": ["2022-01-01", "2022-01-01"],
                }
            ),
        )

        source = AkshareSource.__new__(AkshareSource)
        with patch("hqdata.sources.akshare._get_akshare", return_value=mock_ak):
            df = source.get_stock_list(trade_date="20200101")

        assert set(df["symbol"]) == {
            "600001.SH",
            "688001.SH",
            "000001.SZ",
            "300002.SZ",
        }
        df = df.set_index("symbol")
        assert df.loc["600001.SH", "board"] == "MB"
        assert df.loc["688001.SH", "board"] == "STAR"
        assert df.loc["000001.SZ", "board"] == "MB"
        assert df.loc["300002.SZ", "board"] == "GEM"
        assert (df["date"] == "20200101").all()
        assert (df["curr_type"] == "CNY").all()

    def test_get_stock_list_by_symbol_exchange_board(self):
        mock_ak = make_mock_akshare(
            sh_main=pd.DataFrame(
                {"证券代码": ["600001"], "证券简称": ["A"], "上市日期": ["2018-01-01"]}
            ),
            sz_list=pd.DataFrame(
                {
                    "板块": ["主板"],
                    "A股代码": ["000001"],
                    "A股简称": ["C"],
                    "A股上市日期": ["2019-01-01"],
                }
            ),
        )
        source = AkshareSource.__new__(AkshareSource)
        with patch("hqdata.sources.akshare._get_akshare", return_value=mock_ak):
            df = source.get_stock_list(trade_date="20260101", symbol="000001.SZ")
            assert list(df["symbol"]) == ["000001.SZ"]

            df = source.get_stock_list(trade_date="20260101", exchange="SSE")
            assert list(df["symbol"]) == ["600001.SH"]

            df = source.get_stock_list(trade_date="20260101", board="MB")
            assert set(df["symbol"]) == {"600001.SH", "000001.SZ"}


class TestAkshareUnsupportedMethods:
    """get_stock_daily_bar/get_stock_factor/get_stock_snapshot proved unreliable
    (page-scraping based, no official rate limit) and were dropped — calling
    them should raise NotImplementedError rather than silently returning data."""

    def test_get_stock_daily_bar_raises(self):
        source = AkshareSource.__new__(AkshareSource)
        with pytest.raises(NotImplementedError):
            source.get_stock_daily_bar("000001.SZ", "20260101", "20260110")

    def test_get_stock_factor_raises(self):
        source = AkshareSource.__new__(AkshareSource)
        with pytest.raises(NotImplementedError):
            source.get_stock_factor(trade_date="20260101", symbol="000001.SZ")

    def test_get_stock_snapshot_raises(self):
        source = AkshareSource.__new__(AkshareSource)
        with pytest.raises(NotImplementedError):
            source.get_stock_snapshot("000001.SZ")


class TestAkshareIntegration(CalendarIntegrationMixin, StockListIntegrationMixin):
    """Integration tests using real, free AKShare data (no credentials needed).

    Only get_calendar and get_stock_list are covered — akshare no longer
    supports get_stock_daily_bar/get_stock_factor/get_stock_snapshot.
    """

    @pytest.fixture(autouse=True)
    def setup(self):
        try:
            self.source = AkshareSource()
        except ImportError:
            pytest.skip("akshare not installed")
        today = date.today()
        start = (today - timedelta(days=30)).strftime("%Y%m%d")
        end = today.strftime("%Y%m%d")
        calendar = self.source.get_calendar(start, end, is_open=True)
        if calendar.empty:
            pytest.skip("No recent trading day available from AKShare")
        self.trade_date = calendar["date"].iloc[-1]

    # -- get_stock_list -------------------------------------------------

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
