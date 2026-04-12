"""Tests for tushare source"""

import os
import pytest
from unittest.mock import patch
import pandas as pd

import hqdata.config
from hqdata.sources.tushare import TushareSource


class TestTushareSource:
    """Unit tests for TushareSource."""

    @patch.dict(os.environ, {}, clear=True)
    def test_init_missing_token_raises(self):
        with pytest.raises(ValueError, match="TUSHARE_TOKEN"):
            TushareSource(token=None)

class TestTushareIntegration:
    """Integration tests using real Tushare API data."""

    @pytest.fixture(autouse=True)
    def setup(self):
        token = os.getenv("TUSHARE_TOKEN")
        if not token:
            pytest.skip("TUSHARE_TOKEN not set")
        self.source = TushareSource(token=token)

    def test_get_calendar(self):
        """Test get_calendar returns well-formed data with all dates."""
        df = self.source.get_calendar("20260101", "20260401")
        expected_columns = {"date", "is_open"}
        assert not df.empty, "get_calendar returned empty DataFrame"
        assert expected_columns.issubset(df.columns), f"Missing columns: {expected_columns - set(df.columns)}"
        assert df["date"].str.match(r"^\d{8}$").all(), "date not in YYYYMMDD format"
        assert df["is_open"].isin(["Y", "N"]).all(), "is_open should only contain Y or N"
        assert len(df) == 91, f"Expected 91 days, got {len(df)}"
        assert df["date"].iloc[0] == "20260101"
        assert df["date"].iloc[-1] == "20260401"

    def test_get_calendar_is_open_true(self):
        """Test get_calendar with is_open=True returns only trading days."""
        df = self.source.get_calendar("20260101", "20260401", is_open=True)
        assert not df.empty, "get_calendar returned empty DataFrame with is_open=True"
        assert (df["is_open"] == "Y").all(), "is_open should be Y for all rows"
        assert df["date"].str.match(r"^\d{8}$").all(), "date not in YYYYMMDD format"
        assert len(df) == 57, f"Expected 57 trading days, got {len(df)}"

    def test_get_calendar_is_open_false(self):
        """Test get_calendar with is_open=False returns only non-trading days."""
        df = self.source.get_calendar("20260101", "20260401", is_open=False)
        assert (df["is_open"] == "N").all(), "is_open should be N for all rows"
        assert df["date"].str.match(r"^\d{8}$").all(), "date not in YYYYMMDD format"
        assert len(df) == 34, f"Expected 34 non-trading days, got {len(df)}"

    def test_get_stock_list(self):
        """Test get_stock_list returns well-formed data for listed stocks."""
        df = self.source.get_stock_list()
        expected_columns = {"symbol", "name", "industry", "board", "exchange",
                            "curr_type", "list_date", "delist_date", "is_hs", "date"}

        assert not df.empty
        assert expected_columns.issubset(df.columns), f"Missing columns: {expected_columns - set(df.columns)}"
        assert df["symbol"].is_unique
        assert df["symbol"].str.match(r"^\d{6}\.(SH|SZ|BJ)$").all(), "symbol format should be xxxxxx.SH/SZ/BJ"
        assert df["date"].str.match(r"^\d{8}$").all(), "date should be in YYYYMMDD format"
        assert df["is_hs"].isin(["Y", "N"]).all(), "is_hs should only contain Y or N"

    def test_get_stock_list_by_single_symbol(self):
        """Test get_stock_list with single symbol filter."""
        df = self.source.get_stock_list(symbol="000001.SZ")
        assert not df.empty, "get_stock_list returned empty DataFrame for symbol=000001.SZ"
        assert len(df) == 1, f"Expected single stock, got {len(df)} rows"
        assert df.iloc[0]["symbol"] == "000001.SZ"

    def test_get_stock_list_by_multiple_symbols(self):
        """Test get_stock_list with comma-separated multiple symbols."""
        df = self.source.get_stock_list(symbol="000001.SZ,600000.SH")
        assert not df.empty, "get_stock_list returned empty DataFrame for multiple symbols"
        assert set(df["symbol"].unique()) == {"000001.SZ", "600000.SH"}

    def test_get_stock_list_by_exchange(self):
        """Test get_stock_list with single exchange filter (SSE)."""
        df = self.source.get_stock_list(exchange="SSE")
        assert not df.empty, "get_stock_list returned empty DataFrame for exchange=SSE"
        assert df["exchange"].str.contains("SSE").all(), "Expected all stocks to be from SSE"

    def test_get_stock_list_by_multiple_exchanges(self):
        """Test get_stock_list with comma-separated multiple exchanges."""
        df = self.source.get_stock_list(exchange="SSE,SZSE")
        assert not df.empty, "get_stock_list returned empty DataFrame for multiple exchanges"
        # Should contain both SSE and SZSE stocks
        has_sse = df["exchange"].str.contains("SSE").any()
        has_szse = df["exchange"].str.contains("SZSE").any()
        assert has_sse and has_szse, "Expected both SSE and SZSE in results"

    def test_get_stock_list_by_board(self):
        """Test get_stock_list with single board filter (MB)."""
        df = self.source.get_stock_list(board="MB")
        assert not df.empty, "get_stock_list returned empty DataFrame for board=MB"
        assert df["board"].str.contains("MB").all(), "Expected all stocks to be from MB"

    def test_get_stock_list_by_multiple_boards(self):
        """Test get_stock_list with comma-separated multiple boards."""
        df = self.source.get_stock_list(board="MB,GEM,STAR")
        assert not df.empty, "get_stock_list returned empty DataFrame for multiple boards"
        has_mb = df["board"].str.contains("MB").any()
        has_gem = df["board"].str.contains("GEM").any()
        has_star = df["board"].str.contains("STAR").any()
        assert has_mb and has_gem and has_star, "Expected MB, GEM and STAR in results"

    def test_get_stock_list_combined_filters(self):
        """Test get_stock_list with multiple filters combined (AND semantics)."""
        # Two params: board + exchange
        df2 = self.source.get_stock_list(board="MB", exchange="SSE")
        assert not df2.empty, "get_stock_list returned empty DataFrame for board=MB,exchange=SSE"
        assert df2["board"].str.contains("MB").all()
        assert df2["exchange"].str.contains("SSE").all()

        # Three params: symbol + board + exchange (all compatible: 000001.SZ is MB on SZSE)
        df3 = self.source.get_stock_list(symbol="000001.SZ", board="MB", exchange="SZSE")
        assert not df3.empty, "get_stock_list returned empty DataFrame for symbol+board+exchange"
        assert "000001.SZ" in df3["symbol"].values
        assert df3["board"].str.contains("MB").all()
        assert df3["exchange"].str.contains("SZSE").all()

    def test_get_stock_daily_bar(self):
        """Test get_stock_daily_bar returns well-formed data for both markets."""
        expected_columns = {"symbol", "date", "open", "close", "high", "low",
                            "pre_close", "change", "pct_change", "volume", "turnover"}

        for symbol in ("000001.SZ", "600000.SH"):
            df = self.source.get_stock_daily_bar(symbol, "20260101", "20260401")
            assert not df.empty, f"{symbol} returned empty DataFrame"
            assert expected_columns.issubset(df.columns), f"Missing columns: {expected_columns - set(df.columns)}"
            assert (df["high"] >= df["low"]).all(), "high < low found"
            assert (df["high"] >= df["close"]).all(), "high < close found"
            assert (df["low"] <= df["close"]).all(), "low > close found"
            assert (df["volume"] > 0).all(), "non-positive volume found"
            assert (df["turnover"] > 0).all(), "non-positive amount found"
            assert df["date"].str.match(r"^\d{8}$").all(), "date not in YYYYMMDD format"

    def test_get_stock_daily_bar_multiple_symbols(self):
        """Test get_stock_daily_bar returns well-formed data for multiple symbols."""
        expected_columns = {"symbol", "date", "open", "close", "high", "low",
                            "pre_close", "change", "pct_change", "volume", "turnover"}

        symbols = "000001.SZ,600000.SH"
        df = self.source.get_stock_daily_bar(symbols, "20260101", "20260401")
        assert not df.empty, f"get_stock_daily_bar returned empty DataFrame for {symbols}"
        assert expected_columns.issubset(df.columns), f"Missing columns: {expected_columns - set(df.columns)}"
        assert set(df["symbol"].unique()) == {"000001.SZ", "600000.SH"}, f"Expected symbols in result: {symbols}"
        assert (df["high"] >= df["low"]).all(), "high < low found"

    def test_get_index_list_single_symbol(self):
        """Test get_index_list with single symbol."""
        expected_columns = {"symbol", "name", "fullname", "market", "base_date", "base_point", "list_date", "date"}

        df = self.source.get_index_list(symbol="000300.SH")
        assert not df.empty, "get_index_list returned empty DataFrame for single symbol"
        assert expected_columns.issubset(df.columns), f"Missing columns: {expected_columns - set(df.columns)}"
        assert len(df) == 1, f"Expected single index, got {len(df)} rows"
        assert df.iloc[0]["symbol"] == "000300.SH"

    def test_get_index_list_multiple_symbols(self):
        """Test get_index_list with comma-separated multiple symbols."""
        expected_columns = {"symbol", "name", "fullname", "market", "base_date", "base_point", "list_date", "date"}

        symbols = "000300.SH,000905.SH"
        df = self.source.get_index_list(symbol=symbols)
        assert not df.empty, f"get_index_list returned empty DataFrame for {symbols}"
        assert expected_columns.issubset(df.columns), f"Missing columns: {expected_columns - set(df.columns)}"
        assert set(df["symbol"].unique()) == {"000300.SH", "000905.SH"}, f"Expected symbols in result: {symbols}"

    def test_get_index_list_single_market(self):
        """Test get_index_list with single market."""
        expected_columns = {"symbol", "name", "fullname", "market", "base_date", "base_point", "list_date", "date"}

        df = self.source.get_index_list(market="SSE")
        assert not df.empty, "get_index_list returned empty DataFrame for SSE market"
        assert expected_columns.issubset(df.columns), f"Missing columns: {expected_columns - set(df.columns)}"
        assert df["symbol"].str.endswith(".SH").all(), "Expected symbols to end with .SH for SSE market"

    def test_get_index_list_multiple_markets(self):
        """Test get_index_list with comma-separated multiple markets."""
        expected_columns = {"symbol", "name", "fullname", "market", "base_date", "base_point", "list_date", "date"}

        markets = "SSE,SZSE"
        df = self.source.get_index_list(market=markets)
        assert not df.empty, f"get_index_list returned empty DataFrame for {markets}"
        assert expected_columns.issubset(df.columns), f"Missing columns: {expected_columns - set(df.columns)}"
        # Result should contain both SSE and SZSE symbols
        has_sh = df["symbol"].str.endswith(".SH").any()
        has_sz = df["symbol"].str.endswith(".SZ").any()
        assert has_sh and has_sz, f"Expected both .SH and .SZ symbols in result for markets: {markets}"

    def test_get_index_list_symbol_ignores_market(self):
        """Test that symbol takes precedence over market (market is ignored when symbol is provided)."""
        expected_columns = {"symbol", "name", "fullname", "market", "base_date", "base_point", "list_date", "date"}

        # When symbol is provided, market should be ignored
        df = self.source.get_index_list(symbol="000300.SH", market="SZSE")
        assert not df.empty, "get_index_list returned empty DataFrame when symbol is provided"
        assert expected_columns.issubset(df.columns), f"Missing columns: {expected_columns - set(df.columns)}"
        assert len(df) == 1, f"Expected single index when symbol provided, got {len(df)} rows"
        assert df.iloc[0]["symbol"] == "000300.SH", "Expected 000300.SH result when symbol is provided"

    def test_get_index_list_without_params(self):
        """Test get_index_list returns all indexes when no params provided."""
        expected_columns = {"symbol", "name", "fullname", "market", "base_date", "base_point", "list_date", "date"}
        df = self.source.get_index_list()
        assert not df.empty, "get_index_list returned empty DataFrame with no params"
        assert expected_columns.issubset(df.columns), f"Missing columns: {expected_columns - set(df.columns)}"

    def test_get_index_daily_bar(self):
        """Test get_index_daily_bar returns well-formed data for major indexes."""
        expected_columns = {"symbol", "date", "open", "close", "high", "low",
                            "pre_close", "change", "pct_change", "volume", "turnover"}

        for symbol in ("000300.SH", "000905.SH", "000852.SH", "932000.CSI"):
            df = self.source.get_index_daily_bar(symbol, "20260101", "20260401")
            assert not df.empty, f"{symbol} returned empty DataFrame"
            assert expected_columns.issubset(df.columns), f"Missing columns: {expected_columns - set(df.columns)}"
            assert (df["high"] >= df["low"]).all(), "high < low found"
            assert (df["high"] >= df["close"]).all(), "high < close found"
            assert (df["low"] <= df["close"]).all(), "low > close found"
            assert (df["volume"] > 0).all(), "non-positive volume found"
            assert (df["turnover"] > 0).all(), "non-positive amount found"

    def test_get_index_daily_bar_multiple_symbols(self):
        """Test get_index_daily_bar returns well-formed data for multiple indexes."""
        expected_columns = {"symbol", "date", "open", "close", "high", "low",
                            "pre_close", "change", "pct_change", "volume", "turnover"}

        symbols = "000300.SH,000905.SH"
        df = self.source.get_index_daily_bar(symbols, "20260101", "20260401")
        assert not df.empty, f"{symbols} returned empty DataFrame"
        assert expected_columns.issubset(df.columns), f"Missing columns: {expected_columns - set(df.columns)}"
        assert set(df["symbol"].unique()) == {"000300.SH", "000905.SH"}, f"Expected symbols in result: {symbols}"


