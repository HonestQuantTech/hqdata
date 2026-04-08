"""Tests for tushare source"""

import os
import pytest
from unittest.mock import patch
import pandas as pd

import hqdata.config
from hqdata.sources.tushare import TushareSource
from hqdata import init_source, get_stock_list, get_stock_bar, get_index_list, get_index_bar


class TestTushareSource:
    """Unit tests for TushareSource."""

    @patch.dict(os.environ, {}, clear=True)
    def test_init_missing_token_raises(self):
        with pytest.raises(ValueError, match="TUSHARE_TOKEN"):
            TushareSource(token=None)

class TestHqdataPackageImports:
    """Test that hqdata package exposes all public APIs correctly.

    This ensures users who install via pip (importing from the installed package)
    get all documented functions.
    """

    def test_all_public_apis_match_all(self):
        """Verify __all__ declares all functions importable from hqdata package."""
        import hqdata

        assert set(hqdata.__all__) == {"init_source", "get_stock_list", "get_stock_bar", "get_index_list", "get_index_bar"}

class TestTushareIntegration:
    """Integration tests using real Tushare API data."""

    @pytest.fixture(autouse=True)
    def setup(self):
        token = os.getenv("TUSHARE_TOKEN")
        if not token:
            pytest.skip("TUSHARE_TOKEN not set")
        self.source = TushareSource(token=token)

    def test_get_stock_list(self):
        """Test get_stock_list returns well-formed data for listed stocks."""
        df = self.source.get_stock_list()
        expected_columns = {"symbol", "name", "industry", "market", "exchange",
                            "curr_type", "list_status", "list_date", "delist_date", "is_hs", "date"}

        assert not df.empty
        assert expected_columns.issubset(df.columns), f"Missing columns: {expected_columns - set(df.columns)}"
        assert (df["list_status"] == "L").all()
        assert df["symbol"].is_unique
        assert df["date"].str.match(r"^\d{8}$").all(), "date should be in YYYYMMDD format"

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

    def test_get_stock_list_by_market(self):
        """Test get_stock_list with single market filter (MB)."""
        df = self.source.get_stock_list(market="MB")
        assert not df.empty, "get_stock_list returned empty DataFrame for market=STAR"
        assert df["market"].str.contains("MB").all(), "Expected all stocks to be from MB"

    def test_get_stock_list_by_multiple_markets(self):
        """Test get_stock_list with comma-separated multiple markets."""
        df = self.source.get_stock_list(market="MB,GEM,STAR")
        assert not df.empty, "get_stock_list returned empty DataFrame for multiple markets"
        has_star = df["market"].str.contains("MB").any()
        has_gem = df["market"].str.contains("GEM").any()
        has_gem = df["market"].str.contains("STAR").any()
        assert has_star and has_gem, "Expected both MB&GEM&STAR in results"

    def test_get_stock_list_by_list_status(self):
        """Test get_stock_list with list_status filter (D)."""
        df = self.source.get_stock_list(list_status="D")
        assert not df.empty, "get_stock_list(list_status='D') returned empty DataFrame"
        assert (df["list_status"] == "D").all(), "Expected all stocks to have list_status='D'"

    def test_get_stock_list_by_is_hs(self):
        """Test get_stock_list with is_hs filter (H)."""
        df = self.source.get_stock_list(is_hs="H")
        assert not df.empty, "get_stock_list returned empty DataFrame for is_hs=H"
        assert (df["is_hs"] == "H").all(), "Expected all stocks to be H"

    def test_get_stock_list_combined_filters(self):
        """Test get_stock_list with multiple filters combined."""
        df = self.source.get_stock_list(list_status="D", market="MB")
        assert not df.empty, "get_stock_list returned empty DataFrame for combined filters"
        assert (df["list_status"] == "D").all()
        assert df["market"].str.contains("MB").all()

    def test_get_stock_bar_single_symbol(self):
        """Test get_stock_bar returns well-formed data for both markets."""
        expected_columns = {"symbol", "date", "open", "high", "low", "close",
                            "pre_close", "change", "pct_change", "volume", "amount"}

        for symbol in ("000001.SZ", "600000.SH"):
            df = self.source.get_stock_bar(symbol, "day", "20260101", "20260401")
            assert not df.empty, f"{symbol} returned empty DataFrame"
            assert expected_columns.issubset(df.columns), f"Missing columns: {expected_columns - set(df.columns)}"
            assert (df["high"] >= df["low"]).all(), "high < low found"
            assert (df["high"] >= df["close"]).all(), "high < close found"
            assert (df["low"] <= df["close"]).all(), "low > close found"
            assert (df["volume"] > 0).all(), "non-positive volume found"
            assert (df["amount"] > 0).all(), "non-positive amount found"

    def test_get_stock_bar_multiple_symbols(self):
        """Test get_stock_bar returns well-formed data for multiple symbols."""
        expected_columns = {"symbol", "date", "open", "high", "low", "close",
                            "pre_close", "change", "pct_change", "volume", "amount"}

        symbols = "000001.SZ,600000.SH"
        df = self.source.get_stock_bar(symbols, "day", "20260101", "20260401")
        assert not df.empty, f"get_stock_bar returned empty DataFrame for {symbols}"
        assert expected_columns.issubset(df.columns), f"Missing columns: {expected_columns - set(df.columns)}"
        assert set(df["symbol"].unique()) == {"000001.SZ", "600000.SH"}, f"Expected symbols in result: {symbols}"
        assert (df["high"] >= df["low"]).all(), "high < low found"
        assert (df["high"] >= df["close"]).all(), "high < close found"
        assert (df["low"] <= df["close"]).all(), "low > close found"

    def test_get_stock_bar_unsupported_frequency(self):
        """Test that unsupported frequency raises NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.source.get_stock_bar("000001.SZ", "1min", "20260101", "20260401")

    def test_get_index_list_single_symbol(self):
        """Test get_index_list with single symbol."""
        expected_columns = {"symbol", "name", "fullname", "market", "base_date", "base_point", "list_date"}

        df = self.source.get_index_list(symbol="000300.SH")
        assert not df.empty, "get_index_list returned empty DataFrame for single symbol"
        assert expected_columns.issubset(df.columns), f"Missing columns: {expected_columns - set(df.columns)}"
        assert len(df) == 1, f"Expected single index, got {len(df)} rows"
        assert df.iloc[0]["symbol"] == "000300.SH"

    def test_get_index_list_multiple_symbols(self):
        """Test get_index_list with comma-separated multiple symbols."""
        expected_columns = {"symbol", "name", "fullname", "market", "base_date", "base_point", "list_date"}

        symbols = "000300.SH,000905.SH"
        df = self.source.get_index_list(symbol=symbols)
        assert not df.empty, f"get_index_list returned empty DataFrame for {symbols}"
        assert expected_columns.issubset(df.columns), f"Missing columns: {expected_columns - set(df.columns)}"
        assert set(df["symbol"].unique()) == {"000300.SH", "000905.SH"}, f"Expected symbols in result: {symbols}"

    def test_get_index_list_single_market(self):
        """Test get_index_list with single market."""
        expected_columns = {"symbol", "name", "fullname", "market", "base_date", "base_point", "list_date"}

        df = self.source.get_index_list(market="SSE")
        assert not df.empty, "get_index_list returned empty DataFrame for SSE market"
        assert expected_columns.issubset(df.columns), f"Missing columns: {expected_columns - set(df.columns)}"
        assert df["symbol"].str.endswith(".SH").all(), "Expected symbols to end with .SH for SSE market"

    def test_get_index_list_multiple_markets(self):
        """Test get_index_list with comma-separated multiple markets."""
        expected_columns = {"symbol", "name", "fullname", "market", "base_date", "base_point", "list_date"}

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
        expected_columns = {"symbol", "name", "fullname", "market", "base_date", "base_point", "list_date"}

        # When symbol is provided, market should be ignored
        df = self.source.get_index_list(symbol="000300.SH", market="SZSE")
        assert not df.empty, "get_index_list returned empty DataFrame when symbol is provided"
        assert expected_columns.issubset(df.columns), f"Missing columns: {expected_columns - set(df.columns)}"
        assert len(df) == 1, f"Expected single index when symbol provided, got {len(df)} rows"
        assert df.iloc[0]["symbol"] == "000300.SH", "Expected 000300.SH result when symbol is provided"

    def test_get_index_list_error_without_params(self):
        """Test that ValueError is raised when neither symbol nor market is provided."""
        with pytest.raises(ValueError, match="At least one of symbol or market must be provided"):
            self.source.get_index_list()

    def test_get_index_bar_single_symbol(self):
        """Test get_index_bar returns well-formed data for major indexes."""
        expected_columns = {"symbol", "date", "open", "high", "low", "close",
                            "pre_close", "change", "pct_change", "volume", "amount"}

        for symbol in ("000300.SH", "000905.SH", "000852.SH", "932000.CSI"):
            df = self.source.get_index_bar(symbol, "20260101", "20260401")
            print(df)
            assert not df.empty, f"{symbol} returned empty DataFrame"
            assert expected_columns.issubset(df.columns), f"Missing columns: {expected_columns - set(df.columns)}"
            assert (df["high"] >= df["low"]).all(), "high < low found"
            assert (df["high"] >= df["close"]).all(), "high < close found"
            assert (df["low"] <= df["close"]).all(), "low > close found"
            assert (df["volume"] > 0).all(), "non-positive volume found"
            assert (df["amount"] > 0).all(), "non-positive amount found"

    def test_get_index_bar_multiple_symbols(self):
        """Test get_index_bar returns well-formed data for multiple indexes."""
        expected_columns = {"symbol", "date", "open", "high", "low", "close",
                            "pre_close", "change", "pct_change", "volume", "amount"}

        symbols = "000300.SH,000905.SH"
        df = self.source.get_index_bar(symbols, "20260101", "20260401")
        assert not df.empty, f"{symbols} returned empty DataFrame"
        assert expected_columns.issubset(df.columns), f"Missing columns: {expected_columns - set(df.columns)}"
        assert set(df["symbol"].unique()) == {"000300.SH", "000905.SH"}, f"Expected symbols in result: {symbols}"

