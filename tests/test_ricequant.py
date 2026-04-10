"""Tests for ricequant source"""

import os
import pytest
from unittest.mock import patch

import hqdata.config  # 加载 .env
from hqdata.sources.ricequant import RicequantSource


class TestRicequantSource:
    """Unit tests for RicequantSource."""

    @patch.dict(os.environ, {}, clear=True)
    def test_init_missing_credentials_raises(self):
        """Raises ValueError when neither license_key nor username/password are provided."""
        with pytest.raises(ValueError, match="RQDATA"):
            RicequantSource(username=None, password=None, license_key=None)


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

    def test_get_stock_bar_single_symbol(self):
        """Test get_stock_bar returns well-formed data for both markets."""
        expected_columns = {"symbol", "date", "open", "high", "low", "close",
                            "pre_close", "change", "pct_change", "volume", "turnover"}

        for symbol in ("000001.SZ", "600000.SH"):
            df = self.source.get_stock_bar(symbol, "day", "20260101", "20260401")
            assert not df.empty, f"{symbol} returned empty DataFrame"
            assert expected_columns.issubset(df.columns), f"Missing columns: {expected_columns - set(df.columns)}"
            assert (df["high"] >= df["low"]).all(), "high < low found"
            assert (df["high"] >= df["close"]).all(), "high < close found"
            assert (df["low"] <= df["close"]).all(), "low > close found"
            assert (df["volume"] > 0).all(), "non-positive volume found"
            assert (df["turnover"] > 0).all(), "non-positive turnover found"
            assert df["date"].str.match(r"^\d{8}$").all(), "date not in YYYYMMDD format"

    def test_get_stock_bar_multiple_symbols(self):
        """Test get_stock_bar returns well-formed data for multiple symbols."""
        expected_columns = {"symbol", "date", "open", "high", "low", "close",
                            "pre_close", "change", "pct_change", "volume", "turnover"}

        symbols = "000001.SZ,600000.SH"
        df = self.source.get_stock_bar(symbols, "day", "20260101", "20260401")
        assert not df.empty, f"get_stock_bar returned empty DataFrame for {symbols}"
        assert expected_columns.issubset(df.columns), f"Missing columns: {expected_columns - set(df.columns)}"
        assert set(df["symbol"].unique()) == {"000001.SZ", "600000.SH"}, f"Expected symbols in result: {symbols}"
        assert (df["high"] >= df["low"]).all(), "high < low found"
        assert (df["high"] >= df["close"]).all(), "high < close found"
        assert (df["low"] <= df["close"]).all(), "low > close found"

    def test_get_stock_bar_unsupported_frequency(self):
        """Test that unsupported frequencies raise NotImplementedError.

        Note: rqdatac does not support 'tick' and 'month'.
        Unlike tushare (day only), rqdatac additionally supports 'week' and intraday frequencies.
        """
        for freq in ("tick", "month"):
            with pytest.raises(NotImplementedError):
                self.source.get_stock_bar("000001.SZ", freq, "20260101", "20260401")

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
        """Test get_index_list with single market.

        Note: rqdatac only supports SSE and SZSE markets;
        CSI, SW, CICC, MSCI, OTH are not supported and return empty — unlike tushare.
        Note: A small number of SSE indexes get a non-.SH suffix after id_convert (e.g. .WI),
        so the .SH check uses .any() instead of .all() — unlike tushare which uses .all().
        """
        expected_columns = {"symbol", "name", "fullname", "market", "base_date", "base_point", "list_date", "date"}

        df = self.source.get_index_list(market="SSE")
        assert not df.empty, "get_index_list returned empty DataFrame for SSE"
        assert expected_columns.issubset(df.columns), f"Missing columns: {expected_columns - set(df.columns)}"
        assert df["symbol"].str.endswith(".SH").any(), "Expected at least some .SH symbols for SSE"
        assert not df["symbol"].str.endswith(".SZ").any(), "SSE should not contain .SZ symbols"

    def test_get_index_list_multiple_markets(self):
        """Test get_index_list with comma-separated multiple markets.

        Note: rqdatac only supports SSE and SZSE; CSI, SW, CICC, MSCI, OTH return empty.
        """
        expected_columns = {"symbol", "name", "fullname", "market", "base_date", "base_point", "list_date", "date"}

        markets = "SSE,SZSE"
        df = self.source.get_index_list(market=markets)
        assert not df.empty, f"get_index_list returned empty DataFrame for {markets}"
        assert expected_columns.issubset(df.columns), f"Missing columns: {expected_columns - set(df.columns)}"
        has_sh = df["symbol"].str.endswith(".SH").any()
        has_sz = df["symbol"].str.endswith(".SZ").any()
        assert has_sh and has_sz, f"Expected both .SH and .SZ symbols in result for markets: {markets}"

    def test_get_index_list_symbol_ignores_market(self):
        """Test that symbol takes precedence over market (market is ignored when symbol is provided)."""
        expected_columns = {"symbol", "name", "fullname", "market", "base_date", "base_point", "list_date", "date"}

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

    def test_get_index_bar_single_symbol(self):
        """Test get_index_bar returns well-formed data for major indexes.

        Note: rqdatac does not support CSI-suffixed indexes (e.g. 932000.CSI) via id_convert,
        so only SSE/SZSE indexes are tested here — unlike tushare which also supports CSI indexes.
        """
        expected_columns = {"symbol", "date", "open", "high", "low", "close",
                            "pre_close", "change", "pct_change", "volume", "turnover"}

        for symbol in ("000300.SH", "000905.SH"):
            df = self.source.get_index_bar(symbol, "20260101", "20260401")
            assert not df.empty, f"{symbol} returned empty DataFrame"
            assert expected_columns.issubset(df.columns), f"Missing columns: {expected_columns - set(df.columns)}"
            assert (df["high"] >= df["low"]).all(), "high < low found"
            assert (df["high"] >= df["close"]).all(), "high < close found"
            assert (df["low"] <= df["close"]).all(), "low > close found"
            assert (df["volume"] > 0).all(), "non-positive volume found"
            assert (df["turnover"] > 0).all(), "non-positive turnover found"
            assert df["date"].str.match(r"^\d{8}$").all(), "date not in YYYYMMDD format"

    def test_get_index_bar_multiple_symbols(self):
        """Test get_index_bar returns well-formed data for multiple indexes."""
        expected_columns = {"symbol", "date", "open", "high", "low", "close",
                            "pre_close", "change", "pct_change", "volume", "turnover"}

        symbols = "000300.SH,000905.SH"
        df = self.source.get_index_bar(symbols, "20260101", "20260401")
        assert not df.empty, f"{symbols} returned empty DataFrame"
        assert expected_columns.issubset(df.columns), f"Missing columns: {expected_columns - set(df.columns)}"
        assert set(df["symbol"].unique()) == {"000300.SH", "000905.SH"}, f"Expected symbols in result: {symbols}"
