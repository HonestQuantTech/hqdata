"""Tests for tushare source"""

import os
import pytest
from unittest.mock import patch
import pandas as pd

import hqdata.config  # 加载 .env
from hqdata.sources.tushare import TushareSource
from hqdata import init_source, get_stock_list, get_stock_bar, get_index_bar


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

        assert set(hqdata.__all__) == {"init_source", "get_stock_list", "get_stock_bar", "get_index_bar"}

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
        df = self.source.get_stock_list(list_status="L")
        expected_columns = {"symbol", "name", "industry", "market", "exchange",
                            "curr_type", "list_status", "list_date", "delist_date", "is_hs"}
        excluded_columns = {"ts_code", "area", "fullname", "enname", "cnspell", "act_name", "act_ent_type"}

        assert not df.empty
        assert expected_columns.issubset(df.columns), f"Missing columns: {expected_columns - set(df.columns)}"
        assert not excluded_columns.intersection(df.columns), f"Unexpected columns present: {excluded_columns & set(df.columns)}"
        assert (df["list_status"] == "L").all()
        assert df["symbol"].is_unique

    def test_get_stock_bar(self):
        """Test get_stock_bar returns well-formed data for both markets."""
        expected_columns = {"symbol", "date", "open", "high", "low", "close",
                            "pre_close", "change", "pct_change", "volume", "amount"}

        for symbol in ("000001.SZ", "600000.SH"):
            df = self.source.get_stock_bar(symbol, "1day", "20260101", "20260401")
            assert not df.empty, f"{symbol} returned empty DataFrame"
            assert expected_columns.issubset(df.columns), f"Missing columns: {expected_columns - set(df.columns)}"
            assert (df["high"] >= df["low"]).all(), "high < low found"
            assert (df["high"] >= df["close"]).all(), "high < close found"
            assert (df["low"] <= df["close"]).all(), "low > close found"
            assert (df["volume"] > 0).all(), "non-positive volume found"
            assert (df["amount"] > 0).all(), "non-positive amount found"

    def test_get_index_bar(self):
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

    def test_get_stock_bar_unsupported_frequency(self):
        """Test that unsupported frequency raises NotImplementedError."""
        with pytest.raises(NotImplementedError):
            self.source.get_stock_bar("000001.SZ", "1min", "20260101", "20260101")
