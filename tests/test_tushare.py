"""Tests for tushare source"""

import os
import pytest
from unittest.mock import patch, MagicMock
import pandas as pd

import hqdata.config  # 加载 .env
from hqdata.sources.tushare import TushareSource


class TestTushareSource:
    """Unit tests for TushareSource."""

    @patch("hqdata.sources.tushare.ts.set_token")
    @patch("hqdata.sources.tushare.ts.pro_api")
    def test_init(self, mock_pro_api, mock_set_token):
        token = os.getenv("TUSHARE_TOKEN", "test_token_123")
        source = TushareSource(token=token)
        mock_set_token.assert_called_once_with(token)
        mock_pro_api.assert_called_once()


class TestTushareIntegration:
    """Integration tests using real Tushare API data.

    Expected data from Tushare for 2026-04-02:
    - 000001.SZ: open=11.15, high=11.32, low=11.13, close=11.27, pre_close=11.15, change=0.12, pct_change=1.0762, vol=1148895.22, amount=1292756.712
    - 600000.SH: open=10.25, high=10.37, low=10.21, close=10.25, pre_close=10.24, change=0.01, pct_change=0.0977, vol=416383.22, amount=427990.04
    """

    @pytest.fixture(autouse=True)
    def setup(self):
        token = os.getenv("TUSHARE_TOKEN")
        if not token:
            pytest.skip("TUSHARE_TOKEN not set")
        self.source = TushareSource(token=token)

    def test_get_bar(self):
        """Test get_bar for both Shanghai and Shenzhen markets."""
        # 深圳 000001.SZ
        df_sz = self.source.get_bar("000001.SZ", "1day", "20260402", "20260402")
        row_sz = df_sz.iloc[0]
        assert row_sz["close"] == 11.27
        assert row_sz["open"] == 11.15
        assert row_sz["high"] == 11.32
        assert row_sz["low"] == 11.13
        assert row_sz["pre_close"] == 11.15
        assert row_sz["change"] == 0.12
        assert abs(row_sz["pct_change"] - 1.0762) < 0.001
        assert abs(row_sz["volume"] - 1148895.22) < 1

        # 上海 600000.SH
        df_sh = self.source.get_bar("600000.SH", "1day", "20260402", "20260402")
        row_sh = df_sh.iloc[0]
        assert row_sh["close"] == 10.25
        assert row_sh["open"] == 10.25
        assert row_sh["high"] == 10.37
        assert row_sh["low"] == 10.21
        assert row_sh["pre_close"] == 10.24
        assert row_sh["change"] == 0.01
        assert abs(row_sh["pct_change"] - 0.0977) < 0.001
        assert abs(row_sh["volume"] - 416383.22) < 1
