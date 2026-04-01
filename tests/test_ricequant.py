"""Tests for ricequant source"""

import os
import pytest
from unittest.mock import patch, MagicMock
import pandas as pd

import hqdata.config  # 加载 .env
from hqdata.sources.ricequant import RicequantSource, _to_rq_symbol


class TestToRqSymbol:
    def test_sse_symbol(self):
        assert _to_rq_symbol("600000", "XSHG") == "600000.XSHG"

    def test_szse_symbol(self):
        assert _to_rq_symbol("000001", "XSHE") == "000001.XSHE"


class TestRicequantSource:
    @patch("hqdata.sources.ricequant.init")
    def test_init(self, mock_init):
        username = os.getenv("RQDATA_USERNAME", "test@test.com")
        password = os.getenv("RQDATA_PASSWORD", "token123")
        source = RicequantSource(username=username, password=password)
        mock_init.assert_called_once_with(
            username,
            password,
            address=("rqdatad-pro.ricequant.com", 16011),
            use_pool=True,
            max_pool_size=1,
            auto_load_plugins=False,
        )

    @patch("hqdata.sources.ricequant.get_price")
    @patch("hqdata.sources.ricequant.init")
    def test_get_tick(self, mock_init, mock_get_price):
        username = os.getenv("RQDATA_USERNAME", "test@test.com")
        password = os.getenv("RQDATA_PASSWORD", "token123")
        # Setup mock
        mock_df = pd.DataFrame({"last": [10.5], "volume": [1000]})
        mock_get_price.return_value = mock_df

        source = RicequantSource(username=username, password=password)
        result = source.get_tick("600000", "XSHG", "2026-04-01", "2026-04-02")

        mock_get_price.assert_called_once()
        call_kwargs = mock_get_price.call_args[1]
        assert call_kwargs["frequency"] == "tick"
        assert call_kwargs["start_date"] == "2026-04-01"
        assert call_kwargs["end_date"] == "2026-04-02"
        assert call_kwargs["market"] == "cn"
        assert "last" in call_kwargs["fields"]
        assert "volume" in call_kwargs["fields"]

