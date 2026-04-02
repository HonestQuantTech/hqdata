"""Tests for ricequant source"""

import os
import pytest
from unittest.mock import patch, MagicMock
import pandas as pd

import hqdata.config  # 加载 .env
from hqdata.sources.ricequant import RicequantSource


class TestRicequantSource:
    """Unit tests for RicequantSource."""

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

    # get_tick is integration test, moved to TestRicequantIntegration


class TestRicequantIntegration:
    """Integration tests using real Ricequant API data."""

    @pytest.fixture(autouse=True)
    def setup(self):
        token = os.getenv("RQDATA_USERNAME")
        password = os.getenv("RQDATA_PASSWORD")
        if not token or not password:
            pytest.skip("RQDATA_USERNAME or RQDATA_PASSWORD not set")
        self.source = RicequantSource(username=token, password=password)

    def test_get_tick(self):
        """Test get_tick returns valid tick data for a given security."""
        # TODO: Add assertions when Ricequant credentials are available
        pass
