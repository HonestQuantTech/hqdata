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
        with pytest.raises(ValueError, match="RQDATA"):
            RicequantSource(username=None, password=None)


class TestRicequantIntegration:
    """Integration tests using real Ricequant API data."""

    @pytest.fixture(autouse=True)
    def setup(self):
        token = os.getenv("RQDATA_USERNAME")
        password = os.getenv("RQDATA_PASSWORD")
        if not token or not password:
            pytest.skip("RQDATA_USERNAME or RQDATA_PASSWORD not set")
        self.source = RicequantSource(username=token, password=password)

    def test_get_stock_bar_not_implemented(self):
        """get_stock_bar raises NotImplementedError until Ricequant adapter is implemented."""
        with pytest.raises(NotImplementedError):
            self.source.get_stock_bar("600000.SH", "1day", "20260101", "20260101")
