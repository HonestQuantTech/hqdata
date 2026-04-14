"""Tests for hqdata package public API exports."""

import pytest


class TestHqdataPackageImports:
    """Test that hqdata package exposes all public APIs correctly.

    This ensures users who install via pip (importing from the installed package)
    get all documented functions.
    """

    def test_all_public_apis_match_all(self):
        """Verify __all__ declares all functions importable from hqdata package."""
        import hqdata

        assert set(hqdata.__all__) == {
            "init_source",
            "get_calendar",
            "is_trading_day",
            "get_current_trading_day",
            "next_trading_day",
            "previous_trading_day",
            "get_stock_list",
            "get_stock_snapshot",
            "get_stock_minute_bar",
            "get_stock_daily_bar",
            "get_index_list",
            "get_index_minute_bar",
            "get_index_daily_bar",
        }
