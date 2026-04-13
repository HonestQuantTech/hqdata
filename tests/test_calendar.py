"""Unit tests for TradingCalendar."""

from unittest.mock import patch
import pandas as pd
import pytest

from hqdata.calendar import TradingCalendar


def make_calendar_fn(trading_days: list[str]):
    """Return a mock get_calendar_fn that returns the given trading days."""

    def get_calendar_fn(start_date: str, end_date: str, is_open=None):
        days = [d for d in trading_days if start_date <= d <= end_date]
        return pd.DataFrame({"date": days, "is_open": [True] * len(days)})

    return get_calendar_fn


# A small set of known trading days around a weekend
# 20260406 Mon, 20260407 Tue, 20260408 Wed, 20260409 Thu, 20260410 Fri
# 20260411 Sat (non-trading), 20260412 Sun (non-trading)
# 20260413 Mon
TRADING_DAYS = [
    "20260406",
    "20260407",
    "20260408",
    "20260409",
    "20260410",
    "20260413",
]


@pytest.fixture
def cal():
    with patch("hqdata.calendar.date") as mock_date:
        mock_date.today.return_value = type(
            "D", (), {"strftime": lambda self, fmt: "20260410"}
        )()
        # Patch timedelta to avoid issues with mock date
        from datetime import timedelta

        mock_date.side_effect = None
        # Use real date for timedelta calculations inside __init__
        from datetime import date as real_date

        mock_date.today.return_value = real_date(2026, 4, 10)
        return TradingCalendar(make_calendar_fn(TRADING_DAYS))


class TestTradingCalendar:
    def test_is_trading_day(self, cal):
        assert cal.is_trading_day("20260410") is True  # Friday
        assert cal.is_trading_day("20260411") is False  # Saturday
        assert cal.is_trading_day("20260412") is False  # Sunday

    def test_get_current_trading_day_on_trading_day(self, cal):
        with patch("hqdata.calendar.date") as mock_date:
            from datetime import date as real_date

            mock_date.today.return_value = real_date(2026, 4, 10)  # Friday
            result = cal.get_current_trading_day()
        assert result == "20260410"

    def test_get_current_trading_day_on_weekend(self, cal):
        with patch("hqdata.calendar.date") as mock_date:
            from datetime import date as real_date

            mock_date.today.return_value = real_date(2026, 4, 12)  # Sunday
            result = cal.get_current_trading_day()
        assert result == "20260410"  # Previous Friday

    def test_next_trading_day(self, cal):
        assert cal.next_trading_day("20260410") == "20260413"  # skip weekend
        assert cal.next_trading_day("20260407") == "20260408"

    def test_previous_trading_day(self, cal):
        assert cal.previous_trading_day("20260413") == "20260410"  # skip weekend
        assert cal.previous_trading_day("20260408") == "20260407"
