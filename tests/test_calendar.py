"""Unit tests for TradingCalendar."""

from datetime import date
from unittest.mock import patch

import pandas as pd
import pytest

from hqdata.calendar import TradingCalendar


def make_calendar_fn(trading_days: list[str]):
    """Return a get_calendar_fn stub that serves the given trading days."""

    def get_calendar_fn(start_date: str, end_date: str, is_open=None):
        days = [d for d in trading_days if start_date <= d <= end_date]
        return pd.DataFrame({"date": days, "is_open": ["Y"] * len(days)})

    return get_calendar_fn


# Trading days around a weekend:
# 20260406(Mon) ... 20260410(Fri) are trading days,
# 20260411(Sat)/20260412(Sun) are not, 20260413(Mon) is.
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
    with patch("hqdata.calendar.date", wraps=date) as mock_date:
        mock_date.today.return_value = date(2026, 4, 10)
        yield TradingCalendar(make_calendar_fn(TRADING_DAYS))


class TestTradingCalendar:
    def test_is_trading_day(self, cal):
        assert cal.is_trading_day("20260410") is True  # Friday
        assert cal.is_trading_day("20260411") is False  # Saturday
        assert cal.is_trading_day("20260412") is False  # Sunday

    def test_get_current_trading_day_on_trading_day(self, cal):
        with patch("hqdata.calendar.date", wraps=date) as mock_date:
            mock_date.today.return_value = date(2026, 4, 10)  # Friday
            assert cal.get_current_trading_day() == "20260410"

    def test_get_current_trading_day_on_weekend(self, cal):
        with patch("hqdata.calendar.date", wraps=date) as mock_date:
            mock_date.today.return_value = date(2026, 4, 12)  # Sunday
            assert cal.get_current_trading_day() == "20260410"  # previous Friday

    def test_next_trading_day(self, cal):
        assert cal.next_trading_day("20260410") == "20260413"  # skip weekend
        assert cal.next_trading_day("20260407") == "20260408"

    def test_previous_trading_day(self, cal):
        assert cal.previous_trading_day("20260413") == "20260410"  # skip weekend
        assert cal.previous_trading_day("20260408") == "20260407"

    def test_count_trading_days(self, cal):
        assert cal.count_trading_days("20260406", "20260413") == 6
        assert cal.count_trading_days("20260411", "20260412") == 0  # weekend only
        assert cal.count_trading_days("20260410", "20260410") == 1  # inclusive bounds
        assert cal.count_trading_days("20260413", "20260406") == 0  # start > end
