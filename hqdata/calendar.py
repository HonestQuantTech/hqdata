"""Trading calendar with local cache and navigation utilities."""

import bisect
from datetime import date, timedelta
from typing import Callable, Optional


class TradingCalendar:
    """Local-cached trading calendar with day navigation.

    Args:
        get_calendar_fn: callable matching signature of BaseSource.get_calendar
    """

    def __init__(self, get_calendar_fn: Callable) -> None:
        self._get_calendar = get_calendar_fn
        today = date.today()
        cache_start = (today - timedelta(days=365 * 10)).strftime("%Y%m%d")
        cache_end = (today + timedelta(days=365)).strftime("%Y%m%d")
        self._cache_start = cache_start
        self._cache_end = cache_end
        self._trading_days: set[str] = set()
        self._sorted_days: list[str] = []
        self._load(cache_start, cache_end)

    def _load(self, start: str, end: str) -> None:
        df = self._get_calendar(start, end, is_open=True)
        new_days = set(df["date"])
        self._trading_days |= new_days
        self._sorted_days = sorted(self._trading_days)

    def _ensure_range(self, d: str) -> None:
        if d < self._cache_start:
            self._load(d, self._cache_start)
            self._cache_start = d
        elif d > self._cache_end:
            self._load(self._cache_end, d)
            self._cache_end = d

    def is_trading_day(self, d: str) -> bool:
        """Return True if d (YYYYMMDD) is a trading day."""
        self._ensure_range(d)
        return d in self._trading_days

    def get_current_trading_day(self) -> str:
        """Return today if it's a trading day, else the most recent trading day."""
        today = date.today().strftime("%Y%m%d")
        self._ensure_range(today)
        if today in self._trading_days:
            return today
        return self.previous_trading_day(today)

    def next_trading_day(self, d: str) -> str:
        """Return the first trading day after d (exclusive)."""
        self._ensure_range(d)
        idx = bisect.bisect_right(self._sorted_days, d)
        if idx >= len(self._sorted_days):
            # Extend cache forward and retry
            new_end = str(int(d[:4]) + 1) + d[4:]
            self._load(self._cache_end, new_end)
            self._cache_end = new_end
            self._sorted_days = sorted(self._trading_days)
            idx = bisect.bisect_right(self._sorted_days, d)
        return self._sorted_days[idx]

    def previous_trading_day(self, d: str) -> str:
        """Return the last trading day before d (exclusive)."""
        self._ensure_range(d)
        idx = bisect.bisect_left(self._sorted_days, d) - 1
        if idx < 0:
            # Extend cache backward and retry
            new_start = str(int(d[:4]) - 1) + d[4:]
            self._load(new_start, self._cache_start)
            self._cache_start = new_start
            self._sorted_days = sorted(self._trading_days)
            idx = bisect.bisect_left(self._sorted_days, d) - 1
        return self._sorted_days[idx]

    def count_trading_days(self, start_date: str, end_date: str) -> int:
        """Return the number of trading days in [start_date, end_date] inclusive."""
        if start_date > end_date:
            return 0
        self._ensure_range(start_date)
        self._ensure_range(end_date)
        lo = bisect.bisect_left(self._sorted_days, start_date)
        hi = bisect.bisect_right(self._sorted_days, end_date)
        return hi - lo
