"""Tushare data source adapter"""

import time
from collections import deque
from typing import Optional
import pandas as pd

from hqdata.sources.base import BaseSource


def _get_tushare():
    """Lazy import tushare to support optional installation."""
    import tushare as ts
    return ts

class _RateLimiter:
    """Sliding window rate limiter for API calls."""

    # TODO 在tushare的所有实现里加入acquire的等待

    def __init__(self, max_calls: int = 200, window_seconds: float = 60.0):
        self.max_calls = max_calls
        self.window_seconds = window_seconds
        self._timestamps: deque[float] = deque()

    def acquire(self) -> None:
        """Block until a call slot is available."""
        while True:
            now = time.time()
            cutoff = now - self.window_seconds
            while self._timestamps and self._timestamps[0] < cutoff:
                self._timestamps.popleft()

            if len(self._timestamps) < self.max_calls:
                self._timestamps.append(now)
                return

            sleep_time = self._timestamps[0] + self.window_seconds - now + 0.01
            if sleep_time > 0:
                time.sleep(sleep_time)

class TushareSource(BaseSource):
    """Tushare data source adapter.

    Requires tushare >= 1.4.29 and valid TUSHARE_TOKEN.
    Token can be set via environment variable TUSHARE_TOKEN.
    """


    # Tushare API allows 200 calls per minute(2000积分以上)
    # Tushare API allows 500 calls per minute(5000积分以上)
    _rate_limiter = _RateLimiter(max_calls=200, window_seconds=60.0)

    def __init__(self, token: Optional[str] = None):
        """Initialize tushare connection.

        Args:
            token: Tushare token, defaults to TUSHARE_TOKEN env var
        """
        token = BaseSource._get_env(
            token, "TUSHARE_TOKEN",
            "Tushare token not provided. Set token in init_source() "
            "or set TUSHARE_TOKEN environment variable."
        )
        ts = _get_tushare()
        ts.set_token(token)
        self.pro = ts.pro_api()

    @staticmethod
    def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns={
            "ts_code": "symbol",
            "trade_date": "date",
            "pct_chg": "pct_change",
            "vol": "volume",
        })

    _STOCK_LIST_FIELDS = (
        "ts_code,name,industry,market,exchange,curr_type,list_status,list_date,delist_date,is_hs"
    )

    def get_stock_list(self, list_status: str = "L") -> pd.DataFrame:
        """Get basic info for stocks.

        Args:
            list_status: Listing status — "L" (listed), "D" (delisted), "P" (suspended)

        Returns:
            DataFrame with columns: symbol, name, industry, market, exchange,
            curr_type, list_status, list_date, delist_date, is_hs
        """
        df = self.pro.stock_basic(list_status=list_status, fields=self._STOCK_LIST_FIELDS)

        if df is not None and not df.empty:
            df = self._rename_columns(df).sort_values("symbol")

        return df

    def get_stock_bar(
        self,
        symbol: str,
        frequency: str = "1day",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get daily bar data for stocks.

        Args:
            symbol: Stock symbol with exchange (e.g., "000001.SZ" or "000001.SZ,600000.SH")
            frequency: Bar frequency ("1day" only, other frequencies not supported)
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format

        Returns:
            DataFrame with columns: symbol, date, open, high, low, close, pre_close, change, pct_change, volume, amount
        """
        if frequency != "1day":
            raise NotImplementedError(
                f"Tushare only supports '1day' frequency, got '{frequency}'"
            )

        df = self.pro.daily(ts_code=symbol, start_date=start_date, end_date=end_date)

        if df is not None and not df.empty:
            df = self._rename_columns(df).sort_values("date")

        return df

    _INDEX_LIST_FIELDS = (
        "ts_code,name,fullname,market,base_date,base_point,list_date"
    )

    def get_index_list(
        self,
        symbol: Optional[str] = None,
        market: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get basic info about an index or the index info of a market.

        Args:
            symbol: Index code with exchange, supports comma-separated multiple codes (e.g., "000300.SH" or "000300.SH,000905.SH")
            market: Index market, supports comma-separated multiple markets (e.g., "CSI" or "CSI,CICC,SSE,SZSE,SW,MSCI,OTH")

        Returns:
            DataFrame with columns: symbol, name, fullname, market, base_date, base_point, list_date
        """
        # Determine whether to query by symbol or by market
        # If symbol is provided (even as comma-separated list), ignore market
        # If symbol is not provided, market must be provided
        use_symbol = symbol and symbol.strip()
        use_market = market and market.strip() if not use_symbol else None

        if not use_symbol and not use_market:
            raise ValueError("At least one of symbol or market must be provided")

        if use_symbol:
            symbols = [s.strip() for s in symbol.split(",")]
            dfs = []
            for s in symbols:
                self._rate_limiter.acquire()
                df = self.pro.index_basic(ts_code=s, fields=self._INDEX_LIST_FIELDS)
                if df is not None and not df.empty:
                    dfs.append(df)
            df = pd.concat(dfs, ignore_index=True) if dfs else None
        else:
            markets = [m.strip() for m in market.split(",")]
            dfs = []
            for m in markets:
                self._rate_limiter.acquire()
                df = self.pro.index_basic(market=m, fields=self._INDEX_LIST_FIELDS)
                if df is not None and not df.empty:
                    dfs.append(df)
            df = pd.concat(dfs, ignore_index=True) if dfs else None

        if df is not None and not df.empty:
            df = self._rename_columns(df).sort_values("symbol")

        return df if df is not None else pd.DataFrame()

    def get_index_bar(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get daily bar data for an index.

        Args:
            symbol: Index code with exchange, supports comma-separated multiple codes (e.g., "000300.SH,000905.SH")
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format

        Returns:
            DataFrame with columns: symbol, date, open, high, low, close, pre_close, change, pct_change, volume, amount
        """
        symbols = [s.strip() for s in symbol.split(",")]

        if len(symbols) == 1:
            self._rate_limiter.acquire()
            df = self.pro.index_daily(ts_code=symbols[0], start_date=start_date, end_date=end_date)
        else:
            dfs = []
            for s in symbols:
                self._rate_limiter.acquire()
                df = self.pro.index_daily(ts_code=s, start_date=start_date, end_date=end_date)
                if df is not None and not df.empty:
                    dfs.append(df)
            df = pd.concat(dfs, ignore_index=True) if dfs else None

        if df is not None and not df.empty:
            df = self._rename_columns(df).sort_values(["symbol", "date"])

        return df if df is not None else pd.DataFrame()
