"""Tushare data source adapter"""

import time
from datetime import date
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
    # Shared across all instances (class-level) so multiple TushareSource objects
    # respect the same global rate limit — intentional singleton-style design.
    _rate_limiter = _RateLimiter(max_calls=200, window_seconds=60.0)

    _MARKET_MAP = {
        "MB": "主板",
        "GEM": "创业板",
        "STAR": "科创板",
        "BJ": "北交所",
    }
    _REVERSE_MARKET_MAP = {v: k for k, v in _MARKET_MAP.items()}
    _STOCK_LIST_FIELDS = (
        "ts_code,name,industry,market,exchange,curr_type,list_date,delist_date,is_hs"
    )

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
            "amount": "turnover",
        })

    def get_stock_list(
        self,
        symbol: Optional[str] = None,
        exchange: Optional[str] = None,
        market: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get basic info for stocks.

        Note:
            - Only today's tradable (listed) stocks are returned.

        Args:
            symbol: see README, supports comma-separated multiple codes
            exchange: see README, supports comma-separated multiple exchanges
            market: Market category，supports comma-separated multiple codes

        Optional Description:
            market: MB(主板),GEM(创业板),STAR(科创板),BJ(北交所)

        Returns:
            DataFrame with columns: symbol, name, industry, market, exchange,
            curr_type, list_date, delist_date, is_hs, date
        """
        # Map English market abbreviations to Chinese names for tushare API
        if market:
            market_names = []
            for m in market.split(","):
                m = m.strip()
                market_names.append(self._MARKET_MAP.get(m, m))
            market = ",".join(market_names)

        self._rate_limiter.acquire()
        df = self.pro.stock_basic(
            ts_code=symbol,
            exchange=exchange,
            market=market,
            list_status="L",
            fields=self._STOCK_LIST_FIELDS,
        )

        if df is None or df.empty:
            return self._empty_stock_list()
        df = self._rename_columns(df).sort_values("symbol")
        df["date"] = date.today().strftime("%Y%m%d")
        # Convert market values from Chinese to English abbreviations
        df["market"] = df["market"].map(lambda x: self._REVERSE_MARKET_MAP.get(x, x))
        # Normalize is_hs: H (沪股通) and S (深股通) → Y; N → N
        df["is_hs"] = df["is_hs"].map({"H": "Y", "S": "Y", "N": "N"}).fillna("N")
        return df

    def get_stock_bar(
        self,
        symbol: str,
        frequency: str = "day",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get K-line/bar data for stocks.

        Args:
            symbol: see README, supports comma-separated multiple codes
            frequency: see README
            start_date: see README
            end_date: see README

        Returns:
            DataFrame with columns: symbol, date, open, high, low, close, pre_close, change, pct_change, volume, turnover
        """
        if frequency != "day":
            raise NotImplementedError(
                f"Tushare only supports 'day' frequency, got '{frequency}'"
            )

        self._rate_limiter.acquire()
        df = self.pro.daily(ts_code=symbol, start_date=start_date, end_date=end_date)

        if df is None or df.empty:
            return self._empty_stock_bar()
        df = self._rename_columns(df).sort_values(["symbol", "date"])
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
            symbol: see README, supports comma-separated multiple codes. If provided, market is ignored.
            market: see README, supports comma-separated multiple markets. Required if symbol is not provided.

        Optional Description:
            market: CSI(中证指数),CICC(中金指数),SSE(上交所指数),SZSE(深交所指数),SW(申万指数),MSCI(MSCI指数),OTH(其他指数)
            
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

        if df is None or df.empty:
            return self._empty_index_list()
        df = self._rename_columns(df).sort_values("symbol")
        return df

    def get_index_bar(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get daily bar data for an index.

        Args:
            symbol: see README, supports comma-separated multiple codes
            start_date: see README
            end_date: see README

        Returns:
            DataFrame with columns: symbol, date, open, high, low, close, pre_close, change, pct_change, volume, turnover
        """
        symbols = [s.strip() for s in symbol.split(",")]
        dfs = []
        for s in symbols:
            self._rate_limiter.acquire()
            d = self.pro.index_daily(ts_code=s, start_date=start_date, end_date=end_date)
            if d is not None and not d.empty:
                dfs.append(d)
        df = pd.concat(dfs, ignore_index=True) if dfs else None

        if df is None or df.empty:
            return self._empty_index_bar()
        df = self._rename_columns(df).sort_values(["symbol", "date"])
        return df
