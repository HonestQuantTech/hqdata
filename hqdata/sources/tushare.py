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

    _BOARD_MAP = {
        "MB": "主板",
        "GEM": "创业板",
        "STAR": "科创板",
        "BJSE": "北交所",
    }
    _REVERSE_BOARD_MAP = {v: k for k, v in _BOARD_MAP.items()}

    _STOCK_LIST_FIELDS = (
        "ts_code,name,industry,market,exchange,curr_type,list_date,delist_date,is_hs"
    )

    _INDEX_LIST_FIELDS = "ts_code,name,fullname,market,base_date,base_point,list_date"

    _MINUTE_FREQ_MAP = {
        "1m": "1min",
        "5m": "5min",
        "15m": "15min",
        "30m": "30min",
        "60m": "60min",
    }

    @staticmethod
    def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(
            columns={
                "ts_code": "symbol",
                "trade_date": "date",
                "pct_chg": "pct_change",
                "vol": "volume",
                "amount": "turnover",
            }
        )

    @staticmethod
    def _normalize_minute_bar(
        df: pd.DataFrame, symbol_col: str = "ts_code"
    ) -> pd.DataFrame:
        """Convert Tushare minute bar to standard format."""
        df = df.rename(
            columns={
                symbol_col: "symbol",
                "trade_time": "datetime_raw",
                "vol": "volume",
                "amount": "turnover",
            }
        )
        # trade_time format: "2024-01-02 09:31:00"
        df["date"] = df["datetime_raw"].str.replace("-", "").str[:8]
        df["datetime"] = (
            df["datetime_raw"]
            .str.replace("-", "")
            .str.replace(" ", "T")
            .str.replace(":", "")
            + "000"
        )
        cols = [
            "symbol",
            "date",
            "open",
            "close",
            "high",
            "low",
            "volume",
            "turnover",
            "datetime",
        ]
        return df[cols].sort_values(["symbol", "datetime"]).reset_index(drop=True)

    def __init__(self, token: Optional[str] = None):
        """Initialize tushare connection.

        Args:
            token: Tushare token, defaults to TUSHARE_TOKEN env var
        """
        token = BaseSource._get_env(
            token,
            "TUSHARE_TOKEN",
            "Tushare token not provided. Set token in init_source() "
            "or set TUSHARE_TOKEN environment variable.",
        )
        ts = _get_tushare()
        ts.set_token(token)
        self.pro = ts.pro_api()

    def get_calendar(
        self,
        start_date: str,
        end_date: str,
        is_open: Optional[bool] = None,
    ) -> pd.DataFrame:
        """Get trading calendar.

        Args:
            start_date: see README
            end_date: see README
            is_open: see README

        Returns:
            DataFrame with columns: date, is_open
        """
        self._rate_limiter.acquire()
        df = self.pro.trade_cal(start_date=start_date, end_date=end_date)
        if df is None or df.empty:
            return self._empty_calendar()
        df = df.rename(columns={"cal_date": "date"})
        df["is_open"] = df["is_open"].map({1: "Y", 0: "N"})
        if is_open is not None:
            filter_val = "Y" if is_open else "N"
            df = df[df["is_open"] == filter_val]
        return df[["date", "is_open"]].sort_values("date").reset_index(drop=True)

    def get_stock_list(
        self,
        symbol: Optional[str] = None,
        exchange: Optional[str] = None,
        board: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get basic info for stocks.

        Note:
            - Only today's tradable (listed) stocks are returned.

        Args:
            symbol: see README, supports comma-separated multiple codes
            exchange: see README, supports comma-separated multiple exchanges
            board: see README, supports comma-separated multiple codes

        Returns:
            DataFrame with columns: symbol, date, name, exchange, board, industry,
            curr_type, list_date, delist_date, is_hs
        """
        # Map English board abbreviations to Chinese names for tushare API
        if board:
            market_names = []
            for m in board.split(","):
                m = m.strip()
                market_names.append(self._BOARD_MAP.get(m, m))
            board = ",".join(market_names)

        self._rate_limiter.acquire()
        df = self.pro.stock_basic(
            ts_code=symbol,
            exchange=exchange,
            market=board,
            list_status="L",
            fields=self._STOCK_LIST_FIELDS,
        )

        if df is None or df.empty:
            return self._empty_stock_list()
        df = self._rename_columns(df).sort_values("symbol")
        df["date"] = date.today().strftime("%Y%m%d")
        df["market"] = df["market"].map(lambda x: self._REVERSE_BOARD_MAP.get(x, x))
        df["is_hs"] = df["is_hs"].map({"H": "Y", "S": "Y", "N": "N"}).fillna("N")
        df = df.rename(columns={"market": "board"})
        cols = [
            "symbol",
            "date",
            "name",
            "exchange",
            "board",
            "industry",
            "curr_type",
            "list_date",
            "delist_date",
            "is_hs",
        ]
        return df[cols]

    def get_stock_minute_bar(
        self,
        symbol: str,
        frequency: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get minute bar data for stocks.

        Args:
            symbol: see README, supports comma-separated multiple codes
            frequency: one of "1m", "5m", "15m", "30m", "60m"
            start_date: see README
            end_date: see README

        Returns:
            DataFrame with columns: symbol, date, open, close, high, low, volume, turnover, datetime
        """
        if frequency not in self._MINUTE_FREQ_MAP:
            raise ValueError(
                f"frequency must be one of {list(self._MINUTE_FREQ_MAP)}, got '{frequency}'"
            )
        freq = self._MINUTE_FREQ_MAP[frequency]
        start_dt = (
            f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]} 00:00:00"
            if start_date
            else None
        )
        end_dt = (
            f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]} 23:59:59"
            if end_date
            else None
        )

        symbols = [s.strip() for s in symbol.split(",")]
        dfs = []
        for s in symbols:
            self._rate_limiter.acquire()
            d = self.pro.stk_mins(
                ts_code=s, freq=freq, start_date=start_dt, end_date=end_dt
            )
            if d is not None and not d.empty:
                dfs.append(d)
        df = pd.concat(dfs, ignore_index=True) if dfs else None

        if df is None or df.empty:
            return self._empty_stock_minute_bar()
        return self._normalize_minute_bar(df)

    def get_stock_daily_bar(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get daily bar data for stocks.

        Args:
            symbol: see README, supports comma-separated multiple codes
            start_date: see README
            end_date: see README

        Returns:
            DataFrame with columns: symbol, date, open, close, high, low, volume, turnover, pre_close, change, pct_change
        """
        self._rate_limiter.acquire()
        df = self.pro.daily(ts_code=symbol, start_date=start_date, end_date=end_date)

        if df is None or df.empty:
            return self._empty_stock_daily_bar()
        df = self._rename_columns(df).sort_values(["symbol", "date"])
        cols = [
            "symbol",
            "date",
            "open",
            "close",
            "high",
            "low",
            "volume",
            "turnover",
            "pre_close",
            "change",
            "pct_change",
        ]
        return df[cols].reset_index(drop=True)

    def get_index_list(
        self,
        symbol: Optional[str] = None,
        market: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get basic info about an index or the index info of a market.

        Args:
            symbol: see README, supports comma-separated multiple codes. If provided, market is ignored.
            market: see README, supports comma-separated multiple markets.

        Returns:
            DataFrame with columns: symbol, date, name, fullname, market, base_date, base_point, list_date
        """
        use_symbol = symbol and symbol.strip()
        use_market = market and market.strip() if not use_symbol else None

        if use_symbol:
            symbols = [s.strip() for s in symbol.split(",")]
            dfs = []
            for s in symbols:
                self._rate_limiter.acquire()
                df = self.pro.index_basic(ts_code=s, fields=self._INDEX_LIST_FIELDS)
                if df is not None and not df.empty:
                    dfs.append(df)
            df = pd.concat(dfs, ignore_index=True) if dfs else None
        elif use_market:
            markets = [m.strip() for m in market.split(",")]
            dfs = []
            for m in markets:
                self._rate_limiter.acquire()
                df = self.pro.index_basic(market=m, fields=self._INDEX_LIST_FIELDS)
                if df is not None and not df.empty:
                    dfs.append(df)
            df = pd.concat(dfs, ignore_index=True) if dfs else None
        else:
            self._rate_limiter.acquire()
            df = self.pro.index_basic(fields=self._INDEX_LIST_FIELDS)

        if df is None or df.empty:
            return self._empty_index_list()
        df = self._rename_columns(df).sort_values("symbol")
        df["date"] = date.today().strftime("%Y%m%d")
        cols = [
            "symbol",
            "date",
            "name",
            "fullname",
            "market",
            "base_date",
            "base_point",
            "list_date",
        ]
        return df[cols]

    def get_index_minute_bar(
        self,
        symbol: str,
        frequency: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get minute bar data for an index.

        Args:
            symbol: see README, supports comma-separated multiple codes
            frequency: one of "1m", "5m", "15m", "30m", "60m"
            start_date: see README
            end_date: see README

        Returns:
            DataFrame with columns: symbol, date, open, close, high, low, volume, turnover, datetime
        """
        if frequency not in self._MINUTE_FREQ_MAP:
            raise ValueError(
                f"frequency must be one of {list(self._MINUTE_FREQ_MAP)}, got '{frequency}'"
            )
        freq = self._MINUTE_FREQ_MAP[frequency]
        start_dt = (
            f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]} 00:00:00"
            if start_date
            else None
        )
        end_dt = (
            f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]} 23:59:59"
            if end_date
            else None
        )

        symbols = [s.strip() for s in symbol.split(",")]
        dfs = []
        for s in symbols:
            self._rate_limiter.acquire()
            d = self.pro.idx_mins(
                ts_code=s, freq=freq, start_date=start_dt, end_date=end_dt
            )
            if d is not None and not d.empty:
                dfs.append(d)
        df = pd.concat(dfs, ignore_index=True) if dfs else None

        if df is None or df.empty:
            return self._empty_index_minute_bar()
        return self._normalize_minute_bar(df)

    def get_index_daily_bar(
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
            DataFrame with columns: symbol, date, open, close, high, low, volume, turnover, pre_close, change, pct_change
        """
        symbols = [s.strip() for s in symbol.split(",")]
        dfs = []
        for s in symbols:
            self._rate_limiter.acquire()
            d = self.pro.index_daily(
                ts_code=s, start_date=start_date, end_date=end_date
            )
            if d is not None and not d.empty:
                dfs.append(d)
        df = pd.concat(dfs, ignore_index=True) if dfs else None

        if df is None or df.empty:
            return self._empty_index_daily_bar()
        df = self._rename_columns(df).sort_values(["symbol", "date"])
        cols = [
            "symbol",
            "date",
            "open",
            "close",
            "high",
            "low",
            "volume",
            "turnover",
            "pre_close",
            "change",
            "pct_change",
        ]
        return df[cols].reset_index(drop=True)
