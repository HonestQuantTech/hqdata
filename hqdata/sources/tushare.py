"""Tushare data source adapter"""

import sys
import time
from collections import deque
from typing import Optional
import pandas as pd
from tqdm import tqdm

from hqdata.sources.base import BaseSource


def _get_tushare():
    """Lazy import tushare to support optional installation."""
    try:
        import tushare as ts
    except ImportError:
        raise ImportError(
            "tushare 未安装，hqdata不会默认安装您不一定需要的依赖。请运行：pip install hqdata[tushare]开启对tushare的支持。"
        ) from None
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

    @staticmethod
    def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(
            columns={
                "ts_code": "symbol",
                "trade_date": "date",
                "pct_chg": "pct_change",
                "vol": "volume",
                "amount": "turnover",
            }
        )
        # Tushare daily bar: volume unit is 手(lots), turnover unit is 千元
        # Normalize turnover to 元; volume stays in 手 to match other sources
        if "volume" in df.columns:
            df["volume"] = df["volume"].astype("int64")
        if "turnover" in df.columns:
            df["turnover"] = df["turnover"] * 1000
        return df

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
        trade_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get basic info for stocks.

        Note:
            - Only today's tradable (listed) stocks are returned.

        Args:
            symbol: see README, supports comma-separated multiple codes
            exchange: see README, supports comma-separated multiple exchanges
            board: see README, supports comma-separated multiple codes
            trade_date: snapshot date (YYYYMMDD); injected by api layer, defaults to current trading day

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

        # stock_basic API returns at most 6000 rows per call.
        # If the result hits this limit, data is likely truncated — treat as error.
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
        if len(df) >= 6000:
            print(
                f"[hqdata][tushare] get_stock_list() returned {len(df)} rows which meets or exceeds "
                "the 6000-row API limit — data may be truncated. Returning empty DataFrame."
            )
            return self._empty_stock_list()
        df = self._rename_columns(df).sort_values("symbol")
        df["date"] = trade_date
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

    def get_stock_snapshot(self, symbol: str) -> pd.DataFrame:
        """Get real-time stock snapshot with 5-level order book.

        Args:
            symbol: see README, supports comma-separated multiple codes

        Returns:
            DataFrame with columns: ets, lts, symbol, pre_close, open, high, low, last,
            volume, turnover, ap1~ap5, av1~av5, bp1~bp5, bv1~bv5
        """
        from datetime import datetime

        ts = _get_tushare()
        symbols = [s.strip() for s in symbol.split(",")]
        chunks = [symbols[i : i + 50] for i in range(0, len(symbols), 50)]
        dfs = []
        for chunk in chunks:
            self._rate_limiter.acquire()
            df_chunk = ts.realtime_quote(ts_code=",".join(chunk), src="sina")
            if df_chunk is not None and not df_chunk.empty:
                dfs.append(df_chunk)
        if not dfs:
            return self._empty_stock_snapshot()
        df = pd.concat(dfs, ignore_index=True)
        df = df.rename(
            columns={
                "TS_CODE": "symbol",
                "PRE_CLOSE": "pre_close",
                "OPEN": "open",
                "HIGH": "high",
                "LOW": "low",
                "PRICE": "last",
                "VOLUME": "volume",
                "AMOUNT": "turnover",
                "DATE": "date_raw",
                "TIME": "time_raw",
                "A1_P": "ap1",
                "A2_P": "ap2",
                "A3_P": "ap3",
                "A4_P": "ap4",
                "A5_P": "ap5",
                "A1_V": "av1",
                "A2_V": "av2",
                "A3_V": "av3",
                "A4_V": "av4",
                "A5_V": "av5",
                "B1_P": "bp1",
                "B2_P": "bp2",
                "B3_P": "bp3",
                "B4_P": "bp4",
                "B5_P": "bp5",
                "B1_V": "bv1",
                "B2_V": "bv2",
                "B3_V": "bv3",
                "B4_V": "bv4",
                "B5_V": "bv5",
            }
        )
        # ets: YYYYMMDD + HH:MM:SS → YYYYMMDDTHHMMSSsss
        df["ets"] = (
            df["date_raw"].astype(str)
            + "T"
            + df["time_raw"].str.replace(":", "", regex=False)
            + "000"
        )
        now = datetime.now()
        lts = now.strftime("%Y%m%dT%H%M%S") + f"{now.microsecond // 1000:03d}"
        df["lts"] = lts
        # volume: 股 → 手
        df["volume"] = (df["volume"] / 100).astype("int64")
        cols = [
            "ets",
            "lts",
            "symbol",
            "pre_close",
            "open",
            "high",
            "low",
            "last",
            "volume",
            "turnover",
            "ap1",
            "ap2",
            "ap3",
            "ap4",
            "ap5",
            "av1",
            "av2",
            "av3",
            "av4",
            "av5",
            "bp1",
            "bp2",
            "bp3",
            "bp4",
            "bp5",
            "bv1",
            "bv2",
            "bv3",
            "bv4",
            "bv5",
        ]
        return df[cols].sort_values(["ets", "symbol"]).reset_index(drop=True)

    def get_stock_minute_bar(
        self,
        symbol: str,
        frequency: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        trading_days: Optional[int] = None,
    ) -> pd.DataFrame:
        """Get minute bar data for stocks.

        Note:
            Not implemented for Tushare. The stk_mins API requires additional subscription
            permissions (分钟线独立权限) that are not currently enabled and cannot be tested.
            Once permission is available, refer to the git history for a working batching
            implementation using stk_mins with
            chunk_size = 7900 // (trading_days * bars_per_day).

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(
            "[hqdata][tushare] get_stock_minute_bar 尚未实现：stk_mins API 需要独立的分钟线权限，"
            "当前账户未开通，无法进行测试。权限开通后可参考 git 历史记录中的实现。"
        )

    def get_stock_daily_bar(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        trading_days: Optional[int] = None,
    ) -> pd.DataFrame:
        """Get daily bar data for stocks.

        Args:
            symbol: see README, supports comma-separated multiple codes
            start_date: see README
            end_date: see README
            trading_days: number of trading days in [start_date, end_date]; injected by api layer for batching

        Returns:
            DataFrame with columns: symbol, date, pre_close, open, high, low, close, volume, turnover, change, pct_change
        """
        if trading_days is None:
            return self._empty_stock_daily_bar()
        if trading_days == 0:
            return self._empty_stock_daily_bar()

        symbols = [s.strip() for s in symbol.split(",")]

        # daily API returns at most 6000 rows per call.
        # chunk_size = floor(5900 / trading_days), at least 1
        chunk_size = max(1, 5900 // trading_days)

        chunks = [symbols[i : i + chunk_size] for i in range(0, len(symbols), chunk_size)]
        dfs = []
        for chunk in tqdm(chunks, desc="stock-daily", unit="batch", file=sys.stderr, disable=not sys.stderr.isatty()):
            self._rate_limiter.acquire()
            d = self.pro.daily(
                ts_code=",".join(chunk), start_date=start_date, end_date=end_date
            )
            if d is None or d.empty:
                continue
            if len(d) >= 6000:
                print(
                    f"[hqdata][tushare] daily returned {len(d)} rows which meets or exceeds "
                    "the 6000-row API limit — data may be truncated. Returning empty DataFrame."
                )
                return self._empty_stock_daily_bar()
            dfs.append(d)
        df = pd.concat(dfs, ignore_index=True) if dfs else None

        if df is None or df.empty:
            return self._empty_stock_daily_bar()
        df = self._rename_columns(df).sort_values(["symbol", "date"])
        cols = [
            "symbol",
            "date",
            "pre_close",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "turnover",
            "change",
            "pct_change",
        ]
        return df[cols].reset_index(drop=True)

    def get_index_list(
        self,
        symbol: Optional[str] = None,
        market: Optional[str] = "SSE,SZSE",
        trade_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get basic info about an index or the index info of a market.

        Args:
            symbol: see README, supports comma-separated multiple codes. If provided, market is ignored.
            market: see README, supports comma-separated multiple markets. Defaults to "SSE,SZSE".
            trade_date: snapshot date (YYYYMMDD); injected by api layer, defaults to current trading day

        Returns:
            DataFrame with columns: symbol, date, name, fullname, market, base_date, base_point, list_date
        """
        use_symbol = symbol and symbol.strip()
        use_market = market and market.strip() if not use_symbol else None

        # index_basic API returns at most 8000 rows per call.
        # If the result hits this limit, data is likely truncated — treat as error.
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
        if len(df) >= 8000:
            print(
                f"[hqdata][tushare] get_index_list() returned {len(df)} rows which meets or exceeds "
                "the 8000-row API limit — data may be truncated. Returning empty DataFrame."
            )
            return self._empty_index_list()
        df = self._rename_columns(df).sort_values("symbol")
        df["date"] = trade_date
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
        trading_days: Optional[int] = None,
    ) -> pd.DataFrame:
        """Get minute bar data for an index.

        Note:
            Not implemented for Tushare. The idx_mins API requires additional subscription
            permissions (分钟线独立权限) that are not currently enabled and cannot be tested.
            Once permission is available, refer to the git history for a working batching
            implementation using idx_mins with
            chunk_size = 7900 // (trading_days * bars_per_day).

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(
            "[hqdata][tushare] get_index_minute_bar 尚未实现：idx_mins API 需要独立的分钟线权限，"
            "当前账户未开通，无法进行测试。权限开通后可参考 git 历史记录中的实现。"
        )

    def get_index_daily_bar(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        trading_days: Optional[int] = None,
    ) -> pd.DataFrame:
        """Get daily bar data for an index.

        Args:
            symbol: see README, supports comma-separated multiple codes
            start_date: see README
            end_date: see README
            trading_days: number of trading days in [start_date, end_date]; injected by api layer for batching

        Returns:
            DataFrame with columns: symbol, date, pre_close, open, high, low, close, volume, turnover, change, pct_change
        """
        if trading_days is None:
            return self._empty_index_daily_bar()
        if trading_days == 0:
            return self._empty_index_daily_bar()

        symbols = [s.strip() for s in symbol.split(",")]

        # index_daily API only accepts a single ts_code per call (unlike daily).
        # Iterate symbol by symbol; each call returns at most trading_days rows,
        # so the 8000-row limit is only a concern for very long date ranges.
        dfs = []
        for s in tqdm(symbols, desc="index-daily", unit="symbol", file=sys.stderr, disable=not sys.stderr.isatty()):
            self._rate_limiter.acquire()
            d = self.pro.index_daily(ts_code=s, start_date=start_date, end_date=end_date)
            if d is None or d.empty:
                continue
            if len(d) >= 8000:
                print(
                    f"[hqdata][tushare] index_daily returned {len(d)} rows which meets or exceeds "
                    "the 8000-row API limit — data may be truncated. Returning empty DataFrame."
                )
                return self._empty_index_daily_bar()
            dfs.append(d)
        df = pd.concat(dfs, ignore_index=True) if dfs else None

        if df is None or df.empty:
            return self._empty_index_daily_bar()
        df = self._rename_columns(df).sort_values(["symbol", "date"])
        cols = [
            "symbol",
            "date",
            "pre_close",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "turnover",
            "change",
            "pct_change",
        ]
        return df[cols].reset_index(drop=True)
