"""Ricequant (米筐) data source adapter"""

import os
import warnings
from datetime import date, datetime, timedelta
from typing import Optional
import pandas as pd

from hqdata.sources.base import BaseSource


def _get_rqdatac():
    """Lazy import rqdatac to support optional installation."""
    try:
        import rqdatac as rq
    except ImportError:
        raise ImportError(
            "rqdatac 未安装，hqdata不会默认安装您不一定需要的依赖。请运行：pip install hqdata[ricequant]开启对ricequant的支持。"
        ) from None
    return rq


class RicequantSource(BaseSource):
    """Ricequant data source adapter.

    Requires rqdatac >= 3.1.4 and valid RQData credentials.
    Supports two authentication modes:
    - License key: Set license_key parameter or RQDATA_LICENSE_KEY env var
    - Username/password: Set username/password parameters or RQDATA_USERNAME/RQDATA_PASSWORD env vars
    """

    _EXCHANGE_MAP = {"SSE": "XSHG", "SZE": "XSHE", "BSE": "BJSE"}
    _REVERSE_EXCHANGE_MAP = {v: k for k, v in _EXCHANGE_MAP.items()}

    _MARKET_MAP = {"SSE": "XSHG", "SZE": "XSHE", "BSE": "BJSE"}
    _REVERSE_MARKET_MAP = {v: k for k, v in _MARKET_MAP.items()}

    _BOARD_MAP = {
        "MB": "MainBoard",
        "GEM": "GEM",
        "STAR": "KSH",
        "BSE": "BJS",
    }
    _REVERSE_BOARD_MAP = {v: k for k, v in _BOARD_MAP.items()}

    _MINUTE_FREQ_MAP = {
        "1m": "1m",
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "60m": "60m",
    }

    @staticmethod
    def _normalize_minute_bar(df: pd.DataFrame, rq) -> pd.DataFrame:
        """Convert get_price() minute output to hqdata standard minute bar format."""
        df = df.reset_index()
        df["symbol"] = rq.id_convert(df["order_book_id"].tolist(), to="normal")
        df["date"] = df["datetime"].dt.strftime("%Y%m%d")
        df["ets"] = df["datetime"].dt.strftime("%Y%m%dT%H%M%S") + "000"
        df = df.rename(columns={"total_turnover": "turnover"})
        # rqdatac minute bar: volume unit is 股(shares), normalize to 手(lots)
        if "volume" in df.columns:
            df["volume"] = (df["volume"] / 100).astype("int64")
        cols = [
            "symbol",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "turnover",
            "ets",
        ]
        return df[cols].sort_values(["symbol", "ets"]).reset_index(drop=True)

    @staticmethod
    def _normalize_daily_bar(df: pd.DataFrame, rq) -> pd.DataFrame:
        """Convert get_price() daily output to hqdata standard daily bar format."""
        df = df.reset_index()
        df["symbol"] = rq.id_convert(df["order_book_id"].tolist(), to="normal")
        df["date"] = df["date"].dt.strftime("%Y%m%d")
        df = df.rename(
            columns={"total_turnover": "turnover", "prev_close": "pre_close"}
        )
        if "pre_close" not in df.columns:
            df["pre_close"] = float("nan")
        df["change"] = (df["close"] - df["pre_close"]).round(4)
        pct = (df["change"] / df["pre_close"]) * 100
        df["pct_change"] = pct.replace([float("inf"), float("-inf")], float("nan")).round(4)
        # rqdatac daily bar: volume unit is 股(shares), normalize to 手(lots)
        if "volume" in df.columns:
            df["volume"] = (df["volume"] / 100).astype("int64")
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
        return df[cols].sort_values(["symbol", "date"]).reset_index(drop=True)

    @staticmethod
    def _get_hs_connect_stocks(rq) -> set:
        """Return set of order_book_ids that are HS Connect eligible stocks.

        Fetches data via get_stock_connect('all_connect') for the most recent
        trading date that has available data (walks back up to 60 trading days).
        Returns an empty set if no data is available.
        """
        today = date.today()
        trading_dates = rq.get_trading_dates(
            start_date=today.replace(year=today.year - 1),
            end_date=today,
            market="cn",
        )
        for d in reversed(trading_dates):
            df = rq.get_stock_connect("all_connect", start_date=d, end_date=d)
            if df is not None:
                return set(df.index.get_level_values("order_book_id").unique())
        return set()

    def __init__(
        self,
        username: Optional[str] = None,
        password: Optional[str] = None,
        license_key: Optional[str] = None,
    ):
        """Initialize ricequant connection.

        Supports two authentication modes:
        - License key: Set license_key parameter or RQDATA_LICENSE_KEY env var
        - Username/password: Set username/password parameters or RQDATA_USERNAME/RQDATA_PASSWORD env vars

        Args:
            username: RQData username (email), defaults to RQDATA_USERNAME env var
            password: RQData password/token, defaults to RQDATA_PASSWORD env var
            license_key: RQData license key, defaults to RQDATA_LICENSE_KEY env var
        """
        rq = _get_rqdatac()

        # Try license key first
        resolved_key = license_key or os.environ.get("RQDATA_LICENSE_KEY")
        if resolved_key:
            rq.init(
                username="license",
                password=resolved_key,
                use_zstd=True,
                enable_bjse=True,
            )
            return

        # Fall back to username/password
        username = BaseSource._get_env(
            username,
            "RQDATA_USERNAME",
            "RQData credentials not provided. Set license_key in init_source() "
            "or set RQDATA_LICENSE_KEY environment variable. "
            "Alternatively, set RQDATA_USERNAME and RQDATA_PASSWORD environment variables.",
        )
        password = BaseSource._get_env(
            password,
            "RQDATA_PASSWORD",
            "RQData credentials not provided. Set license_key in init_source() "
            "or set RQDATA_LICENSE_KEY environment variable. "
            "Alternatively, set RQDATA_USERNAME and RQDATA_PASSWORD environment variables.",
        )
        rq.init(
            username,
            password,
            address=("rqdatad-pro.ricequant.com", 16011),
            use_pool=True,
            max_pool_size=1,
            auto_load_plugins=False,
            use_zstd=True,
            enable_bjse=True,
        )

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
        rq = _get_rqdatac()
        start = datetime.strptime(start_date, "%Y%m%d").date()
        end = datetime.strptime(end_date, "%Y%m%d").date()
        trading_dates = set(rq.get_trading_dates(start_date, end_date, market="cn"))
        all_dates, is_open_list = [], []
        cur = start
        while cur <= end:
            all_dates.append(cur.strftime("%Y%m%d"))
            is_open_list.append("Y" if cur in trading_dates else "N")
            cur += timedelta(days=1)
        df = pd.DataFrame({"date": all_dates, "is_open": is_open_list})
        if is_open is not None:
            df = df[df["is_open"] == ("Y" if is_open else "N")].reset_index(drop=True)
        return df

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
        rq = _get_rqdatac()
        df = rq.all_instruments(type="CS", date=trade_date)

        if df is None or df.empty:
            return self._empty_stock_list()

        df = df[df["status"] == "Active"].reset_index(drop=True)

        # Apply filters on raw rqdatac values (before mapping to hqdata conventions)
        if symbol:
            rq_symbols = rq.id_convert([s.strip() for s in symbol.split(",")])
            if isinstance(rq_symbols, str):
                rq_symbols = [rq_symbols]
            df = df[df["order_book_id"].isin(rq_symbols)].reset_index(drop=True)

        if exchange:
            rq_exchanges = [
                self._EXCHANGE_MAP[e.strip()]
                for e in exchange.split(",")
                if e.strip() in self._EXCHANGE_MAP
            ]
            df = df[df["exchange"].isin(rq_exchanges)].reset_index(drop=True)

        if board:
            rq_board_types = [
                self._BOARD_MAP[m.strip()]
                for m in board.split(",")
                if m.strip() in self._BOARD_MAP
            ]
            df = df[df["board_type"].isin(rq_board_types)].reset_index(drop=True)

        if df.empty:
            return self._empty_stock_list()

        hs_stocks = self._get_hs_connect_stocks(rq)
        result = pd.DataFrame(
            {
                "symbol": rq.id_convert(df["order_book_id"].tolist(), to="normal"),
                "date": trade_date,
                "name": df["symbol"].tolist(),
                "exchange": df["exchange"].map(self._REVERSE_EXCHANGE_MAP).tolist(),
                "board": df["board_type"].map(self._REVERSE_BOARD_MAP).tolist(),
                "industry": df["industry_name"].tolist(),
                "curr_type": "CNY",
                "list_date": df["listed_date"].tolist(),
                "delist_date": df["de_listed_date"].tolist(),
                "is_hs": df["order_book_id"]
                .isin(hs_stocks)
                .map({True: "Y", False: "N"})
                .tolist(),
            }
        )
        return result.sort_values("symbol").reset_index(drop=True)

    def get_stock_snapshot(self, symbol: str) -> pd.DataFrame:
        """Get real-time stock snapshot with 5-level order book.

        Args:
            symbol: see README, supports comma-separated multiple codes

        Returns:
            DataFrame with columns: ets, lts, symbol, pre_close, open, high, low, last,
            volume, turnover, ap1~ap5, av1~av5, bp1~bp5, bv1~bv5
        """
        from datetime import datetime

        rq = _get_rqdatac()
        rq_symbols = rq.id_convert([s.strip() for s in symbol.split(",")])
        if isinstance(rq_symbols, str):
            rq_symbols = [rq_symbols]
        ticks = rq.current_snapshot(rq_symbols)
        if not ticks:
            return self._empty_stock_snapshot()
        if not isinstance(ticks, list):
            ticks = [ticks]
        now = datetime.now()
        lts = now.strftime("%Y%m%dT%H%M%S") + f"{now.microsecond // 1000:03d}"
        rows = []
        for tick in ticks:
            row = {
                "ets": tick.datetime.strftime("%Y%m%dT%H%M%S")
                + f"{tick.datetime.microsecond // 1000:03d}",
                "lts": lts,
                "symbol": rq.id_convert(tick.order_book_id, to="normal"),
                "pre_close": tick.prev_close,
                "open": tick.open,
                "high": tick.high,
                "low": tick.low,
                "last": tick.last,
                "volume": int(tick.volume / 100),
                "turnover": tick.total_turnover,
            }
            asks = tick.asks if isinstance(tick.asks, list) else []
            ask_vols = tick.ask_vols if isinstance(tick.ask_vols, list) else []
            bids = tick.bids if isinstance(tick.bids, list) else []
            bid_vols = tick.bid_vols if isinstance(tick.bid_vols, list) else []
            for i in range(5):
                row[f"ap{i+1}"] = asks[i] if i < len(asks) else None
                row[f"av{i+1}"] = int(ask_vols[i] / 100) if i < len(ask_vols) else None
                row[f"bp{i+1}"] = bids[i] if i < len(bids) else None
                row[f"bv{i+1}"] = int(bid_vols[i] / 100) if i < len(bid_vols) else None
            rows.append(row)
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
        return (
            pd.DataFrame(rows, columns=cols)
            .sort_values(["ets", "symbol"])
            .reset_index(drop=True)
        )

    def get_stock_minute_bar(
        self,
        symbol: str,
        frequency: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        trading_days: Optional[int] = None,
    ) -> pd.DataFrame:
        """Get minute bar data for stocks.

        Args:
            symbol: see README, supports comma-separated multiple codes
            frequency: one of "1m", "5m", "15m", "30m", "60m"
            start_date: see README
            end_date: see README
            trading_days: number of trading days in [start_date, end_date]; injected by api layer for batching

        Returns:
            DataFrame with columns: symbol, date, open, high, low, close, volume, turnover, ets
        """
        if frequency not in self._MINUTE_FREQ_MAP:
            raise ValueError(
                f"frequency must be one of {list(self._MINUTE_FREQ_MAP)}, got '{frequency}'"
            )

        rq = _get_rqdatac()
        rq_symbols = rq.id_convert([s.strip() for s in symbol.split(",")])
        if isinstance(rq_symbols, str):
            rq_symbols = [rq_symbols]
        df = rq.get_price(
            rq_symbols,
            start_date=start_date,
            end_date=end_date,
            frequency=self._MINUTE_FREQ_MAP[frequency],
            adjust_type="none",
            expect_df=True,
        )
        if df is None or df.empty:
            return self._empty_stock_minute_bar()
        return self._normalize_minute_bar(df, rq)

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
        rq = _get_rqdatac()
        rq_symbols = rq.id_convert([s.strip() for s in symbol.split(",")])
        if isinstance(rq_symbols, str):
            rq_symbols = [rq_symbols]
        df = rq.get_price(
            rq_symbols,
            start_date=start_date,
            end_date=end_date,
            frequency="1d",
            adjust_type="none",
            expect_df=True,
        )
        if df is None or df.empty:
            return self._empty_stock_daily_bar()
        return self._normalize_daily_bar(df, rq)

    def get_index_list(
        self,
        symbol: Optional[str] = None,
        market: Optional[str] = "SSE,SZE",
        trade_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get basic info about an index or the index info of a market.

        Note:
            - fullname is the same as name (rqdatac does not provide a separate full name).
            - market filter only supports SSE and SZE; CSI/SW/CICC/MSCI/OTH return empty DataFrame.

        Args:
            symbol: see README, supports comma-separated multiple codes. If provided, market is ignored.
            market: see README, supports comma-separated multiple markets. Defaults to "SSE,SZE".
            trade_date: snapshot date (YYYYMMDD); injected by api layer, defaults to current trading day

        Returns:
            DataFrame with columns: symbol, date, name, fullname, market, base_date, base_point, list_date
        """
        use_symbol = symbol and symbol.strip()
        use_market = market and market.strip() if not use_symbol else None

        rq = _get_rqdatac()
        df = rq.all_instruments(type="INDX", date=trade_date)

        if df is None or df.empty:
            return self._empty_index_list()

        df = df.reset_index(drop=True)

        if use_symbol:
            rq_symbols = rq.id_convert([s.strip() for s in symbol.split(",")])
            if isinstance(rq_symbols, str):
                rq_symbols = [rq_symbols]
            df = df[df["order_book_id"].isin(rq_symbols)].reset_index(drop=True)
        elif use_market:
            rq_exchanges = [
                self._MARKET_MAP[m.strip()]
                for m in market.split(",")
                if m.strip() in self._MARKET_MAP
            ]
            if not rq_exchanges:
                return self._empty_index_list()
            df = df[df["exchange"].isin(rq_exchanges)].reset_index(drop=True)

        if df.empty:
            return self._empty_index_list()
        result = pd.DataFrame(
            {
                "symbol": rq.id_convert(df["order_book_id"].tolist(), to="normal"),
                "date": trade_date,
                "name": df["symbol"].tolist(),
                "fullname": df[
                    "symbol"
                ].tolist(),  # rqdatac does not provide a separate full name
                "market": df["exchange"].map(self._REVERSE_MARKET_MAP).tolist(),
                "base_date": df["base_date"].tolist(),
                "base_point": df["base_point"].tolist(),
                "list_date": df["listed_date"].tolist(),
            }
        )
        return result.sort_values("symbol").reset_index(drop=True)

    def get_index_minute_bar(
        self,
        symbol: str,
        frequency: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        trading_days: Optional[int] = None,
    ) -> pd.DataFrame:
        """Get minute bar data for an index.

        Args:
            symbol: see README, supports comma-separated multiple codes
            frequency: one of "1m", "5m", "15m", "30m", "60m"
            start_date: see README
            end_date: see README
            trading_days: number of trading days in [start_date, end_date]; injected by api layer for batching

        Returns:
            DataFrame with columns: symbol, date, open, high, low, close, volume, turnover, ets
        """
        if frequency not in self._MINUTE_FREQ_MAP:
            raise ValueError(
                f"frequency must be one of {list(self._MINUTE_FREQ_MAP)}, got '{frequency}'"
            )

        rq = _get_rqdatac()
        rq_symbols = rq.id_convert([s.strip() for s in symbol.split(",")])
        if isinstance(rq_symbols, str):
            rq_symbols = [rq_symbols]
        rq_symbols = [s for s in rq_symbols if s is not None]
        if not rq_symbols:
            return self._empty_index_minute_bar()
        # 某些指数（如 SSE180.XSHG、SSE50.XSHG）在 rqdatac 内部 validator 会触发
        # UserWarning 或 Exception（invalid order_book_id）。两种情况均返回空。
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=UserWarning, module="rqdatac.validators")
                df = rq.get_price(
                    rq_symbols,
                    start_date=start_date,
                    end_date=end_date,
                    frequency=self._MINUTE_FREQ_MAP[frequency],
                    adjust_type="none",
                    expect_df=True,
                )
        except Exception:
            return self._empty_index_minute_bar()
        if df is None or df.empty:
            return self._empty_index_minute_bar()
        return self._normalize_minute_bar(df, rq)

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
        rq = _get_rqdatac()
        rq_symbols = rq.id_convert([s.strip() for s in symbol.split(",")])
        if isinstance(rq_symbols, str):
            rq_symbols = [rq_symbols]
        rq_symbols = [s for s in rq_symbols if s is not None]
        if not rq_symbols:
            return self._empty_index_daily_bar()
        # 某些指数（如 SSE180.XSHG、SSE50.XSHG）在 rqdatac 内部 validator 会触发
        # UserWarning 或 Exception（invalid order_book_id）。两种情况均返回空。
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=UserWarning, module="rqdatac.validators")
                df = rq.get_price(
                    rq_symbols,
                    start_date=start_date,
                    end_date=end_date,
                    frequency="1d",
                    adjust_type="none",
                    expect_df=True,
                )
        except Exception:
            return self._empty_index_daily_bar()
        if df is None or df.empty:
            return self._empty_index_daily_bar()
        return self._normalize_daily_bar(df, rq)
