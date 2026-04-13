"""Ricequant (米筐) data source adapter"""

import os
from datetime import date, datetime, timedelta
from typing import Optional
import pandas as pd

from hqdata.sources.base import BaseSource


def _get_rqdatac():
    """Lazy import rqdatac to support optional installation."""
    import rqdatac as rq

    return rq


class RicequantSource(BaseSource):
    """Ricequant data source adapter.

    Requires rqdatac >= 3.1.4 and valid RQData credentials.
    Supports two authentication modes:
    - License key: Set license_key parameter or RQDATA_LICENSE_KEY env var
    - Username/password: Set username/password parameters or RQDATA_USERNAME/RQDATA_PASSWORD env vars
    """

    _EXCHANGE_MAP = {"SSE": "XSHG", "SZSE": "XSHE", "BJSE": "BJSE"}
    _REVERSE_EXCHANGE_MAP = {v: k for k, v in _EXCHANGE_MAP.items()}

    _BOARD_MAP = {
        "MB": "MainBoard",
        "GEM": "GEM",
        "STAR": "KSH",
        "BJSE": "BJS",
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
        df["datetime"] = df["datetime"].dt.strftime("%Y%m%dT%H%M%S") + "000"
        df = df.rename(columns={"total_turnover": "turnover"})
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

    @staticmethod
    def _normalize_daily_bar(df: pd.DataFrame, rq) -> pd.DataFrame:
        """Convert get_price() daily output to hqdata standard daily bar format."""
        df = df.reset_index()
        df["symbol"] = rq.id_convert(df["order_book_id"].tolist(), to="normal")
        df["date"] = df["date"].dt.strftime("%Y%m%d")
        df = df.rename(
            columns={"total_turnover": "turnover", "prev_close": "pre_close"}
        )
        df["change"] = (df["close"] - df["pre_close"]).round(4)
        df["pct_change"] = ((df["change"] / df["pre_close"]) * 100).round(4)
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
        rq = _get_rqdatac()
        df = rq.all_instruments(type="CS", date=date.today())

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
                "date": date.today().strftime("%Y%m%d"),
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
    ) -> pd.DataFrame:
        """Get daily bar data for stocks.

        Args:
            symbol: see README, supports comma-separated multiple codes
            start_date: see README
            end_date: see README

        Returns:
            DataFrame with columns: symbol, date, open, close, high, low, volume, turnover, pre_close, change, pct_change
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
        market: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get basic info about an index or the index info of a market.

        Note:
            - fullname is the same as name (rqdatac does not provide a separate full name).
            - market filter only supports SSE and SZSE; CSI/SW/CICC/MSCI/OTH return empty DataFrame.

        Args:
            symbol: see README, supports comma-separated multiple codes. If provided, market is ignored.
            market: see README, supports comma-separated multiple markets.

        Returns:
            DataFrame with columns: symbol, date, name, fullname, market, base_date, base_point, list_date
        """
        use_symbol = symbol and symbol.strip()
        use_market = market and market.strip() if not use_symbol else None

        rq = _get_rqdatac()
        df = rq.all_instruments(type="INDX", date=date.today())

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
                self._EXCHANGE_MAP[m.strip()]
                for m in market.split(",")
                if m.strip() in self._EXCHANGE_MAP
            ]
            if not rq_exchanges:
                return self._empty_index_list()
            df = df[df["exchange"].isin(rq_exchanges)].reset_index(drop=True)

        if df.empty:
            return self._empty_index_list()
        result = pd.DataFrame(
            {
                "symbol": rq.id_convert(df["order_book_id"].tolist(), to="normal"),
                "date": date.today().strftime("%Y%m%d"),
                "name": df["symbol"].tolist(),
                "fullname": df[
                    "symbol"
                ].tolist(),  # rqdatac does not provide a separate full name
                "market": df["exchange"].map(self._REVERSE_EXCHANGE_MAP).tolist(),
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
            return self._empty_index_minute_bar()
        return self._normalize_minute_bar(df, rq)

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
            return self._empty_index_daily_bar()
        return self._normalize_daily_bar(df, rq)
