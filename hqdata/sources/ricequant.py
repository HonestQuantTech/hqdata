"""Ricequant (米筐) data source adapter"""

import os
from datetime import date
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

    _FREQUENCY_MAP = {
        'day': '1d', 'week': '1w',
        '1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m', '60m': '60m',
    }

    _BOARD_TYPE_MAP = {'MainBoard': 'MB', 'SME': 'MB', 'GEM': 'GEM', 'KSH': 'STAR'}
    _EXCHANGE_MAP = {'XSHG': 'SSE', 'XSHE': 'SZSE'}
    _REVERSE_EXCHANGE_MAP = {'SSE': 'XSHG', 'SZSE': 'XSHE'}

    # hqdata market → rqdatac board_type values
    _MARKET_FILTER_MAP = {
        'MB': ['MainBoard', 'SME'],
        'GEM': ['GEM'],
        'STAR': ['KSH'],
        'BJ': [],  # Not supported in rqdatac
    }

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
            rq.init(username="license", password=resolved_key)
            return

        # Fall back to username/password
        username = BaseSource._get_env(
            username, "RQDATA_USERNAME",
            "RQData credentials not provided. Set license_key in init_source() "
            "or set RQDATA_LICENSE_KEY environment variable. "
            "Alternatively, set RQDATA_USERNAME and RQDATA_PASSWORD environment variables."
        )
        password = BaseSource._get_env(
            password, "RQDATA_PASSWORD",
            "RQData credentials not provided. Set license_key in init_source() "
            "or set RQDATA_LICENSE_KEY environment variable. "
            "Alternatively, set RQDATA_USERNAME and RQDATA_PASSWORD environment variables."
        )
        rq.init(
            username,
            password,
            address=("rqdatad-pro.ricequant.com", 16011),
            use_pool=True,
            max_pool_size=1,
            auto_load_plugins=False,
        )

    @staticmethod
    def _normalize_bar(df: pd.DataFrame, rq) -> pd.DataFrame:
        """Convert get_price() output to hqdata standard bar format.

        get_price with a list of order_book_ids returns a MultiIndex DataFrame
        with (order_book_id, date) as index. This method flattens it and
        normalizes all column names and values to hqdata conventions.
        """
        df = df.reset_index()
        df['symbol'] = rq.id_convert(df['order_book_id'].tolist(), to='normal')
        df['date'] = df['date'].dt.strftime('%Y%m%d')
        df = df.rename(columns={'total_turnover': 'turnover', 'prev_close': 'pre_close'})
        df['change'] = (df['close'] - df['pre_close']).round(4)
        df['pct_change'] = ((df['change'] / df['pre_close']) * 100).round(4)
        cols = ['symbol', 'date', 'open', 'high', 'low', 'close',
                'pre_close', 'change', 'pct_change', 'volume', 'turnover']
        return df[cols].sort_values(['symbol', 'date']).reset_index(drop=True)

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
            market='cn',
        )
        for d in reversed(trading_dates):
            df = rq.get_stock_connect('all_connect', start_date=d, end_date=d)
            if df is not None:
                return set(df.index.get_level_values('order_book_id').unique())
        return set()

    def get_stock_list(
        self,
        symbol: Optional[str] = None,
        exchange: Optional[str] = None,
        market: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get basic info for stocks.

        Note:
            - BJ (Beijing Stock Exchange) stocks are not supported.
            - Only today's tradable (listed) stocks are returned.

        Args:
            symbol: see README, supports comma-separated multiple codes
            exchange: see README, supports comma-separated multiple exchanges
            market: Market category, supports comma-separated multiple codes

        Optional Description:
            market: MB(主板), GEM(创业板), STAR(科创板), BJ(北交所, unsupported)

        Returns:
            DataFrame with columns: symbol, name, industry, market, exchange,
            curr_type, list_date, delist_date, is_hs, date
        """
        rq = _get_rqdatac()
        df = rq.all_instruments(type='CS', date=date.today())

        if df is None or df.empty:
            return self._empty_stock_list()

        df = df[df['status'] == 'Active'].reset_index(drop=True)

        # Apply filters on raw rqdatac values (before mapping to hqdata conventions)
        if symbol:
            rq_symbols = rq.id_convert([s.strip() for s in symbol.split(",")])
            if isinstance(rq_symbols, str):
                rq_symbols = [rq_symbols]
            df = df[df['order_book_id'].isin(rq_symbols)].reset_index(drop=True)

        if exchange:
            rq_exchanges = [
                self._REVERSE_EXCHANGE_MAP[e.strip()]
                for e in exchange.split(",")
                if e.strip() in self._REVERSE_EXCHANGE_MAP
            ]
            df = df[df['exchange'].isin(rq_exchanges)].reset_index(drop=True)

        if market:
            rq_board_types: list = []
            for m in market.split(","):
                rq_board_types.extend(self._MARKET_FILTER_MAP.get(m.strip(), []))
            df = df[df['board_type'].isin(rq_board_types)].reset_index(drop=True)

        if df.empty:
            return self._empty_stock_list()

        hs_stocks = self._get_hs_connect_stocks(rq)
        result = pd.DataFrame({
            'symbol': rq.id_convert(df['order_book_id'].tolist(), to='normal'),
            'name': df['symbol'].tolist(),
            'industry': df['industry_name'].tolist(),
            'market': df['board_type'].map(self._BOARD_TYPE_MAP).tolist(),
            'exchange': df['exchange'].map(self._EXCHANGE_MAP).tolist(),
            'curr_type': 'CNY',
            'list_date': df['listed_date'].tolist(),
            'delist_date': df['de_listed_date'].tolist(),
            'is_hs': df['order_book_id'].isin(hs_stocks).map({True: 'Y', False: 'N'}).tolist(),
            'date': date.today().strftime('%Y%m%d'),
        })
        return result.sort_values('symbol').reset_index(drop=True)

    def get_stock_bar(
        self,
        symbol: str,
        frequency: str = "day",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get K-line/bar data for stocks.

        Note: 'month' and 'tick' frequencies are not supported by rqdatac.

        Args:
            symbol: see README, supports comma-separated multiple codes
            frequency: see README (supported: day, week, 1m, 5m, 15m, 30m, 60m)
            start_date: see README
            end_date: see README

        Returns:
            DataFrame with columns: symbol, date, open, high, low, close, pre_close,
            change, pct_change, volume, turnover
        """
        if frequency not in self._FREQUENCY_MAP:
            raise NotImplementedError(
                f"Ricequant does not support '{frequency}' frequency. "
                f"Supported frequencies: {list(self._FREQUENCY_MAP.keys())}"
            )

        rq = _get_rqdatac()
        rq_symbols = rq.id_convert([s.strip() for s in symbol.split(",")])
        if isinstance(rq_symbols, str):
            rq_symbols = [rq_symbols]

        df = rq.get_price(
            rq_symbols, start_date=start_date, end_date=end_date,
            frequency=self._FREQUENCY_MAP[frequency],
            adjust_type='none', expect_df=True,
        )

        if df is None or df.empty:
            return self._empty_stock_bar()
        return self._normalize_bar(df, rq)

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
            market: SSE(上交所) or SZSE(深交所) only. Required if symbol is not provided.

        Optional Description:
            market: SSE(上交所指数) and SZSE(深交所指数) are supported.
                    CSI, CICC, SW, MSCI, OTH are not supported.

        Returns:
            DataFrame with columns: symbol, name, fullname, market, base_date, base_point, list_date
        """
        use_symbol = symbol and symbol.strip()
        use_market = market and market.strip() if not use_symbol else None

        if not use_symbol and not use_market:
            raise ValueError("At least one of symbol or market must be provided")

        rq = _get_rqdatac()
        df = rq.all_instruments(type='INDX', date=date.today())

        if df is None or df.empty:
            return self._empty_index_list()

        df = df.reset_index(drop=True)

        if use_symbol:
            rq_symbols = rq.id_convert([s.strip() for s in symbol.split(",")])
            if isinstance(rq_symbols, str):
                rq_symbols = [rq_symbols]
            df = df[df['order_book_id'].isin(rq_symbols)].reset_index(drop=True)
        else:
            rq_exchanges = [
                self._REVERSE_EXCHANGE_MAP[m.strip()]
                for m in market.split(",")
                if m.strip() in self._REVERSE_EXCHANGE_MAP
            ]
            if not rq_exchanges:
                return self._empty_index_list()
            df = df[df['exchange'].isin(rq_exchanges)].reset_index(drop=True)

        if df.empty:
            return self._empty_index_list()
        result = pd.DataFrame({
            'symbol': rq.id_convert(df['order_book_id'].tolist(), to='normal'),
            'name': df['symbol'].tolist(),
            'fullname': df['symbol'].tolist(),  # rqdatac does not provide a separate full name
            'market': df['exchange'].map(self._EXCHANGE_MAP).tolist(),
            'base_date': df['base_date'].tolist(),
            'base_point': df['base_point'].tolist(),
            'list_date': df['listed_date'].tolist(),
        })
        return result.sort_values('symbol').reset_index(drop=True)

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
            DataFrame with columns: symbol, date, open, high, low, close, pre_close,
            change, pct_change, volume, turnover
        """
        rq = _get_rqdatac()
        rq_symbols = rq.id_convert([s.strip() for s in symbol.split(",")])
        if isinstance(rq_symbols, str):
            rq_symbols = [rq_symbols]
        df = rq.get_price(
            rq_symbols, start_date=start_date, end_date=end_date,
            frequency='1d', adjust_type='none', expect_df=True,
        )

        if df is None or df.empty:
            return self._empty_index_bar()
        return self._normalize_bar(df, rq)
