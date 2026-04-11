"""Base class for data sources"""

import os
from abc import ABC, abstractmethod
from typing import Optional
import pandas as pd


class BaseSource(ABC):
    """Abstract base class for data source adapters."""

    @staticmethod
    def _get_env(param: Optional[str], env_var: str, error_msg: str) -> str:
        value = param or os.getenv(env_var)
        if not value:
            raise ValueError(error_msg)
        return value

    @staticmethod
    def _empty_stock_list() -> pd.DataFrame:
        return pd.DataFrame(columns=[
            'symbol', 'name', 'exchange', 'board', 'industry',
            'curr_type', 'list_date', 'delist_date', 'is_hs', 'date',
        ])

    @staticmethod
    def _empty_stock_bar() -> pd.DataFrame:
        return pd.DataFrame(columns=[
            'symbol', 'date', 'open', 'high', 'low', 'close',
            'pre_close', 'change', 'pct_change', 'volume', 'turnover',
        ])

    @staticmethod
    def _empty_index_list() -> pd.DataFrame:
        return pd.DataFrame(columns=[
            'symbol', 'name', 'fullname', 'market', 'base_date', 'base_point', 'list_date', 'date',
        ])

    @staticmethod
    def _empty_index_bar() -> pd.DataFrame:
        return pd.DataFrame(columns=[
            'symbol', 'date', 'open', 'high', 'low', 'close',
            'pre_close', 'change', 'pct_change', 'volume', 'turnover',
        ])

    @staticmethod
    def _empty_calendar() -> pd.DataFrame:
        return pd.DataFrame(columns=['date', 'is_open'])

    @abstractmethod
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
        pass

    @abstractmethod
    def get_stock_list(
        self,
        symbol: Optional[str] = None,
        exchange: Optional[str] = None,
        board: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get basic info for stocks.

        Args:
            symbol: see README, supports comma-separated multiple codes
            exchange: see README, supports comma-separated multiple exchanges
            board: see README, supports comma-separated multiple codes

        Returns:
            DataFrame with columns: symbol, name, exchange, board, industry,
            curr_type, list_date, delist_date, is_hs, date
        """
        pass

    @abstractmethod
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
        pass

    @abstractmethod
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
            DataFrame with columns: symbol, name, fullname, market, base_date, base_point, list_date, date
        """
        pass

    @abstractmethod
    def get_index_bar(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get daily bar data for an index or multiple indexes.

        Args:
            symbol: see README, supports comma-separated multiple codes
            start_date: see README
            end_date: see README

        Returns:
            DataFrame with columns: symbol, date, open, high, low, close, pre_close, change, pct_change, volume, turnover
        """
        pass
