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
            'symbol', 'name', 'industry', 'market', 'exchange',
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
            'symbol', 'name', 'fullname', 'market', 'base_date', 'base_point', 'list_date',
        ])

    @staticmethod
    def _empty_index_bar() -> pd.DataFrame:
        return pd.DataFrame(columns=[
            'symbol', 'date', 'open', 'high', 'low', 'close',
            'pre_close', 'change', 'pct_change', 'volume', 'turnover',
        ])

    @abstractmethod
    def get_stock_list(
        self,
        symbol: Optional[str] = None,
        exchange: Optional[str] = None,
        market: Optional[str] = None,
        is_hs: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get basic info for stocks.

        Args:
            symbol: see README, supports comma-separated multiple codes
            exchange: see README, supports comma-separated multiple exchanges
            market: Market category，supports comma-separated multiple codes
            is_hs: see README

        Optional Description:
            market: MB(主板),GEM(创业板),STAR(科创板),BJ(北交所)

        Returns:
            DataFrame with columns: symbol, name, industry, market, exchange,
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
            market: see README, supports comma-separated multiple markets. Required if symbol is not provided.

        Optional Description:
            market: CSI(中证指数),CICC(中金指数),SSE(上交所指数),SZSE(深交所指数),SW(申万指数),MSCI(MSCI指数),OTH(其他指数)
            
        Returns:
            DataFrame with columns: symbol, name, fullname, market, base_date, base_point, list_date
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
