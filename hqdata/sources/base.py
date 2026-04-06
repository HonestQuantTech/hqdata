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

    @abstractmethod
    def get_stock_list(self, list_status: str = "L") -> pd.DataFrame:
        """Get basic info for stocks.

        Args:
            list_status: Listing status — "L" (listed), "D" (delisted), "P" (suspended)

        Returns:
            DataFrame with columns: symbol, name, industry, market, exchange,
            curr_type, list_status, list_date, delist_date, is_hs
        """
        pass

    @abstractmethod
    def get_stock_bar(
        self,
        symbol: str,
        frequency: str = "1day",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get K-line/bar data for stocks.

        Args:
            symbol: Stock symbol with exchange (e.g., "000001.SZ" or "000001.SZ,600000.SH")
            frequency: Bar frequency ("1min" | "5min" | "15min" | "30min" | "60min" | "1day"(default) | "1week" | "1month")
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format

        Returns:
            DataFrame with columns: symbol, date, open, high, low, close, pre_close, change, pct_change, volume, amount
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
            symbol: Index code with exchange, supports comma-separated multiple codes (e.g., "000300.SH" or "000300.SH,000905.SH"). If provided, market is ignored.
            market: Index market, supports comma-separated multiple markets (e.g., "CSI" or "CSI,CICC,SSE,SZSE,SW,MSCI,OTH"). Required if symbol is not provided.

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
            symbol: Index code with exchange, supports comma-separated multiple codes (e.g., "000300.SH" or "000300.SH,000905.SH")
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format

        Returns:
            DataFrame with columns: symbol, date, open, high, low, close, pre_close, change, pct_change, volume, amount
        """
        pass
