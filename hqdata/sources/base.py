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
    def get_bar(
        self,
        symbol: str,
        frequency: str = "1day",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get K-line/bar data for a stock.

        Args:
            symbol: Stock symbol with exchange (e.g., "600000.SH" or "000001.SZ")
            frequency: Bar frequency ("1min", "5min", "15min", "30min", "60min", "1day", "1week", "1month")
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format

        Returns:
            DataFrame with columns: symbol, date, open, high, low, close, pre_close, change, pct_change, volume, amount
        """
        pass

    @abstractmethod
    def get_index_bar(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get daily bar data for an index.

        Args:
            symbol: Index code with exchange (e.g., "000001.SH", "399300.SZ")
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format

        Returns:
            DataFrame with columns: symbol, date, open, high, low, close, pre_close, change, pct_change, volume, amount
        """
        pass
