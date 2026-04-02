"""Base class for data sources"""

from abc import ABC, abstractmethod
from typing import Optional
import pandas as pd


class BaseSource(ABC):
    """Abstract base class for data source adapters."""

    @abstractmethod
    def get_tick(
        self,
        symbol: str,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> pd.DataFrame:
        """Get tick data for a stock.

        Args:
            symbol: Stock symbol with exchange (e.g., "600000.SH" or "000001.SZ")
            start_date: Start date (e.g., "2024-01-01")
            end_date: End date (e.g., "2024-01-02")

        Returns:
            DataFrame with tick data
        """
        pass

    @abstractmethod
    def get_bar(
        self,
        symbol: str,
        frequency: str,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> pd.DataFrame:
        """Get K-line/bar data for a stock or index.

        Args:
            symbol: Stock symbol with exchange (e.g., "600000.SH" or "000001.SZ")
            frequency: Bar frequency ("1d", "1w", "1m", "5m", "60m", etc.)
            start_date: Start date (e.g., "2024-01-01")
            end_date: End date (e.g., "2024-01-02")

        Returns:
            DataFrame with bar data
        """
        pass

