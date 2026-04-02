"""Base class for data sources"""

from abc import ABC, abstractmethod
from typing import Optional
import pandas as pd


class BaseSource(ABC):
    """Abstract base class for data source adapters."""

    @abstractmethod
    def get_bar(
        self,
        symbol: str,
        frequency: str = "tick",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get K-line/bar data for a stock or index.

        Args:
            symbol: Stock symbol with exchange (e.g., "600000.SH" or "000001.SZ")
            frequency: Bar frequency ("tick", "1min", "5min", "15min", "30min", "60min", "1day", "1week", "1month")
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format

        Returns:
            DataFrame with bar data
        """
        pass
