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
        exchange: str,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> pd.DataFrame:
        """Get tick data for a stock.

        Args:
            symbol: Stock code (e.g., "600000")
            exchange: Exchange code ("XSHG" for SSE, "XSHE" for SZSE)
            start_date: Start date (e.g., "2024-01-01")
            end_date: End date (e.g., "2024-01-02")

        Returns:
            DataFrame with tick data
        """
        pass

