"""Tushare data source adapter"""

import os
from typing import Optional
import pandas as pd
import tushare as ts

from hqdata.sources.base import BaseSource


class TushareSource(BaseSource):
    """Tushare data source adapter.

    Requires tushare >= 1.4.0 and valid TUSHARE_TOKEN.
    Token can be set via environment variable TUSHARE_TOKEN.
    """

    def __init__(self, token: Optional[str] = None):
        """Initialize tushare connection.

        Args:
            token: Tushare token, defaults to TUSHARE_TOKEN env var
        """
        token = token or os.getenv("TUSHARE_TOKEN")
        if not token:
            raise ValueError(
                "Tushare token not provided. Set token in init_source() "
                "or set TUSHARE_TOKEN environment variable."
            )
        ts.set_token(token)
        self.pro = ts.pro_api()

    def get_tick(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get tick data for a stock.

        Note: Tushare tick data is limited. For historical daily bars, use get_bar().

        Args:
            symbol: Stock symbol with exchange (e.g., "600000.SH")
            start_date: Start date (e.g., "2024-01-01")
            end_date: End date (e.g., "2024-01-02")

        Returns:
            DataFrame with tick data
        """
        df = ts.realtime_quote(ts_symbol=symbol)
        return df

    def get_bar(
        self,
        symbol: str,
        frequency: str = "1d",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get K-line/bar data for a stock or index.

        Args:
            symbol: Stock symbol with exchange (e.g., "600000.SH" or "000001.SZ")
            frequency: Bar frequency ("1d", "1w", "1m", "5m", "15m", "30m", "60m")
            start_date: Start date (e.g., "20240101" or "2024-01-01")
            end_date: End date (e.g., "20240102" or "2024-01-02")

        Returns:
            DataFrame with bar data including: open, high, low, close, volume, amount
        """
        # Tushare pro_bar expects ts_code format (e.g., "600000.SH")
        freq_map = {
            "1d": "D",
            "1w": "W",
            "1m": "M",
            "5m": "5",
            "15m": "15",
            "30m": "30",
            "60m": "60",
        }
        freq = freq_map.get(frequency, "D")

        # pro_bar returns daily data when asset='E' (equity/index)
        df = ts.pro_bar(
            ts_code=symbol,
            freq=freq,
            start_date=start_date.replace("-", "") if start_date else None,
            end_date=end_date.replace("-", "") if end_date else None,
            asset="E",
        )

        # Rename columns to standard format
        if df is not None and not df.empty:
            df = df.rename(columns={
                "trade_date": "date",
                "vol": "volume",
            })
            df = df.sort_values("date")

        return df
