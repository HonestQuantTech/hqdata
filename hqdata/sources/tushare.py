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

    def get_bar(
        self,
        symbol: str,
        frequency: str = "tick",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get daily bar data for a stock.

        Args:
            symbol: Stock symbol with exchange (e.g., "600000.SH" or "000001.SZ")
            frequency: Bar frequency ("1day" only, other frequencies not supported)
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format

        Returns:
            DataFrame with columns: symbol, date, open, high, low, close, pre_close, change, pct_change, volume, amount
        """
        if frequency != "1day":
            raise NotImplementedError(
                f"Tushare only supports '1day' frequency, got '{frequency}'"
            )

        df = self.pro.daily(
            ts_code=symbol,
            start_date=start_date,
            end_date=end_date,
        )

        # Rename columns to standard format
        if df is not None and not df.empty:
            df = df.rename(
                columns={
                    "ts_code": "symbol",
                    "trade_date": "date",
                    "pct_chg": "pct_change",
                    "vol": "volume",
                }
            )
            df = df.sort_values("date")

        return df
