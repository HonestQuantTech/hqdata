"""Tushare data source adapter"""

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
        token = BaseSource._get_env(
            token, "TUSHARE_TOKEN",
            "Tushare token not provided. Set token in init_source() "
            "or set TUSHARE_TOKEN environment variable."
        )
        ts.set_token(token)
        self.pro = ts.pro_api()

    @staticmethod
    def _rename_daily_columns(df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns={
            "ts_code": "symbol",
            "trade_date": "date",
            "vol": "volume",
            "pct_chg": "pct_change",
        })

    _STOCK_LIST_FIELDS = (
        "symbol,name,industry,market,exchange,curr_type,list_status,list_date,delist_date,is_hs"
    )

    def get_stock_list(self, list_status: str = "L") -> pd.DataFrame:
        """Get basic info for stocks.

        Args:
            list_status: Listing status — "L" (listed), "D" (delisted), "P" (suspended)

        Returns:
            DataFrame with columns: symbol, name, industry, market, exchange,
            curr_type, list_status, list_date, delist_date, is_hs
        """
        return self.pro.stock_basic(list_status=list_status, fields=self._STOCK_LIST_FIELDS)

    def get_bar(
        self,
        symbol: str,
        frequency: str = "1day",
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

        df = self.pro.daily(ts_code=symbol, start_date=start_date, end_date=end_date)

        if df is not None and not df.empty:
            df = self._rename_daily_columns(df).sort_values("date")

        return df

    def get_index_bar(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get daily bar data for an index.

        Args:
            symbol: Index code with exchange (e.g., "000300.SH", "000905.SH")
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format

        Returns:
            DataFrame with columns: symbol, date, open, high, low, close, pre_close, change, pct_change, volume, amount
        """
        df = self.pro.index_daily(ts_code=symbol, start_date=start_date, end_date=end_date)

        if df is not None and not df.empty:
            df = self._rename_daily_columns(df).sort_values("date")

        return df
