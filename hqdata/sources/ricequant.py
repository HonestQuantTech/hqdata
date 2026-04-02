"""Ricequant (米筐) data source adapter"""

from typing import Optional
import pandas as pd
from rqdatac import init, get_price
from rqdatac.share.errors import RQDataError

from hqdata.sources.base import BaseSource


class RicequantSource(BaseSource):
    """Ricequant data source adapter.

    Requires rqdatac >= 3.1.4 and valid RQData credentials.
    Credentials can be provided via arguments or environment variables
    (RQDATA_USERNAME, RQDATA_PASSWORD).
    """

    def __init__(
        self,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        """Initialize ricequant connection.

        Args:
            username: RQData username (email), defaults to RQDATA_USERNAME env var
            password: RQData password/token, defaults to RQDATA_PASSWORD env var
        """
        import os

        username = username or os.getenv("RQDATA_USERNAME")
        password = password or os.getenv("RQDATA_PASSWORD")

        if not username or not password:
            raise ValueError(
                "RQData credentials not provided. Set username/password in init_source() "
                "or set RQDATA_USERNAME and RQDATA_PASSWORD environment variables."
            )

        init(
            username,
            password,
            address=("rqdatad-pro.ricequant.com", 16011),
            use_pool=True,
            max_pool_size=1,
            auto_load_plugins=False,
        )

    def get_tick(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get tick data for a stock.

        Args:
            symbol: Stock symbol with exchange (e.g., "600000.XSHG")
            start_date: Start date (e.g., "2024-01-01")
            end_date: End date (e.g., "2024-01-02")

        Returns:
            DataFrame with tick data including:
            - open, high, low, last (OHLC)
            - prev_close, limit_up, limit_down
            - volume, total_turnover
            - b1-b5, a1-a5 (bid/ask prices)
            - b1_v-b5_v, a1_v-a5_v (bid/ask volumes)
        """
        fields = [
            "open", "high", "low", "last", "prev_close",
            "volume", "total_turnover",
            "limit_up", "limit_down",
            "b1", "b2", "b3", "b4", "b5",
            "a1", "a2", "a3", "a4", "a5",
            "b1_v", "b2_v", "b3_v", "b4_v", "b5_v",
            "a1_v", "a2_v", "a3_v", "a4_v", "a5_v",
        ]

        df = get_price(
            symbol,
            frequency="tick",
            fields=fields,
            start_date=start_date,
            end_date=end_date,
            market="cn",  # China stocks require market='cn'
        )
        return df

    def get_bar(
        self,
        symbol: str,
        frequency: str = "1d",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get K-line/bar data for a stock.

        Note: Ricequant tick data only, bar data not implemented yet.

        Args:
            symbol: Stock symbol with exchange (e.g., "600000.XSHG")
            frequency: Bar frequency ("1d", "1w", "1m", "5m", "60m")
            start_date: Start date (e.g., "2024-01-01")
            end_date: End date (e.g., "2024-01-02")

        Returns:
            DataFrame with bar data
        """
        raise NotImplementedError("Ricequant get_bar not implemented yet. Use Tushare for bar data.")
