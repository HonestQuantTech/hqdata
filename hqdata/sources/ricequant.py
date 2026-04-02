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

    def get_bar(
        self,
        symbol: str,
        frequency: str = "tick",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get K-line/bar data for a stock.

        Args:
            symbol: Stock symbol with exchange (e.g., "600000.SH" or "000001.SZ")
            frequency: Bar frequency ("tick", "1min", "5min", "15min", "30min", "60min", "1day", "1week", "1month")
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format

        Returns:
            DataFrame with bar data
        """
        raise NotImplementedError("Ricequant get_bar not implemented yet.")
