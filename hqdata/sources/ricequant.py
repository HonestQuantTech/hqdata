"""Ricequant (米筐) data source adapter"""

from typing import Optional
import pandas as pd
from rqdatac import init, get_price
from rqdatac.services.basic import all_instruments
from rqdatac.share.errors import RQDataError

from hqdata.sources.base import BaseSource


# Exchange mapping: local code -> ricequant code
EXCHANGE_MAP = {
    "XSHG": "XSHG",  # Shanghai Stock Exchange
    "XSHE": "XSHE",  # Shenzhen Stock Exchange
}


def _to_rq_symbol(symbol: str, exchange: str) -> str:
    """Convert local symbol to ricequant format.

    Args:
        symbol: Stock code (e.g., "600000")
        exchange: Exchange code ("XSHG" or "XSHE")

    Returns:
        Ricequant symbol format (e.g., "600000.XSHG")
    """
    return f"{symbol}.{exchange}"


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
        exchange: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get tick data for a stock.

        Args:
            symbol: Stock code (e.g., "600000")
            exchange: Exchange code ("XSHG" or "XSHE")
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
        rq_symbol = _to_rq_symbol(symbol, exchange)

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
            rq_symbol,
            frequency="tick",
            fields=fields,
            start_date=start_date,
            end_date=end_date,
            market="cn",  # China stocks require market='cn'
        )
        return df
