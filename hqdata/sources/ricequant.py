"""Ricequant (米筐) data source adapter"""

from typing import Optional
import pandas as pd

from hqdata.sources.base import BaseSource


def _get_rqdatac():
    """Lazy import rqdatac to support optional installation."""
    from rqdatac import init, get_price
    from rqdatac.share.errors import RQDataError
    return init, get_price, RQDataError


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
        username = BaseSource._get_env(
            username, "RQDATA_USERNAME",
            "RQData credentials not provided. Set username/password in init_source() "
            "or set RQDATA_USERNAME and RQDATA_PASSWORD environment variables."
        )
        password = BaseSource._get_env(
            password, "RQDATA_PASSWORD",
            "RQData credentials not provided. Set username/password in init_source() "
            "or set RQDATA_USERNAME and RQDATA_PASSWORD environment variables."
        )

        init, _, _ = _get_rqdatac()
        init(
            username,
            password,
            address=("rqdatad-pro.ricequant.com", 16011),
            use_pool=True,
            max_pool_size=1,
            auto_load_plugins=False,
        )

    def get_stock_list(
        self,
        symbol: Optional[str] = None,
        exchange: Optional[str] = None,
        market: Optional[str] = None,
        list_status: str = "L",
        is_hs: Optional[str] = None,
    ) -> pd.DataFrame:
        raise NotImplementedError("Ricequant get_stock_list not implemented yet.")

    def get_stock_bar(
        self,
        symbol: str,
        frequency: str = "day",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        raise NotImplementedError("Ricequant get_stock_bar not implemented yet.")

    def get_index_list(
        self,
        symbol: Optional[str] = None,
        market: Optional[str] = None,
    ) -> pd.DataFrame:
        raise NotImplementedError("Ricequant get_index_list not implemented yet.")

    def get_index_bar(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        raise NotImplementedError("Ricequant get_index_bar not implemented yet.")
