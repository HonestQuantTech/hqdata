"""hqdata public API - only entry point for upper layers"""

from datetime import date
from typing import Literal, Optional
import pandas as pd

# Singleton source instance
_source: Optional["BaseSource"] = None


def init_source(source_type: Literal["ricequant", "tushare"], **kwargs) -> None:
    """Initialize data source.

    Args:
        source_type: "ricequant" or "tushare"
        **kwargs: Source-specific credentials
    """
    global _source
    if source_type == "ricequant":
        from hqdata.sources.ricequant import RicequantSource

        _source = RicequantSource(**kwargs)
    elif source_type == "tushare":
        from hqdata.sources.tushare import TushareSource

        _source = TushareSource(**kwargs)
    else:
        raise ValueError(f"Unknown source type: {source_type}")


def get_stock_list(
    symbol: Optional[str] = None,
    exchange: Optional[str] = None,
    market: Optional[str] = None,
    list_status: str = "L",
    is_hs: Optional[str] = None,
) -> pd.DataFrame:
    """Get basic info for stocks.

    Args:
        symbol: see README, supports comma-separated multiple codes
        exchange: see README, supports comma-separated multiple exchanges
        market: Market category，supports comma-separated multiple codes
        list_status: see README
        is_hs: see README

    Special:
        market: MB(主板),GEM(创业板),STAR(科创板),BJ(北交所)
        
    Returns:
        DataFrame with columns: symbol, name, industry, market, exchange,
        curr_type, list_status, list_date, delist_date, is_hs, date
    """

    if _source is None:
        raise RuntimeError("Data source not initialized. Call init_source() first.")
    return _source.get_stock_list(symbol=symbol, exchange=exchange, market=market, list_status=list_status, is_hs=is_hs)


def get_stock_bar(
    symbol: str,
    frequency: str = "day",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """Get K-line/bar data for stocks.

    Args:
        symbol: see README, supports comma-separated multiple codes
        frequency: see README
        start_date: see README
        end_date: see README

    Returns:
        DataFrame with columns: symbol, date, open, high, low, close, pre_close, change, pct_change, volume, amount
    """
    if _source is None:
        raise RuntimeError("Data source not initialized. Call init_source() first.")
    today = date.today().strftime("%Y%m%d")
    start_date = start_date or today
    end_date = end_date or today
    return _source.get_stock_bar(symbol, frequency, start_date, end_date)

def get_index_list(
    symbol: Optional[str] = None,
    market: Optional[str] = None,
) -> pd.DataFrame:
    """Get basic info about an index or the index info of a market.

    Args:
        symbol: see README, supports comma-separated multiple codes. If provided, market is ignored.
        market: see README, supports comma-separated multiple markets. Required if symbol is not provided.

    Special:
        market: CSI(中证指数),CICC(中金指数),SSE(上交所指数),SZSE(深交所指数),SW(申万指数),MSCI(MSCI指数),OTH(其他指数)
        
    Returns:
        DataFrame with columns: symbol, name, fullname, market, base_date, base_point, list_date
    """

    if _source is None:
        raise RuntimeError("Data source not initialized. Call init_source() first.")
    return _source.get_index_list(symbol, market)

def get_index_bar(
    symbol: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """Get daily bar data for an index or multiple indexes.

    Args:
        symbol: see README, supports comma-separated multiple codes
        start_date: see README
        end_date: see README
        
    Returns:
        DataFrame with columns: symbol, date, open, high, low, close, pre_close, change, pct_change, volume, amount
    """

    if _source is None:
        raise RuntimeError("Data source not initialized. Call init_source() first.")
    today = date.today().strftime("%Y%m%d")
    start_date = start_date or today
    end_date = end_date or today
    return _source.get_index_bar(symbol, start_date, end_date)
