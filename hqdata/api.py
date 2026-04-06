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


def get_stock_list(list_status: str = "L") -> pd.DataFrame:
    """Get basic info for stocks.

    Args:
        list_status: Listing status — "L" (listed), "D" (delisted), "P" (suspended)

    Returns:
        DataFrame with columns: symbol, name, industry, market, exchange,
        curr_type, list_status, list_date, delist_date, is_hs
    """

    # TODO more parameters for filtering (e.g., by symbol, exchange, market)
    # TODO return with date

    if _source is None:
        raise RuntimeError("Data source not initialized. Call init_source() first.")
    return _source.get_stock_list(list_status)


def get_stock_bar(
    symbol: str,
    frequency: str = "1day",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """Get K-line/bar data for stocks.

    Args:
        symbol: Stock symbol with exchange (e.g., "000001.SZ" or "000001.SZ,600000.SH")
        frequency: Bar frequency ("1min" | "5min" | "15min" | "30min" | "60min" | "1day"(default) | "1week" | "1month")
        start_date: Start date in YYYYMMDD format (defaults to today)
        end_date: End date in YYYYMMDD format (defaults to today)

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
        symbol: Index code with exchange, supports comma-separated multiple codes (e.g., "000300.SH" or "000300.SH,000905.SH"). If provided, market is ignored.
        market: Index market, supports comma-separated multiple markets (e.g., "CSI" or "CSI,CICC,SSE,SZSE,SW,MSCI,OTH"). Required if symbol is not provided.

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
        symbol: Index code with exchange, supports comma-separated multiple codes (e.g., "000300.SH" or "000300.SH,000905.SH")
        start_date: Start date in YYYYMMDD format (defaults to today)
        end_date: End date in YYYYMMDD format (defaults to today)

    Returns:
        DataFrame with columns: symbol, date, open, high, low, close, pre_close, change, pct_change, volume, amount
    """

    if _source is None:
        raise RuntimeError("Data source not initialized. Call init_source() first.")
    today = date.today().strftime("%Y%m%d")
    start_date = start_date or today
    end_date = end_date or today
    return _source.get_index_bar(symbol, start_date, end_date)
