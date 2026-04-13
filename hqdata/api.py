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


def get_calendar(
    start_date: str,
    end_date: str,
    is_open: Optional[bool] = None,
) -> pd.DataFrame:
    """Get trading calendar.

    Args:
        start_date: see README
        end_date: see README
        is_open: see README

    Returns:
        DataFrame with columns: date, is_open
    """
    if _source is None:
        raise RuntimeError("Data source not initialized. Call init_source() first.")
    return _source.get_calendar(start_date, end_date, is_open)


def get_stock_list(
    symbol: Optional[str] = None,
    exchange: Optional[str] = None,
    board: Optional[str] = None,
) -> pd.DataFrame:
    """Get basic info for stocks.

    Args:
        symbol: see README, supports comma-separated multiple codes
        exchange: see README, supports comma-separated multiple exchanges
        board: see README, supports comma-separated multiple codes

    Returns:
        DataFrame with columns: symbol, date, name, exchange, board, industry,
        curr_type, list_date, delist_date, is_hs
    """
    if _source is None:
        raise RuntimeError("Data source not initialized. Call init_source() first.")
    return _source.get_stock_list(symbol=symbol, exchange=exchange, board=board)


def get_stock_minute_bar(
    symbol: str,
    frequency: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """Get minute bar data for stocks.

    Args:
        symbol: see README, supports comma-separated multiple codes
        frequency: one of "1m", "5m", "15m", "30m", "60m"
        start_date: see README
        end_date: see README

    Returns:
        DataFrame with columns: symbol, date, open, close, high, low, volume, turnover, datetime
    """
    if _source is None:
        raise RuntimeError("Data source not initialized. Call init_source() first.")
    today = date.today().strftime("%Y%m%d")
    start_date = start_date or today
    end_date = end_date or today
    return _source.get_stock_minute_bar(symbol, frequency, start_date, end_date)


def get_stock_daily_bar(
    symbol: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """Get daily bar data for stocks.

    Args:
        symbol: see README, supports comma-separated multiple codes
        start_date: see README
        end_date: see README

    Returns:
        DataFrame with columns: symbol, date, open, close, high, low, volume, turnover, pre_close, change, pct_change
    """
    if _source is None:
        raise RuntimeError("Data source not initialized. Call init_source() first.")
    today = date.today().strftime("%Y%m%d")
    start_date = start_date or today
    end_date = end_date or today
    return _source.get_stock_daily_bar(symbol, start_date, end_date)


def get_index_list(
    symbol: Optional[str] = None,
    market: Optional[str] = None,
) -> pd.DataFrame:
    """Get basic info about an index or the index info of a market.

    Args:
        symbol: see README, supports comma-separated multiple codes. If provided, market is ignored.
        market: see README, supports comma-separated multiple markets.

    Returns:
        DataFrame with columns: symbol, date, name, fullname, market, base_date, base_point, list_date
    """
    if _source is None:
        raise RuntimeError("Data source not initialized. Call init_source() first.")
    return _source.get_index_list(symbol, market)


def get_index_minute_bar(
    symbol: str,
    frequency: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """Get minute bar data for an index.

    Args:
        symbol: see README, supports comma-separated multiple codes
        frequency: one of "1m", "5m", "15m", "30m", "60m"
        start_date: see README
        end_date: see README

    Returns:
        DataFrame with columns: symbol, date, open, close, high, low, volume, turnover, datetime
    """
    if _source is None:
        raise RuntimeError("Data source not initialized. Call init_source() first.")
    today = date.today().strftime("%Y%m%d")
    start_date = start_date or today
    end_date = end_date or today
    return _source.get_index_minute_bar(symbol, frequency, start_date, end_date)


def get_index_daily_bar(
    symbol: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """Get daily bar data for an index.

    Args:
        symbol: see README, supports comma-separated multiple codes
        start_date: see README
        end_date: see README

    Returns:
        DataFrame with columns: symbol, date, open, close, high, low, volume, turnover, pre_close, change, pct_change
    """
    if _source is None:
        raise RuntimeError("Data source not initialized. Call init_source() first.")
    today = date.today().strftime("%Y%m%d")
    start_date = start_date or today
    end_date = end_date or today
    return _source.get_index_daily_bar(symbol, start_date, end_date)
