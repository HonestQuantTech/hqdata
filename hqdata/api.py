"""hqdata public API - only entry point for upper layers"""

from datetime import date
from typing import Literal, Optional
import pandas as pd

# Singleton source instance
_source: Optional["BaseSource"] = None
_calendar: Optional["TradingCalendar"] = None


def init_source(source_type: Literal["ricequant", "tushare"], **kwargs) -> None:
    """Initialize data source.

    Args:
        source_type: "ricequant" or "tushare"
        **kwargs: Source-specific credentials
    """
    global _source, _calendar
    if source_type == "ricequant":
        from hqdata.sources.ricequant import RicequantSource

        _source = RicequantSource(**kwargs)
    elif source_type == "tushare":
        from hqdata.sources.tushare import TushareSource

        _source = TushareSource(**kwargs)
    else:
        raise ValueError(f"Unknown source type: {source_type}")

    from hqdata.calendar import TradingCalendar

    _calendar = TradingCalendar(_source.get_calendar)


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


def is_trading_day(d: str) -> bool:
    """Return True if d (YYYYMMDD) is a trading day.

    Args:
        d: date string in YYYYMMDD format

    Returns:
        True if trading day, False otherwise
    """
    if _calendar is None:
        raise RuntimeError("Data source not initialized. Call init_source() first.")
    return _calendar.is_trading_day(d)


def get_current_trading_day() -> str:
    """Return today if it's a trading day, else the most recent trading day.

    Returns:
        Trading day string in YYYYMMDD format
    """
    if _calendar is None:
        raise RuntimeError("Data source not initialized. Call init_source() first.")
    return _calendar.get_current_trading_day()


def next_trading_day(d: str) -> str:
    """Return the first trading day after d (exclusive).

    Args:
        d: date string in YYYYMMDD format

    Returns:
        Next trading day string in YYYYMMDD format
    """
    if _calendar is None:
        raise RuntimeError("Data source not initialized. Call init_source() first.")
    return _calendar.next_trading_day(d)


def previous_trading_day(d: str) -> str:
    """Return the last trading day before d (exclusive).

    Args:
        d: date string in YYYYMMDD format

    Returns:
        Previous trading day string in YYYYMMDD format
    """
    if _calendar is None:
        raise RuntimeError("Data source not initialized. Call init_source() first.")
    return _calendar.previous_trading_day(d)


def count_trading_days(start_date: str, end_date: str) -> int:
    """Return the number of trading days in [start_date, end_date] inclusive.

    Args:
        start_date: see README
        end_date: see README

    Returns:
        Number of trading days (0 if start_date > end_date)
    """
    if _calendar is None:
        raise RuntimeError("Data source not initialized. Call init_source() first.")
    return _calendar.count_trading_days(start_date, end_date)


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
    trade_date = get_current_trading_day()
    return _source.get_stock_list(symbol=symbol, exchange=exchange, board=board, trade_date=trade_date)


def get_stock_snapshot(symbol: str) -> pd.DataFrame:
    """Get real-time stock snapshot with 5-level order book.

    Args:
        symbol: see README, supports comma-separated multiple codes

    Returns:
        DataFrame with columns: ets, lts, symbol, pre_close, open, high, low, last,
        volume, turnover, ap1~ap5, av1~av5, bp1~bp5, bv1~bv5
    """
    if _source is None:
        raise RuntimeError("Data source not initialized. Call init_source() first.")
    return _source.get_stock_snapshot(symbol)


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
        DataFrame with columns: symbol, date, open, high, low, close, volume, turnover, ets
    """
    if _source is None:
        raise RuntimeError("Data source not initialized. Call init_source() first.")
    today = get_current_trading_day()
    start_date = start_date or today
    end_date = end_date or today
    trading_days = count_trading_days(start_date, end_date)
    return _source.get_stock_minute_bar(symbol, frequency, start_date, end_date, trading_days)


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
        DataFrame with columns: symbol, date, pre_close, open, high, low, close, volume, turnover, change, pct_change
    """
    if _source is None:
        raise RuntimeError("Data source not initialized. Call init_source() first.")
    today = get_current_trading_day()
    start_date = start_date or today
    end_date = end_date or today
    trading_days = count_trading_days(start_date, end_date)
    return _source.get_stock_daily_bar(symbol, start_date, end_date, trading_days)


def get_index_list(
    symbol: Optional[str] = None,
    market: Optional[str] = "SSE,SZE",
) -> pd.DataFrame:
    """Get basic info about an index or the index info of a market.

    Args:
        symbol: see README, supports comma-separated multiple codes. If provided, market is ignored.
        market: see README, supports comma-separated multiple markets. Defaults to "SSE,SZSE".

    Returns:
        DataFrame with columns: symbol, date, name, fullname, market, base_date, base_point, list_date
    """
    if _source is None:
        raise RuntimeError("Data source not initialized. Call init_source() first.")
    trade_date = get_current_trading_day()
    return _source.get_index_list(symbol, market, trade_date)


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
        DataFrame with columns: symbol, date, open, high, low, close, volume, turnover, ets
    """
    if _source is None:
        raise RuntimeError("Data source not initialized. Call init_source() first.")
    today = get_current_trading_day()
    start_date = start_date or today
    end_date = end_date or today
    trading_days = count_trading_days(start_date, end_date)
    return _source.get_index_minute_bar(symbol, frequency, start_date, end_date, trading_days)


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
        DataFrame with columns: symbol, date, pre_close, open, high, low, close, volume, turnover, change, pct_change
    """
    if _source is None:
        raise RuntimeError("Data source not initialized. Call init_source() first.")
    today = get_current_trading_day()
    start_date = start_date or today
    end_date = end_date or today
    trading_days = count_trading_days(start_date, end_date)
    return _source.get_index_daily_bar(symbol, start_date, end_date, trading_days)
