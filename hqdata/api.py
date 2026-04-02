"""hqdata public API - only entry point for upper layers"""

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


def get_tick(
    symbol: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """Get tick data for a stock.

    Args:
        symbol: Stock symbol with exchange (e.g., "600000.SH" or "000001.SZ")
        start_date: Start date (e.g., "2024-01-01")
        end_date: End date (e.g., "2024-01-02")

    Returns:
        DataFrame with tick data
    """
    if _source is None:
        raise RuntimeError("Data source not initialized. Call init_source() first.")
    return _source.get_tick(symbol, start_date, end_date)


def get_bar(
    symbol: str,
    frequency: str = "1d",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """Get K-line/bar data for a stock or index.

    Args:
        symbol: Stock symbol with exchange (e.g., "600000.SH" or "000001.SZ")
        frequency: Bar frequency ("1d", "1w", "1m", "5m", "60m", etc.)
        start_date: Start date (e.g., "2024-01-01")
        end_date: End date (e.g., "2024-01-02")

    Returns:
        DataFrame with bar data
    """
    if _source is None:
        raise RuntimeError("Data source not initialized. Call init_source() first.")
    return _source.get_bar(symbol, frequency, start_date, end_date)


