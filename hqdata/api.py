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


def get_bar(
    symbol: str,
    frequency: str = "1day",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """Get K-line/bar data for a stock or index.

    Args:
        symbol: Stock symbol with exchange (e.g., "600000.SH" or "000001.SZ")
        frequency: Bar frequency ("tick", "1min", "5min", "15min", "30min", "60min", "1day", "1week", "1month")
        start_date: Start date in YYYYMMDD format (defaults to today)
        end_date: End date in YYYYMMDD format (defaults to today)

    Returns:
        DataFrame with bar data
    """
    if _source is None:
        raise RuntimeError("Data source not initialized. Call init_source() first.")
    # Default dates to today
    today = date.today().strftime("%Y%m%d")
    start_date = start_date or today
    end_date = end_date or today
    return _source.get_bar(symbol, frequency, start_date, end_date)
