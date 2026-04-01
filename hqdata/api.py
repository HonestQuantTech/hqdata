"""hqdata public API - only entry point for upper layers"""

from typing import Literal, Optional
import pandas as pd

# Singleton source instance
_source: Optional["BaseSource"] = None

Exchange = Literal["XSHG", "XSHE"]  # XSHG=SSE, XSHE=SZSE


def init_source(source_type: Literal["ricequant"], **kwargs) -> None:
    """Initialize data source.

    Args:
        source_type: "ricequant"
        **kwargs: Source-specific credentials (e.g., username, password for ricequant)
    """
    global _source
    if source_type == "ricequant":
        from hqdata.sources.ricequant import RicequantSource
        _source = RicequantSource(**kwargs)
    else:
        raise ValueError(f"Unknown source type: {source_type}")


def get_tick(
    symbol: str,
    exchange: Exchange = "XSHG",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """Get tick data for a stock.

    Args:
        symbol: Stock code (e.g., "600000")
        exchange: "XSHG" (SSE) or "XSHE" (SZSE)
        start_date: Start date (e.g., "2024-01-01")
        end_date: End date (e.g., "2024-01-02")

    Returns:
        DataFrame with tick data
    """
    if _source is None:
        raise RuntimeError("Data source not initialized. Call init_source() first.")
    return _source.get_tick(symbol, exchange, start_date, end_date)


