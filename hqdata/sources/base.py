"""Base class for data sources"""

import os
from abc import ABC, abstractmethod
from typing import Optional
import pandas as pd


class BaseSource(ABC):
    """Abstract base class for data source adapters."""

    @staticmethod
    def _get_env(param: Optional[str], env_var: str, error_msg: str) -> str:
        value = param or os.getenv(env_var)
        if not value:
            raise ValueError(error_msg)
        return value

    @staticmethod
    def _empty_calendar() -> pd.DataFrame:
        return pd.DataFrame(columns=["date", "is_open"])

    @staticmethod
    def _empty_stock_list() -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "symbol",
                "date",
                "name",
                "exchange",
                "board",
                "curr_type",
                "list_date",
                "delist_date",
            ]
        )

    @staticmethod
    def _empty_stock_factor() -> pd.DataFrame:
        return pd.DataFrame(columns=["symbol", "date", "factor"])

    @staticmethod
    def _empty_stock_daily_bar() -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "symbol",
                "date",
                "pre_close",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "turnover",
                "change",
                "pct_change",
            ]
        )

    @staticmethod
    def _empty_stock_snapshot() -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "ets",
                "lts",
                "symbol",
                "pre_close",
                "open",
                "high",
                "low",
                "last",
                "volume",
                "turnover",
                "ap1",
                "ap2",
                "ap3",
                "ap4",
                "ap5",
                "av1",
                "av2",
                "av3",
                "av4",
                "av5",
                "bp1",
                "bp2",
                "bp3",
                "bp4",
                "bp5",
                "bv1",
                "bv2",
                "bv3",
                "bv4",
                "bv5",
            ]
        )

    @abstractmethod
    def get_calendar(
        self,
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
        pass

    @abstractmethod
    def get_stock_list(
        self,
        trade_date: str,
        symbol: Optional[str] = None,
        exchange: Optional[str] = None,
        board: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get basic info for stocks.

        Args:
            trade_date: snapshot date (YYYYMMDD); injected by api layer
            symbol: see README, supports comma-separated multiple codes
            exchange: see README, supports comma-separated multiple exchanges
            board: see README, supports comma-separated multiple codes

        Returns:
            DataFrame with columns: symbol, date, name, exchange, board,
            curr_type, list_date, delist_date
        """
        pass

    @abstractmethod
    def get_stock_daily_bar(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        trading_days: Optional[int] = None,
    ) -> pd.DataFrame:
        """Get daily bar data for stocks.

        Args:
            symbol: see README, supports comma-separated multiple codes
            start_date: see README
            end_date: see README
            trading_days: number of trading days in [start_date, end_date]; injected by api layer for batching

        Returns:
            DataFrame with columns: symbol, date, pre_close, open, high, low, close, volume, turnover, change, pct_change
        """
        pass

    @abstractmethod
    def get_stock_factor(
        self,
        trade_date: str,
        symbol: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get cumulative price adjustment factors for stocks.

        Note:
            factor is a cumulative back-adjustment (后复权) multiplier: raw_close * factor
            reconstructs the source's back-adjusted price series. It is not comparable
            across sources by raw value (each anchors its cumulative factor to a different
            base point); only day-over-day ratios (factor[t] / factor[t-1]) are comparable.

        Args:
            trade_date: snapshot date (YYYYMMDD); injected by api layer
            symbol: see README, supports comma-separated multiple codes; defaults to
                every stock in that day's stock list

        Returns:
            DataFrame with columns: symbol, date, factor
        """
        pass

    @abstractmethod
    def get_stock_snapshot(self, symbol: str) -> pd.DataFrame:
        """Get real-time stock snapshot with 5-level order book.

        Args:
            symbol: see README, supports comma-separated multiple codes

        Returns:
            DataFrame with columns: ets, lts, symbol, pre_close, open, high, low, last,
            volume, turnover, ap1~ap5, av1~av5, bp1~bp5, bv1~bv5
        """
        pass
