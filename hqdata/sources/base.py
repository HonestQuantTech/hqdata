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
                "industry",
                "curr_type",
                "list_date",
                "delist_date",
                "is_hs",
            ]
        )

    @staticmethod
    def _empty_stock_minute_bar() -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "symbol",
                "date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "turnover",
                "ets",
            ]
        )

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

    @staticmethod
    def _empty_index_list() -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "symbol",
                "date",
                "name",
                "fullname",
                "market",
                "base_date",
                "base_point",
                "list_date",
            ]
        )

    @staticmethod
    def _empty_index_minute_bar() -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "symbol",
                "date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "turnover",
                "ets",
            ]
        )

    @staticmethod
    def _empty_index_daily_bar() -> pd.DataFrame:
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
        symbol: Optional[str] = None,
        exchange: Optional[str] = None,
        board: Optional[str] = None,
        trade_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get basic info for stocks.

        Args:
            symbol: see README, supports comma-separated multiple codes
            exchange: see README, supports comma-separated multiple exchanges
            board: see README, supports comma-separated multiple codes
            trade_date: snapshot date (YYYYMMDD); injected by api layer, defaults to current trading day

        Returns:
            DataFrame with columns: symbol, date, name, exchange, board, industry,
            curr_type, list_date, delist_date, is_hs
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

    @abstractmethod
    def get_stock_minute_bar(
        self,
        symbol: str,
        frequency: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        trading_days: Optional[int] = None,
    ) -> pd.DataFrame:
        """Get minute bar data for stocks.

        Args:
            symbol: see README, supports comma-separated multiple codes
            frequency: one of "1m", "5m", "15m", "30m", "60m"
            start_date: see README
            end_date: see README
            trading_days: number of trading days in [start_date, end_date]; injected by api layer for batching

        Returns:
            DataFrame with columns: symbol, date, open, high, low, close, volume, turnover, ets
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
    def get_index_list(
        self,
        symbol: Optional[str] = None,
        market: Optional[str] = "SSE,SZE",
        trade_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get basic info about an index or the index info of a market.

        Args:
            symbol: see README, supports comma-separated multiple codes. If provided, market is ignored.
            market: see README, supports comma-separated multiple markets. Defaults to "SSE,SZSE".
            trade_date: snapshot date (YYYYMMDD); injected by api layer, defaults to current trading day

        Returns:
            DataFrame with columns: symbol, date, name, fullname, market, base_date, base_point, list_date
        """
        pass

    @abstractmethod
    def get_index_minute_bar(
        self,
        symbol: str,
        frequency: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        trading_days: Optional[int] = None,
    ) -> pd.DataFrame:
        """Get minute bar data for an index.

        Args:
            symbol: see README, supports comma-separated multiple codes
            frequency: one of "1m", "5m", "15m", "30m", "60m"
            start_date: see README
            end_date: see README
            trading_days: number of trading days in [start_date, end_date]; injected by api layer for batching

        Returns:
            DataFrame with columns: symbol, date, open, high, low, close, volume, turnover, ets
        """
        pass

    @abstractmethod
    def get_index_daily_bar(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        trading_days: Optional[int] = None,
    ) -> pd.DataFrame:
        """Get daily bar data for an index.

        Args:
            symbol: see README, supports comma-separated multiple codes
            start_date: see README
            end_date: see README
            trading_days: number of trading days in [start_date, end_date]; injected by api layer for batching

        Returns:
            DataFrame with columns: symbol, date, pre_close, open, high, low, close, volume, turnover, change, pct_change
        """
        pass
