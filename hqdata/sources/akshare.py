"""AKShare data source adapter"""

from datetime import datetime, timedelta
from typing import Optional
import pandas as pd

from hqdata.sources.base import BaseSource


def _get_akshare():
    """Lazy import akshare to support optional installation."""
    try:
        import akshare as ak
    except ImportError:
        raise ImportError(
            """akshare is not installed.

            hqdata does not install dependencies you may not need by default.
            Please run: pip install hqdata[akshare] to enable akshare support.
            """
        ) from None
    return ak


class AkshareSource(BaseSource):
    """AKShare data source adapter.

    Requires akshare >= 1.18.91. No credentials needed — akshare wraps free,
    public endpoints (Sina Finance, East Money, exchange websites).

    Note:
        Only get_calendar and get_stock_list are supported. akshare's
        per-symbol quote endpoints (daily bar, adjustment factor, snapshot)
        proved unreliable in practice (page-scraping based, no official rate
        limit, prone to temporary IP blocks) and are intentionally not
        implemented — calling them raises NotImplementedError (see BaseSource).
    """

    # akshare's current-listing endpoints are queried per board (SH) or expose
    # a native 板块 column (SZ), so the hqdata board code is known statically
    # at the call site rather than parsed from a shared mapping table.
    _RAW_STOCK_LIST_COLUMNS = [
        "symbol",
        "name",
        "exchange",
        "board",
        "curr_type",
        "list_date",
        "delist_date",
    ]

    _EXCHANGE_SUFFIX_MAP = {"SSE": "SH", "SZE": "SZ", "BSE": "BJ"}

    @staticmethod
    def _empty_raw_stock_list() -> pd.DataFrame:
        return pd.DataFrame(columns=AkshareSource._RAW_STOCK_LIST_COLUMNS)

    @staticmethod
    def _normalize_akdate(value: object) -> str:
        """Normalize an akshare date cell (datetime.date/str/NaN) to YYYYMMDD or ''."""
        if pd.isna(value):
            return ""
        text = str(value).strip()
        if text in ("", "0000-00-00"):
            return ""
        return text.replace("-", "")

    def __init__(self):
        """Initialize akshare source. No credentials required."""
        _get_akshare()

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
        ak = _get_akshare()
        start = datetime.strptime(start_date, "%Y%m%d").date()
        end = datetime.strptime(end_date, "%Y%m%d").date()

        trade_date_df = ak.tool_trade_date_hist_sina()
        trading_dates = set(trade_date_df["trade_date"])

        all_dates, is_open_list = [], []
        cur = start
        while cur <= end:
            all_dates.append(cur.strftime("%Y%m%d"))
            is_open_list.append("Y" if cur in trading_dates else "N")
            cur += timedelta(days=1)
        df = pd.DataFrame({"date": all_dates, "is_open": is_open_list})
        if is_open is not None:
            df = df[df["is_open"] == ("Y" if is_open else "N")].reset_index(drop=True)
        return df

    def _fetch_sh_current(self, ak) -> pd.DataFrame:
        """Currently-listed SSE A-shares (main board + STAR board; B-shares excluded)."""
        frames = []
        for board, ak_symbol in (("MB", "主板A股"), ("STAR", "科创板")):
            raw = ak.stock_info_sh_name_code(symbol=ak_symbol)
            if raw is None or raw.empty:
                continue
            frames.append(
                pd.DataFrame(
                    {
                        "code": raw["证券代码"].astype(str),
                        "name": raw["证券简称"],
                        "exchange": "SSE",
                        "board": board,
                        "list_date": raw["上市日期"].map(self._normalize_akdate),
                        "delist_date": "",
                    }
                )
            )
        if not frames:
            return self._empty_raw_stock_list()
        return pd.concat(frames, ignore_index=True)

    def _fetch_sz_current(self, ak) -> pd.DataFrame:
        """Currently-listed SZE A-shares (main board + GEM; B/CDR shares excluded)."""
        raw = ak.stock_info_sz_name_code(symbol="A股列表")
        if raw is None or raw.empty:
            return self._empty_raw_stock_list()
        board_map = {"主板": "MB", "创业板": "GEM"}
        return pd.DataFrame(
            {
                "code": raw["A股代码"].astype(str),
                "name": raw["A股简称"],
                "exchange": "SZE",
                "board": raw["板块"].map(lambda x: board_map.get(x, x)),
                "list_date": raw["A股上市日期"].map(self._normalize_akdate),
                "delist_date": "",
            }
        )

    def _fetch_bj_current(self, ak) -> pd.DataFrame:
        """Currently-listed BSE stocks."""
        raw = ak.stock_info_bj_name_code()
        if raw is None or raw.empty:
            return self._empty_raw_stock_list()
        return pd.DataFrame(
            {
                "code": raw["证券代码"].astype(str),
                "name": raw["证券简称"],
                "exchange": "BSE",
                "board": "BSE",
                "list_date": raw["上市日期"].map(self._normalize_akdate),
                "delist_date": "",
            }
        )

    def _fetch_sh_delisted(self, ak) -> pd.DataFrame:
        """Delisted/suspended SSE stocks (main board + STAR board; B-shares excluded).

        stock_info_sh_delist has no board column — board is derived from the
        code prefix: 688 is STAR, everything else in this main-board+STAR-only
        endpoint is main board.
        """
        raw = ak.stock_info_sh_delist(symbol="全部")
        if raw is None or raw.empty:
            return self._empty_raw_stock_list()
        codes = raw["公司代码"].astype(str)
        raw = raw[~codes.str.startswith("9")]  # exclude B-shares
        codes = raw["公司代码"].astype(str)
        if raw.empty:
            return self._empty_raw_stock_list()
        return pd.DataFrame(
            {
                "code": codes,
                "name": raw["公司简称"],
                "exchange": "SSE",
                "board": codes.str.startswith("688").map({True: "STAR", False: "MB"}),
                "list_date": raw["上市日期"].map(self._normalize_akdate),
                "delist_date": raw["暂停上市日期"].map(self._normalize_akdate),
            }
        )

    def _fetch_sz_delisted(self, ak) -> pd.DataFrame:
        """Delisted SZE stocks (main board + GEM; B-shares excluded).

        stock_info_sz_delist has no board column — board is derived from the
        code prefix: 300/301/302 is GEM, everything else is main board.
        """
        raw = ak.stock_info_sz_delist(symbol="终止上市公司")
        if raw is None or raw.empty:
            return self._empty_raw_stock_list()
        codes = raw["证券代码"].astype(str).str.zfill(6)
        keep = ~codes.str.startswith("2")  # exclude B-shares
        raw, codes = raw[keep], codes[keep]
        if raw.empty:
            return self._empty_raw_stock_list()
        gem_prefixes = ("300", "301", "302")
        return pd.DataFrame(
            {
                "code": codes,
                "name": raw["证券简称"],
                "exchange": "SZE",
                "board": codes.str[:3].map(
                    lambda p: "GEM" if p in gem_prefixes else "MB"
                ),
                "list_date": raw["上市日期"].map(self._normalize_akdate),
                "delist_date": raw["终止上市日期"].map(self._normalize_akdate),
            }
        )

    def get_stock_list(
        self,
        trade_date: str,
        symbol: Optional[str] = None,
        exchange: Optional[str] = None,
        board: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get basic info for stocks.

        Note:
            - Returns the stock universe for the given snapshot date.
            - Historical universe is reconstructed from currently-listed stocks
              (stock_info_sh/sz/bj_name_code) plus delisted stocks
              (stock_info_sh/sz_delist), using list_date <= trade_date < delist_date,
              or open-ended if delist_date is empty.
            - akshare has no delisted-stock endpoint for BSE, so BSE stocks that
              have delisted are missing from the reconstructed historical
              universe — a known akshare limitation (see README).

        Args:
            trade_date: snapshot date (YYYYMMDD); injected by api layer
            symbol: see README, supports comma-separated multiple codes
            exchange: see README, supports comma-separated multiple exchanges
            board: see README, supports comma-separated multiple codes

        Returns:
            DataFrame with columns: symbol, date, name, exchange, board,
            curr_type, list_date, delist_date
        """
        ak = _get_akshare()

        frames = [
            self._fetch_sh_current(ak),
            self._fetch_sz_current(ak),
            self._fetch_bj_current(ak),
            self._fetch_sh_delisted(ak),
            self._fetch_sz_delisted(ak),
        ]
        frames = [f for f in frames if f is not None and not f.empty]
        if not frames:
            return self._empty_stock_list()
        df = pd.concat(frames, ignore_index=True)
        # A stock listed in both the current and delisted endpoints (shouldn't
        # happen, but the two are independent HTTP calls) keeps the delisted
        # row, since it carries the authoritative delist_date.
        df = df.drop_duplicates(subset=["exchange", "code"], keep="last")

        list_date = df["list_date"].fillna("")
        delist_date = df["delist_date"].fillna("")
        active_mask = (list_date <= trade_date) & (
            (delist_date == "") | (trade_date < delist_date)
        )
        df = df[active_mask]
        if df.empty:
            return self._empty_stock_list()

        df = df.copy()
        df["symbol"] = df["code"] + "." + df["exchange"].map(self._EXCHANGE_SUFFIX_MAP)

        if symbol:
            symbols = {s.strip() for s in symbol.split(",")}
            df = df[df["symbol"].isin(symbols)]
        if exchange:
            exchanges = {e.strip() for e in exchange.split(",")}
            df = df[df["exchange"].isin(exchanges)]
        if board:
            boards = {b.strip() for b in board.split(",")}
            df = df[df["board"].isin(boards)]
        if df.empty:
            return self._empty_stock_list()

        df["date"] = trade_date
        df["curr_type"] = "CNY"
        cols = [
            "symbol",
            "date",
            "name",
            "exchange",
            "board",
            "curr_type",
            "list_date",
            "delist_date",
        ]
        return df[cols].sort_values("symbol").reset_index(drop=True)
