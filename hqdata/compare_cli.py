"""Subcommands for comparing stored data across sources."""

from pathlib import Path
from typing import Callable

import click
import pandas as pd


_EXCHANGE_NORMALIZE_MAP = {
    "BJSE": "BSE",
    "XSHG": "SSE",
    "XSHE": "SZE",
    "SZSE": "SZE",
}

_STOCK_LIST_COLUMNS = [
    "symbol",
    "date",
    "name",
    "exchange",
    "board",
    "curr_type",
    "list_date",
    "delist_date",
]

_DIFF_COLUMNS = [
    "date",
    "symbol",
    "status",
    "field",
    "tushare_value",
    "ricequant_value",
]

_STOCK_COMPARE_FIELDS = ["exchange", "board", "curr_type", "list_date", "delist_date"]

_STOCK_DAILY_COLUMNS = [
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

# field -> absolute tolerance (0.0 = exact). Thresholds derived from a full-corpus
# survey of 139 days x ~5500 stocks of real stored data:
# - turnover: tushare's 千元->元 conversion is only precise to the yuan while
#   ricequant keeps cents, so noise runs up to 1 yuan — and the float form of a
#   one-yuan gap can slightly exceed 1.0 (e.g. 8249685.0 - 8249683.999999999).
#   The tolerance is set to 2.0: comfortably above the noise, still three orders
#   of magnitude below the smallest real discrepancy observed (> 1000 yuan).
# - pct_change: the ricequant value is computed locally by hqdata and can differ
#   from tushare's official figure by one final-digit rounding step (1e-4).
#   The tolerance is set midway between one step and two steps (2e-4) because
#   the float representation of a one-step difference can slightly exceed 1e-4
#   (e.g. 2.0001 - 2.0 == 0.00010000000000021103).
# Everything else matched exactly across all 763k row pairs, so any difference
# there is a real data discrepancy worth reporting.
_STOCK_DAILY_FIELD_TOLERANCES = {
    "pre_close": 0.0,
    "open": 0.0,
    "high": 0.0,
    "low": 0.0,
    "close": 0.0,
    "volume": 0.0,
    "turnover": 2.0,
    "change": 0.0,
    "pct_change": 0.00015,
}


# ---------------------------------------------------------------------------
# generic helpers (shared by every `compare` subcommand)
# ---------------------------------------------------------------------------


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")


def _normalize_basic_date(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text in {"", "0000-00-00", "None", "nan", "NaT", "<NA>"}:
        return ""
    return text.replace("-", "")


def _normalize_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text in {"None", "nan", "NaT", "<NA>"}:
        return ""
    return text


def _normalize_exchange(value: object) -> str:
    text = _normalize_text(value).upper()
    return _EXCHANGE_NORMALIZE_MAP.get(text, text)


def _load_dated_csv(
    path: Path, source: str, kind: str, columns: list[str]
) -> pd.DataFrame:
    """Load a CSV as str dtype, checking the file exists and has the required columns."""
    if not path.exists():
        raise click.ClickException(f"Missing {kind} file for {source}: {path}")

    df = pd.read_csv(path, dtype=str)
    missing = set(columns) - set(df.columns)
    if missing:
        missing_cols = ", ".join(sorted(missing))
        raise click.ClickException(
            f"Invalid {kind} file for {source}: missing column(s) {missing_cols}"
        )
    return df[columns].copy()


def _validate_date_matches_filename(
    df: pd.DataFrame, path: Path, source: str, kind: str, date_col: str = "date"
) -> None:
    """Raise if any row's date column disagrees with the {date}.csv file name."""
    bad_dates = sorted(set(df.loc[df[date_col] != path.stem, date_col]))
    if bad_dates:
        raise click.ClickException(
            f"Invalid {kind} file for {source}: {path} contains date value(s) "
            f"{', '.join(bad_dates)} not matching the file name"
        )


def _load_dated_dir(
    path: Path, source: str, kind: str, load_one: Callable[[Path, str], pd.DataFrame]
) -> dict[str, pd.DataFrame]:
    """Load every {date}.csv file in a directory via load_one(file, source)."""
    if not path.exists() or not path.is_dir():
        raise click.ClickException(f"Missing {kind} directory for {source}: {path}")
    return {file.stem: load_one(file, source) for file in sorted(path.glob("*.csv"))}


def _load_non_trading_days(output_root: Path, source: str) -> set[str]:
    """Return calendar dates with is_open != Y; empty set if no calendar stored."""
    path = output_root / source / "calendar.csv"
    if not path.exists():
        return set()
    calendar = _load_calendar_csv(path, source)
    return set(calendar.loc[calendar["is_open"] != "Y", "date"])


def _diff_row(
    date: object,
    symbol: object,
    status: str,
    field: str = "",
    tushare_value: object = "",
    ricequant_value: object = "",
) -> dict[str, object]:
    return {
        "date": date,
        "symbol": symbol,
        "status": status,
        "field": field,
        "tushare_value": tushare_value,
        "ricequant_value": ricequant_value,
    }


def _diff_file_presence(
    output_root: Path, tushare_dates: set[str], ricequant_dates: set[str]
) -> list[dict[str, object]]:
    """Diff rows for file-level presence: non-trading-day files and one-sided files.

    Shared by every per-date compare command (stock-list, stock-daily): each of
    these stores one file per trading day, so file presence should always agree
    with each source's own calendar.csv and with the other source.
    """
    rows: list[dict[str, object]] = []

    for source, dates in (("tushare", tushare_dates), ("ricequant", ricequant_dates)):
        non_trading_days = _load_non_trading_days(output_root, source)
        for date in sorted(dates & non_trading_days):
            rows.append(
                _diff_row(
                    date,
                    "",
                    f"file_not_trading_day_{source}",
                    tushare_value="present" if source == "tushare" else "",
                    ricequant_value="present" if source == "ricequant" else "",
                )
            )

    for date in sorted(tushare_dates - ricequant_dates):
        rows.append(_diff_row(date, "", "file_only_tushare", tushare_value="present"))

    for date in sorted(ricequant_dates - tushare_dates):
        rows.append(
            _diff_row(date, "", "file_only_ricequant", ricequant_value="present")
        )

    return rows


def _finish_compare(
    ctx: click.Context, tag: str, diff: pd.DataFrame, report_path: Path
) -> None:
    """Write the diff report (or remove a stale one) and set the exit code."""
    if diff.empty:
        if report_path.exists():
            report_path.unlink()
        click.echo(f"[{tag}] No differences found.")
        return

    _write_csv(diff, report_path)
    click.echo(
        f"[{tag}] Differences found: {len(diff)} rows. Report written to {report_path}"
    )
    ctx.exit(1)


# ---------------------------------------------------------------------------
# calendar
# ---------------------------------------------------------------------------


def _load_calendar_csv(path: Path, source: str) -> pd.DataFrame:
    df = _load_dated_csv(path, source, "calendar", ["date", "is_open"])
    normalized = df.copy()
    normalized["date"] = normalized["date"].str.replace("-", "", regex=False)
    normalized["is_open"] = normalized["is_open"].str.strip().str.upper()
    return (
        normalized.sort_values("date")
        .drop_duplicates(subset=["date"], keep="last")
        .reset_index(drop=True)
    )


def _compare_calendar_frames(
    tushare_df: pd.DataFrame, ricequant_df: pd.DataFrame
) -> pd.DataFrame:
    merged = tushare_df.merge(
        ricequant_df,
        on="date",
        how="outer",
        suffixes=("_tushare", "_ricequant"),
        indicator=True,
    )

    only_tushare = merged[merged["_merge"] == "left_only"].copy()
    only_tushare["status"] = "only_tushare"

    only_ricequant = merged[merged["_merge"] == "right_only"].copy()
    only_ricequant["status"] = "only_ricequant"

    mismatched = merged[
        (merged["_merge"] == "both")
        & (merged["is_open_tushare"] != merged["is_open_ricequant"])
    ].copy()
    mismatched["status"] = "mismatch_is_open"

    diff = pd.concat([mismatched, only_tushare, only_ricequant], ignore_index=True)
    if diff.empty:
        return pd.DataFrame(
            columns=["date", "status", "tushare_is_open", "ricequant_is_open"]
        )

    diff = diff.rename(
        columns={
            "is_open_tushare": "tushare_is_open",
            "is_open_ricequant": "ricequant_is_open",
        }
    )
    cols = ["date", "status", "tushare_is_open", "ricequant_is_open"]
    return diff[cols].sort_values(["date", "status"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# stock list
# ---------------------------------------------------------------------------


def _load_stock_list_csv(path: Path, source: str) -> pd.DataFrame:
    df = _load_dated_csv(path, source, "stock_list", _STOCK_LIST_COLUMNS)

    normalized = df.copy()
    normalized["symbol"] = normalized["symbol"].map(_normalize_text)
    normalized["date"] = normalized["date"].map(_normalize_basic_date)
    normalized["name"] = normalized["name"].map(_normalize_text)
    normalized["exchange"] = normalized["exchange"].map(_normalize_exchange)
    normalized["board"] = normalized["board"].map(_normalize_text).str.upper()
    normalized["curr_type"] = normalized["curr_type"].map(_normalize_text).str.upper()
    normalized["list_date"] = normalized["list_date"].map(_normalize_basic_date)
    normalized["delist_date"] = normalized["delist_date"].map(_normalize_basic_date)

    _validate_date_matches_filename(normalized, path, source, "stock_list")

    return (
        normalized.sort_values("symbol")
        .drop_duplicates(subset=["symbol"], keep="last")
        .reset_index(drop=True)
    )


def _load_stock_list_dir(path: Path, source: str) -> dict[str, pd.DataFrame]:
    return _load_dated_dir(path, source, "stock_list", _load_stock_list_csv)


def _compare_stock_list_frames(
    tushare_df: pd.DataFrame, ricequant_df: pd.DataFrame
) -> list[dict[str, object]]:
    merged = tushare_df.merge(
        ricequant_df,
        on=["date", "symbol"],
        how="outer",
        suffixes=("_tushare", "_ricequant"),
        indicator=True,
    )

    rows: list[dict[str, object]] = []

    for row in merged[merged["_merge"] == "left_only"].itertuples():
        rows.append(
            _diff_row(
                row.date, row.symbol, "symbol_only_tushare", tushare_value="present"
            )
        )

    for row in merged[merged["_merge"] == "right_only"].itertuples():
        rows.append(
            _diff_row(
                row.date, row.symbol, "symbol_only_ricequant", ricequant_value="present"
            )
        )

    both = merged[merged["_merge"] == "both"]
    for field in _STOCK_COMPARE_FIELDS:
        tushare_values = both[f"{field}_tushare"]
        ricequant_values = both[f"{field}_ricequant"]
        if field == "delist_date":
            # A delist date later than the snapshot date has not taken effect yet;
            # sources fill it at different times, so treat it as empty.
            tushare_values = tushare_values.mask(tushare_values > both["date"], "")
            ricequant_values = ricequant_values.mask(
                ricequant_values > both["date"], ""
            )
        mismatch = tushare_values != ricequant_values
        for date, symbol, tushare_value, ricequant_value in zip(
            both.loc[mismatch, "date"],
            both.loc[mismatch, "symbol"],
            tushare_values[mismatch],
            ricequant_values[mismatch],
        ):
            rows.append(
                _diff_row(
                    date,
                    symbol,
                    "value_mismatch",
                    field,
                    tushare_value,
                    ricequant_value,
                )
            )

    return rows


# ---------------------------------------------------------------------------
# stock daily
# ---------------------------------------------------------------------------


def _load_stock_daily_csv(path: Path, source: str) -> pd.DataFrame:
    df = _load_dated_csv(path, source, "stock_daily", _STOCK_DAILY_COLUMNS)

    normalized = df.copy()
    normalized["symbol"] = normalized["symbol"].map(_normalize_text)
    normalized["date"] = normalized["date"].map(_normalize_basic_date)
    for field in _STOCK_DAILY_FIELD_TOLERANCES:
        normalized[field] = pd.to_numeric(normalized[field], errors="coerce")

    _validate_date_matches_filename(normalized, path, source, "stock_daily")

    return (
        normalized.sort_values("symbol")
        .drop_duplicates(subset=["symbol"], keep="last")
        .reset_index(drop=True)
    )


def _load_stock_daily_dir(path: Path, source: str) -> dict[str, pd.DataFrame]:
    return _load_dated_dir(path, source, "stock_daily", _load_stock_daily_csv)


def _compare_stock_daily_frames(
    tushare_df: pd.DataFrame, ricequant_df: pd.DataFrame
) -> list[dict[str, object]]:
    merged = tushare_df.merge(
        ricequant_df,
        on=["date", "symbol"],
        how="outer",
        suffixes=("_tushare", "_ricequant"),
        indicator=True,
    )

    rows: list[dict[str, object]] = []

    for row in merged[merged["_merge"] == "left_only"].itertuples():
        rows.append(
            _diff_row(
                row.date, row.symbol, "symbol_only_tushare", tushare_value="present"
            )
        )

    # rqdatac's get_price pads suspension days with placeholder rows
    # (volume=0, OHLC=pre_close) while tushare's daily omits suspended stocks
    # entirely — that's a representation difference, not a data difference.
    only_ricequant = merged[
        (merged["_merge"] == "right_only") & (merged["volume_ricequant"] > 0)
    ]
    for row in only_ricequant.itertuples():
        rows.append(
            _diff_row(
                row.date, row.symbol, "symbol_only_ricequant", ricequant_value="present"
            )
        )

    both = merged[merged["_merge"] == "both"]
    for field, tolerance in _STOCK_DAILY_FIELD_TOLERANCES.items():
        tushare_values = both[f"{field}_tushare"]
        ricequant_values = both[f"{field}_ricequant"]
        # ~(diff <= tol) rather than (diff > tol) so NaN on either side counts
        # as a mismatch instead of being silently treated as equal.
        mismatch = ~((tushare_values - ricequant_values).abs() <= tolerance)
        for date, symbol, tushare_value, ricequant_value in zip(
            both.loc[mismatch, "date"],
            both.loc[mismatch, "symbol"],
            tushare_values[mismatch],
            ricequant_values[mismatch],
        ):
            rows.append(
                _diff_row(
                    date,
                    symbol,
                    "value_mismatch",
                    field,
                    tushare_value,
                    ricequant_value,
                )
            )

    return rows


# ---------------------------------------------------------------------------
# CLI group
# ---------------------------------------------------------------------------


@click.group("compare")
def compare() -> None:
    """Compare stored data files across sources."""


@compare.command("calendar")
@click.pass_context
def cmd_compare_calendar(ctx: click.Context) -> None:
    """Compare stored tushare/ricequant calendar.csv files."""

    output_root = ctx.obj["output_root"]
    tushare_df = _load_calendar_csv(output_root / "tushare" / "calendar.csv", "tushare")
    ricequant_df = _load_calendar_csv(
        output_root / "ricequant" / "calendar.csv", "ricequant"
    )
    diff = _compare_calendar_frames(tushare_df, ricequant_df)

    _finish_compare(
        ctx, "compare calendar", diff, output_root / "compare" / "calendar_diff.csv"
    )


@compare.command("stock-list")
@click.pass_context
def cmd_compare_stock_list(ctx: click.Context) -> None:
    """Compare stored tushare/ricequant stock_list CSV files."""

    output_root = ctx.obj["output_root"]
    tushare_files = _load_stock_list_dir(
        output_root / "tushare" / "stock_list", "tushare"
    )
    ricequant_files = _load_stock_list_dir(
        output_root / "ricequant" / "stock_list", "ricequant"
    )
    tushare_dates = set(tushare_files)
    ricequant_dates = set(ricequant_files)

    rows = _diff_file_presence(output_root, tushare_dates, ricequant_dates)
    for date in sorted(tushare_dates & ricequant_dates):
        rows.extend(
            _compare_stock_list_frames(tushare_files[date], ricequant_files[date])
        )

    diff = (
        pd.DataFrame(rows, columns=_DIFF_COLUMNS)
        .sort_values(["date", "symbol", "status", "field"])
        .reset_index(drop=True)
    )
    _finish_compare(
        ctx,
        "compare stock-list",
        diff,
        output_root / "compare" / "stock_list_diff.csv",
    )


@compare.command("stock-daily")
@click.pass_context
def cmd_compare_stock_daily(ctx: click.Context) -> None:
    """Compare stored tushare/ricequant stock_daily CSV files."""

    output_root = ctx.obj["output_root"]
    tushare_files = _load_stock_daily_dir(
        output_root / "tushare" / "stock_daily", "tushare"
    )
    ricequant_files = _load_stock_daily_dir(
        output_root / "ricequant" / "stock_daily", "ricequant"
    )
    tushare_dates = set(tushare_files)
    ricequant_dates = set(ricequant_files)

    rows = _diff_file_presence(output_root, tushare_dates, ricequant_dates)
    for date in sorted(tushare_dates & ricequant_dates):
        rows.extend(
            _compare_stock_daily_frames(tushare_files[date], ricequant_files[date])
        )

    diff = (
        pd.DataFrame(rows, columns=_DIFF_COLUMNS)
        .sort_values(["date", "symbol", "status", "field"])
        .reset_index(drop=True)
    )
    _finish_compare(
        ctx,
        "compare stock-daily",
        diff,
        output_root / "compare" / "stock_daily_diff.csv",
    )
