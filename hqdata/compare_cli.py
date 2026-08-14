"""Subcommands for comparing stored data across sources."""

from pathlib import Path
from typing import Callable

import click
import pandas as pd


# compare calendar/stock-list can compare any two of these.
_VALID_SOURCES = ["tushare", "ricequant", "akshare"]

# compare stock-daily/stock-factor exclude akshare: it doesn't produce that
# data (see CLAUDE.md/README), so there's nothing to compare it against yet.
_VALID_SOURCES_DAILY_FACTOR = ["tushare", "ricequant"]

_DEFAULT_SOURCE_PAIR = ("tushare", "ricequant")

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

_STOCK_COMPARE_FIELDS = [
    "name",
    "exchange",
    "board",
    "curr_type",
    "list_date",
    "delist_date",
]

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

_STOCK_FACTOR_COLUMNS = ["symbol", "date", "factor"]

# factor is a cumulative back-adjustment multiplier anchored to a source-specific
# base point, so raw values are never comparable across sources — only
# day-over-day ratios (factor[t] / factor[t-1]) are, since a ratio of 1.0 means
# "no corporate action that day" and a ratio > 1.0 means "this day's ex-rights
# event scaled the back-adjusted price by this much" regardless of anchor.
# Tolerance derived from a real-data survey (23 trading days x ~5500 stocks,
# ~120k day-over-day pairs): 99.9% of pairs match to float noise (< 2e-6).
# The 17 pairs that differ more are all real ex-rights days where tushare and
# ricequant round the ex-rights price differently before deriving the ratio —
# worst observed gap 6.03e-4 (002107.SZ, a cash-dividend-only event). Set to
# 0.001: comfortably above that known rounding-convention gap, while still
# catching a materially wrong ratio (a missed or double-counted corporate
# action moves the ratio by several percent or more).
_STOCK_FACTOR_RATIO_TOLERANCE = 0.001

# field -> absolute tolerance (0.0 = exact). Thresholds derived from a full-corpus
# survey of 139 days x ~5500 stocks of real stored data:
# - turnover: tushare's thousands-of-yuan -> yuan conversion is only precise to
#   the yuan while ricequant keeps cents, so noise runs up to 1 yuan — and the
#   float form of a one-yuan gap can slightly exceed 1.0 (e.g. 8249685.0 -
#   8249683.999999999). The tolerance is set to 2.0: comfortably above the
#   noise, still three orders of magnitude below the smallest real discrepancy
#   observed (> 1000 yuan).
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


def _parse_source_pair(
    sources: str, valid_sources: list[str] = _VALID_SOURCES
) -> tuple[str, str]:
    """Parse and validate a "SOURCE_A,SOURCE_B" --sources option value."""
    parts = [s.strip() for s in sources.split(",")]
    if len(parts) != 2:
        raise click.BadParameter(
            f"Expected exactly 2 comma-separated sources, got: {sources}",
            param_hint="'--sources'",
        )
    source_a, source_b = parts
    invalid = [s for s in (source_a, source_b) if s not in valid_sources]
    if invalid:
        raise click.BadParameter(
            f"Invalid: {', '.join(invalid)}. Valid: {', '.join(valid_sources)}",
            param_hint="'--sources'",
        )
    if source_a == source_b:
        raise click.BadParameter(
            f"Sources must be different, got '{source_a}' twice",
            param_hint="'--sources'",
        )
    return source_a, source_b


def _diff_report_filename(base: str, source_a: str, source_b: str) -> str:
    """{base}.csv for the default tushare/ricequant pair; {base}_{a}_{b}.csv otherwise."""
    if (source_a, source_b) == _DEFAULT_SOURCE_PAIR:
        return f"{base}.csv"
    return f"{base}_{source_a}_{source_b}.csv"


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


def _diff_columns(source_a: str, source_b: str) -> list[str]:
    return [
        "date",
        "symbol",
        "status",
        "field",
        f"{source_a}_value",
        f"{source_b}_value",
    ]


def _diff_row(
    source_a: str,
    source_b: str,
    date: object,
    symbol: object,
    status: str,
    field: str = "",
    value_a: object = "",
    value_b: object = "",
) -> dict[str, object]:
    return {
        "date": date,
        "symbol": symbol,
        "status": status,
        "field": field,
        f"{source_a}_value": value_a,
        f"{source_b}_value": value_b,
    }


def _diff_file_presence(
    output_root: Path,
    source_a: str,
    dates_a: set[str],
    source_b: str,
    dates_b: set[str],
) -> list[dict[str, object]]:
    """Diff rows for file-level presence: non-trading-day files and one-sided files.

    Shared by every per-date compare command (stock-list, stock-daily): each of
    these stores one file per trading day, so file presence should always agree
    with each source's own calendar.csv and with the other source.
    """
    rows: list[dict[str, object]] = []

    for source, dates in ((source_a, dates_a), (source_b, dates_b)):
        non_trading_days = _load_non_trading_days(output_root, source)
        for date in sorted(dates & non_trading_days):
            rows.append(
                _diff_row(
                    source_a,
                    source_b,
                    date,
                    "",
                    f"file_not_trading_day_{source}",
                    value_a="present" if source == source_a else "",
                    value_b="present" if source == source_b else "",
                )
            )

    for date in sorted(dates_a - dates_b):
        rows.append(
            _diff_row(
                source_a, source_b, date, "", f"file_only_{source_a}", value_a="present"
            )
        )

    for date in sorted(dates_b - dates_a):
        rows.append(
            _diff_row(
                source_a, source_b, date, "", f"file_only_{source_b}", value_b="present"
            )
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
    source_a: str, df_a: pd.DataFrame, source_b: str, df_b: pd.DataFrame
) -> pd.DataFrame:
    suffix_a, suffix_b = f"_{source_a}", f"_{source_b}"
    is_open_a, is_open_b = f"is_open{suffix_a}", f"is_open{suffix_b}"
    col_a, col_b = f"{source_a}_is_open", f"{source_b}_is_open"

    merged = df_a.merge(
        df_b,
        on="date",
        how="outer",
        suffixes=(suffix_a, suffix_b),
        indicator=True,
    )

    only_a = merged[merged["_merge"] == "left_only"].copy()
    only_a["status"] = f"only_{source_a}"

    only_b = merged[merged["_merge"] == "right_only"].copy()
    only_b["status"] = f"only_{source_b}"

    mismatched = merged[
        (merged["_merge"] == "both") & (merged[is_open_a] != merged[is_open_b])
    ].copy()
    mismatched["status"] = "mismatch_is_open"

    diff = pd.concat([mismatched, only_a, only_b], ignore_index=True)
    if diff.empty:
        return pd.DataFrame(columns=["date", "status", col_a, col_b])

    diff = diff.rename(columns={is_open_a: col_a, is_open_b: col_b})
    cols = ["date", "status", col_a, col_b]
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
    source_a: str, df_a: pd.DataFrame, source_b: str, df_b: pd.DataFrame
) -> list[dict[str, object]]:
    suffix_a, suffix_b = f"_{source_a}", f"_{source_b}"
    merged = df_a.merge(
        df_b,
        on=["date", "symbol"],
        how="outer",
        suffixes=(suffix_a, suffix_b),
        indicator=True,
    )

    rows: list[dict[str, object]] = []

    for row in merged[merged["_merge"] == "left_only"].itertuples():
        rows.append(
            _diff_row(
                source_a,
                source_b,
                row.date,
                row.symbol,
                f"symbol_only_{source_a}",
                value_a="present",
            )
        )

    for row in merged[merged["_merge"] == "right_only"].itertuples():
        rows.append(
            _diff_row(
                source_a,
                source_b,
                row.date,
                row.symbol,
                f"symbol_only_{source_b}",
                value_b="present",
            )
        )

    both = merged[merged["_merge"] == "both"]
    # Stocks approaching delisting (delist_date set and later than the snapshot
    # date) are renamed to the delisting-period name ("XX退") at different times
    # by different sources (e.g. ricequant renames early, tushare keeps the *ST
    # name), so name differences are only compared outside that window.
    pending_delist = (
        (both[f"delist_date{suffix_a}"] != "")
        & (both[f"delist_date{suffix_a}"] > both["date"])
    ) | (
        (both[f"delist_date{suffix_b}"] != "")
        & (both[f"delist_date{suffix_b}"] > both["date"])
    )
    for field in _STOCK_COMPARE_FIELDS:
        values_a = both[f"{field}{suffix_a}"]
        values_b = both[f"{field}{suffix_b}"]
        if field == "name":
            values_a = values_a.mask(pending_delist, "")
            values_b = values_b.mask(pending_delist, "")
        if field == "delist_date":
            # A delist date later than the snapshot date has not taken effect yet;
            # sources fill it at different times, so treat it as empty.
            values_a = values_a.mask(values_a > both["date"], "")
            values_b = values_b.mask(values_b > both["date"], "")
        mismatch = values_a != values_b
        for date, symbol, value_a, value_b in zip(
            both.loc[mismatch, "date"],
            both.loc[mismatch, "symbol"],
            values_a[mismatch],
            values_b[mismatch],
        ):
            rows.append(
                _diff_row(
                    source_a,
                    source_b,
                    date,
                    symbol,
                    "value_mismatch",
                    field,
                    value_a,
                    value_b,
                )
            )

    return rows


# ---------------------------------------------------------------------------
# stock factor
# ---------------------------------------------------------------------------


def _load_stock_factor_csv(path: Path, source: str) -> pd.DataFrame:
    df = _load_dated_csv(path, source, "stock_factor", _STOCK_FACTOR_COLUMNS)

    normalized = df.copy()
    normalized["symbol"] = normalized["symbol"].map(_normalize_text)
    normalized["date"] = normalized["date"].map(_normalize_basic_date)
    normalized["factor"] = pd.to_numeric(normalized["factor"], errors="coerce")

    _validate_date_matches_filename(normalized, path, source, "stock_factor")

    return (
        normalized.sort_values("symbol")
        .drop_duplicates(subset=["symbol"], keep="last")
        .reset_index(drop=True)
    )


def _load_stock_factor_dir(path: Path, source: str) -> dict[str, pd.DataFrame]:
    return _load_dated_dir(path, source, "stock_factor", _load_stock_factor_csv)


def _compute_factor_ratio(files: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Day-over-day factor ratio per symbol, across every loaded date.

    factor's absolute value isn't comparable across sources (see
    _STOCK_FACTOR_RATIO_TOLERANCE), so comparison happens on this ratio instead.
    The ratio is computed against each symbol's previous *available* row, not
    literally date-1 — a symbol missing from one day's file (e.g. not yet
    listed) just gets skipped over, matching how the two sources' file sets
    are expected to align in practice.
    """
    if not files:
        return pd.DataFrame(columns=["symbol", "date", "ratio"])
    all_df = pd.concat(files.values(), ignore_index=True).sort_values(
        ["symbol", "date"]
    )
    all_df["ratio"] = all_df["factor"] / all_df.groupby("symbol")["factor"].shift(1)
    return all_df[["symbol", "date", "ratio"]].reset_index(drop=True)


def _compare_stock_factor_frames(
    source_a: str,
    files_a: dict[str, pd.DataFrame],
    source_b: str,
    files_b: dict[str, pd.DataFrame],
) -> list[dict[str, object]]:
    suffix_a, suffix_b = f"_{source_a}", f"_{source_b}"
    rows: list[dict[str, object]] = []

    common_dates = sorted(set(files_a) & set(files_b))
    for date in common_dates:
        symbols_a = set(files_a[date]["symbol"])
        symbols_b = set(files_b[date]["symbol"])
        for symbol in sorted(symbols_a - symbols_b):
            rows.append(
                _diff_row(
                    source_a,
                    source_b,
                    date,
                    symbol,
                    f"symbol_only_{source_a}",
                    value_a="present",
                )
            )
        for symbol in sorted(symbols_b - symbols_a):
            rows.append(
                _diff_row(
                    source_a,
                    source_b,
                    date,
                    symbol,
                    f"symbol_only_{source_b}",
                    value_b="present",
                )
            )

    ratio_a = _compute_factor_ratio(files_a)
    ratio_b = _compute_factor_ratio(files_b)
    merged = ratio_a.merge(
        ratio_b,
        on=["date", "symbol"],
        how="inner",
        suffixes=(suffix_a, suffix_b),
    )
    # A NaN ratio just means "no prior data point in the loaded window" (e.g.
    # the very first date on disk) on one or both sides — not a discrepancy.
    comparable = merged.dropna(subset=[f"ratio{suffix_a}", f"ratio{suffix_b}"])
    mismatch = ~(
        (comparable[f"ratio{suffix_a}"] - comparable[f"ratio{suffix_b}"]).abs()
        <= _STOCK_FACTOR_RATIO_TOLERANCE
    )
    for date, symbol, value_a, value_b in zip(
        comparable.loc[mismatch, "date"],
        comparable.loc[mismatch, "symbol"],
        comparable.loc[mismatch, f"ratio{suffix_a}"],
        comparable.loc[mismatch, f"ratio{suffix_b}"],
    ):
        rows.append(
            _diff_row(
                source_a,
                source_b,
                date,
                symbol,
                "value_mismatch",
                "ratio",
                value_a,
                value_b,
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
    source_a: str, df_a: pd.DataFrame, source_b: str, df_b: pd.DataFrame
) -> list[dict[str, object]]:
    suffix_a, suffix_b = f"_{source_a}", f"_{source_b}"
    merged = df_a.merge(
        df_b,
        on=["date", "symbol"],
        how="outer",
        suffixes=(suffix_a, suffix_b),
        indicator=True,
    )

    rows: list[dict[str, object]] = []

    # rqdatac's get_price pads suspension days with placeholder rows
    # (volume=0, OHLC=pre_close) while tushare's daily omits suspended stocks
    # entirely — that's a representation difference, not a data difference.
    # Only ricequant does this, so the one-sided-only-if-volume>0 filter is
    # applied to whichever side is ricequant, regardless of a/b position.
    only_a = merged[merged["_merge"] == "left_only"]
    if source_a == "ricequant":
        only_a = only_a[only_a[f"volume{suffix_a}"] > 0]
    for row in only_a.itertuples():
        rows.append(
            _diff_row(
                source_a,
                source_b,
                row.date,
                row.symbol,
                f"symbol_only_{source_a}",
                value_a="present",
            )
        )

    only_b = merged[merged["_merge"] == "right_only"]
    if source_b == "ricequant":
        only_b = only_b[only_b[f"volume{suffix_b}"] > 0]
    for row in only_b.itertuples():
        rows.append(
            _diff_row(
                source_a,
                source_b,
                row.date,
                row.symbol,
                f"symbol_only_{source_b}",
                value_b="present",
            )
        )

    both = merged[merged["_merge"] == "both"]
    for field, tolerance in _STOCK_DAILY_FIELD_TOLERANCES.items():
        values_a = both[f"{field}{suffix_a}"]
        values_b = both[f"{field}{suffix_b}"]
        # ~(diff <= tol) rather than (diff > tol) so NaN on either side counts
        # as a mismatch instead of being silently treated as equal.
        mismatch = ~((values_a - values_b).abs() <= tolerance)
        for date, symbol, value_a, value_b in zip(
            both.loc[mismatch, "date"],
            both.loc[mismatch, "symbol"],
            values_a[mismatch],
            values_b[mismatch],
        ):
            rows.append(
                _diff_row(
                    source_a,
                    source_b,
                    date,
                    symbol,
                    "value_mismatch",
                    field,
                    value_a,
                    value_b,
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
@click.option(
    "--sources",
    default="tushare,ricequant",
    metavar="SOURCE_A,SOURCE_B",
    help=f"Comma-separated pair of sources to compare. Valid: {', '.join(_VALID_SOURCES)}. Default: tushare,ricequant",
)
@click.pass_context
def cmd_compare_calendar(ctx: click.Context, sources: str) -> None:
    """Compare stored calendar.csv files between two sources."""

    source_a, source_b = _parse_source_pair(sources)
    output_root = ctx.obj["output_root"]
    df_a = _load_calendar_csv(output_root / source_a / "calendar.csv", source_a)
    df_b = _load_calendar_csv(output_root / source_b / "calendar.csv", source_b)
    diff = _compare_calendar_frames(source_a, df_a, source_b, df_b)

    _finish_compare(
        ctx,
        "compare calendar",
        diff,
        output_root
        / "compare"
        / _diff_report_filename("calendar_diff", source_a, source_b),
    )


@compare.command("stock-list")
@click.option(
    "--sources",
    default="tushare,ricequant",
    metavar="SOURCE_A,SOURCE_B",
    help=f"Comma-separated pair of sources to compare. Valid: {', '.join(_VALID_SOURCES)}. Default: tushare,ricequant",
)
@click.pass_context
def cmd_compare_stock_list(ctx: click.Context, sources: str) -> None:
    """Compare stored stock_list CSV files between two sources."""

    source_a, source_b = _parse_source_pair(sources)
    output_root = ctx.obj["output_root"]
    files_a = _load_stock_list_dir(output_root / source_a / "stock_list", source_a)
    files_b = _load_stock_list_dir(output_root / source_b / "stock_list", source_b)
    dates_a = set(files_a)
    dates_b = set(files_b)

    rows = _diff_file_presence(output_root, source_a, dates_a, source_b, dates_b)
    for date in sorted(dates_a & dates_b):
        rows.extend(
            _compare_stock_list_frames(source_a, files_a[date], source_b, files_b[date])
        )

    diff = (
        pd.DataFrame(rows, columns=_diff_columns(source_a, source_b))
        .sort_values(["date", "symbol", "status", "field"])
        .reset_index(drop=True)
    )
    _finish_compare(
        ctx,
        "compare stock-list",
        diff,
        output_root
        / "compare"
        / _diff_report_filename("stock_list_diff", source_a, source_b),
    )


@compare.command("stock-daily")
@click.option(
    "--sources",
    default="tushare,ricequant",
    metavar="SOURCE_A,SOURCE_B",
    help=f"Comma-separated pair of sources to compare. Valid: {', '.join(_VALID_SOURCES_DAILY_FACTOR)}. Default: tushare,ricequant",
)
@click.pass_context
def cmd_compare_stock_daily(ctx: click.Context, sources: str) -> None:
    """Compare stored stock_daily CSV files between two sources."""

    source_a, source_b = _parse_source_pair(sources, _VALID_SOURCES_DAILY_FACTOR)
    output_root = ctx.obj["output_root"]
    files_a = _load_stock_daily_dir(output_root / source_a / "stock_daily", source_a)
    files_b = _load_stock_daily_dir(output_root / source_b / "stock_daily", source_b)
    dates_a = set(files_a)
    dates_b = set(files_b)

    rows = _diff_file_presence(output_root, source_a, dates_a, source_b, dates_b)
    for date in sorted(dates_a & dates_b):
        rows.extend(
            _compare_stock_daily_frames(
                source_a, files_a[date], source_b, files_b[date]
            )
        )

    diff = (
        pd.DataFrame(rows, columns=_diff_columns(source_a, source_b))
        .sort_values(["date", "symbol", "status", "field"])
        .reset_index(drop=True)
    )
    _finish_compare(
        ctx,
        "compare stock-daily",
        diff,
        output_root
        / "compare"
        / _diff_report_filename("stock_daily_diff", source_a, source_b),
    )


@compare.command("stock-factor")
@click.option(
    "--sources",
    default="tushare,ricequant",
    metavar="SOURCE_A,SOURCE_B",
    help=f"Comma-separated pair of sources to compare. Valid: {', '.join(_VALID_SOURCES_DAILY_FACTOR)}. Default: tushare,ricequant",
)
@click.pass_context
def cmd_compare_stock_factor(ctx: click.Context, sources: str) -> None:
    """Compare stored stock_factor CSV files between two sources.

    \b
    factor's raw value isn't comparable across sources (each anchors its
    cumulative factor to a different base point), so this compares
    day-over-day ratios (factor[t] / factor[t-1]) instead — that ratio is
    1.0 on a normal day and reflects the ex-rights scale on a corporate-action
    day, regardless of anchor.
    """

    source_a, source_b = _parse_source_pair(sources, _VALID_SOURCES_DAILY_FACTOR)
    output_root = ctx.obj["output_root"]
    files_a = _load_stock_factor_dir(output_root / source_a / "stock_factor", source_a)
    files_b = _load_stock_factor_dir(output_root / source_b / "stock_factor", source_b)
    dates_a = set(files_a)
    dates_b = set(files_b)

    rows = _diff_file_presence(output_root, source_a, dates_a, source_b, dates_b)
    rows.extend(_compare_stock_factor_frames(source_a, files_a, source_b, files_b))

    diff = (
        pd.DataFrame(rows, columns=_diff_columns(source_a, source_b))
        .sort_values(["date", "symbol", "status", "field"])
        .reset_index(drop=True)
    )
    _finish_compare(
        ctx,
        "compare stock-factor",
        diff,
        output_root
        / "compare"
        / _diff_report_filename("stock_factor_diff", source_a, source_b),
    )
