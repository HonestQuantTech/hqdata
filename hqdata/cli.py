"""hqdata command line tool for fetching and storing market data as CSV."""

import argparse
import sys
from pathlib import Path
from typing import List

import pandas as pd

import hqdata

VALID_SOURCES = ["tushare", "ricequant"]
VALID_FREQUENCIES = ["1m", "5m", "15m", "30m", "60m"]


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")


def _write_by_date(frames: List[pd.DataFrame], out_dir: Path, tag: str) -> None:
    """Concat frames, split by 'date' column, write one CSV per date."""
    combined = pd.concat(frames, ignore_index=True)
    for date_str, group in combined.groupby("date"):
        _write_csv(group, out_dir / f"{date_str}.csv")
    print(f"[{tag}] Done. Written to {out_dir}")


# ---------------------------------------------------------------------------
# fetch functions
# ---------------------------------------------------------------------------


def _fetch_bar_data(
    source: str,
    cmd: str,
    get_list_fn,
    get_bar_fn,
    sub_dir: str,
    args,
    output_root: Path,
) -> None:
    print(f"[{source}][{cmd}] Fetching list...")
    list_df = get_list_fn()
    symbols: List[str] = list_df["symbol"].tolist()
    total = len(symbols)
    bar_kwargs = {}
    if hasattr(args, "frequency"):
        bar_kwargs["frequency"] = args.frequency
    bar_kwargs["start_date"] = getattr(args, "start", None)
    bar_kwargs["end_date"] = getattr(args, "end", None)

    print(f"[{source}][{cmd}] {total} symbols found. Fetching bars...")
    all_symbols = ",".join(symbols)
    try:
        df = get_bar_fn(all_symbols, **bar_kwargs)
    except Exception as e:
        print(f"[{source}][{cmd}] ERROR: {e}", file=sys.stderr)
        return

    if df.empty:
        print(f"[{source}][{cmd}] No data fetched.")
        return

    out_dir = output_root / source / sub_dir
    _write_by_date([df], out_dir, f"{source}][{cmd}")


def fetch_stock_daily(source: str, args, output_root: Path) -> None:
    _fetch_bar_data(
        source,
        "stock-daily",
        hqdata.get_stock_list,
        hqdata.get_stock_daily_bar,
        "stock_daily",
        args,
        output_root,
    )


def fetch_stock_minute(source: str, args, output_root: Path) -> None:
    _fetch_bar_data(
        source,
        "stock-minute",
        hqdata.get_stock_list,
        hqdata.get_stock_minute_bar,
        "stock_minute",
        args,
        output_root,
    )


def fetch_index_daily(source: str, args, output_root: Path) -> None:
    _fetch_bar_data(
        source,
        "index-daily",
        hqdata.get_index_list,
        hqdata.get_index_daily_bar,
        "index_daily",
        args,
        output_root,
    )


def fetch_index_minute(source: str, args, output_root: Path) -> None:
    _fetch_bar_data(
        source,
        "index-minute",
        hqdata.get_index_list,
        hqdata.get_index_minute_bar,
        "index_minute",
        args,
        output_root,
    )


def fetch_stock_list(source: str, args, output_root: Path) -> None:
    print(f"[{source}][stock-list] Fetching stock list...")
    df = hqdata.get_stock_list()
    today = hqdata.get_current_trading_day()
    out_path = output_root / source / "stock_list" / f"{today}.csv"
    _write_csv(df, out_path)
    print(f"[{source}][stock-list] Done. Written to {out_path}")


def fetch_index_list(source: str, args, output_root: Path) -> None:
    market = getattr(args, "market", None)
    print(f"[{source}][index-list] Fetching index list (market={market})...")
    df = hqdata.get_index_list(market=market)
    today = hqdata.get_current_trading_day()
    out_path = output_root / source / "index_list" / f"{today}.csv"
    _write_csv(df, out_path)
    print(f"[{source}][index-list] Done. Written to {out_path}")


def fetch_calendar(source: str, args, output_root: Path) -> None:
    print(f"[{source}][calendar] Fetching calendar ({args.start} ~ {args.end})...")
    df = hqdata.get_calendar(args.start, args.end)
    out_path = output_root / source / "calendar.csv"
    _write_csv(df, out_path)
    print(f"[{source}][calendar] Done. Written to {out_path}")


_FETCH_FUNCS = {
    "stock-daily": fetch_stock_daily,
    "stock-minute": fetch_stock_minute,
    "index-daily": fetch_index_daily,
    "index-minute": fetch_index_minute,
    "stock-list": fetch_stock_list,
    "index-list": fetch_index_list,
    "calendar": fetch_calendar,
}

# ---------------------------------------------------------------------------
# argument parser
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="hqdata",
        description=(
            "Fetch A-share market data from configured sources and save as CSV files.\n"
            "Output directory: {output}/{source}/{type}/{date}.csv\n"
            "Calendar output: {output}/{source}/calendar.csv"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--source",
        default="tushare",
        metavar="SOURCE[,SOURCE...]",
        help=(
            "Comma-separated data source(s). "
            f"Valid values: {', '.join(VALID_SOURCES)}. Default: tushare"
        ),
    )
    parser.add_argument(
        "--output",
        default=str(Path.home() / ".hqdata"),
        metavar="DIR",
        help="Root output directory. Default: ~/.hqdata",
    )

    subparsers = parser.add_subparsers(dest="command", metavar="COMMAND", required=True)

    # --- stock-daily ---
    p_sd = subparsers.add_parser("stock-daily", help="Fetch stock daily bar data")
    p_sd.add_argument(
        "--start",
        default=None,
        metavar="YYYYMMDD",
        help="Start date (default: current trading day)",
    )
    p_sd.add_argument(
        "--end",
        default=None,
        metavar="YYYYMMDD",
        help="End date (default: current trading day)",
    )

    # --- stock-minute ---
    p_sm = subparsers.add_parser("stock-minute", help="Fetch stock minute bar data")
    p_sm.add_argument(
        "--start",
        default=None,
        metavar="YYYYMMDD",
        help="Start date (default: current trading day)",
    )
    p_sm.add_argument(
        "--end",
        default=None,
        metavar="YYYYMMDD",
        help="End date (default: current trading day)",
    )
    p_sm.add_argument(
        "--frequency",
        default="1m",
        choices=VALID_FREQUENCIES,
        help=f"Bar frequency. Choices: {', '.join(VALID_FREQUENCIES)}. Default: 1m",
    )

    # --- index-daily ---
    p_id = subparsers.add_parser("index-daily", help="Fetch index daily bar data")
    p_id.add_argument(
        "--start",
        default=None,
        metavar="YYYYMMDD",
        help="Start date (default: current trading day)",
    )
    p_id.add_argument(
        "--end",
        default=None,
        metavar="YYYYMMDD",
        help="End date (default: current trading day)",
    )

    # --- index-minute ---
    p_im = subparsers.add_parser("index-minute", help="Fetch index minute bar data")
    p_im.add_argument(
        "--start",
        default=None,
        metavar="YYYYMMDD",
        help="Start date (default: current trading day)",
    )
    p_im.add_argument(
        "--end",
        default=None,
        metavar="YYYYMMDD",
        help="End date (default: current trading day)",
    )
    p_im.add_argument(
        "--frequency",
        default="1m",
        choices=VALID_FREQUENCIES,
        help=f"Bar frequency. Choices: {', '.join(VALID_FREQUENCIES)}. Default: 1m",
    )

    # --- stock-list ---
    subparsers.add_parser(
        "stock-list", help="Fetch today's stock list and save as {today}.csv"
    )

    # --- index-list ---
    p_il = subparsers.add_parser(
        "index-list", help="Fetch today's index list and save as {today}.csv"
    )
    p_il.add_argument(
        "--market",
        default="SSE,SZSE",
        help="Market filter (e.g. CSI, SSE, SZSE, SW). Default: SSE,SZSE",
    )

    # --- calendar ---
    p_cal = subparsers.add_parser(
        "calendar", help="Fetch trading calendar and save as calendar.csv"
    )
    p_cal.add_argument("--start", required=True, metavar="YYYYMMDD", help="Start date")
    p_cal.add_argument("--end", required=True, metavar="YYYYMMDD", help="End date")

    return parser


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    sources = [s.strip() for s in args.source.split(",")]
    invalid = [s for s in sources if s not in VALID_SOURCES]
    if invalid:
        parser.error(
            f"Invalid source(s): {', '.join(invalid)}. "
            f"Valid values: {', '.join(VALID_SOURCES)}"
        )

    output_root = Path(args.output).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    fetch_fn = _FETCH_FUNCS[args.command]

    for source in sources:
        print(f"\n=== Initializing source: {source} ===")
        hqdata.init_source(source)
        fetch_fn(source, args, output_root)

    print("\nAll done.")


if __name__ == "__main__":
    main()
