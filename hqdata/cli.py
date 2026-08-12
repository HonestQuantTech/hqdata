"""hqdata command line tool for fetching and storing market data as CSV."""

from pathlib import Path
from typing import Callable, Optional

import click
import pandas as pd

import hqdata
from hqdata.compare_cli import compare

VALID_SOURCES = ["tushare", "ricequant"]


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")


def _run_for_sources(obj: dict, fn: Callable[[str, Path], None]) -> None:
    for source in obj["sources"]:
        click.echo(f"\n=== Initializing source: {source} ===")
        hqdata.init_source(source)
        fn(source, obj["output_root"])
    click.echo("\nAll done.")


# ---------------------------------------------------------------------------
# CLI group
# ---------------------------------------------------------------------------


@click.group()
@click.option(
    "--source",
    default="tushare",
    metavar="SOURCE[,SOURCE...]",
    help=f"Comma-separated data source(s). Valid: {', '.join(VALID_SOURCES)}. Default: tushare",
)
@click.option(
    "--output",
    default=str(Path.home() / ".hqdata"),
    metavar="DIR",
    help="Root output directory. Default: ~/.hqdata",
)
@click.pass_context
def cli(ctx: click.Context, source: str, output: str) -> None:
    """Fetch A-share market data from configured sources and save as CSV files.

    \b
    Output: {output}/{source}/{type}/{date}.csv
    Calendar: {output}/{source}/calendar.csv
    """
    ctx.ensure_object(dict)
    sources = [s.strip() for s in source.split(",")]
    invalid = [s for s in sources if s not in VALID_SOURCES]
    if invalid:
        raise click.BadParameter(
            f"Invalid: {', '.join(invalid)}. Valid: {', '.join(VALID_SOURCES)}",
            param_hint="'--source'",
        )
    output_root = Path(output).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    ctx.obj["sources"] = sources
    ctx.obj["output_root"] = output_root


cli.add_command(compare)


# ---------------------------------------------------------------------------
# commands (in api.py order)
# ---------------------------------------------------------------------------


@cli.command("calendar")
@click.option("--start", required=True, metavar="YYYYMMDD", help="Start date")
@click.option("--end", required=True, metavar="YYYYMMDD", help="End date")
@click.pass_obj
def cmd_calendar(obj: dict, start: str, end: str) -> None:
    """Fetch trading calendar and save as calendar.csv."""

    def fetch(source: str, output_root: Path) -> None:
        click.echo(f"[{source}][calendar] Fetching calendar ({start} ~ {end})...")
        df = hqdata.get_calendar(start, end)
        out_path = output_root / source / "calendar.csv"
        _write_csv(df, out_path)
        click.echo(f"[{source}][calendar] Done. Written to {out_path}")

    _run_for_sources(obj, fetch)


@cli.command("stock-list")
@click.option(
    "--start",
    default=None,
    metavar="YYYYMMDD",
    help="Start date (default: current trading day)",
)
@click.option(
    "--end",
    default=None,
    metavar="YYYYMMDD",
    help="End date (default: current trading day)",
)
@click.pass_obj
def cmd_stock_list(obj: dict, start: Optional[str], end: Optional[str]) -> None:
    """Fetch stock list for a date range and save one CSV per trading day.

    \b
    When --start/--end are omitted, only the current trading day is fetched.
    """

    def fetch(source: str, output_root: Path) -> None:
        today = hqdata.get_current_trading_day()
        actual_start = start or today
        actual_end = end or today

        calendar_df = hqdata.get_calendar(actual_start, actual_end, is_open=True)
        trading_days = calendar_df["date"].tolist()

        out_dir = output_root / source / "stock_list"
        skipped = 0
        for d in trading_days:
            out_path = out_dir / f"{d}.csv"
            if out_path.exists():
                skipped += 1
                continue
            click.echo(f"[{source}][stock-list] Fetching {d}...")
            df = hqdata.get_stock_list(trade_date=d)
            bad_dates = sorted(set(df.loc[df["date"] != d, "date"]))
            if bad_dates:
                raise click.ClickException(
                    f"[{source}][stock-list] date column contains {', '.join(bad_dates)} "
                    f"while fetching {d}; refusing to write {out_path}"
                )
            _write_csv(df, out_path)

        if skipped:
            click.echo(
                f"[{source}][stock-list] Skipped {skipped} already-existing file(s)."
            )
        click.echo(f"[{source}][stock-list] Done. Written to {out_dir}")

    _run_for_sources(obj, fetch)


@cli.command("stock-daily")
@click.option(
    "--start",
    default=None,
    metavar="YYYYMMDD",
    help="Start date (default: current trading day)",
)
@click.option(
    "--end",
    default=None,
    metavar="YYYYMMDD",
    help="End date (default: current trading day)",
)
@click.pass_obj
def cmd_stock_daily(obj: dict, start: Optional[str], end: Optional[str]) -> None:
    """Fetch stock daily bar data for a date range and save one CSV per trading day.

    \b
    Writing per day (rather than accumulating everything and writing once at
    the end) means a day already on disk is skipped on re-run, and an error
    on one day does not discard bars already fetched for earlier days.
    """

    def fetch(source: str, output_root: Path) -> None:
        today = hqdata.get_current_trading_day()
        actual_start = start or today
        actual_end = end or today

        calendar_df = hqdata.get_calendar(actual_start, actual_end, is_open=True)
        trading_days = calendar_df["date"].tolist()

        out_dir = output_root / source / "stock_daily"
        skipped = 0
        written = 0
        for d in trading_days:
            out_path = out_dir / f"{d}.csv"
            if out_path.exists():
                skipped += 1
                continue

            click.echo(f"[{source}][stock-daily] Fetching stock list for {d}...")
            symbols = hqdata.get_stock_list(trade_date=d)["symbol"].tolist()
            if not symbols:
                continue

            click.echo(
                f"[{source}][stock-daily] Fetching bars for {d} "
                f"({len(symbols)} symbols)..."
            )
            try:
                df = hqdata.get_stock_daily_bar(
                    ",".join(symbols), start_date=d, end_date=d
                )
            except Exception as e:
                click.echo(f"[{source}][stock-daily] ERROR: {e}", err=True)
                click.echo(
                    f"[{source}][stock-daily] {written} day(s) already written. "
                    "Re-run the same command to resume.",
                    err=True,
                )
                return

            if df is not None and not df.empty:
                _write_csv(df, out_path)
                written += 1

        if skipped:
            click.echo(
                f"[{source}][stock-daily] Skipped {skipped} already-existing file(s)."
            )
        if written == 0 and skipped == 0:
            click.echo(f"[{source}][stock-daily] No data fetched.")
        else:
            click.echo(f"[{source}][stock-daily] Done. Written to {out_dir}")

    _run_for_sources(obj, fetch)


@cli.command("stock-factor")
@click.option(
    "--start",
    default=None,
    metavar="YYYYMMDD",
    help="Start date (default: current trading day)",
)
@click.option(
    "--end",
    default=None,
    metavar="YYYYMMDD",
    help="End date (default: current trading day)",
)
@click.pass_obj
def cmd_stock_factor(obj: dict, start: Optional[str], end: Optional[str]) -> None:
    """Fetch stock adjustment factors for a date range and save one CSV per trading day.

    \b
    Only fetches factors for stocks in that day's stock list, matching stock-daily.
    """

    def fetch(source: str, output_root: Path) -> None:
        today = hqdata.get_current_trading_day()
        actual_start = start or today
        actual_end = end or today

        calendar_df = hqdata.get_calendar(actual_start, actual_end, is_open=True)
        trading_days = calendar_df["date"].tolist()

        out_dir = output_root / source / "stock_factor"
        skipped = 0
        written = 0
        for d in trading_days:
            out_path = out_dir / f"{d}.csv"
            if out_path.exists():
                skipped += 1
                continue

            click.echo(f"[{source}][stock-factor] Fetching stock list for {d}...")
            symbols = hqdata.get_stock_list(trade_date=d)["symbol"].tolist()
            if not symbols:
                continue

            click.echo(
                f"[{source}][stock-factor] Fetching factors for {d} "
                f"({len(symbols)} symbols)..."
            )
            try:
                df = hqdata.get_stock_factor(",".join(symbols), trade_date=d)
            except Exception as e:
                click.echo(f"[{source}][stock-factor] ERROR: {e}", err=True)
                click.echo(
                    f"[{source}][stock-factor] {written} day(s) already written. "
                    "Re-run the same command to resume.",
                    err=True,
                )
                return

            if df is not None and not df.empty:
                _write_csv(df, out_path)
                written += 1

        if skipped:
            click.echo(
                f"[{source}][stock-factor] Skipped {skipped} already-existing file(s)."
            )
        if written == 0 and skipped == 0:
            click.echo(f"[{source}][stock-factor] No data fetched.")
        else:
            click.echo(f"[{source}][stock-factor] Done. Written to {out_dir}")

    _run_for_sources(obj, fetch)


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------


def main() -> None:
    cli()


if __name__ == "__main__":
    main()
