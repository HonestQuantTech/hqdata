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


def _fetch_stock_bar_by_trading_day(
    source: str,
    get_bar_fn,
    start: Optional[str],
    end: Optional[str],
    out_dir: Path,
    tag: str,
) -> None:
    """Fetch bars for each trading day and write that day's CSV immediately.

    Writing per day (rather than accumulating everything and writing once at
    the end) means a day already on disk is skipped on re-run, and an error
    on one day does not discard bars already fetched for earlier days.
    """
    if start is None or end is None:
        today = hqdata.get_current_trading_day()
    else:
        today = None
    actual_start = start or today
    actual_end = end or today

    calendar_df = hqdata.get_calendar(actual_start, actual_end, is_open=True)
    trading_days = calendar_df["date"].tolist()

    skipped = 0
    written = 0
    for trading_day in trading_days:
        out_path = out_dir / f"{trading_day}.csv"
        if out_path.exists():
            skipped += 1
            continue

        click.echo(f"[{tag}] Fetching stock list for {trading_day}...")
        symbols = hqdata.get_stock_list(trade_date=trading_day)["symbol"].tolist()
        if not symbols:
            continue

        click.echo(
            f"[{tag}] Fetching bars for {trading_day} ({len(symbols)} symbols)..."
        )
        try:
            df = get_bar_fn(
                ",".join(symbols), start_date=trading_day, end_date=trading_day
            )
        except Exception as e:
            click.echo(f"[{tag}] ERROR: {e}", err=True)
            click.echo(
                f"[{tag}] {written} day(s) already written. "
                "Re-run the same command to resume.",
                err=True,
            )
            return

        if df is not None and not df.empty:
            _write_csv(df, out_path)
            written += 1

    if skipped:
        click.echo(f"[{tag}] Skipped {skipped} already-existing file(s).")
    if written == 0 and skipped == 0:
        click.echo(f"[{tag}] No data fetched.")
    else:
        click.echo(f"[{tag}] Done. Written to {out_dir}")


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
    """Fetch stock daily bar data."""

    def fetch(source: str, output_root: Path) -> None:
        _fetch_stock_bar_by_trading_day(
            source=source,
            get_bar_fn=hqdata.get_stock_daily_bar,
            start=start,
            end=end,
            out_dir=output_root / source / "stock_daily",
            tag=f"{source}][stock-daily",
        )

    _run_for_sources(obj, fetch)


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------


def main() -> None:
    cli()


if __name__ == "__main__":
    main()
