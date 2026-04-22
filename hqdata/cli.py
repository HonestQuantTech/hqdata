"""hqdata command line tool for fetching and storing market data as CSV."""

from pathlib import Path
from typing import Callable, Optional

import click
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


def _write_by_date(df: pd.DataFrame, out_dir: Path, tag: str) -> None:
    """Split by 'date' column, write one CSV per date."""
    for date_str, group in df.groupby("date"):
        _write_csv(group, out_dir / f"{date_str}.csv")
    click.echo(f"[{tag}] Done. Written to {out_dir}")


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
@click.pass_obj
def cmd_stock_list(obj: dict) -> None:
    """Fetch today's stock list and save as {today}.csv."""

    def fetch(source: str, output_root: Path) -> None:
        click.echo(f"[{source}][stock-list] Fetching stock list...")
        df = hqdata.get_stock_list()
        today = hqdata.get_current_trading_day()
        out_path = output_root / source / "stock_list" / f"{today}.csv"
        _write_csv(df, out_path)
        click.echo(f"[{source}][stock-list] Done. Written to {out_path}")

    _run_for_sources(obj, fetch)


@cli.command("stock-minute")
@click.option("--start", default=None, metavar="YYYYMMDD", help="Start date (default: current trading day)")
@click.option("--end", default=None, metavar="YYYYMMDD", help="End date (default: current trading day)")
@click.option(
    "--frequency", "-f",
    default="1m",
    type=click.Choice(VALID_FREQUENCIES),
    show_default=True,
    help="Bar frequency.",
)
@click.pass_obj
def cmd_stock_minute(obj: dict, start: Optional[str], end: Optional[str], frequency: str) -> None:
    """Fetch stock minute bar data (ricequant only)."""

    def fetch(source: str, output_root: Path) -> None:
        click.echo(f"[{source}][stock-minute] Fetching stock list...")
        list_df = hqdata.get_stock_list()
        symbols = list_df["symbol"].tolist()
        click.echo(f"[{source}][stock-minute] {len(symbols)} symbols found. Fetching bars...")
        try:
            df = hqdata.get_stock_minute_bar(",".join(symbols), frequency, start_date=start, end_date=end)
        except Exception as e:
            click.echo(f"[{source}][stock-minute] ERROR: {e}", err=True)
            return
        if df.empty:
            click.echo(f"[{source}][stock-minute] No data fetched.")
            return
        _write_by_date(df, output_root / source / "stock_minute", f"{source}][stock-minute")

    _run_for_sources(obj, fetch)


@cli.command("stock-daily")
@click.option("--start", default=None, metavar="YYYYMMDD", help="Start date (default: current trading day)")
@click.option("--end", default=None, metavar="YYYYMMDD", help="End date (default: current trading day)")
@click.pass_obj
def cmd_stock_daily(obj: dict, start: Optional[str], end: Optional[str]) -> None:
    """Fetch stock daily bar data."""

    def fetch(source: str, output_root: Path) -> None:
        click.echo(f"[{source}][stock-daily] Fetching stock list...")
        list_df = hqdata.get_stock_list()
        symbols = list_df["symbol"].tolist()
        click.echo(f"[{source}][stock-daily] {len(symbols)} symbols found. Fetching bars...")
        try:
            df = hqdata.get_stock_daily_bar(",".join(symbols), start_date=start, end_date=end)
        except Exception as e:
            click.echo(f"[{source}][stock-daily] ERROR: {e}", err=True)
            return
        if df.empty:
            click.echo(f"[{source}][stock-daily] No data fetched.")
            return
        _write_by_date(df, output_root / source / "stock_daily", f"{source}][stock-daily")

    _run_for_sources(obj, fetch)


@cli.command("index-list")
@click.option(
    "--market",
    default="SSE,SZSE",
    show_default=True,
    help="Market filter (e.g. CSI, SSE, SZSE, SW).",
)
@click.pass_obj
def cmd_index_list(obj: dict, market: str) -> None:
    """Fetch today's index list and save as {today}.csv."""

    def fetch(source: str, output_root: Path) -> None:
        click.echo(f"[{source}][index-list] Fetching index list (market={market})...")
        df = hqdata.get_index_list(market=market)
        today = hqdata.get_current_trading_day()
        out_path = output_root / source / "index_list" / f"{today}.csv"
        _write_csv(df, out_path)
        click.echo(f"[{source}][index-list] Done. Written to {out_path}")

    _run_for_sources(obj, fetch)


@cli.command("index-minute")
@click.option("--start", default=None, metavar="YYYYMMDD", help="Start date (default: current trading day)")
@click.option("--end", default=None, metavar="YYYYMMDD", help="End date (default: current trading day)")
@click.option(
    "--frequency", "-f",
    default="1m",
    type=click.Choice(VALID_FREQUENCIES),
    show_default=True,
    help="Bar frequency.",
)
@click.pass_obj
def cmd_index_minute(obj: dict, start: Optional[str], end: Optional[str], frequency: str) -> None:
    """Fetch index minute bar data (ricequant only)."""

    def fetch(source: str, output_root: Path) -> None:
        click.echo(f"[{source}][index-minute] Fetching index list...")
        list_df = hqdata.get_index_list()
        symbols = list_df["symbol"].tolist()
        click.echo(f"[{source}][index-minute] {len(symbols)} symbols found. Fetching bars...")
        try:
            df = hqdata.get_index_minute_bar(",".join(symbols), frequency, start_date=start, end_date=end)
        except Exception as e:
            click.echo(f"[{source}][index-minute] ERROR: {e}", err=True)
            return
        if df.empty:
            click.echo(f"[{source}][index-minute] No data fetched.")
            return
        _write_by_date(df, output_root / source / "index_minute", f"{source}][index-minute")

    _run_for_sources(obj, fetch)


@cli.command("index-daily")
@click.option("--start", default=None, metavar="YYYYMMDD", help="Start date (default: current trading day)")
@click.option("--end", default=None, metavar="YYYYMMDD", help="End date (default: current trading day)")
@click.pass_obj
def cmd_index_daily(obj: dict, start: Optional[str], end: Optional[str]) -> None:
    """Fetch index daily bar data."""

    def fetch(source: str, output_root: Path) -> None:
        click.echo(f"[{source}][index-daily] Fetching index list...")
        list_df = hqdata.get_index_list()
        symbols = list_df["symbol"].tolist()
        click.echo(f"[{source}][index-daily] {len(symbols)} symbols found. Fetching bars...")
        try:
            df = hqdata.get_index_daily_bar(",".join(symbols), start_date=start, end_date=end)
        except Exception as e:
            click.echo(f"[{source}][index-daily] ERROR: {e}", err=True)
            return
        if df.empty:
            click.echo(f"[{source}][index-daily] No data fetched.")
            return
        _write_by_date(df, output_root / source / "index_daily", f"{source}][index-daily")

    _run_for_sources(obj, fetch)


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------


def main() -> None:
    cli()


if __name__ == "__main__":
    main()
