"""hqdata command line tool for fetching and storing market data as CSV."""

import json
import sys
from pathlib import Path
from typing import Callable, Optional

import click
import pandas as pd
from tqdm import tqdm

import hqdata

VALID_SOURCES = ["tushare", "ricequant"]
VALID_FREQUENCIES = ["1m", "5m", "15m", "30m", "60m"]

# CLI-level chunk sizes: how many symbols per get_*_bar call.
# index commands use chunk_size=1 (tushare iterates per-symbol internally anyway).
# stock commands use chunk_size=100 (fits in one tushare daily batch for typical date ranges).
_CHUNK_SIZES = {
    "stock-daily": 100,
    "stock-minute": 100,
    "index-daily": 1,
    "index-minute": 1,
}


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


def _cleanup_partial(partial_dir: Path, checkpoint_path: Path) -> None:
    if partial_dir.exists():
        for f in partial_dir.glob("chunk_*.csv"):
            f.unlink()
        try:
            partial_dir.rmdir()
        except OSError:
            pass
    if checkpoint_path.exists():
        checkpoint_path.unlink()


def _run_for_sources(obj: dict, fn: Callable[[str, Path], None]) -> None:
    for source in obj["sources"]:
        click.echo(f"\n=== Initializing source: {source} ===")
        hqdata.init_source(source)
        fn(source, obj["output_root"])
    click.echo("\nAll done.")


def _fetch_bar_with_checkpoint(
    cmd: str,
    source: str,
    get_bar_fn,
    symbols: list,
    bar_kwargs: dict,
    out_dir: Path,
    tag: str,
) -> None:
    """Fetch bar data with checkpoint-based resume support.

    The symbol list used for the run is saved in the checkpoint. On resume,
    the saved list is reused — ensuring consistency even if the live list changes
    between runs (e.g., new listings or delistings).
    """
    checkpoint_path = out_dir / ".checkpoint.json"
    partial_dir = out_dir / ".partial"
    chunk_size = _CHUNK_SIZES.get(cmd, 50)

    # Load checkpoint; invalidate if source/cmd/date params changed
    done_set: set = set()
    if checkpoint_path.exists():
        try:
            cp = json.loads(checkpoint_path.read_text())
            cur = (source, cmd, bar_kwargs.get("start_date"), bar_kwargs.get("end_date"))
            saved = (cp.get("source"), cp.get("cmd"), cp.get("start"), cp.get("end"))
            if cur == saved and "symbols" in cp:
                # Restore the exact symbol list from the first run for consistency
                symbols = cp["symbols"]
                done_set = set(cp.get("done", []))
                all_chunks = [symbols[i : i + chunk_size] for i in range(0, len(symbols), chunk_size)]
                remaining = sum(1 for c in all_chunks if not all(s in done_set for s in c))
                click.echo(
                    f"[{tag}] Resuming from checkpoint: {len(done_set)}/{len(symbols)} symbols done, "
                    f"{remaining}/{len(all_chunks)} chunks remaining."
                )
            else:
                click.echo(f"[{tag}] Checkpoint params changed — starting fresh.")
                _cleanup_partial(partial_dir, checkpoint_path)
        except Exception:
            _cleanup_partial(partial_dir, checkpoint_path)

    all_chunks = [symbols[i : i + chunk_size] for i in range(0, len(symbols), chunk_size)]
    partial_dir.mkdir(parents=True, exist_ok=True)

    # Write initial checkpoint with the full symbol list before fetching starts
    if not checkpoint_path.exists():
        checkpoint_path.write_text(
            json.dumps({
                "source": source,
                "cmd": cmd,
                "start": bar_kwargs.get("start_date"),
                "end": bar_kwargs.get("end_date"),
                "symbols": symbols,
                "done": [],
            })
        )

    error_occurred = False

    with tqdm(total=len(all_chunks), desc=cmd, unit="batch", file=sys.stderr) as pbar:
        for chunk_idx, chunk in enumerate(all_chunks):
            if all(s in done_set for s in chunk):
                pbar.update(1)
                continue

            try:
                df = get_bar_fn(",".join(chunk), **bar_kwargs)
            except Exception as e:
                click.echo(f"\n[{tag}] ERROR: {e}", err=True)
                click.echo(
                    f"[{tag}] {len(done_set)}/{len(symbols)} symbols saved. "
                    "Re-run the same command to resume.",
                    err=True,
                )
                error_occurred = True
                break

            if df is not None and not df.empty:
                df.to_csv(partial_dir / f"chunk_{chunk_idx:05d}.csv", index=False, encoding="utf-8")

            done_set.update(chunk)
            checkpoint_path.write_text(
                json.dumps({
                    "source": source,
                    "cmd": cmd,
                    "start": bar_kwargs.get("start_date"),
                    "end": bar_kwargs.get("end_date"),
                    "symbols": symbols,
                    "done": list(done_set),
                })
            )
            pbar.update(1)

    if error_occurred:
        return

    # Merge all partial chunk files into date-based CSVs
    all_files = sorted(partial_dir.glob("chunk_*.csv"))
    if not all_files:
        click.echo(f"[{tag}] No data fetched.")
    else:
        merged = pd.concat([pd.read_csv(f) for f in all_files], ignore_index=True)
        _write_by_date(merged, out_dir, tag)

    _cleanup_partial(partial_dir, checkpoint_path)


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
        symbols = hqdata.get_stock_list()["symbol"].tolist()
        click.echo(f"[{source}][stock-minute] {len(symbols)} symbols found. Fetching bars...")
        _fetch_bar_with_checkpoint(
            cmd="stock-minute",
            source=source,
            get_bar_fn=lambda s, **kw: hqdata.get_stock_minute_bar(s, frequency, **kw),
            symbols=symbols,
            bar_kwargs={"start_date": start, "end_date": end},
            out_dir=output_root / source / "stock_minute",
            tag=f"{source}][stock-minute",
        )

    _run_for_sources(obj, fetch)


@cli.command("stock-daily")
@click.option("--start", default=None, metavar="YYYYMMDD", help="Start date (default: current trading day)")
@click.option("--end", default=None, metavar="YYYYMMDD", help="End date (default: current trading day)")
@click.pass_obj
def cmd_stock_daily(obj: dict, start: Optional[str], end: Optional[str]) -> None:
    """Fetch stock daily bar data."""

    def fetch(source: str, output_root: Path) -> None:
        click.echo(f"[{source}][stock-daily] Fetching stock list...")
        symbols = hqdata.get_stock_list()["symbol"].tolist()
        click.echo(f"[{source}][stock-daily] {len(symbols)} symbols found. Fetching bars...")
        _fetch_bar_with_checkpoint(
            cmd="stock-daily",
            source=source,
            get_bar_fn=hqdata.get_stock_daily_bar,
            symbols=symbols,
            bar_kwargs={"start_date": start, "end_date": end},
            out_dir=output_root / source / "stock_daily",
            tag=f"{source}][stock-daily",
        )

    _run_for_sources(obj, fetch)


@cli.command("index-list")
@click.option(
    "--market",
    default="SSE,SZE",
    show_default=True,
    help="Market filter (e.g. CSI, SSE, SZE, SW).",
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
        symbols = hqdata.get_index_list()["symbol"].tolist()
        click.echo(f"[{source}][index-minute] {len(symbols)} symbols found. Fetching bars...")
        _fetch_bar_with_checkpoint(
            cmd="index-minute",
            source=source,
            get_bar_fn=lambda s, **kw: hqdata.get_index_minute_bar(s, frequency, **kw),
            symbols=symbols,
            bar_kwargs={"start_date": start, "end_date": end},
            out_dir=output_root / source / "index_minute",
            tag=f"{source}][index-minute",
        )

    _run_for_sources(obj, fetch)


@cli.command("index-daily")
@click.option("--start", default=None, metavar="YYYYMMDD", help="Start date (default: current trading day)")
@click.option("--end", default=None, metavar="YYYYMMDD", help="End date (default: current trading day)")
@click.pass_obj
def cmd_index_daily(obj: dict, start: Optional[str], end: Optional[str]) -> None:
    """Fetch index daily bar data."""

    def fetch(source: str, output_root: Path) -> None:
        click.echo(f"[{source}][index-daily] Fetching index list...")
        symbols = hqdata.get_index_list()["symbol"].tolist()
        click.echo(f"[{source}][index-daily] {len(symbols)} symbols found. Fetching bars...")
        _fetch_bar_with_checkpoint(
            cmd="index-daily",
            source=source,
            get_bar_fn=hqdata.get_index_daily_bar,
            symbols=symbols,
            bar_kwargs={"start_date": start, "end_date": end},
            out_dir=output_root / source / "index_daily",
            tag=f"{source}][index-daily",
        )

    _run_for_sources(obj, fetch)


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------


def main() -> None:
    cli()


if __name__ == "__main__":
    main()
