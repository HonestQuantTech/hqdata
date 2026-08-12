"""Unit tests for hqdata CLI (hqdata/cli.py)"""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import call, patch

import pandas as pd
import pytest
from click.testing import CliRunner

from hqdata.cli import cli
from tests.helpers import make_calendar

# ---------------------------------------------------------------------------
# fixtures / helpers
# ---------------------------------------------------------------------------

STOCK_LIST_DF = pd.DataFrame(
    {
        "symbol": ["600000.SH", "000001.SZ"],
        "date": ["20260101", "20260101"],
        "name": ["浦发银行", "平安银行"],
        "exchange": ["SSE", "SZE"],
        "board": ["MB", "MB"],
        "curr_type": ["CNY", "CNY"],
        "list_date": ["19991110", "19910403"],
        "delist_date": ["", ""],
    }
)

DAILY_BAR_DF = pd.DataFrame(
    {
        "symbol": ["600000.SH"],
        "date": ["20260102"],
        "pre_close": [10.0],
        "open": [10.1],
        "high": [10.5],
        "low": [9.9],
        "close": [10.2],
        "volume": [1000],
        "turnover": [10200.0],
        "change": [0.2],
        "pct_change": [2.0],
    }
)

STOCK_FACTOR_DF = pd.DataFrame(
    {
        "symbol": ["600000.SH"],
        "date": ["20260102"],
        "factor": [1.0],
    }
)


def _stock_list_stub(trade_date):
    """Mimic real sources: stamp the requested trade_date into the date column."""
    return STOCK_LIST_DF.assign(date=trade_date)


def assert_success(result):
    assert (
        result.exit_code == 0
    ), f"CLI failed (exit {result.exit_code}):\n{result.output}"


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def api():
    """Patch every hqdata API entry point the CLI touches, with workable defaults.

    The default world is a single trading day 20260102. Tests override the
    returned mocks' return_value/side_effect where they need something else.
    """
    with (
        patch("hqdata.cli.hqdata.init_source") as init_source,
        patch("hqdata.cli.hqdata.get_current_trading_day") as current_trading_day,
        patch("hqdata.cli.hqdata.get_calendar") as calendar,
        patch("hqdata.cli.hqdata.get_stock_list") as stock_list,
        patch("hqdata.cli.hqdata.get_stock_daily_bar") as stock_daily_bar,
        patch("hqdata.cli.hqdata.get_stock_factor") as stock_factor,
    ):
        current_trading_day.return_value = "20260102"
        calendar.return_value = make_calendar("20260102")
        stock_list.side_effect = _stock_list_stub
        stock_daily_bar.return_value = DAILY_BAR_DF
        stock_factor.return_value = STOCK_FACTOR_DF
        yield SimpleNamespace(
            init_source=init_source,
            current_trading_day=current_trading_day,
            calendar=calendar,
            stock_list=stock_list,
            stock_daily_bar=stock_daily_bar,
            stock_factor=stock_factor,
        )


# ---------------------------------------------------------------------------
# --source validation / CLI option defaults
# ---------------------------------------------------------------------------


class TestSourceValidation:
    def test_invalid_source_exits(self, runner):
        result = runner.invoke(cli, ["--source", "unknown", "stock-list"])
        assert result.exit_code != 0
        assert "Invalid" in result.output

    def test_partial_invalid_source_exits(self, runner):
        result = runner.invoke(cli, ["--source", "tushare,badone", "stock-list"])
        assert result.exit_code != 0
        assert "Invalid" in result.output


class TestCLIDefaults:
    def test_default_source_is_tushare(self, runner, api, tmp_path):
        result = runner.invoke(cli, ["--output", str(tmp_path), "stock-list"])
        assert_success(result)
        assert (tmp_path / "tushare" / "stock_list" / "20260102.csv").exists()

    def test_calendar_requires_start_end(self, runner):
        result = runner.invoke(cli, ["calendar"])
        assert result.exit_code != 0

    def test_default_output_expanduser(self, runner, api):
        """Default output ~/.hqdata must be expanded (not literal ~) in echoed paths."""
        with patch("hqdata.cli._write_csv"):  # keep the real ~/.hqdata untouched
            result = runner.invoke(cli, ["stock-list"])
        assert_success(result)
        assert "~" not in result.output
        assert str(Path.home()) in result.output


# ---------------------------------------------------------------------------
# calendar
# ---------------------------------------------------------------------------


class TestFetchCalendar:
    def test_writes_calendar_csv_no_subdir(self, runner, api, tmp_path):
        api.calendar.return_value = make_calendar("20260102", closed=("20260101",))
        result = runner.invoke(
            cli,
            [
                "--output",
                str(tmp_path),
                "calendar",
                "--start",
                "20260101",
                "--end",
                "20260131",
            ],
        )
        assert_success(result)
        out_file = tmp_path / "tushare" / "calendar.csv"
        assert out_file.exists()
        assert not (tmp_path / "tushare" / "calendar").is_dir()
        df = pd.read_csv(out_file, dtype=str)
        assert list(df.columns) == ["date", "is_open"]
        assert list(df["date"]) == ["20260101", "20260102"]
        assert list(df["is_open"]) == ["N", "Y"]


# ---------------------------------------------------------------------------
# stock-list
# ---------------------------------------------------------------------------


class TestFetchStockList:
    def test_writes_today_csv_no_args(self, runner, api, tmp_path):
        result = runner.invoke(cli, ["--output", str(tmp_path), "stock-list"])
        assert_success(result)
        out_file = tmp_path / "tushare" / "stock_list" / "20260102.csv"
        assert out_file.exists()
        out_file.read_bytes().decode("utf-8")  # must be valid utf-8
        df = pd.read_csv(out_file)
        assert list(df.columns) == list(STOCK_LIST_DF.columns)

    def test_date_range_writes_csv_per_trading_day(self, runner, api, tmp_path):
        """One CSV per trading day; get_stock_list called with each day's trade_date."""
        api.current_trading_day.return_value = "20260518"
        api.calendar.return_value = make_calendar("20260514", "20260515")
        result = runner.invoke(
            cli,
            [
                "--source",
                "ricequant",
                "--output",
                str(tmp_path),
                "stock-list",
                "--start",
                "20260514",
                "--end",
                "20260515",
            ],
        )
        assert_success(result)
        assert (tmp_path / "ricequant" / "stock_list" / "20260514.csv").exists()
        assert (tmp_path / "ricequant" / "stock_list" / "20260515.csv").exists()
        assert api.stock_list.call_args_list == [
            call(trade_date="20260514"),
            call(trade_date="20260515"),
        ]

    def test_tushare_history_also_supported(self, runner, api, tmp_path):
        """tushare historical stock lists work since the unified refactor."""
        api.current_trading_day.return_value = "20260518"
        api.calendar.return_value = make_calendar("20260514", "20260515")
        result = runner.invoke(
            cli,
            [
                "--source",
                "tushare",
                "--output",
                str(tmp_path),
                "stock-list",
                "--start",
                "20260514",
                "--end",
                "20260515",
            ],
        )
        assert_success(result)
        assert (tmp_path / "tushare" / "stock_list" / "20260514.csv").exists()
        assert (tmp_path / "tushare" / "stock_list" / "20260515.csv").exists()

    def test_existing_file_is_skipped(self, runner, api, tmp_path):
        """A CSV that already exists should not trigger another API call."""
        api.current_trading_day.return_value = "20260518"
        api.calendar.return_value = make_calendar("20260514", "20260515")
        out_dir = tmp_path / "ricequant" / "stock_list"
        out_dir.mkdir(parents=True)
        (out_dir / "20260514.csv").write_text("placeholder")

        result = runner.invoke(
            cli,
            [
                "--source",
                "ricequant",
                "--output",
                str(tmp_path),
                "stock-list",
                "--start",
                "20260514",
                "--end",
                "20260515",
            ],
        )
        assert_success(result)
        assert api.stock_list.call_args_list == [call(trade_date="20260515")]

    def test_refuses_to_write_when_date_column_mismatches(self, runner, api, tmp_path):
        """A source returning rows with a wrong date column must not be written."""
        api.stock_list.side_effect = None
        api.stock_list.return_value = STOCK_LIST_DF.assign(date="20260101")
        result = runner.invoke(cli, ["--output", str(tmp_path), "stock-list"])
        assert result.exit_code != 0
        assert "date column contains" in result.output
        assert not (tmp_path / "tushare" / "stock_list" / "20260102.csv").exists()


# ---------------------------------------------------------------------------
# stock-daily
# ---------------------------------------------------------------------------


class TestFetchStockDaily:
    def test_writes_csv_per_date(self, runner, api, tmp_path):
        result = runner.invoke(
            cli,
            [
                "--output",
                str(tmp_path),
                "stock-daily",
                "--start",
                "20260101",
                "--end",
                "20260103",
            ],
        )
        assert_success(result)
        out_file = tmp_path / "tushare" / "stock_daily" / "20260102.csv"
        assert out_file.exists()
        df = pd.read_csv(out_file)
        assert df["symbol"].iloc[0] == "600000.SH"

    def test_uses_trade_date_stock_pool_per_day(self, runner, api, tmp_path):
        """Each trading day's bars are fetched for that day's stock universe."""
        api.calendar.return_value = make_calendar("20260102", "20260103")
        api.stock_list.side_effect = [
            pd.DataFrame({"symbol": ["600000.SH"], "date": ["20260102"]}),
            pd.DataFrame({"symbol": ["000001.SZ"], "date": ["20260103"]}),
        ]
        result = runner.invoke(
            cli,
            [
                "--output",
                str(tmp_path),
                "stock-daily",
                "--start",
                "20260101",
                "--end",
                "20260103",
            ],
        )
        assert_success(result)
        assert api.stock_list.call_args_list == [
            call(trade_date="20260102"),
            call(trade_date="20260103"),
        ]
        assert api.stock_daily_bar.call_args_list == [
            call("600000.SH", start_date="20260102", end_date="20260102"),
            call("000001.SZ", start_date="20260103", end_date="20260103"),
        ]

    def test_no_data_no_file(self, runner, api, tmp_path):
        api.stock_daily_bar.return_value = DAILY_BAR_DF.iloc[0:0]
        result = runner.invoke(
            cli,
            [
                "--output",
                str(tmp_path),
                "stock-daily",
                "--start",
                "20260101",
                "--end",
                "20260103",
            ],
        )
        assert_success(result)
        assert "No data fetched" in result.output
        out_dir = tmp_path / "tushare" / "stock_daily"
        assert not out_dir.exists() or not any(out_dir.iterdir())

    def test_batch_error_reported_and_nothing_written(self, runner, api, tmp_path):
        api.stock_daily_bar.side_effect = RuntimeError("API error")
        result = runner.invoke(
            cli,
            [
                "--output",
                str(tmp_path),
                "stock-daily",
                "--start",
                "20260101",
                "--end",
                "20260103",
            ],
        )
        assert_success(result)  # the CLI reports the error but does not crash
        assert "ERROR" in result.output
        out_dir = tmp_path / "tushare" / "stock_daily"
        assert not out_dir.exists() or not any(out_dir.glob("*.csv"))

    def test_error_on_later_day_preserves_earlier_writes(self, runner, api, tmp_path):
        """A day already written to disk must survive an error on a later day."""
        api.calendar.return_value = make_calendar("20260102", "20260103")
        api.stock_daily_bar.side_effect = [DAILY_BAR_DF, RuntimeError("API error")]
        result = runner.invoke(
            cli,
            [
                "--output",
                str(tmp_path),
                "stock-daily",
                "--start",
                "20260101",
                "--end",
                "20260103",
            ],
        )
        assert_success(result)
        assert "ERROR" in result.output
        assert (tmp_path / "tushare" / "stock_daily" / "20260102.csv").exists()
        assert not (tmp_path / "tushare" / "stock_daily" / "20260103.csv").exists()

    def test_existing_file_is_skipped(self, runner, api, tmp_path):
        """A day already on disk is not re-fetched on re-run."""
        api.calendar.return_value = make_calendar("20260102", "20260103")
        out_dir = tmp_path / "tushare" / "stock_daily"
        out_dir.mkdir(parents=True)
        (out_dir / "20260102.csv").write_text("placeholder")

        result = runner.invoke(
            cli,
            [
                "--output",
                str(tmp_path),
                "stock-daily",
                "--start",
                "20260101",
                "--end",
                "20260103",
            ],
        )
        assert_success(result)
        assert api.stock_list.call_args_list == [call(trade_date="20260103")]
        assert (out_dir / "20260102.csv").read_text() == "placeholder"


# ---------------------------------------------------------------------------
# stock-factor
# ---------------------------------------------------------------------------


class TestFetchStockFactor:
    def test_writes_csv_per_date(self, runner, api, tmp_path):
        result = runner.invoke(
            cli,
            [
                "--output",
                str(tmp_path),
                "stock-factor",
                "--start",
                "20260101",
                "--end",
                "20260103",
            ],
        )
        assert_success(result)
        out_file = tmp_path / "tushare" / "stock_factor" / "20260102.csv"
        assert out_file.exists()
        df = pd.read_csv(out_file)
        assert df["symbol"].iloc[0] == "600000.SH"

    def test_uses_trade_date_stock_pool_per_day(self, runner, api, tmp_path):
        """Each trading day's factors are fetched for that day's stock universe."""
        api.calendar.return_value = make_calendar("20260102", "20260103")
        api.stock_list.side_effect = [
            pd.DataFrame({"symbol": ["600000.SH"], "date": ["20260102"]}),
            pd.DataFrame({"symbol": ["000001.SZ"], "date": ["20260103"]}),
        ]
        result = runner.invoke(
            cli,
            [
                "--output",
                str(tmp_path),
                "stock-factor",
                "--start",
                "20260101",
                "--end",
                "20260103",
            ],
        )
        assert_success(result)
        assert api.stock_list.call_args_list == [
            call(trade_date="20260102"),
            call(trade_date="20260103"),
        ]
        assert api.stock_factor.call_args_list == [
            call("600000.SH", trade_date="20260102"),
            call("000001.SZ", trade_date="20260103"),
        ]

    def test_no_data_no_file(self, runner, api, tmp_path):
        api.stock_factor.return_value = STOCK_FACTOR_DF.iloc[0:0]
        result = runner.invoke(
            cli,
            [
                "--output",
                str(tmp_path),
                "stock-factor",
                "--start",
                "20260101",
                "--end",
                "20260103",
            ],
        )
        assert_success(result)
        assert "No data fetched" in result.output
        out_dir = tmp_path / "tushare" / "stock_factor"
        assert not out_dir.exists() or not any(out_dir.iterdir())

    def test_batch_error_reported_and_nothing_written(self, runner, api, tmp_path):
        api.stock_factor.side_effect = RuntimeError("API error")
        result = runner.invoke(
            cli,
            [
                "--output",
                str(tmp_path),
                "stock-factor",
                "--start",
                "20260101",
                "--end",
                "20260103",
            ],
        )
        assert_success(result)  # the CLI reports the error but does not crash
        assert "ERROR" in result.output
        out_dir = tmp_path / "tushare" / "stock_factor"
        assert not out_dir.exists() or not any(out_dir.glob("*.csv"))

    def test_error_on_later_day_preserves_earlier_writes(self, runner, api, tmp_path):
        """A day already written to disk must survive an error on a later day."""
        api.calendar.return_value = make_calendar("20260102", "20260103")
        api.stock_factor.side_effect = [STOCK_FACTOR_DF, RuntimeError("API error")]
        result = runner.invoke(
            cli,
            [
                "--output",
                str(tmp_path),
                "stock-factor",
                "--start",
                "20260101",
                "--end",
                "20260103",
            ],
        )
        assert_success(result)
        assert "ERROR" in result.output
        assert (tmp_path / "tushare" / "stock_factor" / "20260102.csv").exists()
        assert not (tmp_path / "tushare" / "stock_factor" / "20260103.csv").exists()

    def test_existing_file_is_skipped(self, runner, api, tmp_path):
        """A day already on disk is not re-fetched on re-run."""
        api.calendar.return_value = make_calendar("20260102", "20260103")
        out_dir = tmp_path / "tushare" / "stock_factor"
        out_dir.mkdir(parents=True)
        (out_dir / "20260102.csv").write_text("placeholder")

        result = runner.invoke(
            cli,
            [
                "--output",
                str(tmp_path),
                "stock-factor",
                "--start",
                "20260101",
                "--end",
                "20260103",
            ],
        )
        assert_success(result)
        assert api.stock_list.call_args_list == [call(trade_date="20260103")]
        assert (out_dir / "20260102.csv").read_text() == "placeholder"


# ---------------------------------------------------------------------------
# multi-source
# ---------------------------------------------------------------------------


class TestMultiSource:
    def test_multi_source_each_written(self, runner, api, tmp_path):
        result = runner.invoke(
            cli,
            ["--source", "tushare,ricequant", "--output", str(tmp_path), "stock-list"],
        )
        assert_success(result)
        assert api.init_source.call_args_list == [call("tushare"), call("ricequant")]
        assert (tmp_path / "tushare" / "stock_list" / "20260102.csv").exists()
        assert (tmp_path / "ricequant" / "stock_list" / "20260102.csv").exists()


# ---------------------------------------------------------------------------
# compare — file-based, no API mocks
# ---------------------------------------------------------------------------

_STOCK_ROW = {
    "symbol": "000001.SZ",
    "name": "平安银行",
    "exchange": "SZE",
    "board": "MB",
    "curr_type": "CNY",
    "list_date": "19910403",
    "delist_date": "",
}

_STOCK_COLUMNS = list(STOCK_LIST_DF.columns)

_DAILY_ROW = {
    "symbol": "000001.SZ",
    "pre_close": 10.0,
    "open": 10.1,
    "high": 10.5,
    "low": 9.9,
    "close": 10.2,
    "volume": 1000,
    "turnover": 10200.0,
    "change": 0.2,
    "pct_change": 2.0,
}

_DAILY_COLUMNS = list(DAILY_BAR_DF.columns)


def write_calendar_csv(output_root, source, calendar_df):
    path = output_root / source / "calendar.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    calendar_df.to_csv(path, index=False)


def write_stock_list_csv(output_root, source, date, rows=({},), filename=None):
    """Write a stock_list CSV; each row spec is merged over a valid default row."""
    frame = pd.DataFrame([{**_STOCK_ROW, "date": date, **spec} for spec in rows])
    path = output_root / source / "stock_list" / (filename or f"{date}.csv")
    path.parent.mkdir(parents=True, exist_ok=True)
    frame[_STOCK_COLUMNS].to_csv(path, index=False)


def write_stock_daily_csv(output_root, source, date, rows=({},), filename=None):
    """Write a stock_daily CSV; each row spec is merged over a valid default row."""
    frame = pd.DataFrame([{**_DAILY_ROW, "date": date, **spec} for spec in rows])
    path = output_root / source / "stock_daily" / (filename or f"{date}.csv")
    path.parent.mkdir(parents=True, exist_ok=True)
    frame[_DAILY_COLUMNS].to_csv(path, index=False)


_FACTOR_ROW = {
    "symbol": "000001.SZ",
    "factor": 1.0,
}

_FACTOR_COLUMNS = list(STOCK_FACTOR_DF.columns)


def write_stock_factor_csv(output_root, source, date, rows=({},), filename=None):
    """Write a stock_factor CSV; each row spec is merged over a valid default row."""
    frame = pd.DataFrame([{**_FACTOR_ROW, "date": date, **spec} for spec in rows])
    path = output_root / source / "stock_factor" / (filename or f"{date}.csv")
    path.parent.mkdir(parents=True, exist_ok=True)
    frame[_FACTOR_COLUMNS].to_csv(path, index=False)


class TestCompareCalendar:
    def test_no_diff(self, runner, tmp_path):
        calendar = make_calendar("20260102", closed=("20260101",))
        write_calendar_csv(tmp_path, "tushare", calendar)
        write_calendar_csv(tmp_path, "ricequant", calendar)

        result = runner.invoke(cli, ["--output", str(tmp_path), "compare", "calendar"])

        assert_success(result)
        assert "No differences found" in result.output
        assert not (tmp_path / "compare" / "calendar_diff.csv").exists()

    def test_writes_diff_report(self, runner, tmp_path):
        write_calendar_csv(
            tmp_path, "tushare", make_calendar("20260102", closed=("20260101",))
        )
        write_calendar_csv(tmp_path, "ricequant", make_calendar("20260101", "20260103"))

        result = runner.invoke(cli, ["--output", str(tmp_path), "compare", "calendar"])

        assert result.exit_code != 0
        assert "Differences found" in result.output
        report = pd.read_csv(tmp_path / "compare" / "calendar_diff.csv", dtype=str)
        assert list(report["status"]) == [
            "mismatch_is_open",  # 20260101: N vs Y
            "only_tushare",  # 20260102
            "only_ricequant",  # 20260103
        ]

    def test_missing_file(self, runner, tmp_path):
        write_calendar_csv(tmp_path, "tushare", make_calendar("20260102"))

        result = runner.invoke(cli, ["--output", str(tmp_path), "compare", "calendar"])

        assert result.exit_code != 0
        assert "Missing calendar file" in result.output


class TestCompareStockList:
    def test_no_diff_after_normalization(self, runner, tmp_path):
        """Source-native spellings (BJSE, cny, dashed dates, 0000-00-00) must not diff."""
        write_stock_list_csv(
            tmp_path,
            "tushare",
            "20260105",
            rows=[
                {
                    "symbol": "920000.BJ",
                    "name": "样本股",
                    "exchange": "BSE",
                    "board": "BSE",
                }
            ],
        )
        write_stock_list_csv(
            tmp_path,
            "ricequant",
            "20260105",
            rows=[
                {
                    "symbol": "920000.BJ",
                    "name": "样本股",
                    "exchange": "BJSE",
                    "board": "BSE",
                    "curr_type": "cny",
                    "list_date": "1991-04-03",
                    "delist_date": "0000-00-00",
                }
            ],
        )

        result = runner.invoke(
            cli, ["--output", str(tmp_path), "compare", "stock-list"]
        )

        assert_success(result)
        assert "No differences found" in result.output
        assert not (tmp_path / "compare" / "stock_list_diff.csv").exists()

    def test_writes_diff_report(self, runner, tmp_path):
        write_stock_list_csv(
            tmp_path,
            "tushare",
            "20260105",
            rows=[
                {},
                {
                    "symbol": "000002.SZ",
                    "name": "万科A",
                },
            ],
        )
        write_stock_list_csv(
            tmp_path,
            "ricequant",
            "20260105",
            rows=[{"board": "GEM", "delist_date": "2026-01-05"}],
        )
        write_stock_list_csv(
            tmp_path,
            "tushare",
            "20260106",
            rows=[{"symbol": "000003.SZ", "name": "样本股"}],
        )

        result = runner.invoke(
            cli, ["--output", str(tmp_path), "compare", "stock-list"]
        )

        assert result.exit_code != 0
        assert "Differences found" in result.output
        report = pd.read_csv(tmp_path / "compare" / "stock_list_diff.csv", dtype=str)
        assert set(report["status"]) == {
            "file_only_tushare",  # 20260106 missing on ricequant side
            "symbol_only_tushare",  # 000002.SZ
            "value_mismatch",  # 000001.SZ board + effective delist_date
        }
        mismatches = report[report["status"] == "value_mismatch"]
        assert set(mismatches["field"]) == {"board", "delist_date"}

    def test_missing_directory(self, runner, tmp_path):
        write_stock_list_csv(tmp_path, "tushare", "20260105")

        result = runner.invoke(
            cli, ["--output", str(tmp_path), "compare", "stock-list"]
        )

        assert result.exit_code != 0
        assert "Missing stock_list directory" in result.output

    def test_ignores_future_delist_date(self, runner, tmp_path):
        """A delist_date later than the snapshot date has not taken effect yet."""
        write_stock_list_csv(
            tmp_path, "tushare", "20260105", rows=[{"delist_date": "20260706"}]
        )
        write_stock_list_csv(
            tmp_path, "ricequant", "20260105", rows=[{"delist_date": ""}]
        )

        result = runner.invoke(
            cli, ["--output", str(tmp_path), "compare", "stock-list"]
        )

        assert_success(result)
        assert "No differences found" in result.output

    def test_ignores_name_diff_while_delisting_pending(self, runner, tmp_path):
        """Sources rename stocks to the delisting-period name at different times."""
        write_stock_list_csv(
            tmp_path,
            "tushare",
            "20260105",
            rows=[{"name": "*ST立方", "delist_date": "20260422"}],
        )
        write_stock_list_csv(
            tmp_path,
            "ricequant",
            "20260105",
            rows=[{"name": "立方退", "delist_date": "20260422"}],
        )

        result = runner.invoke(
            cli, ["--output", str(tmp_path), "compare", "stock-list"]
        )

        assert_success(result)
        assert "No differences found" in result.output

    def test_reports_name_diff_without_pending_delisting(self, runner, tmp_path):
        """A name difference on a normally-listed stock is a real discrepancy."""
        write_stock_list_csv(
            tmp_path, "tushare", "20260105", rows=[{"name": "平安银行"}]
        )
        write_stock_list_csv(
            tmp_path, "ricequant", "20260105", rows=[{"name": "平安錕行"}]
        )

        result = runner.invoke(
            cli, ["--output", str(tmp_path), "compare", "stock-list"]
        )

        assert result.exit_code != 0
        report = pd.read_csv(tmp_path / "compare" / "stock_list_diff.csv", dtype=str)
        assert list(report["status"]) == ["value_mismatch"]
        assert list(report["field"]) == ["name"]

    def test_detects_non_trading_day_file(self, runner, tmp_path):
        calendar = make_calendar("20260102", closed=("20260101",))
        write_calendar_csv(tmp_path, "tushare", calendar)
        write_calendar_csv(tmp_path, "ricequant", calendar)
        write_stock_list_csv(tmp_path, "tushare", "20260101")
        write_stock_list_csv(tmp_path, "ricequant", "20260101")

        result = runner.invoke(
            cli, ["--output", str(tmp_path), "compare", "stock-list"]
        )

        assert result.exit_code != 0
        report = pd.read_csv(tmp_path / "compare" / "stock_list_diff.csv", dtype=str)
        assert set(report["status"]) == {
            "file_not_trading_day_tushare",
            "file_not_trading_day_ricequant",
        }

    def test_rejects_date_filename_mismatch(self, runner, tmp_path):
        write_stock_list_csv(tmp_path, "tushare", "20260101", filename="20260102.csv")
        write_stock_list_csv(tmp_path, "ricequant", "20260102")

        result = runner.invoke(
            cli, ["--output", str(tmp_path), "compare", "stock-list"]
        )

        assert result.exit_code != 0
        assert "not matching the file name" in result.output


class TestCompareStockDaily:
    def test_no_diff_within_tolerance(self, runner, tmp_path):
        """turnover diffs <= 1.0 yuan and pct_change diffs <= 0.0001 must not report."""
        write_stock_daily_csv(tmp_path, "tushare", "20260105")
        write_stock_daily_csv(
            tmp_path,
            "ricequant",
            "20260105",
            rows=[{"turnover": 10200.5, "pct_change": 2.0001}],
        )

        result = runner.invoke(
            cli, ["--output", str(tmp_path), "compare", "stock-daily"]
        )

        assert_success(result)
        assert "No differences found" in result.output
        assert not (tmp_path / "compare" / "stock_daily_diff.csv").exists()

    def test_writes_diff_report(self, runner, tmp_path):
        write_stock_daily_csv(
            tmp_path,
            "tushare",
            "20260105",
            rows=[
                {},
                {"symbol": "000002.SZ", "close": 20.0},
            ],
        )
        write_stock_daily_csv(
            tmp_path,
            "ricequant",
            "20260105",
            rows=[{"close": 10.3, "turnover": 10203.0}],
        )

        result = runner.invoke(
            cli, ["--output", str(tmp_path), "compare", "stock-daily"]
        )

        assert result.exit_code != 0
        assert "Differences found" in result.output
        report = pd.read_csv(tmp_path / "compare" / "stock_daily_diff.csv", dtype=str)
        assert set(report["status"]) == {
            "symbol_only_tushare",  # 000002.SZ
            "value_mismatch",  # 000001.SZ close (exact) + turnover (> 1.0 yuan)
        }
        mismatches = report[report["status"] == "value_mismatch"]
        assert set(mismatches["field"]) == {"close", "turnover"}

    def test_ignores_suspension_padding_rows(self, runner, tmp_path):
        """ricequant pads suspension days (volume=0, OHLC=pre_close); tushare omits them."""
        write_stock_daily_csv(tmp_path, "tushare", "20260105")
        write_stock_daily_csv(
            tmp_path,
            "ricequant",
            "20260105",
            rows=[
                {},
                {
                    "symbol": "600058.SH",
                    "pre_close": 11.52,
                    "open": 11.52,
                    "high": 11.52,
                    "low": 11.52,
                    "close": 11.52,
                    "volume": 0,
                    "turnover": 0.0,
                    "change": 0.0,
                    "pct_change": 0.0,
                },
            ],
        )

        result = runner.invoke(
            cli, ["--output", str(tmp_path), "compare", "stock-daily"]
        )

        assert_success(result)
        assert "No differences found" in result.output

    def test_reports_ricequant_only_with_volume(self, runner, tmp_path):
        """A ricequant-only row that actually traded is a real discrepancy."""
        write_stock_daily_csv(tmp_path, "tushare", "20260105")
        write_stock_daily_csv(
            tmp_path,
            "ricequant",
            "20260105",
            rows=[{}, {"symbol": "600058.SH"}],
        )

        result = runner.invoke(
            cli, ["--output", str(tmp_path), "compare", "stock-daily"]
        )

        assert result.exit_code != 0
        report = pd.read_csv(tmp_path / "compare" / "stock_daily_diff.csv", dtype=str)
        assert list(report["status"]) == ["symbol_only_ricequant"]
        assert list(report["symbol"]) == ["600058.SH"]

    def test_missing_directory(self, runner, tmp_path):
        write_stock_daily_csv(tmp_path, "tushare", "20260105")

        result = runner.invoke(
            cli, ["--output", str(tmp_path), "compare", "stock-daily"]
        )

        assert result.exit_code != 0
        assert "Missing stock_daily directory" in result.output

    def test_rejects_date_filename_mismatch(self, runner, tmp_path):
        write_stock_daily_csv(tmp_path, "tushare", "20260101", filename="20260102.csv")
        write_stock_daily_csv(tmp_path, "ricequant", "20260102")

        result = runner.invoke(
            cli, ["--output", str(tmp_path), "compare", "stock-daily"]
        )

        assert result.exit_code != 0
        assert "not matching the file name" in result.output


class TestCompareStockFactor:
    def test_no_diff_first_day_has_no_prior_ratio(self, runner, tmp_path):
        """A single day has no prior factor to ratio against — not a mismatch."""
        write_stock_factor_csv(
            tmp_path, "tushare", "20260105", rows=[{"factor": 108.031}]
        )
        write_stock_factor_csv(
            tmp_path, "ricequant", "20260105", rows=[{"factor": 45.6}]
        )

        result = runner.invoke(
            cli, ["--output", str(tmp_path), "compare", "stock-factor"]
        )

        assert_success(result)
        assert "No differences found" in result.output
        assert not (tmp_path / "compare" / "stock_factor_diff.csv").exists()

    def test_no_diff_within_tolerance(self, runner, tmp_path):
        """Matching day-over-day ratios must not report, even with different anchors."""
        write_stock_factor_csv(
            tmp_path, "tushare", "20260104", rows=[{"factor": 100.0}]
        )
        write_stock_factor_csv(
            tmp_path, "tushare", "20260105", rows=[{"factor": 121.7847}]
        )
        write_stock_factor_csv(
            tmp_path, "ricequant", "20260104", rows=[{"factor": 45.6}]
        )
        write_stock_factor_csv(
            tmp_path, "ricequant", "20260105", rows=[{"factor": 55.5399}]
        )

        result = runner.invoke(
            cli, ["--output", str(tmp_path), "compare", "stock-factor"]
        )

        assert_success(result)
        assert "No differences found" in result.output
        assert not (tmp_path / "compare" / "stock_factor_diff.csv").exists()

    def test_writes_diff_report_on_ratio_mismatch(self, runner, tmp_path):
        """A day-over-day ratio disagreement is a real corporate-action discrepancy."""
        write_stock_factor_csv(
            tmp_path, "tushare", "20260104", rows=[{"factor": 100.0}]
        )
        write_stock_factor_csv(
            tmp_path, "tushare", "20260105", rows=[{"factor": 121.7847}]
        )
        write_stock_factor_csv(
            tmp_path, "ricequant", "20260104", rows=[{"factor": 45.6}]
        )
        # ricequant's ratio (1.05) disagrees with tushare's ratio (1.217847)
        write_stock_factor_csv(
            tmp_path, "ricequant", "20260105", rows=[{"factor": 47.88}]
        )

        result = runner.invoke(
            cli, ["--output", str(tmp_path), "compare", "stock-factor"]
        )

        assert result.exit_code != 0
        assert "Differences found" in result.output
        report = pd.read_csv(tmp_path / "compare" / "stock_factor_diff.csv", dtype=str)
        assert list(report["status"]) == ["value_mismatch"]
        assert list(report["field"]) == ["ratio"]
        assert list(report["date"]) == ["20260105"]

    def test_reports_symbol_only_on_one_side(self, runner, tmp_path):
        write_stock_factor_csv(
            tmp_path,
            "tushare",
            "20260105",
            rows=[{}, {"symbol": "600000.SH", "factor": 16.0}],
        )
        write_stock_factor_csv(tmp_path, "ricequant", "20260105")

        result = runner.invoke(
            cli, ["--output", str(tmp_path), "compare", "stock-factor"]
        )

        assert result.exit_code != 0
        report = pd.read_csv(tmp_path / "compare" / "stock_factor_diff.csv", dtype=str)
        assert list(report["status"]) == ["symbol_only_tushare"]
        assert list(report["symbol"]) == ["600000.SH"]

    def test_missing_directory(self, runner, tmp_path):
        write_stock_factor_csv(tmp_path, "tushare", "20260105")

        result = runner.invoke(
            cli, ["--output", str(tmp_path), "compare", "stock-factor"]
        )

        assert result.exit_code != 0
        assert "Missing stock_factor directory" in result.output

    def test_rejects_date_filename_mismatch(self, runner, tmp_path):
        write_stock_factor_csv(tmp_path, "tushare", "20260101", filename="20260102.csv")
        write_stock_factor_csv(tmp_path, "ricequant", "20260102")

        result = runner.invoke(
            cli, ["--output", str(tmp_path), "compare", "stock-factor"]
        )

        assert result.exit_code != 0
        assert "not matching the file name" in result.output
