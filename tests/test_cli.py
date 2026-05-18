"""Unit tests for hqdata CLI (hqdata/cli.py)"""

from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pandas as pd
import pytest
from click.testing import CliRunner

from hqdata.cli import cli

# ---------------------------------------------------------------------------
# fixtures / helpers
# ---------------------------------------------------------------------------

STOCK_LIST_DF = pd.DataFrame(
    {
        "symbol": ["600000.SH", "000001.SZ"],
        "date": ["20260101", "20260101"],
        "name": ["浦发银行", "平安银行"],
        "exchange": ["SSE", "SZSE"],
        "board": ["MB", "MB"],
        "industry": ["银行", "银行"],
        "curr_type": ["CNY", "CNY"],
        "list_date": ["19991110", "19910403"],
        "delist_date": [None, None],
        "is_hs": [False, True],
    }
)

INDEX_LIST_DF = pd.DataFrame(
    {
        "symbol": ["000300.SH", "000905.SH"],
        "date": ["20260101", "20260101"],
        "name": ["沪深300", "中证500"],
        "fullname": ["沪深300指数", "中证500指数"],
        "market": ["CSI", "CSI"],
        "base_date": ["20041231", "20041231"],
        "base_point": [1000.0, 1000.0],
        "list_date": ["20050404", "20070115"],
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

MINUTE_BAR_DF = pd.DataFrame(
    {
        "symbol": ["600000.SH"],
        "date": ["20260102"],
        "open": [10.1],
        "high": [10.2],
        "low": [10.0],
        "close": [10.15],
        "volume": [100],
        "turnover": [1015.0],
        "ets": ["20260102T093000000"],
    }
)

CALENDAR_DF = pd.DataFrame({"date": ["20260101", "20260102"], "is_open": ["Y", "Y"]})


@pytest.fixture
def runner():
    return CliRunner()


# ---------------------------------------------------------------------------
# --source validation
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


# ---------------------------------------------------------------------------
# CLI option defaults
# ---------------------------------------------------------------------------


class TestCLIDefaults:
    def test_default_source_is_tushare(self, runner):
        calendar = pd.DataFrame({"date": ["20260102"], "is_open": ["Y"]})
        with (
            patch("hqdata.cli.hqdata.init_source"),
            patch("hqdata.cli.hqdata.get_stock_list", return_value=STOCK_LIST_DF),
            patch("hqdata.cli.hqdata.get_current_trading_day", return_value="20260102"),
            patch("hqdata.cli.hqdata.get_calendar", return_value=calendar),
        ):
            result = runner.invoke(cli, ["stock-list"])
        assert result.exit_code == 0

    def test_stock_minute_invalid_frequency(self, runner):
        result = runner.invoke(cli, ["stock-minute", "--frequency", "2m"])
        assert result.exit_code != 0

    def test_calendar_requires_start_end(self, runner):
        result = runner.invoke(cli, ["calendar"])
        assert result.exit_code != 0

    def test_index_list_default_market(self, runner, tmp_path):
        with (
            patch("hqdata.cli.hqdata.init_source"),
            patch("hqdata.cli.hqdata.get_index_list", return_value=INDEX_LIST_DF) as mock_list,
            patch("hqdata.cli.hqdata.get_current_trading_day", return_value="20260102"),
        ):
            runner.invoke(cli, ["--output", str(tmp_path), "index-list"])
        mock_list.assert_called_once_with(market="SSE,SZE")


# ---------------------------------------------------------------------------
# stock-list (no date args — backward compat)
# ---------------------------------------------------------------------------


class TestFetchStockList:
    def test_writes_today_csv_no_args(self, runner, tmp_path):
        calendar = pd.DataFrame({"date": ["20260102"], "is_open": ["Y"]})
        with (
            patch("hqdata.cli.hqdata.init_source"),
            patch("hqdata.cli.hqdata.get_stock_list", return_value=STOCK_LIST_DF),
            patch("hqdata.cli.hqdata.get_current_trading_day", return_value="20260102"),
            patch("hqdata.cli.hqdata.get_calendar", return_value=calendar),
        ):
            result = runner.invoke(cli, ["--output", str(tmp_path), "stock-list"])

        assert result.exit_code == 0
        out_file = tmp_path / "tushare" / "stock_list" / "20260102.csv"
        assert out_file.exists()
        df = pd.read_csv(out_file)
        assert list(df.columns) == list(STOCK_LIST_DF.columns)

    def test_csv_encoding_utf8(self, runner, tmp_path):
        calendar = pd.DataFrame({"date": ["20260102"], "is_open": ["Y"]})
        with (
            patch("hqdata.cli.hqdata.init_source"),
            patch("hqdata.cli.hqdata.get_stock_list", return_value=STOCK_LIST_DF),
            patch("hqdata.cli.hqdata.get_current_trading_day", return_value="20260102"),
            patch("hqdata.cli.hqdata.get_calendar", return_value=calendar),
        ):
            runner.invoke(cli, ["--output", str(tmp_path), "stock-list"])

        out_file = tmp_path / "tushare" / "stock_list" / "20260102.csv"
        content = out_file.read_bytes()
        content.decode("utf-8")  # should not raise


# ---------------------------------------------------------------------------
# stock-list date range
# ---------------------------------------------------------------------------


class TestStockListDateRange:
    def test_writes_csv_per_trading_day(self, runner, tmp_path):
        calendar = pd.DataFrame({"date": ["20260514", "20260515"], "is_open": ["Y", "Y"]})
        with (
            patch("hqdata.cli.hqdata.init_source"),
            patch("hqdata.cli.hqdata.get_current_trading_day", return_value="20260518"),
            patch("hqdata.cli.hqdata.get_calendar", return_value=calendar),
            patch("hqdata.cli.hqdata.get_stock_list", return_value=STOCK_LIST_DF),
        ):
            result = runner.invoke(
                cli,
                ["--source", "ricequant", "--output", str(tmp_path), "stock-list",
                 "--start", "20260514", "--end", "20260515"],
            )

        assert result.exit_code == 0
        assert (tmp_path / "ricequant" / "stock_list" / "20260514.csv").exists()
        assert (tmp_path / "ricequant" / "stock_list" / "20260515.csv").exists()

    def test_trade_date_passed_per_day(self, runner, tmp_path):
        """get_stock_list should be called with each trading day's trade_date."""
        calendar = pd.DataFrame({"date": ["20260514", "20260515"], "is_open": ["Y", "Y"]})
        mock_list = MagicMock(return_value=STOCK_LIST_DF)
        with (
            patch("hqdata.cli.hqdata.init_source"),
            patch("hqdata.cli.hqdata.get_current_trading_day", return_value="20260518"),
            patch("hqdata.cli.hqdata.get_calendar", return_value=calendar),
            patch("hqdata.cli.hqdata.get_stock_list", mock_list),
        ):
            runner.invoke(
                cli,
                ["--source", "ricequant", "--output", str(tmp_path), "stock-list",
                 "--start", "20260514", "--end", "20260515"],
            )

        assert mock_list.call_count == 2
        assert mock_list.call_args_list == [
            call(trade_date="20260514"),
            call(trade_date="20260515"),
        ]

    def test_existing_file_is_skipped(self, runner, tmp_path):
        """A CSV that already exists should not trigger another API call."""
        calendar = pd.DataFrame({"date": ["20260514", "20260515"], "is_open": ["Y", "Y"]})
        out_dir = tmp_path / "ricequant" / "stock_list"
        out_dir.mkdir(parents=True)
        (out_dir / "20260514.csv").write_text("placeholder")

        mock_list = MagicMock(return_value=STOCK_LIST_DF)
        with (
            patch("hqdata.cli.hqdata.init_source"),
            patch("hqdata.cli.hqdata.get_current_trading_day", return_value="20260518"),
            patch("hqdata.cli.hqdata.get_calendar", return_value=calendar),
            patch("hqdata.cli.hqdata.get_stock_list", mock_list),
        ):
            result = runner.invoke(
                cli,
                ["--source", "ricequant", "--output", str(tmp_path), "stock-list",
                 "--start", "20260514", "--end", "20260515"],
            )

        assert result.exit_code == 0
        assert mock_list.call_count == 1
        assert mock_list.call_args_list == [call(trade_date="20260515")]

    def test_tushare_history_errors_out(self, runner, tmp_path):
        """tushare with historical dates should print an error and not write files."""
        with (
            patch("hqdata.cli.hqdata.init_source"),
            patch("hqdata.cli.hqdata.get_current_trading_day", return_value="20260518"),
        ):
            result = runner.invoke(
                cli,
                ["--source", "tushare", "--output", str(tmp_path), "stock-list",
                 "--start", "20260501", "--end", "20260515"],
            )

        assert result.exit_code != 0
        combined = result.output + (result.stderr or "")
        assert "暂未支持" in combined
        out_dir = tmp_path / "tushare" / "stock_list"
        assert not out_dir.exists() or not any(out_dir.iterdir())

    def test_tushare_today_still_works(self, runner, tmp_path):
        """tushare without --start/--end (defaulting to today) should work fine."""
        calendar = pd.DataFrame({"date": ["20260518"], "is_open": ["Y"]})
        with (
            patch("hqdata.cli.hqdata.init_source"),
            patch("hqdata.cli.hqdata.get_current_trading_day", return_value="20260518"),
            patch("hqdata.cli.hqdata.get_calendar", return_value=calendar),
            patch("hqdata.cli.hqdata.get_stock_list", return_value=STOCK_LIST_DF),
        ):
            result = runner.invoke(cli, ["--source", "tushare", "--output", str(tmp_path), "stock-list"])

        assert result.exit_code == 0
        assert (tmp_path / "tushare" / "stock_list" / "20260518.csv").exists()


# ---------------------------------------------------------------------------
# stock-daily
# ---------------------------------------------------------------------------


class TestFetchStockDaily:
    def test_writes_csv_per_date(self, runner, tmp_path):
        with (
            patch("hqdata.cli.hqdata.init_source"),
            patch("hqdata.cli.hqdata.get_stock_list", return_value=STOCK_LIST_DF),
            patch("hqdata.cli.hqdata.get_stock_daily_bar", return_value=DAILY_BAR_DF),
        ):
            result = runner.invoke(
                cli,
                ["--output", str(tmp_path), "stock-daily", "--start", "20260101", "--end", "20260103"],
            )

        assert result.exit_code == 0
        out_file = tmp_path / "tushare" / "stock_daily" / "20260102.csv"
        assert out_file.exists()
        df = pd.read_csv(out_file)
        assert "symbol" in df.columns
        assert df["symbol"].iloc[0] == "600000.SH"

    def test_single_batch_call(self, runner, tmp_path):
        mock_bar = MagicMock(return_value=DAILY_BAR_DF)
        with (
            patch("hqdata.cli.hqdata.init_source"),
            patch("hqdata.cli.hqdata.get_stock_list", return_value=STOCK_LIST_DF),
            patch("hqdata.cli.hqdata.get_stock_daily_bar", mock_bar),
        ):
            runner.invoke(
                cli,
                ["--output", str(tmp_path), "stock-daily", "--start", "20260101", "--end", "20260103"],
            )

        assert mock_bar.call_count == 1
        called_symbols = mock_bar.call_args[0][0]
        assert called_symbols == "600000.SH,000001.SZ"

    def test_no_data_no_file(self, runner, tmp_path):
        empty_bar = pd.DataFrame(
            columns=["symbol", "date", "pre_close", "open", "high", "low", "close",
                     "volume", "turnover", "change", "pct_change"]
        )
        with (
            patch("hqdata.cli.hqdata.init_source"),
            patch("hqdata.cli.hqdata.get_stock_list", return_value=STOCK_LIST_DF),
            patch("hqdata.cli.hqdata.get_stock_daily_bar", return_value=empty_bar),
        ):
            runner.invoke(
                cli,
                ["--output", str(tmp_path), "stock-daily", "--start", "20260101", "--end", "20260103"],
            )

        out_dir = tmp_path / "tushare" / "stock_daily"
        assert not out_dir.exists() or not any(out_dir.iterdir())

    def test_batch_error_no_crash(self, runner, tmp_path):
        with (
            patch("hqdata.cli.hqdata.init_source"),
            patch("hqdata.cli.hqdata.get_stock_list", return_value=STOCK_LIST_DF),
            patch("hqdata.cli.hqdata.get_stock_daily_bar", side_effect=RuntimeError("API error")),
        ):
            runner.invoke(
                cli,
                ["--output", str(tmp_path), "stock-daily", "--start", "20260101", "--end", "20260103"],
            )

        # On error, checkpoint/partial infrastructure may remain, but no date-based CSV should exist
        out_dir = tmp_path / "tushare" / "stock_daily"
        assert not any(out_dir.glob("*.csv")) if out_dir.exists() else True


# ---------------------------------------------------------------------------
# stock-minute
# ---------------------------------------------------------------------------


class TestFetchStockMinute:
    def test_writes_csv_per_date(self, runner, tmp_path):
        with (
            patch("hqdata.cli.hqdata.init_source"),
            patch("hqdata.cli.hqdata.get_stock_list", return_value=STOCK_LIST_DF),
            patch("hqdata.cli.hqdata.get_stock_minute_bar", return_value=MINUTE_BAR_DF),
        ):
            result = runner.invoke(
                cli,
                ["--output", str(tmp_path), "stock-minute", "--start", "20260401", "--end", "20260407",
                 "--frequency", "5m"],
            )

        assert result.exit_code == 0
        out_file = tmp_path / "tushare" / "stock_minute" / "20260102.csv"
        assert out_file.exists()

    def test_frequency_passed(self, runner, tmp_path):
        mock_bar = MagicMock(return_value=MINUTE_BAR_DF)
        with (
            patch("hqdata.cli.hqdata.init_source"),
            patch("hqdata.cli.hqdata.get_stock_list", return_value=STOCK_LIST_DF),
            patch("hqdata.cli.hqdata.get_stock_minute_bar", mock_bar),
        ):
            runner.invoke(
                cli,
                ["--output", str(tmp_path), "stock-minute", "--start", "20260401", "--end", "20260407",
                 "--frequency", "15m"],
            )

        for c in mock_bar.call_args_list:
            # frequency is passed as a positional or keyword arg via the lambda wrapper
            call_args = c[0]  # positional args tuple: (symbols_str, frequency, ...)
            assert call_args[1] == "15m"


# ---------------------------------------------------------------------------
# index-daily / index-minute
# ---------------------------------------------------------------------------


class TestFetchIndexDaily:
    def test_writes_csv_per_date(self, runner, tmp_path):
        with (
            patch("hqdata.cli.hqdata.init_source"),
            patch("hqdata.cli.hqdata.get_index_list", return_value=INDEX_LIST_DF),
            patch("hqdata.cli.hqdata.get_index_daily_bar", return_value=DAILY_BAR_DF),
        ):
            result = runner.invoke(
                cli,
                ["--output", str(tmp_path), "index-daily", "--start", "20260101", "--end", "20260103"],
            )

        assert result.exit_code == 0
        out_file = tmp_path / "tushare" / "index_daily" / "20260102.csv"
        assert out_file.exists()


class TestFetchIndexMinute:
    def test_writes_csv_per_date(self, runner, tmp_path):
        with (
            patch("hqdata.cli.hqdata.init_source"),
            patch("hqdata.cli.hqdata.get_index_list", return_value=INDEX_LIST_DF),
            patch("hqdata.cli.hqdata.get_index_minute_bar", return_value=MINUTE_BAR_DF),
        ):
            result = runner.invoke(
                cli,
                ["--output", str(tmp_path), "index-minute", "--start", "20260401", "--end", "20260407",
                 "--frequency", "1m"],
            )

        assert result.exit_code == 0
        out_file = tmp_path / "tushare" / "index_minute" / "20260102.csv"
        assert out_file.exists()


# ---------------------------------------------------------------------------
# index-list
# ---------------------------------------------------------------------------


class TestFetchIndexList:
    def test_writes_today_csv(self, runner, tmp_path):
        with (
            patch("hqdata.cli.hqdata.init_source"),
            patch("hqdata.cli.hqdata.get_index_list", return_value=INDEX_LIST_DF),
            patch("hqdata.cli.hqdata.get_current_trading_day", return_value="20260102"),
        ):
            result = runner.invoke(cli, ["--output", str(tmp_path), "index-list"])

        assert result.exit_code == 0
        out_file = tmp_path / "tushare" / "index_list" / "20260102.csv"
        assert out_file.exists()

    def test_market_passed_to_api(self, runner, tmp_path):
        mock_list = MagicMock(return_value=INDEX_LIST_DF)
        with (
            patch("hqdata.cli.hqdata.init_source"),
            patch("hqdata.cli.hqdata.get_index_list", mock_list),
            patch("hqdata.cli.hqdata.get_current_trading_day", return_value="20260102"),
        ):
            runner.invoke(cli, ["--output", str(tmp_path), "index-list", "--market", "CSI"])

        mock_list.assert_called_once_with(market="CSI")


# ---------------------------------------------------------------------------
# calendar
# ---------------------------------------------------------------------------


class TestFetchCalendar:
    def test_writes_calendar_csv_no_subdir(self, runner, tmp_path):
        with (
            patch("hqdata.cli.hqdata.init_source"),
            patch("hqdata.cli.hqdata.get_calendar", return_value=CALENDAR_DF),
        ):
            result = runner.invoke(
                cli,
                ["--output", str(tmp_path), "calendar", "--start", "20260101", "--end", "20260131"],
            )

        assert result.exit_code == 0
        out_file = tmp_path / "tushare" / "calendar.csv"
        assert out_file.exists()
        assert not (tmp_path / "tushare" / "calendar").is_dir()

    def test_calendar_csv_content(self, runner, tmp_path):
        with (
            patch("hqdata.cli.hqdata.init_source"),
            patch("hqdata.cli.hqdata.get_calendar", return_value=CALENDAR_DF),
        ):
            runner.invoke(
                cli,
                ["--output", str(tmp_path), "calendar", "--start", "20260101", "--end", "20260131"],
            )

        df = pd.read_csv(tmp_path / "tushare" / "calendar.csv")
        assert list(df.columns) == ["date", "is_open"]
        assert len(df) == 2


# ---------------------------------------------------------------------------
# multi-source integration
# ---------------------------------------------------------------------------


class TestMultiSource:
    def test_multi_source_each_written(self, runner, tmp_path):
        calendar = pd.DataFrame({"date": ["20260102"], "is_open": ["Y"]})
        with (
            patch("hqdata.cli.hqdata.init_source") as mock_init,
            patch("hqdata.cli.hqdata.get_stock_list", return_value=STOCK_LIST_DF),
            patch("hqdata.cli.hqdata.get_current_trading_day", return_value="20260102"),
            patch("hqdata.cli.hqdata.get_calendar", return_value=calendar),
        ):
            result = runner.invoke(
                cli,
                ["--source", "tushare,ricequant", "--output", str(tmp_path), "stock-list"],
            )

        assert result.exit_code == 0
        assert mock_init.call_count == 2
        assert mock_init.call_args_list == [call("tushare"), call("ricequant")]
        assert (tmp_path / "tushare" / "stock_list" / "20260102.csv").exists()
        assert (tmp_path / "ricequant" / "stock_list" / "20260102.csv").exists()

    def test_default_output_expanduser(self, runner):
        """Verify that default output ~/.hqdata is expanded (not literal ~) in the echoed path."""
        calendar = pd.DataFrame({"date": ["20260102"], "is_open": ["Y"]})
        with (
            patch("hqdata.cli.hqdata.init_source"),
            patch("hqdata.cli.hqdata.get_stock_list", return_value=STOCK_LIST_DF),
            patch("hqdata.cli.hqdata.get_current_trading_day", return_value="20260102"),
            patch("hqdata.cli.hqdata.get_calendar", return_value=calendar),
            patch("hqdata.cli._write_csv"),
        ):
            result = runner.invoke(cli, ["stock-list"])

        # The echoed "Done. Written to ..." path must not contain a literal ~
        assert "~" not in result.output
        # Should contain the real expanded home path
        assert str(Path.home()) in result.output
