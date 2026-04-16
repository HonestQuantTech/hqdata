"""Unit tests for hqdata CLI (hqdata/cli.py)"""

import argparse
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pandas as pd
import pytest

import hqdata.cli as cli
from hqdata.cli import (
    VALID_FREQUENCIES,
    VALID_SOURCES,
    build_parser,
    fetch_calendar,
    fetch_index_daily,
    fetch_index_list,
    fetch_index_minute,
    fetch_stock_daily,
    fetch_stock_list,
    fetch_stock_minute,
    main,
)

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

CALENDAR_DF = pd.DataFrame(
    {"date": ["20260101", "20260102"], "is_open": [False, True]}
)


# ---------------------------------------------------------------------------
# Parser tests
# ---------------------------------------------------------------------------


class TestBuildParser:
    def test_defaults(self):
        parser = build_parser()
        args = parser.parse_args(["stock-list"])
        assert args.source == "tushare"
        assert args.output == str(Path.home() / ".hqdata")
        assert args.command == "stock-list"

    def test_source_comma_separated(self):
        parser = build_parser()
        args = parser.parse_args(["--source", "tushare,ricequant", "stock-list"])
        assert args.source == "tushare,ricequant"

    def test_output_override(self):
        parser = build_parser()
        args = parser.parse_args(["--output", "/tmp/test", "stock-list"])
        assert args.output == "/tmp/test"

    def test_stock_daily_dates(self):
        parser = build_parser()
        args = parser.parse_args(
            ["stock-daily", "--start", "20260101", "--end", "20260131"]
        )
        assert args.command == "stock-daily"
        assert args.start == "20260101"
        assert args.end == "20260131"

    def test_stock_minute_frequency_default(self):
        parser = build_parser()
        args = parser.parse_args(["stock-minute"])
        assert args.frequency == "1m"

    def test_stock_minute_frequency_choices(self):
        parser = build_parser()
        for freq in VALID_FREQUENCIES:
            args = parser.parse_args(["stock-minute", "--frequency", freq])
            assert args.frequency == freq

    def test_index_minute_frequency_invalid(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["index-minute", "--frequency", "2m"])

    def test_index_list_market(self):
        parser = build_parser()
        args = parser.parse_args(["index-list", "--market", "CSI"])
        assert args.market == "CSI"

    def test_calendar_requires_start_end(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["calendar"])
        args = parser.parse_args(
            ["calendar", "--start", "20260101", "--end", "20260131"]
        )
        assert args.start == "20260101"
        assert args.end == "20260131"

    def test_no_command_exits(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args([])


# ---------------------------------------------------------------------------
# main() source validation
# ---------------------------------------------------------------------------


class TestMainSourceValidation:
    def test_invalid_source_exits(self):
        with patch("sys.argv", ["hqdata", "--source", "unknown", "stock-list"]):
            with pytest.raises(SystemExit):
                main()

    def test_partial_invalid_source_exits(self):
        with patch(
            "sys.argv", ["hqdata", "--source", "tushare,badone", "stock-list"]
        ):
            with pytest.raises(SystemExit):
                main()


# ---------------------------------------------------------------------------
# fetch_stock_daily
# ---------------------------------------------------------------------------


class TestFetchStockDaily:
    def test_writes_csv_per_date(self, tmp_path):
        args = argparse.Namespace(start="20260101", end="20260103")
        with (
            patch("hqdata.cli.hqdata.get_stock_list", return_value=STOCK_LIST_DF),
            patch(
                "hqdata.cli.hqdata.get_stock_daily_bar", return_value=DAILY_BAR_DF
            ),
        ):
            fetch_stock_daily("tushare", args, tmp_path)

        out_file = tmp_path / "tushare" / "stock_daily" / "20260102.csv"
        assert out_file.exists()
        df = pd.read_csv(out_file)
        assert "symbol" in df.columns
        assert df["symbol"].iloc[0] == "600000.SH"

    def test_one_call_per_symbol(self, tmp_path):
        args = argparse.Namespace(start="20260101", end="20260103")
        mock_bar = MagicMock(return_value=DAILY_BAR_DF)
        with (
            patch("hqdata.cli.hqdata.get_stock_list", return_value=STOCK_LIST_DF),
            patch("hqdata.cli.hqdata.get_stock_daily_bar", mock_bar),
        ):
            fetch_stock_daily("tushare", args, tmp_path)

        assert mock_bar.call_count == len(STOCK_LIST_DF)

    def test_no_data_no_file(self, tmp_path):
        args = argparse.Namespace(start="20260101", end="20260103")
        empty_bar = pd.DataFrame(
            columns=["symbol", "date", "pre_close", "open", "high", "low", "close",
                     "volume", "turnover", "change", "pct_change"]
        )
        with (
            patch("hqdata.cli.hqdata.get_stock_list", return_value=STOCK_LIST_DF),
            patch("hqdata.cli.hqdata.get_stock_daily_bar", return_value=empty_bar),
        ):
            fetch_stock_daily("tushare", args, tmp_path)

        out_dir = tmp_path / "tushare" / "stock_daily"
        assert not out_dir.exists() or not any(out_dir.iterdir())

    def test_symbol_error_continues(self, tmp_path):
        """A failing symbol should not abort the whole fetch."""
        args = argparse.Namespace(start="20260101", end="20260103")

        def side_effect(symbol, **kwargs):
            if symbol == "000001.SZ":
                raise RuntimeError("API error")
            return DAILY_BAR_DF

        with (
            patch("hqdata.cli.hqdata.get_stock_list", return_value=STOCK_LIST_DF),
            patch("hqdata.cli.hqdata.get_stock_daily_bar", side_effect=side_effect),
        ):
            fetch_stock_daily("tushare", args, tmp_path)

        out_file = tmp_path / "tushare" / "stock_daily" / "20260102.csv"
        assert out_file.exists()


# ---------------------------------------------------------------------------
# fetch_stock_minute
# ---------------------------------------------------------------------------


class TestFetchStockMinute:
    def test_writes_csv_per_date(self, tmp_path):
        args = argparse.Namespace(start="20260101", end="20260103", frequency="5m")
        with (
            patch("hqdata.cli.hqdata.get_stock_list", return_value=STOCK_LIST_DF),
            patch(
                "hqdata.cli.hqdata.get_stock_minute_bar", return_value=MINUTE_BAR_DF
            ),
        ):
            fetch_stock_minute("tushare", args, tmp_path)

        out_file = tmp_path / "tushare" / "stock_minute" / "20260102.csv"
        assert out_file.exists()

    def test_frequency_passed(self, tmp_path):
        args = argparse.Namespace(start="20260101", end="20260101", frequency="15m")
        mock_bar = MagicMock(return_value=MINUTE_BAR_DF)
        with (
            patch("hqdata.cli.hqdata.get_stock_list", return_value=STOCK_LIST_DF),
            patch("hqdata.cli.hqdata.get_stock_minute_bar", mock_bar),
        ):
            fetch_stock_minute("tushare", args, tmp_path)

        for c in mock_bar.call_args_list:
            assert c.kwargs.get("frequency") == "15m"


# ---------------------------------------------------------------------------
# fetch_index_daily / fetch_index_minute
# ---------------------------------------------------------------------------


class TestFetchIndexDaily:
    def test_writes_csv_per_date(self, tmp_path):
        args = argparse.Namespace(start="20260101", end="20260103")
        with (
            patch("hqdata.cli.hqdata.get_index_list", return_value=INDEX_LIST_DF),
            patch(
                "hqdata.cli.hqdata.get_index_daily_bar", return_value=DAILY_BAR_DF
            ),
        ):
            fetch_index_daily("tushare", args, tmp_path)

        out_file = tmp_path / "tushare" / "index_daily" / "20260102.csv"
        assert out_file.exists()


class TestFetchIndexMinute:
    def test_writes_csv_per_date(self, tmp_path):
        args = argparse.Namespace(start="20260101", end="20260103", frequency="1m")
        with (
            patch("hqdata.cli.hqdata.get_index_list", return_value=INDEX_LIST_DF),
            patch(
                "hqdata.cli.hqdata.get_index_minute_bar", return_value=MINUTE_BAR_DF
            ),
        ):
            fetch_index_minute("tushare", args, tmp_path)

        out_file = tmp_path / "tushare" / "index_minute" / "20260102.csv"
        assert out_file.exists()


# ---------------------------------------------------------------------------
# fetch_stock_list / fetch_index_list
# ---------------------------------------------------------------------------


class TestFetchStockList:
    def test_writes_today_csv(self, tmp_path):
        args = argparse.Namespace()
        with (
            patch("hqdata.cli.hqdata.get_stock_list", return_value=STOCK_LIST_DF),
            patch(
                "hqdata.cli.hqdata.get_current_trading_day", return_value="20260102"
            ),
        ):
            fetch_stock_list("tushare", args, tmp_path)

        out_file = tmp_path / "tushare" / "stock_list" / "20260102.csv"
        assert out_file.exists()
        df = pd.read_csv(out_file)
        assert list(df.columns) == list(STOCK_LIST_DF.columns)

    def test_csv_encoding_utf8(self, tmp_path):
        args = argparse.Namespace()
        with (
            patch("hqdata.cli.hqdata.get_stock_list", return_value=STOCK_LIST_DF),
            patch(
                "hqdata.cli.hqdata.get_current_trading_day", return_value="20260102"
            ),
        ):
            fetch_stock_list("tushare", args, tmp_path)

        out_file = tmp_path / "tushare" / "stock_list" / "20260102.csv"
        content = out_file.read_bytes()
        content.decode("utf-8")  # should not raise


class TestFetchIndexList:
    def test_writes_today_csv(self, tmp_path):
        args = argparse.Namespace(market="SSE,SZSE")
        with (
            patch("hqdata.cli.hqdata.get_index_list", return_value=INDEX_LIST_DF),
            patch(
                "hqdata.cli.hqdata.get_current_trading_day", return_value="20260102"
            ),
        ):
            fetch_index_list("tushare", args, tmp_path)

        out_file = tmp_path / "tushare" / "index_list" / "20260102.csv"
        assert out_file.exists()

    def test_default_market_is_sse_szse(self, tmp_path):
        """Default --market should be SSE,SZSE."""
        parser = build_parser()
        args = parser.parse_args(["index-list"])
        assert args.market == "SSE,SZSE"

    def test_market_passed_to_api(self, tmp_path):
        args = argparse.Namespace(market="CSI")
        mock_list = MagicMock(return_value=INDEX_LIST_DF)
        with (
            patch("hqdata.cli.hqdata.get_index_list", mock_list),
            patch(
                "hqdata.cli.hqdata.get_current_trading_day", return_value="20260102"
            ),
        ):
            fetch_index_list("tushare", args, tmp_path)

        mock_list.assert_called_once_with(market="CSI")


# ---------------------------------------------------------------------------
# fetch_calendar
# ---------------------------------------------------------------------------


class TestFetchCalendar:
    def test_writes_calendar_csv_no_subdir(self, tmp_path):
        args = argparse.Namespace(start="20260101", end="20260131")
        with patch("hqdata.cli.hqdata.get_calendar", return_value=CALENDAR_DF):
            fetch_calendar("tushare", args, tmp_path)

        out_file = tmp_path / "tushare" / "calendar.csv"
        assert out_file.exists()
        # should NOT create a 'calendar/' subdirectory
        assert not (tmp_path / "tushare" / "calendar").is_dir()

    def test_calendar_csv_content(self, tmp_path):
        args = argparse.Namespace(start="20260101", end="20260131")
        with patch("hqdata.cli.hqdata.get_calendar", return_value=CALENDAR_DF):
            fetch_calendar("tushare", args, tmp_path)

        df = pd.read_csv(tmp_path / "tushare" / "calendar.csv")
        assert list(df.columns) == ["date", "is_open"]
        assert len(df) == 2


# ---------------------------------------------------------------------------
# main() integration (mocked)
# ---------------------------------------------------------------------------


class TestMain:
    def test_single_source_stock_list(self, tmp_path):
        with (
            patch(
                "sys.argv",
                ["hqdata", "--source", "tushare", "--output", str(tmp_path), "stock-list"],
            ),
            patch("hqdata.cli.hqdata.init_source"),
            patch("hqdata.cli.hqdata.get_stock_list", return_value=STOCK_LIST_DF),
            patch(
                "hqdata.cli.hqdata.get_current_trading_day", return_value="20260102"
            ),
        ):
            main()

        assert (tmp_path / "tushare" / "stock_list" / "20260102.csv").exists()

    def test_multi_source_each_written(self, tmp_path):
        with (
            patch(
                "sys.argv",
                [
                    "hqdata",
                    "--source", "tushare,ricequant",
                    "--output", str(tmp_path),
                    "stock-list",
                ],
            ),
            patch("hqdata.cli.hqdata.init_source") as mock_init,
            patch("hqdata.cli.hqdata.get_stock_list", return_value=STOCK_LIST_DF),
            patch(
                "hqdata.cli.hqdata.get_current_trading_day", return_value="20260102"
            ),
        ):
            main()

        assert mock_init.call_count == 2
        assert mock_init.call_args_list == [call("tushare"), call("ricequant")]
        assert (tmp_path / "tushare" / "stock_list" / "20260102.csv").exists()
        assert (tmp_path / "ricequant" / "stock_list" / "20260102.csv").exists()

    def test_default_output_expanduser(self, tmp_path):
        """Verify that default output ~/.hqdata is expanded (not literal ~)."""
        with (
            patch(
                "sys.argv",
                ["hqdata", "stock-list"],
            ),
            patch("hqdata.cli.hqdata.init_source"),
            patch("hqdata.cli.hqdata.get_stock_list", return_value=STOCK_LIST_DF),
            patch(
                "hqdata.cli.hqdata.get_current_trading_day", return_value="20260102"
            ),
            patch("hqdata.cli._write_csv") as mock_write,
        ):
            main()

        # output_root should be an expanded absolute path (no ~)
        written_path: Path = mock_write.call_args[0][1]
        assert "~" not in str(written_path)
