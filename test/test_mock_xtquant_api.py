from __future__ import annotations

import importlib
import os
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from test_duckdb_data import build_test_db


class MockXtQuantApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "qmt_mock_test.duckdb"
        build_test_db(self.db_path)
        self.old_db = os.environ.get("QMT_MOCK_DUCKDB")
        os.environ["QMT_MOCK_DUCKDB"] = str(self.db_path)

        self.xtdata = importlib.import_module("xtquant.xtdata")
        self.xtdata.close()

    def tearDown(self) -> None:
        self.xtdata.close()
        if self.old_db is None:
            os.environ.pop("QMT_MOCK_DUCKDB", None)
        else:
            os.environ["QMT_MOCK_DUCKDB"] = self.old_db
        self.tmp.cleanup()

    def test_download_history_data_validates_local_data(self) -> None:
        self.assertTrue(self.xtdata.download_history_data("600000.SH", "1d", "20220101", "20220721"))
        self.assertTrue(self.xtdata.download_history_data2(["600000.SH"], "tick", "20220720092000", "20220720092600"))

        with self.assertRaises(FileNotFoundError):
            self.xtdata.download_history_data("000001.SZ", "1d", "19900101", "19900102")
        with self.assertRaises(NotImplementedError):
            self.xtdata.download_history_data("600000.SH", "5m", "20220720", "20220721")
        with self.assertRaises(NotImplementedError):
            self.xtdata.download_history_data("600000.SH", "1d", "20220720", "20220721", incrementally=True)
        with self.assertRaises(NotImplementedError):
            self.xtdata.download_history_data("600000.SH", "1d", "20220720", "20220721", foo=True)

    def test_get_market_data_ex_returns_front_ratio_intraday_semantics(self) -> None:
        df = self.xtdata.get_market_data_ex(
            ["time", "close", "raw_close", "foreAdjustFactor", "anchor_factor"],
            ["600000.SH"],
            "1m",
            "20220720091500",
            "20220721093100",
            dividend_type="front_ratio",
        )["600000.SH"]

        self.assertGreater(len(df), 0)
        self.assertIn("raw_close", df.columns)
        self.assertAlmostEqual(df.iloc[0]["close"], df.iloc[0]["raw_close"] * 0.5)
        self.assertAlmostEqual(df.iloc[0]["anchor_factor"], 1.0)

    def test_get_market_data_ex_returns_back_ratio_daily_semantics(self) -> None:
        df = self.xtdata.get_market_data_ex(
            ["time", "close", "raw_close", "backAdjustFactor"],
            ["600000.SH"],
            "1d",
            "20220720",
            "20220721",
            dividend_type="back_ratio",
        )["600000.SH"]

        self.assertEqual(len(df), 2)
        self.assertAlmostEqual(df.iloc[0]["raw_close"], 10.5)
        self.assertAlmostEqual(df.iloc[0]["backAdjustFactor"], 2.0)
        self.assertAlmostEqual(df.iloc[0]["close"], 21.0)

    def test_get_full_tick_uses_mock_now(self) -> None:
        self.xtdata.set_mock_now("20220720093050")
        full_tick = self.xtdata.get_full_tick(["600000.SH"])

        self.assertIn("600000.SH", full_tick)
        row = full_tick["600000.SH"]
        self.assertEqual(row["lastPrice"], 10.4)
        self.assertEqual(row["volume"], 6.0)
        self.assertEqual(row["amount"], 6160.0)

    def test_unimplemented_xtdata_apis_raise(self) -> None:
        with self.assertRaises(NotImplementedError):
            self.xtdata.get_market_data_ex(["time"], ["600000.SH"], "1m", "20220720091500", "20220720093100", fill_data=True)
        with self.assertRaises(NotImplementedError):
            self.xtdata.get_market_data_ex(["missing"], ["600000.SH"], "1m", "20220720091500", "20220720093100")
        with self.assertRaises(NotImplementedError):
            self.xtdata.subscribe_quote("600000.SH")
        with self.assertRaises(NotImplementedError):
            self.xtdata.subscribe_whole_quote(["600000.SH"])
        with self.assertRaises(NotImplementedError):
            self.xtdata.unsubscribe_quote(1)
        with self.assertRaises(NotImplementedError):
            self.xtdata.run()

    def test_unimplemented_xttrader_apis_raise(self) -> None:
        xttrader = importlib.import_module("xtquant.xttrader")
        trader = xttrader.XtQuantTrader("userdata", 1)

        with self.assertRaises(NotImplementedError):
            trader.start()
        with self.assertRaises(NotImplementedError):
            trader.connect()
        with self.assertRaises(NotImplementedError):
            trader.subscribe(object())
        with self.assertRaises(NotImplementedError):
            trader.query_stock_asset(object())
        with self.assertRaises(NotImplementedError):
            trader.query_stock_positions(object())
        with self.assertRaises(NotImplementedError):
            trader.query_stock_orders(object())
        with self.assertRaises(NotImplementedError):
            trader.query_stock_trades(object())


if __name__ == "__main__":
    unittest.main()
