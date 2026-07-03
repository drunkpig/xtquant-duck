from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb
import pandas as pd

from qmt_data_layer.duckdb_data import DuckDbMarketData, normalize_qmt_code, parse_qmt_time


def build_test_db(path: Path) -> None:
    con = duckdb.connect(str(path))
    try:
        con.execute(
            """
            create table raw_tick_v3 (
                ts timestamp,
                trade_date date,
                code varchar,
                local_code varchar,
                exchange varchar,
                last_price double,
                trade_count bigint,
                amount_delta double,
                volume_lots double,
                volume_shares double,
                side varchar,
                bid_price1 double,
                bid_price2 double,
                bid_price3 double,
                bid_price4 double,
                bid_price5 double,
                ask_price1 double,
                ask_price2 double,
                ask_price3 double,
                ask_price4 double,
                ask_price5 double,
                bid_vol1 double,
                bid_vol2 double,
                bid_vol3 double,
                bid_vol4 double,
                bid_vol5 double,
                ask_vol1 double,
                ask_vol2 double,
                ask_vol3 double,
                ask_vol4 double,
                ask_vol5 double,
                source_file varchar,
                source_row bigint
            )
            """
        )
        con.execute(
            """
            create table raw_daily_v1 (
                date date,
                code varchar,
                local_code varchar,
                open double,
                high double,
                low double,
                close double,
                volume double,
                volume_lots double,
                volume_shares double,
                amount double,
                minute_count integer,
                source_file varchar
            )
            """
        )
        con.execute(
            """
            create table baostock_adjust_factor_events (
                code varchar,
                factor_date date,
                foreAdjustFactor double,
                backAdjustFactor double
            )
            """
        )
        tick_rows = [
            ("600000.SH", "sh600000", "2022-07-20 09:25:00", "2022-07-20", 10.0, 1, 1000.0, 1),
            ("600000.SH", "sh600000", "2022-07-20 09:30:10", "2022-07-20", 10.2, 2, 2040.0, 2),
            ("600000.SH", "sh600000", "2022-07-20 09:30:50", "2022-07-20", 10.4, 3, 3120.0, 3),
            ("600000.SH", "sh600000", "2022-07-20 10:30:00", "2022-07-20", 10.8, 4, 4320.0, 4),
            ("600000.SH", "sh600000", "2022-07-21 09:25:00", "2022-07-21", 20.0, 5, 10000.0, 5),
            ("600000.SH", "sh600000", "2022-07-21 09:30:10", "2022-07-21", 20.2, 6, 12120.0, 6),
            ("600000.SH", "sh600000", "2022-07-21 09:30:50", "2022-07-21", 20.4, 7, 14280.0, 7),
            ("600000.SH", "sh600000", "2022-07-21 10:30:00", "2022-07-21", 20.8, 8, 16640.0, 8),
            ("000001.SZ", "sz000001", "2022-07-20 09:25:00", "2022-07-20", 30.0, 1, 3000.0, 1),
            ("000001.SZ", "sz000001", "2022-07-20 09:30:10", "2022-07-20", 30.2, 2, 6040.0, 2),
            ("000001.SZ", "sz000001", "2022-07-20 09:30:50", "2022-07-20", 30.4, 3, 9120.0, 3),
        ]
        for code, local_code, ts, trade_date, price, volume, amount, source_row in tick_rows:
            con.execute(
                """
                insert into raw_tick_v3 values (
                    ?, ?, ?, ?, ?, ?, 1, ?, ?, ?,
                    '', 9.9, 9.8, 9.7, 9.6, 9.5, 10.1, 10.2, 10.3, 10.4, 10.5,
                    100, 90, 80, 70, 60, 110, 120, 130, 140, 150, 'unit', ?
                )
                """,
                [ts, trade_date, code, local_code, code.split(".")[1], price, amount, volume, volume * 100, source_row],
            )
        daily_rows = [
            ("600000.SH", "sh600000", "2022-07-20", 10.0, 11.0, 9.0, 10.5, 1000.0, 10500.0),
            ("600000.SH", "sh600000", "2022-07-21", 20.0, 21.0, 19.0, 20.5, 2000.0, 41000.0),
            ("000001.SZ", "sz000001", "2022-07-20", 30.0, 31.0, 29.0, 30.5, 3000.0, 91500.0),
        ]
        for code, local_code, date, open_, high, low, close, volume_lots, amount in daily_rows:
            con.execute(
                """
                insert into raw_daily_v1 values (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 240, 'unit'
                )
                """,
                [date, code, local_code, open_, high, low, close, volume_lots, volume_lots, volume_lots * 100, amount],
            )
        con.executemany(
            "insert into baostock_adjust_factor_events values (?, ?, ?, ?)",
            [
                ("600000.SH", "1900-01-01", 0.5, 2.0),
                ("600000.SH", "2022-07-21", 1.0, 4.0),
                ("000001.SZ", "1900-01-01", 1.0, 1.0),
            ],
        )
    finally:
        con.close()


class DuckDbMarketDataTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "qmt_mock_test.duckdb"
        build_test_db(self.db_path)
        self.data = DuckDbMarketData(self.db_path)

    def tearDown(self) -> None:
        self.data.close()
        self.tmp.cleanup()

    def test_code_and_time_helpers_match_qmt_shapes(self) -> None:
        self.assertEqual(normalize_qmt_code("sh600000"), "600000.SH")
        self.assertEqual(normalize_qmt_code("000001"), "000001.SZ")
        self.assertEqual(parse_qmt_time("20220720093010"), pd.Timestamp("2022-07-20 09:30:10"))

    def test_tick_returns_cumulative_volume_amount_and_open(self) -> None:
        df = self.data.tick("600000.SH", pd.Timestamp("2022-07-20 09:20:00"), pd.Timestamp("2022-07-20 09:31:00"))

        self.assertEqual(len(df), 3)
        self.assertEqual(df.iloc[0]["open"], 10.0)
        self.assertEqual(df.iloc[-1]["volume"], 6.0)
        self.assertEqual(df.iloc[-1]["amount"], 6160.0)
        self.assertEqual(df.iloc[-1]["lastPrice"], 10.4)

    def test_minute_front_ratio_adjusts_intraday_and_preserves_raw_fields(self) -> None:
        raw = self.data.minute("600000.SH", pd.Timestamp("2022-07-20 09:15:00"), pd.Timestamp("2022-07-21 09:31:00"))
        adjusted = self.data.minute(
            "600000.SH",
            pd.Timestamp("2022-07-20 09:15:00"),
            pd.Timestamp("2022-07-21 09:31:00"),
            dividend_type="front_ratio",
        )

        first = adjusted.iloc[0]
        self.assertEqual(first["raw_close"], raw.iloc[0]["close"])
        self.assertAlmostEqual(first["foreAdjustFactor"], 0.5)
        self.assertAlmostEqual(first["anchor_factor"], 1.0)
        self.assertAlmostEqual(first["close"], raw.iloc[0]["close"] * 0.5)
        self.assertAlmostEqual(adjusted.iloc[-1]["close"], raw.iloc[-1]["close"])

    def test_daily_front_ratio_uses_anchor_date_and_keeps_raw_fields(self) -> None:
        df = self.data.daily(
            "600000.SH",
            pd.Timestamp("2022-07-20"),
            pd.Timestamp("2022-07-21"),
            dividend_type="front_ratio",
        )

        self.assertEqual(len(df), 2)
        self.assertAlmostEqual(df.iloc[0]["raw_close"], 10.5)
        self.assertAlmostEqual(df.iloc[0]["close"], 5.25)
        self.assertAlmostEqual(df.iloc[1]["close"], 20.5)
        self.assertAlmostEqual(df.iloc[1]["preClose"], 5.25)

    def test_daily_back_ratio_uses_back_adjust_factor(self) -> None:
        df = self.data.daily(
            "600000.SH",
            pd.Timestamp("2022-07-20"),
            pd.Timestamp("2022-07-21"),
            dividend_type="back_ratio",
        )

        self.assertEqual(len(df), 2)
        self.assertAlmostEqual(df.iloc[0]["raw_close"], 10.5)
        self.assertAlmostEqual(df.iloc[0]["backAdjustFactor"], 2.0)
        self.assertAlmostEqual(df.iloc[0]["close"], 21.0)
        self.assertAlmostEqual(df.iloc[1]["backAdjustFactor"], 4.0)
        self.assertAlmostEqual(df.iloc[1]["close"], 82.0)
        self.assertAlmostEqual(df.iloc[1]["preClose"], 21.0)

    def test_get_market_data_ex_filters_fields_and_count(self) -> None:
        out = self.data.get_market_data_ex(
            ["close"],
            ["600000.SH"],
            "1m",
            "20220720091500",
            "20220721093100",
            count=1,
        )["600000.SH"]

        self.assertEqual(len(out), 1)
        self.assertEqual(list(out.columns), ["time", "close"])

    def test_get_market_data_ex_batches_multiple_codes_for_minute(self) -> None:
        single = self.data.get_market_data_ex(
            [],
            ["600000.SH"],
            "1m",
            "20220720091500",
            "20220720093100",
        )["600000.SH"]
        batched = self.data.get_market_data_ex(
            [],
            ["600000.SH", "000001.SZ"],
            "1m",
            "20220720091500",
            "20220720093100",
        )

        self.assertEqual(len(batched["600000.SH"]), len(single))
        self.assertEqual(batched["600000.SH"]["close"].tolist(), single["close"].tolist())
        self.assertGreater(len(batched["000001.SZ"]), 0)
        self.assertAlmostEqual(batched["000001.SZ"].iloc[-1]["close"], 30.4)

    def test_unsupported_semantics_raise_not_implemented(self) -> None:
        with self.assertRaises(NotImplementedError):
            self.data.get_market_data_ex(["not_a_field"], ["600000.SH"], "1m", "20220720091500", "20220720093100")
        with self.assertRaises(NotImplementedError):
            self.data.get_market_data_ex(["time"], ["600000.SH"], "1m", "20220720091500", "20220720093100", fill_data=True)
        with self.assertRaises(NotImplementedError):
            self.data.get_market_data_ex([], ["600000.SH"], "tick", "20220720091500", "20220720093100", dividend_type="front_ratio")
        with self.assertRaises(NotImplementedError):
            self.data.get_market_data_ex([], ["600000.SH"], "5m", "20220720091500", "20220720093100")
        with self.assertRaises(NotImplementedError):
            self.data.get_market_data_ex([], ["600000.SH"], "1m", "20220720091500", "20220720093100", dividend_type="back_ratio")

    def test_has_history_data_reports_supported_ranges(self) -> None:
        self.assertTrue(self.data.has_history_data("600000.SH", "1d", pd.Timestamp("2022-01-01"), pd.Timestamp("2022-07-20")))
        self.assertTrue(self.data.has_history_data("600000.SH", "tick", pd.Timestamp("2022-07-20 09:20:00"), pd.Timestamp("2022-07-20 09:26:00")))
        self.assertFalse(self.data.has_history_data("600000.SH", "1d", pd.Timestamp("1990-01-01"), pd.Timestamp("1990-01-02")))


if __name__ == "__main__":
    unittest.main()
