from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb
import pandas as pd

from qmt_data_layer.duckdb_data import DuckDbMarketData, normalize_qmt_code, parse_qmt_time
from scripts.rebuild_qmt_canonical_tables import (
    A50_TICK_SOURCE_LIKE,
    OLD_DAILY_SOURCE_LIKE,
    OLD_TICK_SOURCE_LIKE,
    create_tables,
    insert_daily_from_raw_daily_old,
    insert_daily_from_qmt_tick,
    insert_missing_daily_from_qmt_tick_old,
    insert_ticks_from_old,
)


def build_test_db(path: Path) -> None:
    con = duckdb.connect(str(path))
    try:
        con.execute(
            """
            create table qmt_tick_v1 (
                ts timestamp,
                trade_date date,
                code varchar,
                local_code varchar,
                exchange varchar,
                lastPrice double,
                volume double,
                amount double,
                bidPrice1 double,
                bidPrice2 double,
                bidPrice3 double,
                bidPrice4 double,
                bidPrice5 double,
                askPrice1 double,
                askPrice2 double,
                askPrice3 double,
                askPrice4 double,
                askPrice5 double,
                bidVol1 double,
                bidVol2 double,
                bidVol3 double,
                bidVol4 double,
                bidVol5 double,
                askVol1 double,
                askVol2 double,
                askVol3 double,
                askVol4 double,
                askVol5 double,
                source_file varchar,
                source_row bigint
            )
            """
        )
        con.execute(
            """
            create table qmt_daily_v1 (
                date date,
                code varchar,
                local_code varchar,
                open double,
                high double,
                low double,
                close double,
                volume double,
                amount double,
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
            ("600000.SH", "sh600000", "2022-07-20 09:30:10", "2022-07-20", 10.2, 3, 3040.0, 2),
            ("600000.SH", "sh600000", "2022-07-20 09:30:50", "2022-07-20", 10.4, 6, 6160.0, 3),
            ("600000.SH", "sh600000", "2022-07-20 10:30:00", "2022-07-20", 10.8, 10, 10480.0, 4),
            ("600000.SH", "sh600000", "2022-07-21 09:25:00", "2022-07-21", 20.0, 5, 10000.0, 5),
            ("600000.SH", "sh600000", "2022-07-21 09:30:10", "2022-07-21", 20.2, 11, 22120.0, 6),
            ("600000.SH", "sh600000", "2022-07-21 09:30:50", "2022-07-21", 20.4, 18, 36400.0, 7),
            ("600000.SH", "sh600000", "2022-07-21 10:30:00", "2022-07-21", 20.8, 26, 53040.0, 8),
            ("000001.SZ", "sz000001", "2022-07-20 09:25:00", "2022-07-20", 30.0, 1, 3000.0, 1),
            ("000001.SZ", "sz000001", "2022-07-20 09:30:10", "2022-07-20", 30.2, 3, 9040.0, 2),
            ("000001.SZ", "sz000001", "2022-07-20 09:30:50", "2022-07-20", 30.4, 6, 18160.0, 3),
        ]
        for code, local_code, ts, trade_date, price, volume, amount, source_row in tick_rows:
            con.execute(
                """
                insert into qmt_tick_v1 values (
                    ?, ?, ?, ?, ?, ?, ?, ?,
                    9.9, 9.8, 9.7, 9.6, 9.5, 10.1, 10.2, 10.3, 10.4, 10.5,
                    100, 90, 80, 70, 60, 110, 120, 130, 140, 150, 'unit', ?
                )
                """,
                [ts, trade_date, code, local_code, code.split(".")[1], price, volume, amount, source_row],
            )
        daily_rows = [
            ("600000.SH", "sh600000", "2022-07-20", 10.0, 11.0, 9.0, 10.5, 1000.0, 10500.0),
            ("600000.SH", "sh600000", "2022-07-21", 20.0, 21.0, 19.0, 20.5, 2000.0, 41000.0),
            ("000001.SZ", "sz000001", "2022-07-20", 30.0, 31.0, 29.0, 30.5, 3000.0, 91500.0),
        ]
        for code, local_code, date, open_, high, low, close, volume, amount in daily_rows:
            con.execute(
                """
                insert into qmt_daily_v1 values (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, 'unit'
                )
                """,
                [date, code, local_code, open_, high, low, close, volume, amount],
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


class CanonicalRebuildTests(unittest.TestCase):
    def test_raw_source_rebuild_uses_qmt_lot_semantics_by_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "rebuild_test.duckdb"
            con = duckdb.connect(str(db_path))
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
                rows = [
                    ("2025-12-01 09:25:00", "2025-12-01", "600000.SH", "sh600000", 10.0, 100.0, 100000.0, r"D:\ticks\data-tick\sh_v3\sh_600000.gz.parquet", 1),
                    ("2025-12-01 09:30:00", "2025-12-01", "600000.SH", "sh600000", 10.0, 50.0, 50000.0, r"D:\ticks\data-tick\sh_v3\sh_600000.gz.parquet", 2),
                    ("2025-12-01 09:25:00", "2025-12-01", "688000.SH", "sh688000", 100.0, 10000.0, 1000000.0, r"D:\ticks\data-tick\sh_v3\sh_688000.gz.parquet", 1),
                    ("2025-12-01 09:30:00", "2025-12-01", "688000.SH", "sh688000", 100.0, 5000.0, 500000.0, r"D:\ticks\data-tick\sh_v3\sh_688000.gz.parquet", 2),
                    ("2026-01-05 09:25:00", "2026-01-05", "688001.SH", "sh688001", 200.0, 100.0, 2000000.0, r"D:\百度盘分钟K10年\A50-2026\sh_688001.parquet", 1),
                    ("2026-01-05 09:30:00", "2026-01-05", "688001.SH", "sh688001", 200.0, 50.0, 1000000.0, r"D:\百度盘分钟K10年\A50-2026\sh_688001.parquet", 2),
                ]
                for ts, trade_date, code, local, price, volume, amount, source, source_row in rows:
                    con.execute(
                        """
                        insert into raw_tick_v3 values (
                            ?, ?, ?, ?, 'SH', ?, 0, ?, ?, ? * 100, null,
                            9, 8, 7, 6, 5, 11, 12, 13, 14, 15,
                            1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                            ?, ?
                        )
                        """,
                        [ts, trade_date, code, local, price, amount, volume, volume, source, source_row],
                    )
                raw_daily_rows = [
                    ("2025-12-01", "600000.SH", "sh600000", 10.0, 10.5, 9.8, 10.0, 15000.0, 150.0, 15000.0, 150000.0),
                    ("2025-12-01", "688000.SH", "sh688000", 100.0, 101.0, 99.0, 100.0, 15000.0, 15000.0, 15000.0, 1500000.0),
                ]
                for row in raw_daily_rows:
                    con.execute(
                        """
                        insert into raw_daily_v1 values (
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, null, 'C:\\data-tick\\sse50-day-raw-v3-vendoropen-2021-2025\\unit.parquet'
                        )
                        """,
                        row,
                    )
                create_tables(con, rebuild=True)
                insert_ticks_from_old(con, "2025-12-01", "2025-12-01", OLD_TICK_SOURCE_LIKE, "unit_old_tick")
                insert_ticks_from_old(con, "2026-01-05", "2026-01-05", A50_TICK_SOURCE_LIKE, "unit_a50_tick")
                insert_daily_from_raw_daily_old(
                    con,
                    "2025-12-01",
                    "2025-12-01",
                    OLD_DAILY_SOURCE_LIKE,
                    "unit_vendor_daily",
                )
                insert_missing_daily_from_qmt_tick_old(
                    con,
                    "2025-12-01",
                    "2025-12-01",
                    OLD_TICK_SOURCE_LIKE,
                    "unit_old_tick_backfill",
                )
                insert_daily_from_qmt_tick(con, "2026-01-05", "2026-01-05", A50_TICK_SOURCE_LIKE, "unit_a50")

                tick = con.execute(
                    """
                    select code, max(volume) as volume, max(amount) as amount
                    from qmt_tick_v1
                    group by code
                    order by code
                    """
                ).fetchdf()
                got = {r.code: (float(r.volume), float(r.amount)) for r in tick.itertuples(index=False)}
                self.assertEqual(got["600000.SH"], (150.0, 150000.0))
                self.assertEqual(got["688000.SH"], (150.0, 1500000.0))
                self.assertEqual(got["688001.SH"], (150.0, 3000000.0))

                daily = con.execute("select code, volume, source_file from qmt_daily_v1 order by code").fetchdf()
                daily_got = {r.code: float(r.volume) for r in daily.itertuples(index=False)}
                self.assertEqual(daily_got["600000.SH"], 150.0)
                self.assertEqual(daily_got["688000.SH"], 150.0)
                self.assertEqual(daily_got["688001.SH"], 150.0)
            finally:
                con.close()


if __name__ == "__main__":
    unittest.main()
