from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb
import pandas as pd

from qmt_data_layer.duckdb_data import DuckDbMarketData
from test_duckdb_data import build_test_db


ROOT = Path(__file__).resolve().parents[1]
RUNTIME_DIRS = [ROOT / "xtquant_duck", ROOT / "qmt_data_layer"]
FORBIDDEN_RUNTIME_TABLES = ("raw_tick_v3", "raw_daily_v1")


class RuntimeDataBoundaryTests(unittest.TestCase):
    def test_runtime_source_does_not_reference_raw_source_tables(self) -> None:
        offenders: list[str] = []
        for directory in RUNTIME_DIRS:
            for path in sorted(directory.rglob("*.py")):
                text = path.read_text(encoding="utf-8")
                for table in FORBIDDEN_RUNTIME_TABLES:
                    if table in text:
                        offenders.append(f"{path.relative_to(ROOT)} references {table}")

        self.assertEqual(offenders, [], "runtime API must only read canonical QMT tables")

    def test_runtime_works_without_raw_source_tables_present(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "runtime_boundary.duckdb"
            build_test_db(db_path)
            con = duckdb.connect(str(db_path), read_only=True)
            try:
                tables = {
                    row[0]
                    for row in con.execute(
                        "select table_name from information_schema.tables where table_schema = 'main'"
                    ).fetchall()
                }
            finally:
                con.close()

            self.assertNotIn("raw_tick_v3", tables)
            self.assertNotIn("raw_daily_v1", tables)

            data = DuckDbMarketData(db_path)
            try:
                tick = data.tick("600000.SH", pd.Timestamp("2022-07-20 09:20:00"), pd.Timestamp("2022-07-20 09:31:00"))
                minute = data.minute(
                    "600000.SH",
                    pd.Timestamp("2022-07-20 09:15:00"),
                    pd.Timestamp("2022-07-20 09:31:00"),
                )
                daily = data.daily("600000.SH", pd.Timestamp("2022-07-20"), pd.Timestamp("2022-07-21"))
            finally:
                data.close()

            self.assertGreater(len(tick), 0)
            self.assertGreater(len(minute), 0)
            self.assertGreater(len(daily), 0)

    def test_missing_canonical_tables_fail_fast(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "empty.duckdb"
            duckdb.connect(str(db_path)).close()

            with self.assertRaisesRegex(RuntimeError, "requires canonical table"):
                DuckDbMarketData(db_path)


if __name__ == "__main__":
    unittest.main()
