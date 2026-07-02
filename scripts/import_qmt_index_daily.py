from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd

from xtquant import xtdata

import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qmt_data_layer.duckdb_data import DEFAULT_DB_PATH, normalize_qmt_code, qmt_to_local_code


def qmt_time(ts: pd.Timestamp) -> str:
    return pd.Timestamp(ts).strftime("%Y%m%d")


def fetch_qmt_daily(code: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    qmt_code = normalize_qmt_code(code)
    xtdata.download_history_data(qmt_code, period="1d", start_time=qmt_time(start))
    data = xtdata.get_market_data_ex(
        [],
        [qmt_code],
        period="1d",
        start_time=qmt_time(start),
        end_time=qmt_time(end),
        count=-1,
        fill_data=False,
        dividend_type="none",
    )
    raw = data.get(qmt_code, pd.DataFrame())
    if raw is None or raw.empty:
        raise RuntimeError(f"QMT returned no daily data for {qmt_code}")
    df = raw.copy()
    if "time" not in df.columns:
        raise RuntimeError(f"QMT daily data for {qmt_code} has no time column")
    df["date"] = pd.to_datetime(df["time"], unit="ms", utc=True).dt.tz_convert("Asia/Shanghai").dt.tz_localize(None).dt.normalize()
    for col in ["open", "high", "low", "close", "volume", "amount"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["date", "open", "high", "low", "close", "volume"]).copy()
    df = df[df["close"].gt(0)].copy()
    return df.sort_values("date").drop_duplicates("date")


def import_index_daily(con: duckdb.DuckDBPyConnection, code: str, start: pd.Timestamp, end: pd.Timestamp, batch_id: str) -> pd.DataFrame:
    qmt_code = normalize_qmt_code(code)
    local_code = qmt_to_local_code(qmt_code)
    df = fetch_qmt_daily(qmt_code, start, end)
    insert = pd.DataFrame(
        {
            "date": df["date"].dt.date,
            "code": qmt_code,
            "local_code": local_code,
            "open": df["open"].astype(float),
            "high": df["high"].astype(float),
            "low": df["low"].astype(float),
            "close": df["close"].astype(float),
            "volume": df["volume"].astype(float),
            "volume_lots": df["volume"].astype(float) / 100.0,
            "volume_shares": df["volume"].astype(float),
            "amount": df["amount"].astype(float),
            "minute_count": None,
            "source_file": f"qmt:xtdata:1d:{qmt_code}",
        }
    )
    con.execute(
        """
        delete from raw_daily_v1
        where code = ?
          and date >= ?
          and date <= ?
        """,
        [qmt_code, start.date(), end.date()],
    )
    con.register("index_daily_df", insert)
    con.execute("insert into raw_daily_v1 select * from index_daily_df")
    con.unregister("index_daily_df")

    factor = pd.DataFrame(
        [
            {
                "code": qmt_code,
                "local_code": local_code,
                "baostock_code": "",
                "factor_date": pd.Timestamp("1900-01-01").date(),
                "foreAdjustFactor": 1.0,
                "backAdjustFactor": 1.0,
                "adjustFactor": 1.0,
                "source_file": f"qmt:index_identity_factor:{qmt_code}",
            }
        ]
    )
    con.execute("delete from baostock_adjust_factor_events where code = ?", [qmt_code])
    con.register("index_factor_df", factor)
    con.execute("insert into baostock_adjust_factor_events select * from index_factor_df")
    con.unregister("index_factor_df")

    con.execute(
        """
        insert into import_manifest
        (batch_id, source_kind, code, source_file, file_size, file_mtime, file_sha1_head, row_count, min_time, max_time)
        values (?, 'qmt_index_daily', ?, ?, 0, to_timestamp(0), '', ?, ?, ?)
        """,
        [batch_id, qmt_code, f"qmt:xtdata:1d:{qmt_code}", int(len(insert)), insert["date"].min(), insert["date"].max()],
    )
    con.execute(
        """
        insert into import_manifest
        (batch_id, source_kind, code, source_file, file_size, file_mtime, file_sha1_head, row_count, min_time, max_time)
        values (?, 'qmt_index_identity_factor', ?, ?, 0, to_timestamp(0), '', 1, date '1900-01-01', date '1900-01-01')
        """,
        [batch_id, qmt_code, f"qmt:index_identity_factor:{qmt_code}"],
    )
    return insert


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--code", default="000016.SH")
    parser.add_argument("--start", default="2021-01-01")
    parser.add_argument("--end", default="2025-12-31")
    args = parser.parse_args()

    code = normalize_qmt_code(args.code)
    start = pd.Timestamp(args.start)
    end = pd.Timestamp(args.end)
    batch_id = f"qmt_index_daily_{code.replace('.', '_')}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}"
    with duckdb.connect(str(args.db)) as con:
        con.execute("pragma threads=4")
        inserted = import_index_daily(con, code, start, end, batch_id)
    print(
        f"imported {code} daily rows={len(inserted)} min={inserted['date'].min()} max={inserted['date'].max()} batch={batch_id}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
