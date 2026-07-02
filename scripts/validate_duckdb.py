from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qmt_data_layer.duckdb_data import DEFAULT_DB_PATH, DuckDbMarketData, normalize_qmt_code, qmt_to_local_code

REPORT_DIR = ROOT / "reports"
CHOICE_CONSTITUENTS = Path(
    r"C:\data-tick\choice_index_constituents_2010_now\choice_index_constituents_annual_snapshots_with_start.csv"
)
RAW_DAILY_ROOT = Path(r"C:\data-tick\sse50-day-raw-v3-vendoropen-2021-2025")


def aggregate_vendoropen_1m_python(tick: pd.DataFrame, code: str) -> pd.DataFrame:
    if tick.empty:
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume", "amount"])
    df = tick.sort_values("ts").copy()
    minute = df["ts"].dt.hour * 60 + df["ts"].dt.minute
    bar_minute = pd.Series(np.nan, index=df.index, dtype=float)
    auction_open = minute.eq(9 * 60 + 25)
    close_auction = minute.eq(15 * 60)
    morning = minute.ge(9 * 60 + 30) & minute.lt(11 * 60 + 30)
    afternoon = minute.ge(13 * 60) & minute.lt(14 * 60 + 58)
    bar_minute.loc[auction_open] = 9 * 60 + 30
    bar_minute.loc[close_auction] = 15 * 60
    bar_minute.loc[morning | afternoon] = minute.loc[morning | afternoon] + 1
    work = df[bar_minute.notna()].copy()
    work["bar_ts"] = work["ts"].dt.normalize() + pd.to_timedelta(bar_minute.loc[work.index].astype(int), unit="m")
    work = work.sort_values(["bar_ts", "ts", "source_row"])
    g = work.groupby("bar_ts", sort=True)
    out = pd.DataFrame(
        {
            "ts": g["bar_ts"].first().index,
            "open": g["last_price"].first().to_numpy(float),
            "high": g["last_price"].max().to_numpy(float),
            "low": g["last_price"].min().to_numpy(float),
            "close": g["last_price"].last().to_numpy(float),
            "volume": g["volume_lots"].sum().to_numpy(float),
            "amount": g["amount_delta"].sum().to_numpy(float),
        }
    )
    out = out[out["volume"].gt(0)].copy()
    return out.sort_values("ts").reset_index(drop=True)


def validate_constituents(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    raw = pd.read_csv(CHOICE_CONSTITUENTS)
    expected = raw[raw["index_key"].eq("sse50") & raw["snapshot_year"].between(2021, 2024)].copy()
    expected["code"] = expected["stock_code"].map(normalize_qmt_code)
    db = con.execute("select snapshot_year, code from choice_sse50_constituents_2022_2025").fetchdf()
    rows = []
    for year in range(2021, 2025):
        a = set(expected.loc[expected["snapshot_year"].eq(year), "code"])
        b = set(db.loc[db["snapshot_year"].eq(year), "code"])
        rows.append(
            {
                "snapshot_year": year,
                "expected": len(a),
                "duckdb": len(b),
                "missing": len(a - b),
                "extra": len(b - a),
                "ok": a == b,
            }
        )
    return pd.DataFrame(rows)


def validate_raw_daily(con: duckdb.DuckDBPyConnection, codes: list[str]) -> pd.DataFrame:
    rows = []
    for code in codes:
        local = qmt_to_local_code(code)
        p = RAW_DAILY_ROOT / f"{local}.parquet"
        if not p.exists():
            rows.append({"code": code, "status": "missing_source"})
            continue
        src = pd.read_parquet(p)
        src["date"] = pd.to_datetime(src["date"]).dt.normalize()
        db = con.execute("select * from raw_daily_v1 where code = ? order by date", [code]).fetchdf()
        db["date"] = pd.to_datetime(db["date"]).dt.normalize()
        merged = src.merge(db, on="date", suffixes=("_src", "_db"))
        diffs = {}
        for col in ["open", "high", "low", "close", "volume_lots", "amount"]:
            diffs[f"max_abs_{col}"] = float((merged[f"{col}_src"] - merged[f"{col}_db"]).abs().max()) if len(merged) else np.nan
        rows.append({"code": code, "source_rows": len(src), "duckdb_rows": len(db), "common_rows": len(merged), **diffs})
    return pd.DataFrame(rows)


def validate_1m(con: duckdb.DuckDBPyConnection, data: DuckDbMarketData, codes: list[str], sample_days: int) -> pd.DataFrame:
    rows = []
    for code in codes:
        days = con.execute(
            """
            select distinct trade_date
            from raw_tick_v3
            where code = ?
              and trade_date between date '2022-01-01' and date '2025-12-31'
            order by trade_date
            limit ?
            """,
            [code, sample_days],
        ).fetchdf()
        for d in days["trade_date"]:
            start = pd.Timestamp(d)
            end = start + pd.Timedelta(hours=15)
            tick = con.execute(
                """
                select ts, last_price, volume_lots, amount_delta, source_row
                from raw_tick_v3
                where code = ? and trade_date = ?
                order by ts, source_row
                """,
                [code, d],
            ).fetchdf()
            py = aggregate_vendoropen_1m_python(tick, code)
            db = data.minute(code, start + pd.Timedelta(hours=9, minutes=15), end)
            cols = ["ts", "open", "high", "low", "close", "volume", "amount"]
            merged = py[cols].merge(db[cols], on="ts", suffixes=("_py", "_db"))
            max_diff = 0.0
            for col in ["open", "high", "low", "close", "volume", "amount"]:
                if len(merged):
                    max_diff = max(max_diff, float((merged[f"{col}_py"] - merged[f"{col}_db"]).abs().max()))
            rows.append(
                {
                    "code": code,
                    "date": d,
                    "python_rows": len(py),
                    "duckdb_rows": len(db),
                    "common_rows": len(merged),
                    "max_abs_diff": max_diff,
                    "ok": len(py) == len(db) == len(merged) and max_diff < 1e-6,
                }
            )
    return pd.DataFrame(rows)


def aggregate_60m_python(tick: pd.DataFrame) -> pd.DataFrame:
    if tick.empty:
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume", "amount"])
    df = tick.sort_values(["ts", "source_row"]).copy()
    t = df["ts"].dt.time
    day = df["ts"].dt.normalize()
    labels = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")
    labels.loc[(t >= pd.Timestamp("09:30").time()) & (t < pd.Timestamp("10:30").time())] = day + pd.Timedelta(hours=10, minutes=30)
    labels.loc[(t >= pd.Timestamp("10:30").time()) & (t < pd.Timestamp("11:30").time())] = day + pd.Timedelta(hours=11, minutes=30)
    labels.loc[(t >= pd.Timestamp("13:00").time()) & (t < pd.Timestamp("14:00").time())] = day + pd.Timedelta(hours=14)
    labels.loc[(t >= pd.Timestamp("14:00").time()) & (t <= pd.Timestamp("15:00:59").time())] = day + pd.Timedelta(hours=15)
    work = df[labels.notna()].copy()
    work["bar_ts"] = labels.loc[work.index]
    g = work.groupby("bar_ts", sort=True)
    return pd.DataFrame(
        {
            "ts": g["bar_ts"].first().index,
            "open": g["last_price"].first().to_numpy(float),
            "high": g["last_price"].max().to_numpy(float),
            "low": g["last_price"].min().to_numpy(float),
            "close": g["last_price"].last().to_numpy(float),
            "volume": g["volume_lots"].sum().to_numpy(float),
            "amount": g["amount_delta"].sum().to_numpy(float),
        }
    ).reset_index(drop=True)


def validate_60m(con: duckdb.DuckDBPyConnection, data: DuckDbMarketData, codes: list[str], sample_days: int) -> pd.DataFrame:
    rows = []
    for code in codes:
        days = con.execute(
            """
            select distinct trade_date
            from raw_tick_v3
            where code = ?
              and trade_date between date '2022-01-01' and date '2025-12-31'
            order by trade_date
            limit ?
            """,
            [code, sample_days],
        ).fetchdf()
        for d in days["trade_date"]:
            start = pd.Timestamp(d)
            end = start + pd.Timedelta(hours=15)
            tick = con.execute(
                """
                select ts, last_price, volume_lots, amount_delta, source_row
                from raw_tick_v3
                where code = ? and trade_date = ?
                order by ts, source_row
                """,
                [code, d],
            ).fetchdf()
            py = aggregate_60m_python(tick)
            db = data.sixty_minute(code, start + pd.Timedelta(hours=9, minutes=30), end)
            cols = ["ts", "open", "high", "low", "close", "volume", "amount"]
            merged = py[cols].merge(db[cols], on="ts", suffixes=("_py", "_db"))
            max_diff = 0.0
            for col in ["open", "high", "low", "close", "volume", "amount"]:
                if len(merged):
                    max_diff = max(max_diff, float((merged[f"{col}_py"] - merged[f"{col}_db"]).abs().max()))
            rows.append(
                {
                    "code": code,
                    "date": d,
                    "python_rows": len(py),
                    "duckdb_rows": len(db),
                    "common_rows": len(merged),
                    "max_abs_diff": max_diff,
                    "ok": len(py) == len(db) == len(merged) and max_diff < 1e-6,
                }
            )
    return pd.DataFrame(rows)


def validate_mock_smoke() -> pd.DataFrame:
    sys.path.insert(0, str(ROOT))
    try:
        from xtquant import xtdata
        from xtquant.xttrader import XtQuantTrader

        daily = xtdata.get_market_data_ex(["time", "open", "close", "volume"], ["600000.SH"], "1d", "20220101", "20220110")
        minute = xtdata.get_market_data_ex(["time", "open", "close", "volume"], ["600000.SH"], "1m", "20220104091500", "20220104100000")
        raw_minute = xtdata.get_market_data_ex(["time", "close"], ["600000.SH"], "1m", "20220720091500", "20220721100000", dividend_type="none")
        front_minute = xtdata.get_market_data_ex(
            ["time", "close", "raw_close", "foreAdjustFactor", "anchor_factor"],
            ["600000.SH"],
            "1m",
            "20220720091500",
            "20220721100000",
            dividend_type="front_ratio",
        )
        tick = xtdata.get_market_data_ex([], ["600000.SH"], "tick", "20220104091500", "20220104093030")
        xtdata.set_mock_now("20220104093030")
        full_tick = xtdata.get_full_tick(["600000.SH"])
        raw_1m = raw_minute.get("600000.SH", pd.DataFrame())
        front_1m = front_minute.get("600000.SH", pd.DataFrame())
        merged_1m = raw_1m.merge(front_1m, on="time", suffixes=("_raw", "_front")) if not raw_1m.empty and not front_1m.empty else pd.DataFrame()
        raw_close_diff = (
            float((merged_1m["close_raw"] - merged_1m["raw_close"]).abs().max())
            if len(merged_1m) and "raw_close" in merged_1m
            else np.nan
        )
        adjusted_close_diff = float((merged_1m["close_front"] - merged_1m["close_raw"]).abs().max()) if len(merged_1m) else 0.0
        try:
            xtdata.get_market_data_ex([], ["600000.SH"], "1m", "20220104091500", "20220104100000", dividend_type="back_ratio")
            back_ratio_raises = False
        except NotImplementedError:
            back_ratio_raises = True
        try:
            xtdata.get_market_data_ex(["not_a_qmt_field"], ["600000.SH"], "1m", "20220104091500", "20220104100000")
            missing_field_raises = False
        except NotImplementedError:
            missing_field_raises = True
        try:
            xtdata.get_market_data_ex(["time"], ["600000.SH"], "1m", "20220104091500", "20220104100000", fill_data=True)
            fill_data_raises = False
        except NotImplementedError:
            fill_data_raises = True
        try:
            xtdata.subscribe_quote("600000.SH")
            subscribe_raises = False
        except NotImplementedError:
            subscribe_raises = True
        try:
            XtQuantTrader("mock", 1).connect()
            trader_raises = False
        except NotImplementedError:
            trader_raises = True
        return pd.DataFrame(
            [
                {"check": "daily_rows", "value": len(daily.get("600000.SH", [])), "ok": len(daily.get("600000.SH", [])) > 0},
                {"check": "minute_rows", "value": len(minute.get("600000.SH", [])), "ok": len(minute.get("600000.SH", [])) > 0},
                {"check": "front_ratio_1m_fields", "value": ",".join(front_1m.columns), "ok": {"raw_close", "foreAdjustFactor", "anchor_factor"}.issubset(front_1m.columns)},
                {"check": "front_ratio_1m_preserves_raw_close", "value": raw_close_diff, "ok": raw_close_diff < 1e-9},
                {"check": "front_ratio_1m_adjusts_cross_factor", "value": adjusted_close_diff, "ok": adjusted_close_diff > 1e-6},
                {"check": "back_ratio_1m_unsupported", "value": back_ratio_raises, "ok": back_ratio_raises},
                {"check": "missing_field_unsupported", "value": missing_field_raises, "ok": missing_field_raises},
                {"check": "fill_data_unsupported", "value": fill_data_raises, "ok": fill_data_raises},
                {"check": "subscribe_unsupported", "value": subscribe_raises, "ok": subscribe_raises},
                {"check": "trader_connect_unsupported", "value": trader_raises, "ok": trader_raises},
                {"check": "tick_rows", "value": len(tick.get("600000.SH", [])), "ok": len(tick.get("600000.SH", [])) > 0},
                {"check": "full_tick_keys", "value": len(full_tick), "ok": "600000.SH" in full_tick},
            ]
        )
    finally:
        try:
            sys.path.remove(str(mock_root))
        except ValueError:
            pass


def validate_qmt_available() -> pd.DataFrame:
    available = importlib.util.find_spec("xtquant") is not None
    return pd.DataFrame([{"check": "real_xtquant_available", "ok": bool(available), "note": "Run QMT alignment scripts from a real MiniQMT environment"}])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--max-codes", type=int, default=5)
    parser.add_argument("--sample-days", type=int, default=2)
    args = parser.parse_args()

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(args.db), read_only=True)
    data = DuckDbMarketData(args.db)
    try:
        codes = [
            r[0]
            for r in con.execute(
                "select distinct code from choice_sse50_constituents_2022_2025 order by code limit ?",
                [args.max_codes],
            ).fetchall()
        ]
        constituents = validate_constituents(con)
        daily = validate_raw_daily(con, codes)
        bars = validate_1m(con, data, codes, args.sample_days)
        bars60 = validate_60m(con, data, codes, args.sample_days)
        mock = validate_mock_smoke()
        qmt = validate_qmt_available()
        outputs = {
            "duckdb_constituents_validation.csv": constituents,
            "duckdb_raw_daily_validation.csv": daily,
            "duckdb_vs_python_1m_validation.csv": bars,
            "duckdb_vs_python_60m_validation.csv": bars60,
            "mock_xtdata_smoke.csv": mock,
            "qmt_validation_availability.csv": qmt,
        }
        for name, df in outputs.items():
            path = REPORT_DIR / name
            df.to_csv(path, index=False, encoding="utf-8-sig")
            print(f"{name}\n{df.to_string(index=False)}\n", flush=True)
    finally:
        data.close()
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
