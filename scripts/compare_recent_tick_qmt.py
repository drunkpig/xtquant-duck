from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qmt_data_layer.duckdb_data import DEFAULT_DB_PATH, normalize_qmt_code

REPORT_DIR = ROOT / "reports"

FIELDS = [
    "time",
    "lastPrice",
    "open",
    "high",
    "low",
    "amount",
    "volume",
    "pvolume",
    "askPrice",
    "bidPrice",
    "askVol",
    "bidVol",
    "transactionNum",
]


def qmt_millis(ts: pd.Timestamp) -> int:
    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        t = t.tz_localize("Asia/Shanghai")
    else:
        t = t.tz_convert("Asia/Shanghai")
    return int(t.tz_convert("UTC").timestamp() * 1000)


def qmt_time_to_local(ms: Any) -> pd.Timestamp:
    return pd.to_datetime(pd.to_numeric(ms), unit="ms", utc=True).tz_convert("Asia/Shanghai").tz_localize(None)


def parse_date(value: str) -> pd.Timestamp:
    return pd.to_datetime(value, format="%Y%m%d").normalize()


def latest_tick_date(con: duckdb.DuckDBPyConnection) -> pd.Timestamp:
    row = con.execute("select max(trade_date) from raw_tick_v3").fetchone()
    if not row or row[0] is None:
        raise RuntimeError("raw_tick_v3 is empty")
    return pd.Timestamp(row[0]).normalize()


def default_codes(con: duckdb.DuckDBPyConnection, date: pd.Timestamp, limit: int) -> list[str]:
    preferred = ["600036.SH", "601899.SH", "688012.SH", "600519.SH", "600309.SH"]
    have = {
        r[0]
        for r in con.execute(
            """
            select distinct code
            from raw_tick_v3
            where trade_date = ?
            """,
            [date.date()],
        ).fetchall()
    }
    selected = [c for c in preferred if c in have]
    if len(selected) >= limit:
        return selected[:limit]
    extra = [
        r[0]
        for r in con.execute(
            """
            select code, count(*) as n
            from raw_tick_v3
            where trade_date = ?
            group by code
            order by n desc, code
            """,
            [date.date()],
        ).fetchall()
        if r[0] not in selected
    ]
    return (selected + extra)[:limit]


def fetch_local_tick(con: duckdb.DuckDBPyConnection, code: str, date: pd.Timestamp) -> pd.DataFrame:
    day_start = date
    day_end = date + pd.Timedelta(hours=15, minutes=1)
    df = con.execute(
        """
        select
            ts,
            last_price,
            volume_lots,
            amount_delta,
            bid_price1, bid_price2, bid_price3, bid_price4, bid_price5,
            ask_price1, ask_price2, ask_price3, ask_price4, ask_price5,
            bid_vol1, bid_vol2, bid_vol3, bid_vol4, bid_vol5,
            ask_vol1, ask_vol2, ask_vol3, ask_vol4, ask_vol5,
            source_row
        from raw_tick_v3
        where code = ?
          and trade_date = ?
          and ts >= ?
          and ts <= ?
          and last_price > 0
        order by ts, source_row
        """,
        [code, date.date(), day_start.to_pydatetime(), day_end.to_pydatetime()],
    ).fetchdf()
    if df.empty:
        return df

    df["time"] = df["ts"].map(qmt_millis).astype("int64")
    df["lastPrice"] = pd.to_numeric(df["last_price"], errors="coerce")
    df["open"] = df["lastPrice"].iloc[0]
    df["high"] = df["lastPrice"].cummax()
    df["low"] = df["lastPrice"].cummin()
    df["volume"] = pd.to_numeric(df["volume_lots"], errors="coerce").cumsum()
    df["amount"] = pd.to_numeric(df["amount_delta"], errors="coerce").cumsum()
    df["askPrice"] = df[[f"ask_price{i}" for i in range(1, 6)]].values.tolist()
    df["bidPrice"] = df[[f"bid_price{i}" for i in range(1, 6)]].values.tolist()
    df["askVol"] = df[[f"ask_vol{i}" for i in range(1, 6)]].values.tolist()
    df["bidVol"] = df[[f"bid_vol{i}" for i in range(1, 6)]].values.tolist()
    keep = ["time", "ts", "lastPrice", "open", "high", "low", "amount", "volume", "askPrice", "bidPrice", "askVol", "bidVol"]
    return df[keep].copy()


def fetch_qmt_tick(code: str, date: pd.Timestamp, download: bool) -> pd.DataFrame:
    from xtquant import xtdata

    xtdata.enable_hello = False
    xtdata.connect()
    if download:
        xtdata.download_history_data(code, period="tick", incrementally=True)
    start = date.strftime("%Y%m%d") + "092500"
    end = date.strftime("%Y%m%d") + "150100"
    data = xtdata.get_market_data_ex(
        FIELDS,
        [code],
        period="tick",
        start_time=start,
        end_time=end,
        count=-1,
        dividend_type="none",
        fill_data=True,
    )
    raw = data.get(code, pd.DataFrame()) if isinstance(data, dict) else data
    df = raw.copy() if isinstance(raw, pd.DataFrame) else pd.DataFrame(raw, columns=FIELDS)
    if df.empty:
        return df
    df["time"] = pd.to_numeric(df["time"], errors="coerce")
    df = df[df["time"].notna()].copy()
    df["time"] = df["time"].astype("int64")
    df["ts"] = df["time"].map(qmt_time_to_local)
    for col in ["lastPrice", "open", "high", "low", "amount", "volume", "pvolume", "transactionNum"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.sort_values("time").reset_index(drop=True)


def expand_ladder(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    if prefix not in df.columns:
        return df
    def as_five(value: Any) -> list[Any]:
        if isinstance(value, (list, tuple, np.ndarray)):
            vals = list(value)[:5]
            return vals + [np.nan] * (5 - len(vals))
        return [np.nan] * 5

    columns = [f"{prefix}{i}" for i in range(1, 6)]
    values = pd.DataFrame([as_five(v) for v in df[prefix]], columns=columns, index=df.index)
    return pd.concat([df.drop(columns=[prefix]), values], axis=1)


def comparable(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ["askPrice", "bidPrice", "askVol", "bidVol"]:
        if col in out.columns:
            out = expand_ladder(out, col)
    cols = [
        "time",
        "ts",
        "lastPrice",
        "open",
        "high",
        "low",
        "amount",
        "volume",
        "askPrice1",
        "askPrice2",
        "askPrice3",
        "askPrice4",
        "askPrice5",
        "bidPrice1",
        "bidPrice2",
        "bidPrice3",
        "bidPrice4",
        "bidPrice5",
        "askVol1",
        "askVol2",
        "askVol3",
        "askVol4",
        "askVol5",
        "bidVol1",
        "bidVol2",
        "bidVol3",
        "bidVol4",
        "bidVol5",
    ]
    return out[[c for c in cols if c in out.columns]].copy()


def compare_code(code: str, date: pd.Timestamp, con: duckdb.DuckDBPyConnection, out_dir: Path, download: bool) -> dict[str, Any]:
    local = comparable(fetch_local_tick(con, code, date))
    qmt_raw = fetch_qmt_tick(code, date, download)
    qmt_zero_rows = int(pd.to_numeric(qmt_raw.get("lastPrice", pd.Series(dtype=float)), errors="coerce").le(0).sum()) if not qmt_raw.empty else 0
    qmt = comparable(qmt_raw[pd.to_numeric(qmt_raw.get("lastPrice", pd.Series(dtype=float)), errors="coerce").gt(0)].copy())

    local.to_csv(out_dir / f"{code}_local_tick.csv", index=False, encoding="utf-8-sig")
    qmt_raw.to_csv(out_dir / f"{code}_qmt_tick_raw.csv", index=False, encoding="utf-8-sig")

    if local.empty or qmt.empty:
        return {
            "code": code,
            "date": date.date(),
            "local_rows": int(len(local)),
            "qmt_rows": int(len(qmt_raw)),
            "qmt_positive_rows": int(len(qmt)),
            "qmt_zero_rows": qmt_zero_rows,
            "status": "empty_side",
        }

    local_dupes = int(local["time"].duplicated().sum())
    qmt_dupes = int(qmt["time"].duplicated().sum())
    local_1 = local[pd.to_numeric(local["time"], errors="coerce").notna()].copy()
    qmt_1 = qmt[pd.to_numeric(qmt["time"], errors="coerce").notna()].copy()
    local_1["time"] = pd.to_numeric(local_1["time"], errors="coerce").astype("int64")
    qmt_1["time"] = pd.to_numeric(qmt_1["time"], errors="coerce").astype("int64")
    local_1 = local_1.drop_duplicates("time", keep="last")
    qmt_1 = qmt_1.drop_duplicates("time", keep="last")
    merged = qmt_1.merge(local_1, on="time", how="inner", suffixes=("_qmt", "_local"))
    qmt_times = set(qmt_1["time"].astype("int64"))
    local_times = set(local_1["time"].astype("int64"))
    missing_local = sorted(qmt_times - local_times)
    missing_qmt = sorted(local_times - qmt_times)

    rows = []
    compare_cols = [c for c in qmt_1.columns if c not in {"time", "ts"} and c in local_1.columns]
    for col in compare_cols:
        q = pd.to_numeric(merged[f"{col}_qmt"], errors="coerce")
        l = pd.to_numeric(merged[f"{col}_local"], errors="coerce")
        diff = q - l
        tol = 1e-6 if "Price" in col or col in {"lastPrice", "open", "high", "low"} else 0.0
        rows.append(
            {
                "code": code,
                "field": col,
                "common_rows": int(len(merged)),
                "nonzero_diff_rows": int(diff.abs().gt(tol).sum()),
                "max_abs_diff": float(diff.abs().max()) if len(diff) else np.nan,
                "mean_abs_diff": float(diff.abs().mean()) if len(diff) else np.nan,
            }
        )
    pd.DataFrame(rows).to_csv(out_dir / f"{code}_field_diff_summary.csv", index=False, encoding="utf-8-sig")

    mismatch_cols = ["time"]
    for col in compare_cols:
        if col in {"amount", "high", "low"}:
            mismatch_cols.extend([f"{col}_qmt", f"{col}_local"])
    if len(merged):
        merged["dt"] = merged["time"].map(qmt_time_to_local)
        any_diff = pd.Series(False, index=merged.index)
        for col in ["lastPrice", "open", "volume", "amount", "high", "low"]:
            if f"{col}_qmt" in merged and f"{col}_local" in merged:
                tol = 1e-6 if col in {"lastPrice", "open", "high", "low"} else 0.0
                any_diff |= (pd.to_numeric(merged[f"{col}_qmt"], errors="coerce") - pd.to_numeric(merged[f"{col}_local"], errors="coerce")).abs().gt(tol)
        sample_cols = ["dt", "time"] + [
            c
            for col in ["lastPrice", "open", "high", "low", "volume", "amount", "askPrice1", "bidPrice1", "askVol1", "bidVol1"]
            for c in (f"{col}_qmt", f"{col}_local")
            if c in merged.columns
        ]
        merged.loc[any_diff, sample_cols].head(200).to_csv(out_dir / f"{code}_mismatch_sample.csv", index=False, encoding="utf-8-sig")

    return {
        "code": code,
        "date": date.date(),
        "local_rows": int(len(local)),
        "qmt_rows": int(len(qmt_raw)),
        "qmt_positive_rows": int(len(qmt)),
        "qmt_zero_rows": qmt_zero_rows,
        "common_rows": int(len(merged)),
        "missing_in_local": int(len(missing_local)),
        "missing_in_qmt": int(len(missing_qmt)),
        "local_duplicate_times": local_dupes,
        "qmt_duplicate_times": qmt_dupes,
        "first_local": local["ts"].min(),
        "last_local": local["ts"].max(),
        "first_qmt": qmt["ts"].min(),
        "last_qmt": qmt["ts"].max(),
        "lastPrice_max_abs_diff": max_abs(merged, "lastPrice"),
        "volume_max_abs_diff": max_abs(merged, "volume"),
        "amount_max_abs_diff": max_abs(merged, "amount"),
        "high_max_abs_diff": max_abs(merged, "high"),
        "low_max_abs_diff": max_abs(merged, "low"),
        "status": "ok",
    }


def max_abs(merged: pd.DataFrame, col: str) -> float:
    q = f"{col}_qmt"
    l = f"{col}_local"
    if q not in merged or l not in merged or merged.empty:
        return np.nan
    return float((pd.to_numeric(merged[q], errors="coerce") - pd.to_numeric(merged[l], errors="coerce")).abs().max())


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--date", default="")
    parser.add_argument("--codes", nargs="*", default=[])
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--download", action="store_true", help="Call QMT tick download_history_data(..., incrementally=True) before querying")
    args = parser.parse_args()

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(args.db), read_only=True)
    date = parse_date(args.date) if args.date else latest_tick_date(con)
    codes = [normalize_qmt_code(c) for c in args.codes] if args.codes else default_codes(con, date, args.limit)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = REPORT_DIR / f"qmt_tick_alignment_{date:%Y%m%d}_{stamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    for code in codes:
        print(f"compare {code} {date:%Y-%m-%d}", flush=True)
        rows.append(compare_code(code, date, con, out_dir, args.download))
    summary = pd.DataFrame(rows)
    summary_path = out_dir / "summary.csv"
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    print(summary.to_string(index=False), flush=True)
    print(f"output={out_dir}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
