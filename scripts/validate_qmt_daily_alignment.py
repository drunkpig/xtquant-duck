from __future__ import annotations

import argparse
import sys
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qmt_data_layer.duckdb_data import DEFAULT_DB_PATH, DuckDbMarketData, normalize_qmt_code, parse_qmt_time

REPORT_DIR = ROOT / "reports"


def qmt_get_data(code: str, period: str, start: str, end: str, dividend_type: str | None) -> pd.DataFrame:
    from xtquant import xtdata

    fields = ["time", "open", "high", "low", "close", "volume", "amount"]
    try:
        xtdata.download_history_data(code, period=period, start_time=start, end_time=end)
    except Exception as exc:
        print(f"download_history_data warning code={code} period={period} err={exc!r}", flush=True)

    data = xtdata.get_market_data_ex(
        fields,
        [code],
        period=period,
        start_time=start,
        end_time=end,
        count=-1,
        dividend_type=dividend_type or "none",
        fill_data=False,
    )
    raw = data.get(code) if isinstance(data, dict) else data
    if raw is None or len(raw) == 0:
        return pd.DataFrame()

    df = pd.DataFrame(raw).copy()
    if "time" in df.columns:
        df["date"] = df["time"].map(parse_qmt_time).dt.normalize()
    else:
        df["date"] = [parse_qmt_time(x).normalize() for x in df.index]
    for c in fields:
        if c != "time" and c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.sort_values("date").drop_duplicates("date").reset_index(drop=True)


def compare_raw(data: DuckDbMarketData, code: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    qmt = qmt_get_data(code, "1d", start.strftime("%Y%m%d"), end.strftime("%Y%m%d"), dividend_type="none")
    local = data.daily(code, start, end, dividend_type=None)
    if qmt.empty or local.empty:
        return pd.DataFrame([{"code": code, "stage": "raw", "status": "empty", "qmt_rows": len(qmt), "local_rows": len(local)}])
    local = local.rename(columns={"ts": "date"})
    qmt["date"] = pd.to_datetime(qmt["date"]).dt.normalize()
    local["date"] = pd.to_datetime(local["date"]).dt.normalize()
    merged = qmt.merge(local, on="date", suffixes=("_qmt", "_local"))
    rows = []
    for col in ["open", "high", "low", "close", "volume", "amount"]:
        if f"{col}_qmt" in merged and f"{col}_local" in merged and len(merged):
            diff = merged[f"{col}_qmt"] - merged[f"{col}_local"]
            rows.append(
                {
                    "code": code,
                    "stage": "raw",
                    "field": col,
                    "qmt_rows": len(qmt),
                    "local_rows": len(local),
                    "common_rows": len(merged),
                    "max_abs_diff": float(diff.abs().max()),
                    "mean_abs_diff": float(diff.abs().mean()),
                }
            )
    return pd.DataFrame(rows)


def compare_front_ratio(data: DuckDbMarketData, code: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    qmt = qmt_get_data(code, "1d", start.strftime("%Y%m%d"), end.strftime("%Y%m%d"), dividend_type="front_ratio")
    if qmt.empty:
        return pd.DataFrame([{"code": code, "stage": "front_ratio", "status": "empty_qmt"}])
    anchor = pd.to_datetime(qmt["date"]).max()
    local = data.dynamic_qfq_daily(code, start, end, anchor)
    local = local.rename(columns={"ts": "date"})
    qmt["date"] = pd.to_datetime(qmt["date"]).dt.normalize()
    local["date"] = pd.to_datetime(local["date"]).dt.normalize()
    merged = qmt.merge(local, on="date", suffixes=("_qmt", "_local"))
    rows = []
    for col in ["open", "high", "low", "close"]:
        if len(merged):
            diff = merged[f"{col}_qmt"] - merged[f"{col}_local"]
            rows.append(
                {
                    "code": code,
                    "stage": "front_ratio",
                    "field": col,
                    "anchor_date": anchor.date(),
                    "qmt_rows": len(qmt),
                    "local_rows": len(local),
                    "common_rows": len(merged),
                    "max_abs_diff": float(diff.abs().max()),
                    "mean_abs_diff": float(diff.abs().mean()),
                    "max_pct_diff": float((diff.abs() / merged[f"{col}_qmt"].replace(0, np.nan)).max()),
                }
            )
    return pd.DataFrame(rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--start", default="20210101")
    parser.add_argument("--end", default="20251231")
    parser.add_argument("--codes", nargs="*", default=["600000.SH", "600036.SH", "601318.SH"])
    args = parser.parse_args()

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    start = parse_qmt_time(args.start)
    end = parse_qmt_time(args.end)
    frames = []
    with DuckDbMarketData(args.db) as data:
        for code in [normalize_qmt_code(c) for c in args.codes]:
            frames.append(compare_raw(data, code, start, end))
            frames.append(compare_front_ratio(data, code, start, end))
    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    path = REPORT_DIR / "qmt_daily_raw_front_ratio_alignment.csv"
    out.to_csv(path, index=False, encoding="utf-8-sig")
    print(out.to_string(index=False), flush=True)
    print(f"output={path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
