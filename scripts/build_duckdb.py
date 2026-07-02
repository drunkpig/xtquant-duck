from __future__ import annotations

import argparse
import hashlib
import sys
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qmt_data_layer.duckdb_data import DEFAULT_DB_PATH, normalize_qmt_code, qmt_to_local_code


CHOICE_CONSTITUENTS = Path(
    r"C:\data-tick\choice_index_constituents_2010_now\choice_index_constituents_annual_snapshots_with_start.csv"
)
RAW_DAILY_ROOT = Path(r"C:\data-tick\sse50-day-raw-v3-vendoropen-2021-2025")
FACTOR_ROOT = Path(r"C:\data-tick\sse50-1mk-hfq\_adjust_factors")
RAW_TICK_ROOTS = [Path(r"D:\ticks\data-tick\sh_v3"), Path(r"D:\ticks\data-tick\sz_v3")]
README_PATH = Path(r"C:\data-tick\duckdb\README.MD")


@dataclass(frozen=True)
class SourceFile:
    kind: str
    code: str
    path: Path


def file_sha1(path: Path, limit_bytes: int = 1_048_576) -> str:
    h = hashlib.sha1()
    with path.open("rb") as f:
        remaining = limit_bytes
        while remaining > 0:
            chunk = f.read(min(65536, remaining))
            if not chunk:
                break
            h.update(chunk)
            remaining -= len(chunk)
    return h.hexdigest()


def qmt_to_baostock(code: str) -> str:
    q = normalize_qmt_code(code)
    six, market = q.split(".", 1)
    return f"{market.lower()}.{six}"


def tick_path_for(code: str) -> Path | None:
    q = normalize_qmt_code(code)
    six, market = q.split(".", 1)
    names = [f"{market.lower()}_{six}.gz.parquet", f"{market.lower()}_{six}.parquet"]
    roots = [r for r in RAW_TICK_ROOTS if r.name.lower().startswith(market.lower())] + RAW_TICK_ROOTS
    for root in roots:
        for name in names:
            p = root / name
            if p.exists():
                return p
    return None


def load_constituents() -> pd.DataFrame:
    raw = pd.read_csv(CHOICE_CONSTITUENTS)
    raw = raw[raw["index_key"].eq("sse50") & raw["snapshot_year"].between(2021, 2024)].copy()
    raw["code"] = raw["stock_code"].map(normalize_qmt_code)
    raw["local_code"] = raw["code"].map(qmt_to_local_code)
    raw["snapshot_date"] = pd.to_datetime(raw["snapshot_date"]).dt.date
    raw["continuous_start_snapshot_date"] = pd.to_datetime(raw["continuous_start_snapshot_date"], errors="coerce").dt.date
    return raw[
        [
            "index_key",
            "index_name",
            "choice_sector_code",
            "snapshot_year",
            "snapshot_date",
            "code",
            "local_code",
            "stock_code",
            "stock_name",
            "continuous_start_snapshot_date",
            "continuous_start_precision",
        ]
    ].drop_duplicates(["snapshot_year", "code"]).sort_values(["snapshot_year", "code"]).reset_index(drop=True)


def connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    con.execute("pragma threads=4")
    con.execute("pragma memory_limit='8GB'")
    return con


def create_schema(con: duckdb.DuckDBPyConnection, rebuild: bool) -> None:
    if rebuild:
        for table in [
            "raw_tick_v3",
            "raw_daily_v1",
            "baostock_adjust_factor_events",
            "choice_sse50_constituents_2022_2025",
            "import_manifest",
        ]:
            con.execute(f"drop table if exists {table}")
    con.execute(
        """
        create table if not exists choice_sse50_constituents_2022_2025 (
            index_key varchar,
            index_name varchar,
            choice_sector_code bigint,
            snapshot_year integer,
            snapshot_date date,
            code varchar,
            local_code varchar,
            stock_code varchar,
            stock_name varchar,
            continuous_start_snapshot_date date,
            continuous_start_precision varchar
        )
        """
    )
    con.execute(
        """
        create table if not exists raw_daily_v1 (
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
        create table if not exists baostock_adjust_factor_events (
            code varchar,
            local_code varchar,
            baostock_code varchar,
            factor_date date,
            foreAdjustFactor double,
            backAdjustFactor double,
            adjustFactor double,
            source_file varchar
        )
        """
    )
    con.execute(
        """
        create table if not exists raw_tick_v3 (
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
        create table if not exists import_manifest (
            batch_id varchar,
            source_kind varchar,
            code varchar,
            source_file varchar,
            file_size bigint,
            file_mtime timestamp,
            file_sha1_head varchar,
            row_count bigint,
            min_time timestamp,
            max_time timestamp,
            imported_at timestamp default current_timestamp
        )
        """
    )


def record_manifest(
    con: duckdb.DuckDBPyConnection,
    batch_id: str,
    kind: str,
    code: str,
    path: Path,
    row_count: int,
    min_time: object,
    max_time: object,
) -> None:
    st = path.stat()
    con.execute(
        """
        insert into import_manifest
        (batch_id, source_kind, code, source_file, file_size, file_mtime, file_sha1_head, row_count, min_time, max_time)
        values (?, ?, ?, ?, ?, to_timestamp(?), ?, ?, ?, ?)
        """,
        [
            batch_id,
            kind,
            code,
            str(path),
            int(st.st_size),
            float(st.st_mtime),
            file_sha1(path),
            int(row_count),
            min_time,
            max_time,
        ],
    )


def record_virtual_manifest(
    con: duckdb.DuckDBPyConnection,
    batch_id: str,
    kind: str,
    code: str,
    source_file: str,
    row_count: int,
    min_time: object,
    max_time: object,
) -> None:
    con.execute(
        """
        insert into import_manifest
        (batch_id, source_kind, code, source_file, file_size, file_mtime, file_sha1_head, row_count, min_time, max_time)
        values (?, ?, ?, ?, 0, null, '', ?, ?, ?)
        """,
        [batch_id, kind, code, source_file, int(row_count), min_time, max_time],
    )


def import_constituents(con: duckdb.DuckDBPyConnection, batch_id: str) -> pd.DataFrame:
    df = load_constituents()
    con.register("constituents_df", df)
    con.execute("delete from choice_sse50_constituents_2022_2025")
    con.execute("insert into choice_sse50_constituents_2022_2025 select * from constituents_df")
    record_manifest(
        con,
        batch_id,
        "choice_sse50_constituents_2022_2025",
        "ALL",
        CHOICE_CONSTITUENTS,
        len(df),
        df["snapshot_date"].min(),
        df["snapshot_date"].max(),
    )
    return df


def import_daily(con: duckdb.DuckDBPyConnection, batch_id: str, codes: list[str]) -> None:
    con.execute("delete from raw_daily_v1")
    for i, code in enumerate(codes, 1):
        local = qmt_to_local_code(code)
        p = RAW_DAILY_ROOT / f"{local}.parquet"
        if not p.exists():
            print(f"daily missing {code} {p}", flush=True)
            continue
        con.execute(
            """
            insert into raw_daily_v1
            select
                cast(date as date) as date,
                ? as code,
                ? as local_code,
                cast(open as double) as open,
                cast(high as double) as high,
                cast(low as double) as low,
                cast(close as double) as close,
                cast(volume as double) as volume,
                cast(volume_lots as double) as volume_lots,
                cast(volume_shares as double) as volume_shares,
                cast(amount as double) as amount,
                cast(minute_count as integer) as minute_count,
                ? as source_file
            from read_parquet(?)
            where cast(date as date) between date '2021-01-01' and date '2025-12-31'
            """,
            [code, local, str(p), str(p)],
        )
        stats = con.execute(
            "select count(*), min(date)::timestamp, max(date)::timestamp from raw_daily_v1 where code = ?",
            [code],
        ).fetchone()
        record_manifest(con, batch_id, "raw_daily_v1", code, p, int(stats[0]), stats[1], stats[2])
        print(f"daily {i:03d}/{len(codes):03d} {code} rows={stats[0]}", flush=True)


def import_factors(con: duckdb.DuckDBPyConnection, batch_id: str, codes: list[str]) -> None:
    con.execute("delete from baostock_adjust_factor_events")
    for i, code in enumerate(codes, 1):
        local = qmt_to_local_code(code)
        p = FACTOR_ROOT / f"{local}.csv"
        if not p.exists():
            print(f"factor missing {code} {p}", flush=True)
            con.execute(
                """
                insert into baostock_adjust_factor_events
                values (?, ?, ?, date '1900-01-01', 1.0, 1.0, 1.0, ?)
                """,
                [code, local, qmt_to_baostock(code), f"missing:{p}"],
            )
            record_virtual_manifest(
                con,
                batch_id,
                "baostock_adjust_factor_events_missing_identity",
                code,
                f"missing:{p}",
                1,
                pd.Timestamp("1900-01-01"),
                pd.Timestamp("1900-01-01"),
            )
            continue
        con.execute(
            """
            insert into baostock_adjust_factor_events
            select
                ? as code,
                ? as local_code,
                code as baostock_code,
                cast(dividOperateDate as date) as factor_date,
                cast(foreAdjustFactor as double) as foreAdjustFactor,
                cast(backAdjustFactor as double) as backAdjustFactor,
                cast(adjustFactor as double) as adjustFactor,
                ? as source_file
            from read_csv_auto(?, header=true)
            where cast(dividOperateDate as date) is not null
            """,
            [code, local, str(p), str(p)],
        )
        stats = con.execute(
            "select count(*), min(factor_date)::timestamp, max(factor_date)::timestamp from baostock_adjust_factor_events where code = ?",
            [code],
        ).fetchone()
        record_manifest(con, batch_id, "baostock_adjust_factor_events", code, p, int(stats[0]), stats[1], stats[2])
        print(f"factor {i:03d}/{len(codes):03d} {code} rows={stats[0]}", flush=True)


def import_ticks(con: duckdb.DuckDBPyConnection, batch_id: str, codes: list[str], max_codes: int = 0) -> None:
    con.execute("delete from raw_tick_v3")
    work = codes[:max_codes] if max_codes and max_codes > 0 else codes
    for i, code in enumerate(work, 1):
        local = qmt_to_local_code(code)
        p = tick_path_for(code)
        if p is None:
            print(f"tick missing {code}", flush=True)
            continue
        exchange = code.split(".", 1)[1]
        con.execute(
            """
            insert into raw_tick_v3
            select
                cast("时间" as timestamp) as ts,
                cast(date_trunc('day', cast("时间" as timestamp)) as date) as trade_date,
                ? as code,
                ? as local_code,
                ? as exchange,
                cast("最新价" as double) as last_price,
                cast("成交笔数" as bigint) as trade_count,
                cast("成交额" as double) as amount_delta,
                cast("成交量" as double) as volume_lots,
                cast("成交量" as double) * 100.0 as volume_shares,
                cast("方向" as varchar) as side,
                cast("买一价" as double) as bid_price1,
                cast("买二价" as double) as bid_price2,
                cast("买三价" as double) as bid_price3,
                cast("买四价" as double) as bid_price4,
                cast("买五价" as double) as bid_price5,
                cast("卖一价" as double) as ask_price1,
                cast("卖二价" as double) as ask_price2,
                cast("卖三价" as double) as ask_price3,
                cast("卖四价" as double) as ask_price4,
                cast("卖五价" as double) as ask_price5,
                cast("买一量" as double) as bid_vol1,
                cast("买二量" as double) as bid_vol2,
                cast("买三量" as double) as bid_vol3,
                cast("买四量" as double) as bid_vol4,
                cast("买五量" as double) as bid_vol5,
                cast("卖一量" as double) as ask_vol1,
                cast("卖二量" as double) as ask_vol2,
                cast("卖三量" as double) as ask_vol3,
                cast("卖四量" as double) as ask_vol4,
                cast("卖五量" as double) as ask_vol5,
                ? as source_file,
                row_number() over () as source_row
            from read_parquet(?)
            where cast("时间" as timestamp) >= timestamp '2021-01-01'
              and cast("时间" as timestamp) < timestamp '2026-01-01'
            """,
            [code, local, exchange, str(p), str(p)],
        )
        stats = con.execute(
            "select count(*), min(ts), max(ts) from raw_tick_v3 where code = ?",
            [code],
        ).fetchone()
        record_manifest(con, batch_id, "raw_tick_v3", code, p, int(stats[0]), stats[1], stats[2])
        print(f"tick {i:03d}/{len(work):03d} {code} rows={stats[0]}", flush=True)


def create_indexes(con: duckdb.DuckDBPyConnection) -> None:
    # DuckDB does not require indexes for analytic scans, but these help the
    # point-like mock get_full_tick path on a single-machine database.
    for sql in [
        "create index if not exists idx_raw_tick_code_ts on raw_tick_v3(code, ts)",
        "create index if not exists idx_raw_daily_code_date on raw_daily_v1(code, date)",
        "create index if not exists idx_factor_code_date on baostock_adjust_factor_events(code, factor_date)",
        "create index if not exists idx_constituents_year_code on choice_sse50_constituents_2022_2025(snapshot_year, code)",
    ]:
        try:
            con.execute(sql)
        except Exception as exc:
            print(f"index skipped: {exc}", flush=True)


def table_summary(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    rows = []
    for table, time_col in [
        ("choice_sse50_constituents_2022_2025", "snapshot_date"),
        ("raw_daily_v1", "date"),
        ("baostock_adjust_factor_events", "factor_date"),
        ("raw_tick_v3", "ts"),
        ("import_manifest", "imported_at"),
    ]:
        stats = con.execute(
            f"select count(*) as rows, min({time_col}) as min_time, max({time_col}) as max_time from {table}"
        ).fetchone()
        code_count = None
        if table not in {"import_manifest"}:
            code_count = con.execute(f"select count(distinct code) from {table}").fetchone()[0]
        rows.append({"table": table, "rows": int(stats[0]), "codes": code_count, "min_time": stats[1], "max_time": stats[2]})
    return pd.DataFrame(rows)


def write_readme(db_path: Path, summary: pd.DataFrame, batch_id: str) -> None:
    README_PATH.parent.mkdir(parents=True, exist_ok=True)
    cols = list(summary.columns)
    lines = ["| " + " | ".join(cols) + " |", "| " + " | ".join("---" for _ in cols) + " |"]
    for row in summary.astype(object).where(pd.notna(summary), "").itertuples(index=False):
        lines.append("| " + " | ".join(str(x) for x in row) + " |")
    table_md = "\n".join(lines)
    text = f"""# qmt_mock.duckdb

This directory contains the canonical local DuckDB market-data database for QMT-compatible backtests.

## Files

- Database: `{db_path}`
- Documentation: `{README_PATH}`
- Import batch: `{batch_id}`

## Canonical Rule

QMT semantics are canonical. Local parquet files are immutable source data only. Strategy code must not read raw parquet, qfq/hfq parquet, experimental CSV caches, or ad-hoc output directories as market-data input. Strategy code should use real `xtquant.xtdata` or the local mock `xtquant.xtdata`.

## Tables

{table_md}

## Sources

- Raw tick: `D:\\ticks\\data-tick\\sh_v3` and `D:\\ticks\\data-tick\\sz_v3`
- Raw daily: `C:\\data-tick\\sse50-day-raw-v3-vendoropen-2021-2025`
- BaoStock adjustment factors: `C:\\data-tick\\sse50-1mk-hfq\\_adjust_factors`
- Choice SSE50 constituents: `C:\\data-tick\\choice_index_constituents_2010_now\\choice_index_constituents_annual_snapshots_with_start.csv`

Only SSE50 snapshots required for 2022-2025 baseline trading are imported: snapshot years 2021-2024. BaoStock factor events are imported for this stock universe through the latest available factor-file date so current QMT `front_ratio` anchors can be reproduced.

## Tick Standardization

`raw_tick_v3` stores raw price-domain tick data in QMT-style code format (`600000.SH`). `volume_lots` is the source `成交量`, `volume_shares` is `成交量 * 100`, and `amount_delta` is source `成交额`. Mock QMT tick responses convert this to QMT semantics where `volume` and `amount` are cumulative from the trading day's first tick to the returned tick.

## Dynamic Bars

No persistent 1m or 60m bar table is used.

- 1m bars are dynamically aggregated from `raw_tick_v3`.
- `09:30` is the opening auction bar.
- Continuous auction 1m bars are right-labeled.
- 60m bars are dynamically aggregated from raw tick and remain raw price-domain.

## Dynamic Front Adjustment

Static qfq/hfq parquet bars are not market-data inputs. Dynamic front-adjusted daily prices are computed from `raw_daily_v1` and `baostock_adjust_factor_events` with an ASOF-style lookup:

`raw_price(date) * foreAdjustFactor(date) / foreAdjustFactor(anchor_date)`

## Validation

Validation reports are written under `C:\\workspace\\xtquant-duck\\reports`.

Expected checks:

- DuckDB 1m aggregation vs current Python `aggregate_vendoropen_1m`.
- DuckDB 60m aggregation vs current raw tick boundary logic.
- DuckDB raw daily vs source raw daily parquet.
- Imported constituents vs Choice CSV filtered to 2021-2024 snapshots.
- QMT raw daily and QMT dynamic front-adjusted daily alignment when QMT is available.

## Rebuild

Run:

```powershell
uv run python C:\\workspace\\xtquant-duck\\scripts\\build_duckdb.py --rebuild
```
"""
    README_PATH.write_text(text, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--rebuild", action="store_true")
    parser.add_argument("--skip-ticks", action="store_true")
    parser.add_argument("--max-tick-codes", type=int, default=0)
    args = parser.parse_args()

    batch_id = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    con = connect(args.db)
    try:
        create_schema(con, args.rebuild)
        constituents = import_constituents(con, batch_id)
        codes = sorted(constituents["code"].drop_duplicates())
        print(f"stock universe codes={len(codes)}", flush=True)
        import_daily(con, batch_id, codes)
        import_factors(con, batch_id, codes)
        if not args.skip_ticks:
            import_ticks(con, batch_id, codes, args.max_tick_codes)
        create_indexes(con)
        summary = table_summary(con)
        print(summary.to_string(index=False), flush=True)
        write_readme(args.db, summary, batch_id)
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
