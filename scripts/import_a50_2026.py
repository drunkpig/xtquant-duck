from __future__ import annotations

import argparse
import hashlib
import sys
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qmt_data_layer.duckdb_data import DEFAULT_DB_PATH, normalize_qmt_code, qmt_to_local_code


DEFAULT_FILE_LIST = ROOT / "reports" / "a50_2026_parquet_files.txt"


def file_sha1_head(path: Path, limit_bytes: int = 1_048_576) -> str:
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


def code_from_path(path: str) -> str:
    stem = Path(path).stem.lower()
    parts = stem.split("_")
    if len(parts) == 2 and parts[0] in {"sh", "sz"}:
        return normalize_qmt_code(f"{parts[0]}{parts[1]}")
    return normalize_qmt_code(stem)


def read_file_list(path: Path) -> list[str]:
    files = [line.strip() for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]
    if not files:
        raise FileNotFoundError(f"empty A50 parquet file list: {path}")
    return files


def quote_codes(codes: list[str]) -> str:
    return "(" + ",".join("'" + c.replace("'", "''") + "'" for c in codes) + ")"


def connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(db_path))
    con.execute("pragma threads=4")
    con.execute("pragma memory_limit='8GB'")
    return con


def record_manifest(
    con: duckdb.DuckDBPyConnection,
    batch_id: str,
    source_kind: str,
    code: str,
    source_file: str,
    row_count: int,
    min_time: object,
    max_time: object,
) -> None:
    path = Path(source_file)
    file_size = int(path.stat().st_size) if path.exists() else 0
    file_mtime = float(path.stat().st_mtime) if path.exists() else 0.0
    sha1 = file_sha1_head(path) if path.exists() else ""
    con.execute(
        """
        insert into import_manifest
        (batch_id, source_kind, code, source_file, file_size, file_mtime, file_sha1_head, row_count, min_time, max_time)
        values (?, ?, ?, ?, ?, to_timestamp(?), ?, ?, ?, ?)
        """,
        [batch_id, source_kind, code, source_file, file_size, file_mtime, sha1, int(row_count), min_time, max_time],
    )


def import_constituents(con: duckdb.DuckDBPyConnection, codes: list[str], snapshot_year: int, batch_id: str) -> None:
    existing_names = con.execute(
        """
        select code, any_value(stock_name) as stock_name
        from choice_sse50_constituents_2022_2025
        group by code
        """
    ).fetchdf()
    name_by_code = dict(zip(existing_names["code"], existing_names["stock_name"])) if not existing_names.empty else {}

    con.execute("delete from choice_sse50_constituents_2022_2025 where snapshot_year = ?", [int(snapshot_year)])
    rows = []
    snapshot_date = pd.Timestamp(f"{snapshot_year}-12-31").date()
    for code in sorted(codes):
        local = qmt_to_local_code(code)
        rows.append(
            {
                "index_key": "a50",
                "index_name": "A50 local 2026",
                "choice_sector_code": 0,
                "snapshot_year": int(snapshot_year),
                "snapshot_date": snapshot_date,
                "code": code,
                "local_code": local,
                "stock_code": code,
                "stock_name": str(name_by_code.get(code, local)),
                "continuous_start_snapshot_date": snapshot_date,
                "continuous_start_precision": "a50_2026_tick_folder",
            }
        )
    df = pd.DataFrame(rows)
    con.register("a50_constituents_df", df)
    con.execute("insert into choice_sse50_constituents_2022_2025 select * from a50_constituents_df")
    con.unregister("a50_constituents_df")
    con.execute(
        """
        insert into import_manifest
        (batch_id, source_kind, code, source_file, file_size, file_mtime, file_sha1_head, row_count, min_time, max_time)
        values (?, 'choice_a50_constituents_2026', '', 'virtual:a50_2026_file_list', 0, to_timestamp(0), '', ?, ?, ?)
        """,
        [batch_id, len(rows), pd.Timestamp(snapshot_date), pd.Timestamp(snapshot_date)],
    )


def import_tick_file(
    con: duckdb.DuckDBPyConnection,
    path: str,
    code: str,
    tick_start: str,
    tick_end: str,
    batch_id: str,
    index: int,
    total: int,
) -> None:
    local = qmt_to_local_code(code)
    exchange = code.split(".", 1)[1]
    con.execute(
        """
        insert into raw_tick_v3
        with source as (
            select
                cast(datetime as timestamp) as ts,
                cast(trade_date as date) as trade_date,
                ? as code,
                ? as local_code,
                ? as exchange,
                cast(lastPrice as double) as last_price,
                cast(amount as double) as cum_amount,
                cast(volume as double) as cum_volume_lots,
                cast(bidPrice1 as double) as bid_price1,
                cast(bidPrice2 as double) as bid_price2,
                cast(bidPrice3 as double) as bid_price3,
                cast(bidPrice4 as double) as bid_price4,
                cast(bidPrice5 as double) as bid_price5,
                cast(askPrice1 as double) as ask_price1,
                cast(askPrice2 as double) as ask_price2,
                cast(askPrice3 as double) as ask_price3,
                cast(askPrice4 as double) as ask_price4,
                cast(askPrice5 as double) as ask_price5,
                cast(bidVol1 as double) as bid_vol1,
                cast(bidVol2 as double) as bid_vol2,
                cast(bidVol3 as double) as bid_vol3,
                cast(bidVol4 as double) as bid_vol4,
                cast(bidVol5 as double) as bid_vol5,
                cast(askVol1 as double) as ask_vol1,
                cast(askVol2 as double) as ask_vol2,
                cast(askVol3 as double) as ask_vol3,
                cast(askVol4 as double) as ask_vol4,
                cast(askVol5 as double) as ask_vol5,
                row_number() over (partition by cast(trade_date as date) order by datetime) as source_row
            from read_parquet(?)
            where cast(trade_date as date) between cast(? as date) and cast(? as date)
              and cast(lastPrice as double) > 0
        ),
        delta as (
            select
                *,
                greatest(cum_amount - coalesce(lag(cum_amount) over (partition by trade_date order by ts, source_row), 0), 0) as amount_delta,
                greatest(cum_volume_lots - coalesce(lag(cum_volume_lots) over (partition by trade_date order by ts, source_row), 0), 0) as volume_lots
            from source
        )
        select
            ts,
            trade_date,
            code,
            local_code,
            exchange,
            last_price,
            0::bigint as trade_count,
            amount_delta,
            volume_lots,
            volume_lots * 100.0 as volume_shares,
            null::varchar as side,
            bid_price1,
            bid_price2,
            bid_price3,
            bid_price4,
            bid_price5,
            ask_price1,
            ask_price2,
            ask_price3,
            ask_price4,
            ask_price5,
            bid_vol1,
            bid_vol2,
            bid_vol3,
            bid_vol4,
            bid_vol5,
            ask_vol1,
            ask_vol2,
            ask_vol3,
            ask_vol4,
            ask_vol5,
            ? as source_file,
            source_row::bigint as source_row
        from delta
        """,
        [code, local, exchange, path, tick_start, tick_end, path],
    )
    stats = con.execute(
        """
        select count(*), min(ts)::timestamp, max(ts)::timestamp
        from raw_tick_v3
        where code = ? and source_file = ?
        """,
        [code, path],
    ).fetchone()
    record_manifest(con, batch_id, "raw_tick_v3_a50_2026", code, path, int(stats[0]), stats[1], stats[2])
    print(f"tick {index:03d}/{total:03d} {code} rows={stats[0]} min={stats[1]} max={stats[2]}", flush=True)


def import_ticks(con: duckdb.DuckDBPyConnection, files: list[str], tick_start: str, tick_end: str, batch_id: str) -> list[str]:
    codes = [code_from_path(p) for p in files]
    code_sql = quote_codes(codes)
    con.execute(
        f"""
        delete from raw_tick_v3
        where code in {code_sql}
          and trade_date between cast(? as date) and cast(? as date)
        """,
        [tick_start, tick_end],
    )
    for i, path in enumerate(files, 1):
        import_tick_file(con, path, codes[i - 1], tick_start, tick_end, batch_id, i, len(files))
    return codes


def import_daily_from_a50_files(
    con: duckdb.DuckDBPyConnection,
    files: list[str],
    daily_start: str,
    daily_end: str,
    batch_id: str,
) -> None:
    codes = [code_from_path(p) for p in files]
    code_sql = quote_codes(codes)
    con.execute(
        f"""
        delete from raw_daily_v1
        where code in {code_sql}
          and date between cast(? as date) and cast(? as date)
        """,
        [daily_start, daily_end],
    )
    for i, path in enumerate(files, 1):
        code = codes[i - 1]
        local = qmt_to_local_code(code)
        con.execute(
            """
            insert into raw_daily_v1
            with source as (
                select
                    cast(trade_date as date) as date,
                    cast(datetime as timestamp) as ts,
                    cast(lastPrice as double) as last_price,
                    cast(amount as double) as cum_amount,
                    cast(volume as double) as cum_volume_lots,
                    row_number() over (partition by cast(trade_date as date) order by datetime) as source_row
                from read_parquet(?)
                where cast(trade_date as date) between cast(? as date) and cast(? as date)
                  and cast(lastPrice as double) > 0
            ),
            delta as (
                select
                    *,
                    greatest(cum_amount - coalesce(lag(cum_amount) over (partition by date order by ts, source_row), 0), 0) as amount_delta,
                    greatest(cum_volume_lots - coalesce(lag(cum_volume_lots) over (partition by date order by ts, source_row), 0), 0) as volume_lots
                from source
            )
            select
                date,
                ? as code,
                ? as local_code,
                first(last_price order by ts, source_row) as open,
                max(last_price) as high,
                min(last_price) as low,
                last(last_price order by ts, source_row) as close,
                sum(volume_lots) * 100.0 as volume,
                sum(volume_lots) as volume_lots,
                sum(volume_lots) * 100.0 as volume_shares,
                sum(amount_delta) as amount,
                null::integer as minute_count,
                ? as source_file
            from delta
            group by date
            having sum(volume_lots) > 0
            order by date
            """,
            [path, daily_start, daily_end, code, local, f"derived_daily:{path}"],
        )
        stats = con.execute(
            """
            select count(*), min(date)::timestamp, max(date)::timestamp
            from raw_daily_v1
            where code = ? and source_file = ?
            """,
            [code, f"derived_daily:{path}"],
        ).fetchone()
        record_manifest(con, batch_id, "raw_daily_v1_a50_2026_from_tick", code, path, int(stats[0]), stats[1], stats[2])
        print(f"daily {i:03d}/{len(files):03d} {code} rows={stats[0]} min={stats[1]} max={stats[2]}", flush=True)


def baostock_code(code: str) -> str:
    six, market = normalize_qmt_code(code).split(".", 1)
    return f"{market.lower()}.{six}"


def import_factors_from_baostock(
    con: duckdb.DuckDBPyConnection,
    codes: list[str],
    factor_end: str,
    batch_id: str,
) -> None:
    import baostock as bs

    login = bs.login()
    if login.error_code != "0":
        raise RuntimeError(f"baostock login failed: {login.error_code} {login.error_msg}")
    try:
        code_sql = quote_codes(codes)
        con.execute(f"delete from baostock_adjust_factor_events where code in {code_sql}")
        for i, code in enumerate(sorted(codes), 1):
            bs_code = baostock_code(code)
            rs = bs.query_adjust_factor(bs_code, start_date="1900-01-01", end_date=factor_end)
            if rs.error_code != "0":
                raise RuntimeError(f"baostock factor failed {code}: {rs.error_code} {rs.error_msg}")
            rows = []
            while rs.next():
                rows.append(dict(zip(rs.fields, rs.get_row_data())))
            if rows:
                df = pd.DataFrame(rows)
                df["code"] = code
                df["local_code"] = qmt_to_local_code(code)
                df["baostock_code"] = bs_code
                df["source_file"] = f"baostock_api:query_adjust_factor:{factor_end}"
                for col in ["foreAdjustFactor", "backAdjustFactor", "adjustFactor"]:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                df["dividOperateDate"] = pd.to_datetime(df["dividOperateDate"], errors="coerce").dt.date
                df = df.dropna(subset=["dividOperateDate"])
                insert = df[
                    [
                        "code",
                        "local_code",
                        "baostock_code",
                        "dividOperateDate",
                        "foreAdjustFactor",
                        "backAdjustFactor",
                        "adjustFactor",
                        "source_file",
                    ]
                ].rename(columns={"dividOperateDate": "factor_date"})
            else:
                insert = pd.DataFrame(
                    [
                        {
                            "code": code,
                            "local_code": qmt_to_local_code(code),
                            "baostock_code": bs_code,
                            "factor_date": pd.Timestamp("1900-01-01").date(),
                            "foreAdjustFactor": 1.0,
                            "backAdjustFactor": 1.0,
                            "adjustFactor": 1.0,
                            "source_file": f"baostock_api:empty_identity:{factor_end}",
                        }
                    ]
                )
            con.register("factor_insert_df", insert)
            con.execute("insert into baostock_adjust_factor_events select * from factor_insert_df")
            con.unregister("factor_insert_df")
            minmax = con.execute(
                """
                select count(*), min(factor_date)::timestamp, max(factor_date)::timestamp
                from baostock_adjust_factor_events
                where code = ?
                """,
                [code],
            ).fetchone()
            con.execute(
                """
                insert into import_manifest
                (batch_id, source_kind, code, source_file, file_size, file_mtime, file_sha1_head, row_count, min_time, max_time)
                values (?, 'baostock_adjust_factor_events_api_a50_2026', ?, ?, 0, to_timestamp(0), '', ?, ?, ?)
                """,
                [batch_id, code, f"baostock_api:{bs_code}", int(minmax[0]), minmax[1], minmax[2]],
            )
            print(f"factor {i:03d}/{len(codes):03d} {code} rows={minmax[0]}", flush=True)
    finally:
        bs.logout()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--file-list", type=Path, default=DEFAULT_FILE_LIST)
    parser.add_argument("--tick-start", default="2025-12-01")
    parser.add_argument("--daily-start", default="2025-03-28")
    parser.add_argument("--end", default="2026-06-15")
    parser.add_argument("--snapshot-year", type=int, default=2025)
    parser.add_argument("--skip-factors", action="store_true")
    parser.add_argument(
        "--run-maintenance",
        action="store_true",
        help="Create indexes and run ANALYZE after import. Disabled by default because it can require substantial memory on the full tick table.",
    )
    args = parser.parse_args()

    files = read_file_list(args.file_list)
    batch_id = f"a50_2026_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}"
    with connect(args.db) as con:
        codes = [code_from_path(p) for p in files]
        print(f"batch={batch_id} files={len(files)} codes={len(codes)}", flush=True)
        import_constituents(con, codes, args.snapshot_year, batch_id)
        if not args.skip_factors:
            import_factors_from_baostock(con, codes, args.end, batch_id)
        import_daily_from_a50_files(con, files, args.daily_start, args.end, batch_id)
        import_ticks(con, files, args.tick_start, args.end, batch_id)
        if args.run_maintenance:
            for sql in [
                "create index if not exists idx_raw_tick_code_ts on raw_tick_v3(code, ts)",
                "create index if not exists idx_raw_tick_code_date on raw_tick_v3(code, trade_date)",
                "create index if not exists idx_raw_daily_code_date on raw_daily_v1(code, date)",
                "create index if not exists idx_factor_code_date on baostock_adjust_factor_events(code, factor_date)",
                "create index if not exists idx_constituents_year_code on choice_sse50_constituents_2022_2025(snapshot_year, code)",
            ]:
                con.execute(sql)
            con.execute("analyze")
        else:
            print("maintenance skipped; pass --run-maintenance to create indexes and analyze", flush=True)
        summary = con.execute(
            """
            select 'raw_tick_v3' as table_name, count(*) as row_count, count(distinct code) as code_count, min(ts)::timestamp as min_time, max(ts)::timestamp as max_time
            from raw_tick_v3
            where code in (select distinct code from choice_sse50_constituents_2022_2025 where snapshot_year = ?)
              and trade_date between cast(? as date) and cast(? as date)
            union all
            select 'raw_daily_v1', count(*), count(distinct code), min(date)::timestamp, max(date)::timestamp
            from raw_daily_v1
            where code in (select distinct code from choice_sse50_constituents_2022_2025 where snapshot_year = ?)
              and date between cast(? as date) and cast(? as date)
            """,
            [args.snapshot_year, args.tick_start, args.end, args.snapshot_year, args.daily_start, args.end],
        ).fetchdf()
        print(summary.to_string(index=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
