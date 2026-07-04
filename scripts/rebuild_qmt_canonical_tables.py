from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd

import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qmt_data_layer.duckdb_data import DEFAULT_DB_PATH


def connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(db_path))
    con.execute("pragma threads=2")
    con.execute("pragma preserve_insertion_order=false")
    con.execute("pragma memory_limit='8GB'")
    return con


def create_tables(con: duckdb.DuckDBPyConnection, rebuild: bool) -> None:
    if rebuild:
        con.execute("drop table if exists qmt_tick_v1")
        con.execute("drop table if exists qmt_daily_v1")
    con.execute(
        """
        create table if not exists qmt_tick_v1 (
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
        create table if not exists qmt_daily_v1 (
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


def source_filter(start: str, end: str, source_like: str) -> str:
    return f"""
        trade_date between date '{start}' and date '{end}'
        and source_file like '{source_like.replace("'", "''")}'
        and ts::time <= time '15:00:59'
    """


def insert_ticks_from_old(con: duckdb.DuckDBPyConnection, start: str, end: str, source_like: str, source_name: str) -> None:
    print(f"building qmt_tick_v1 from {source_name}: {start}..{end}", flush=True)
    where_sql = source_filter(start, end, source_like)
    con.execute(
        f"""
        insert into qmt_tick_v1
        with source as (
            select
                *,
                case
                    when amount_delta > 0
                         and last_price > 0
                         and volume_lots > 0
                         and amount_delta / nullif(volume_lots * last_price, 0) between 0.2 and 10
                        then volume_lots / 100.0
                    else volume_lots
                end as delta_volume_lots,
                amount_delta as delta_amount
            from raw_tick_v3
            where {where_sql}
              and last_price > 0
        ),
        cumulative as (
            select
                ts,
                trade_date,
                code,
                local_code,
                exchange,
                last_price,
                sum(greatest(delta_volume_lots, 0)) over (
                    partition by code, trade_date
                    order by ts, source_row
                    rows between unbounded preceding and current row
                ) as cum_volume,
                sum(greatest(delta_amount, 0)) over (
                    partition by code, trade_date
                    order by ts, source_row
                    rows between unbounded preceding and current row
                ) as cum_amount,
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
                source_file,
                source_row
            from source
        )
        select
            ts,
            trade_date,
            code,
            local_code,
            exchange,
            last_price as lastPrice,
            cum_volume as volume,
            cum_amount as amount,
            bid_price1 as bidPrice1,
            bid_price2 as bidPrice2,
            bid_price3 as bidPrice3,
            bid_price4 as bidPrice4,
            bid_price5 as bidPrice5,
            ask_price1 as askPrice1,
            ask_price2 as askPrice2,
            ask_price3 as askPrice3,
            ask_price4 as askPrice4,
            ask_price5 as askPrice5,
            bid_vol1 as bidVol1,
            bid_vol2 as bidVol2,
            bid_vol3 as bidVol3,
            bid_vol4 as bidVol4,
            bid_vol5 as bidVol5,
            ask_vol1 as askVol1,
            ask_vol2 as askVol2,
            ask_vol3 as askVol3,
            ask_vol4 as askVol4,
            ask_vol5 as askVol5,
            source_file,
            source_row
        from cumulative
        """
    )


def insert_ticks_from_raw_sources(con: duckdb.DuckDBPyConnection, start: str, end: str, source_name: str) -> None:
    print(f"building qmt_tick_v1 from raw_tick_v3 all sources: {start}..{end}", flush=True)
    con.execute(
        """
        insert into qmt_tick_v1
        with source as (
            select
                *,
                case
                    when amount_delta > 0
                         and last_price > 0
                         and volume_lots > 0
                         and amount_delta / nullif(volume_lots * last_price, 0) between 0.2 and 10
                        then volume_lots / 100.0
                    else volume_lots
                end as delta_volume_lots,
                amount_delta as delta_amount
            from raw_tick_v3
            where trade_date between cast(? as date) and cast(? as date)
              and ts::time <= time '15:00:59'
              and last_price > 0
        ),
        dedup_source as (
            select *
            from source
            qualify row_number() over (
                partition by code, trade_date, ts, source_row
                order by case when source_file like 'D:\\ticks\\data-tick\\%' then 0 else 1 end, source_file
            ) = 1
        ),
        cumulative as (
            select
                ts,
                trade_date,
                code,
                local_code,
                exchange,
                last_price,
                sum(greatest(delta_volume_lots, 0)) over (
                    partition by code, trade_date
                    order by ts, source_row
                    rows between unbounded preceding and current row
                ) as cum_volume,
                sum(greatest(delta_amount, 0)) over (
                    partition by code, trade_date
                    order by ts, source_row
                    rows between unbounded preceding and current row
                ) as cum_amount,
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
                source_file,
                source_row
            from dedup_source
        )
        select
            ts,
            trade_date,
            code,
            local_code,
            exchange,
            last_price as lastPrice,
            cum_volume as volume,
            cum_amount as amount,
            bid_price1 as bidPrice1,
            bid_price2 as bidPrice2,
            bid_price3 as bidPrice3,
            bid_price4 as bidPrice4,
            bid_price5 as bidPrice5,
            ask_price1 as askPrice1,
            ask_price2 as askPrice2,
            ask_price3 as askPrice3,
            ask_price4 as askPrice4,
            ask_price5 as askPrice5,
            bid_vol1 as bidVol1,
            bid_vol2 as bidVol2,
            bid_vol3 as bidVol3,
            bid_vol4 as bidVol4,
            bid_vol5 as bidVol5,
            ask_vol1 as askVol1,
            ask_vol2 as askVol2,
            ask_vol3 as askVol3,
            ask_vol4 as askVol4,
            ask_vol5 as askVol5,
            source_file,
            source_row
        from cumulative
        """,
        [start, end],
    )


def insert_daily_from_qmt_tick(con: duckdb.DuckDBPyConnection, start: str, end: str, source_like: str, source_name: str) -> None:
    print(f"building qmt_daily_v1 from qmt_tick_v1 {source_name}: {start}..{end}", flush=True)
    con.execute(
        """
        insert into qmt_daily_v1
        select
            trade_date as date,
            code,
            any_value(local_code) as local_code,
            first(lastPrice order by ts, source_row) as open,
            max(lastPrice) as high,
            min(lastPrice) as low,
            last(lastPrice order by ts, source_row) as close,
            max(volume) as volume,
            max(amount) as amount,
            ? as source_file
        from qmt_tick_v1
        where trade_date between cast(? as date) and cast(? as date)
          and source_file like ?
          and lastPrice > 0
        group by trade_date, code
        having max(volume) > 0
        """,
        [f"derived_from_qmt_tick_v1:{source_name}", start, end, source_like],
    )


def insert_daily_from_qmt_tick_all_sources(con: duckdb.DuckDBPyConnection, start: str, end: str, source_name: str) -> None:
    print(f"building qmt_daily_v1 from qmt_tick_v1 all sources {source_name}: {start}..{end}", flush=True)
    con.execute(
        """
        insert into qmt_daily_v1
        select
            trade_date as date,
            code,
            any_value(local_code) as local_code,
            first(lastPrice order by ts, source_row) as open,
            max(lastPrice) as high,
            min(lastPrice) as low,
            last(lastPrice order by ts, source_row) as close,
            max(volume) as volume,
            max(amount) as amount,
            ? as source_file
        from qmt_tick_v1
        where trade_date between cast(? as date) and cast(? as date)
          and lastPrice > 0
        group by trade_date, code
        having max(volume) > 0
        """,
        [f"derived_from_qmt_tick_v1:{source_name}", start, end],
    )


def create_indexes(con: duckdb.DuckDBPyConnection) -> None:
    for sql in [
        "create index if not exists idx_qmt_tick_code_ts on qmt_tick_v1(code, ts)",
        "create index if not exists idx_qmt_tick_code_date on qmt_tick_v1(code, trade_date)",
        "create index if not exists idx_qmt_daily_code_date on qmt_daily_v1(code, date)",
    ]:
        try:
            con.execute(sql)
        except Exception as exc:
            print(f"index skipped: {exc}", flush=True)


def summarize(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return con.execute(
        """
        select 'qmt_tick_v1' as table_name, count(*) as row_count, count(distinct code) as code_count,
               min(ts)::timestamp as min_time, max(ts)::timestamp as max_time
        from qmt_tick_v1
        union all
        select 'qmt_daily_v1', count(*), count(distinct code), min(date)::timestamp, max(date)::timestamp
        from qmt_daily_v1
        """
    ).fetchdf()


def validate_samples(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return con.execute(
        """
        with tick_day as (
            select
                code,
                trade_date,
                max(volume) as tick_volume,
                max(amount) as tick_amount
            from qmt_tick_v1
            where code in ('600028.SH', '688041.SH', '688012.SH', '688981.SH')
            group by code, trade_date
        )
        select
            t.code,
            t.trade_date,
            t.tick_volume,
            d.volume as daily_volume,
            t.tick_amount,
            d.amount as daily_amount,
            t.tick_volume / nullif(d.volume, 0) as volume_ratio,
            t.tick_amount / nullif(d.amount, 0) as amount_ratio
        from tick_day t
        join qmt_daily_v1 d
          on t.code = d.code and t.trade_date = d.date
        where t.trade_date in (date '2025-08-22', date '2026-03-02')
        order by t.code, t.trade_date
        """
    ).fetchdf()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--rebuild", action="store_true")
    parser.add_argument("--skip-indexes", action="store_true")
    parser.add_argument("--old-start", default="2022-01-01")
    parser.add_argument("--old-end", default="2025-12-31")
    parser.add_argument("--a50-start", default="2026-01-01")
    parser.add_argument("--a50-end", default="2026-12-31")
    parser.add_argument(
        "--use-raw-sources",
        action="store_true",
        help="Build canonical tables from every current raw_tick_v3 source in the requested date range.",
    )
    args = parser.parse_args()

    with connect(args.db) as con:
        create_tables(con, args.rebuild)
        if args.rebuild:
            if args.use_raw_sources:
                insert_ticks_from_raw_sources(con, args.old_start, args.old_end, "raw_tick_v3_all_sources_old")
                insert_ticks_from_raw_sources(con, args.a50_start, args.a50_end, "raw_tick_v3_all_sources_a50")
                insert_daily_from_qmt_tick_all_sources(con, args.old_start, args.old_end, "raw_tick_v3_all_sources_old")
                insert_daily_from_qmt_tick_all_sources(con, args.a50_start, args.a50_end, "raw_tick_v3_all_sources_a50")
            else:
                insert_ticks_from_old(con, args.old_start, args.old_end, r"D:\ticks\data-tick\%", "sh_sz_v3")
                insert_ticks_from_old(con, args.a50_start, args.a50_end, r"D:\百度盘分钟K10年\A50-2026\%", "a50_2026")
                insert_daily_from_qmt_tick(con, args.old_start, args.old_end, r"D:\ticks\data-tick\%", "sh_sz_v3")
                insert_daily_from_qmt_tick(con, args.a50_start, args.a50_end, r"D:\百度盘分钟K10年\A50-2026\%", "a50_2026")
        if not args.skip_indexes:
            create_indexes(con)
        summary = summarize(con)
        print(summary.to_string(index=False), flush=True)
        validation = validate_samples(con)
        print(validation.to_string(index=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
