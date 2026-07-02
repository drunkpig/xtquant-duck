from __future__ import annotations

import datetime as dt
import os
from pathlib import Path
from typing import Any, Iterable

import duckdb
import numpy as np
import pandas as pd


DEFAULT_DB_PATH = Path(r"C:\data-tick\duckdb\qmt_mock.duckdb")


def normalize_qmt_code(code: str) -> str:
    text = str(code).strip()
    if not text:
        return text
    if "." in text:
        six, market = text.split(".", 1)
        return f"{six.zfill(6)}.{market.upper()}"
    lower = text.lower().replace("_", "")
    if lower.startswith("sh") or lower.startswith("sz"):
        market = lower[:2].upper()
        six = lower[2:].zfill(6)
        return f"{six}.{market}"
    six = lower[-6:].zfill(6)
    market = "SH" if six.startswith(("5", "6", "9")) else "SZ"
    return f"{six}.{market}"


def qmt_to_local_code(code: str) -> str:
    qmt = normalize_qmt_code(code)
    six, market = qmt.split(".", 1)
    return f"{market.lower()}{six}"


def parse_qmt_time(value: Any) -> pd.Timestamp:
    if isinstance(value, pd.Timestamp):
        return value.tz_localize(None) if value.tzinfo is not None else value
    if isinstance(value, dt.datetime):
        return pd.Timestamp(value).tz_localize(None) if value.tzinfo is not None else pd.Timestamp(value)
    if isinstance(value, dt.date):
        return pd.Timestamp(value)
    try:
        n = int(value)
        s = str(n)
        if len(s) == 14:
            return pd.to_datetime(s, format="%Y%m%d%H%M%S")
        if len(s) == 8:
            return pd.to_datetime(s, format="%Y%m%d")
        if n > 10_000_000_000:
            return pd.to_datetime(n, unit="ms", utc=True).tz_convert("Asia/Shanghai").tz_localize(None)
        if n > 1_000_000_000:
            return pd.to_datetime(n, unit="s", utc=True).tz_convert("Asia/Shanghai").tz_localize(None)
        return pd.to_datetime(s)
    except Exception:
        return pd.to_datetime(value)


def format_qmt_time(ts: pd.Timestamp | dt.datetime | dt.date) -> int:
    t = pd.Timestamp(ts)
    if t.hour == 0 and t.minute == 0 and t.second == 0 and t.microsecond == 0:
        return int(t.strftime("%Y%m%d"))
    return int(t.strftime("%Y%m%d%H%M%S"))


def qmt_millis(ts: pd.Timestamp | dt.datetime) -> int:
    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        t = t.tz_localize("Asia/Shanghai")
    else:
        t = t.tz_convert("Asia/Shanghai")
    return int(t.tz_convert("UTC").timestamp() * 1000)


def _quote_list(values: Iterable[str]) -> str:
    vals = [str(v).replace("'", "''") for v in values]
    if not vals:
        return "('')"
    return "(" + ",".join(f"'{v}'" for v in vals) + ")"


def _empty_bar() -> pd.DataFrame:
    return pd.DataFrame(columns=["time", "ts", "open", "high", "low", "close", "volume", "amount", "preClose"])


def _empty_daily() -> pd.DataFrame:
    return pd.DataFrame(columns=["time", "ts", "open", "high", "low", "close", "volume", "amount", "preClose"])


def _empty_adjusted_bar() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "time",
            "ts",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "preClose",
            "raw_open",
            "raw_high",
            "raw_low",
            "raw_close",
            "raw_preClose",
            "foreAdjustFactor",
            "backAdjustFactor",
            "anchor_factor",
        ]
    )


def _empty_tick() -> pd.DataFrame:
    return pd.DataFrame(columns=["time", "ts", "lastPrice", "volume", "amount", "open", "askPrice", "bidPrice", "askVol", "bidVol"])


class DuckDbMarketData:
    def __init__(self, db_path: str | Path | None = None):
        self.db_path = Path(db_path or os.environ.get("QMT_MOCK_DUCKDB", DEFAULT_DB_PATH))
        self.con = duckdb.connect(str(self.db_path), read_only=True)

    def close(self) -> None:
        self.con.close()

    def __enter__(self) -> "DuckDbMarketData":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def has_history_data(self, code: str, period: str, start: pd.Timestamp, end: pd.Timestamp) -> bool:
        code = normalize_qmt_code(code)
        period_norm = str(period).lower()
        if period_norm in {"tick", "1m", "1min", "60m", "60min", "1h"}:
            source_end = end
            if end.hour == 15 and end.minute == 0 and end.second == 0:
                source_end = end + pd.Timedelta(seconds=59)
            count = self.con.execute(
                """
                select count(*)
                from raw_tick_v3
                where code = ?
                  and ts >= ?
                  and ts <= ?
                  and last_price > 0
                limit 1
                """,
                [code, start.to_pydatetime(), source_end.to_pydatetime()],
            ).fetchone()[0]
            return bool(count)
        if period_norm in {"1d", "day", "d"}:
            count = self.con.execute(
                """
                select count(*)
                from raw_daily_v1
                where code = ?
                  and date >= date_trunc('day', ?)::date
                  and date <= date_trunc('day', ?)::date
                limit 1
                """,
                [code, start.to_pydatetime(), end.to_pydatetime()],
            ).fetchone()[0]
            return bool(count)
        raise NotImplementedError(f"mock period is not implemented: {period!r}")

    def get_market_data_ex(
        self,
        field_list: list[str] | None,
        stock_list: list[str],
        period: str,
        start_time: str = "",
        end_time: str = "",
        count: int = -1,
        fill_data: bool = False,
        dividend_type: str | None = None,
        **kwargs: Any,
    ) -> dict[str, pd.DataFrame]:
        if kwargs:
            raise NotImplementedError(f"mock get_market_data_ex keyword arguments are not implemented: {sorted(kwargs)}")
        if fill_data:
            raise NotImplementedError("mock get_market_data_ex fill_data=True is not implemented")
        codes = [normalize_qmt_code(c) for c in stock_list]
        period_norm = str(period).lower()
        start = parse_qmt_time(start_time) if start_time else pd.Timestamp("1900-01-01")
        end = parse_qmt_time(end_time) if end_time else pd.Timestamp("2100-01-01")
        out: dict[str, pd.DataFrame] = {}
        for code in codes:
            if period_norm == "tick":
                if dividend_type not in {None, "", "none"}:
                    raise NotImplementedError("mock tick dividend_type is not implemented")
                df = self.tick(code, start, end)
            elif period_norm in {"1m", "1min"}:
                df = self.minute(code, start, end, dividend_type=dividend_type)
            elif period_norm in {"60m", "60min", "1h"}:
                df = self.sixty_minute(code, start, end, dividend_type=dividend_type)
            elif period_norm in {"1d", "day", "d"}:
                df = self.daily(code, start, end, dividend_type=dividend_type)
            else:
                raise NotImplementedError(f"mock period is not implemented: {period!r}")
            if field_list:
                missing = [c for c in field_list if c not in df.columns]
                if missing:
                    raise NotImplementedError(f"mock fields are not implemented for period {period!r}: {missing}")
                keep = [c for c in field_list if c in df.columns]
                if "time" in df.columns and "time" not in keep:
                    keep = ["time"] + keep
                df = df[keep].copy() if keep else pd.DataFrame(index=df.index)
            if count and count > 0 and len(df) > count:
                df = df.tail(count).reset_index(drop=True)
            out[code] = df
        return out

    def get_full_tick(self, codes: list[str], now: pd.Timestamp | None = None) -> dict[str, dict[str, Any]]:
        clock = pd.Timestamp(now or os.environ.get("QMT_MOCK_NOW") or pd.Timestamp.now())
        result: dict[str, dict[str, Any]] = {}
        for code in [normalize_qmt_code(c) for c in codes]:
            row = self.con.execute(
                """
                with d as (
                    select date_trunc('day', ?)::timestamp as day_start
                ),
                day_open as (
                    select last_price as open_price
                    from raw_tick_v3, d
                    where code = ?
                      and ts >= day_start
                      and ts <= ?
                      and last_price > 0
                    order by ts, source_row
                    limit 1
                ),
                latest as (
                    select *
                    from raw_tick_v3, d
                    where code = ?
                      and ts >= day_start
                      and ts <= ?
                      and last_price > 0
                    order by ts desc, source_row desc
                    limit 1
                ),
                cum as (
                    select
                        sum(volume_lots) as cum_volume,
                        sum(amount_delta) as cum_amount
                    from raw_tick_v3, d
                    where code = ?
                      and ts >= day_start
                      and ts <= ?
                )
                select latest.*, day_open.open_price, cum.cum_volume, cum.cum_amount
                from latest, day_open, cum
                """,
                [clock.to_pydatetime(), code, clock.to_pydatetime(), code, clock.to_pydatetime(), code, clock.to_pydatetime()],
            ).fetchdf()
            if row.empty:
                continue
            r = row.iloc[0]
            result[code] = self._tick_record_to_qmt(r)
        return result

    def tick(self, code: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        code = normalize_qmt_code(code)
        df = self.con.execute(
            """
            with ticks as (
                select *
                from raw_tick_v3
                where code = ?
                  and ts >= ?
                  and ts <= ?
                  and last_price > 0
                order by ts, source_row
            ),
            enriched as (
                select
                    *,
                    first_value(last_price) over (
                        partition by code, trade_date
                        order by ts, source_row
                        rows between unbounded preceding and unbounded following
                    ) as open_price,
                    sum(volume_lots) over (
                        partition by code, trade_date
                        order by ts, source_row
                        rows between unbounded preceding and current row
                    ) as cum_volume,
                    sum(amount_delta) over (
                        partition by code, trade_date
                        order by ts, source_row
                        rows between unbounded preceding and current row
                    ) as cum_amount
                from ticks
            )
            select * from enriched
            """,
            [code, start.to_pydatetime(), end.to_pydatetime()],
        ).fetchdf()
        if df.empty:
            return _empty_tick()
        rows = [self._tick_record_to_qmt(r) for _, r in df.iterrows()]
        return pd.DataFrame(rows)

    def minute(self, code: str, start: pd.Timestamp, end: pd.Timestamp, dividend_type: str | None = None) -> pd.DataFrame:
        code = normalize_qmt_code(code)
        raw = self._bar_from_ticks(code, start, end, period="1m")
        return self._apply_intraday_dividend(code, raw, dividend_type)

    def sixty_minute(self, code: str, start: pd.Timestamp, end: pd.Timestamp, dividend_type: str | None = None) -> pd.DataFrame:
        code = normalize_qmt_code(code)
        raw = self._bar_from_ticks(code, start, end, period="60m")
        return self._apply_intraday_dividend(code, raw, dividend_type)

    def daily(self, code: str, start: pd.Timestamp, end: pd.Timestamp, dividend_type: str | None = None) -> pd.DataFrame:
        code = normalize_qmt_code(code)
        raw = self.con.execute(
            """
            with source as (
                select
                    date as ts,
                    cast(strftime(date, '%Y%m%d') as bigint) as time,
                    open,
                    high,
                    low,
                    close,
                    volume_lots as volume,
                    amount,
                    lag(close) over (order by date) as preClose
                from raw_daily_v1
                where code = ?
                  and date <= date_trunc('day', ?)::date
                order by date
            )
            select *
            from source
            where ts >= date_trunc('day', ?)::date
            order by ts
            """,
            [code, end.to_pydatetime(), start.to_pydatetime()],
        ).fetchdf()
        raw["time"] = raw["ts"].map(qmt_millis)
        if dividend_type in {None, "", "none"}:
            return raw if not raw.empty else _empty_daily()
        if dividend_type in {"back_ratio", "back"}:
            return self.back_adjusted_daily(code, start, end)
        if dividend_type not in {"front_ratio", "front"}:
            raise ValueError(f"Unsupported mock daily dividend_type: {dividend_type!r}")
        anchor_date = raw["ts"].max()
        return self.dynamic_qfq_daily(code, start, end, pd.Timestamp(anchor_date))

    def back_adjusted_daily(self, code: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        code = normalize_qmt_code(code)
        df = self.con.execute(
            """
            with daily as (
                select *
                from raw_daily_v1
                where code = ?
                  and date >= date_trunc('day', ?)::date
                  and date <= date_trunc('day', ?)::date
                order by date
            ),
            adjusted as (
                select
                    d.date as ts,
                    cast(strftime(d.date, '%Y%m%d') as bigint) as time,
                    d.open * coalesce(f.backAdjustFactor, 1.0) as open,
                    d.high * coalesce(f.backAdjustFactor, 1.0) as high,
                    d.low * coalesce(f.backAdjustFactor, 1.0) as low,
                    d.close * coalesce(f.backAdjustFactor, 1.0) as close,
                    d.volume_lots as volume,
                    d.amount,
                    d.open as raw_open,
                    d.high as raw_high,
                    d.low as raw_low,
                    d.close as raw_close,
                    f.backAdjustFactor
                from daily d
                asof left join baostock_adjust_factor_events f
                  on d.code = f.code and d.date >= f.factor_date
            )
            select
                *,
                lag(close) over (order by ts) as preClose,
                lag(raw_close) over (order by ts) as raw_preClose
            from adjusted
            order by ts
            """,
            [code, start.to_pydatetime(), end.to_pydatetime()],
        ).fetchdf()
        if not df.empty:
            df["time"] = df["ts"].map(qmt_millis)
        return df if not df.empty else _empty_adjusted_bar()

    def dynamic_qfq_daily(self, code: str, start: pd.Timestamp, end: pd.Timestamp, anchor_date: pd.Timestamp) -> pd.DataFrame:
        code = normalize_qmt_code(code)
        df = self.con.execute(
            """
            with daily as (
                select *
                from raw_daily_v1
                where code = ?
                  and date >= date_trunc('day', ?)::date
                  and date <= date_trunc('day', ?)::date
                order by date
            ),
            d_fac as (
                select d.*, f.foreAdjustFactor
                from daily d
                asof left join baostock_adjust_factor_events f
                  on d.code = f.code and d.date >= f.factor_date
            ),
            anchor as (
                select f.foreAdjustFactor as anchor_factor
                from baostock_adjust_factor_events f
                where f.code = ?
                  and f.factor_date <= date_trunc('day', ?)::date
                order by f.factor_date desc
                limit 1
            ),
            adjusted as (
                select
                    d.date as ts,
                    cast(strftime(d.date, '%Y%m%d') as bigint) as time,
                    d.open * coalesce(d.foreAdjustFactor, 1.0) / coalesce(anchor.anchor_factor, 1.0) as open,
                    d.high * coalesce(d.foreAdjustFactor, 1.0) / coalesce(anchor.anchor_factor, 1.0) as high,
                    d.low * coalesce(d.foreAdjustFactor, 1.0) / coalesce(anchor.anchor_factor, 1.0) as low,
                    d.close * coalesce(d.foreAdjustFactor, 1.0) / coalesce(anchor.anchor_factor, 1.0) as close,
                    d.volume_lots as volume,
                    d.amount,
                    d.open as raw_open,
                    d.high as raw_high,
                    d.low as raw_low,
                    d.close as raw_close,
                    d.foreAdjustFactor,
                    anchor.anchor_factor
                from d_fac d, anchor
            )
            select
                *,
                lag(close) over (order by ts) as preClose,
                lag(raw_close) over (order by ts) as raw_preClose
            from adjusted
            order by ts
            """,
            [code, start.to_pydatetime(), end.to_pydatetime(), code, anchor_date.to_pydatetime()],
        ).fetchdf()
        if not df.empty:
            df["time"] = df["ts"].map(qmt_millis)
        return df if not df.empty else _empty_adjusted_bar()

    def sse50_members(self, snapshot_year: int) -> pd.DataFrame:
        return self.con.execute(
            """
            select *
            from choice_sse50_constituents_2022_2025
            where snapshot_year = ?
            order by code
            """,
            [int(snapshot_year)],
        ).fetchdf()

    def _bar_from_ticks(self, code: str, start: pd.Timestamp, end: pd.Timestamp, period: str) -> pd.DataFrame:
        code = normalize_qmt_code(code)
        source_end = end
        if end.hour == 15 and end.minute == 0 and end.second == 0:
            source_end = end + pd.Timedelta(seconds=59)
        if period == "1m":
            label_expr = """
                case
                    when date_part('hour', ts) = 9 and date_part('minute', ts) = 25 then date_trunc('day', ts) + interval 9 hour + interval 30 minute
                    when date_part('hour', ts) = 15 and date_part('minute', ts) = 0 then date_trunc('day', ts) + interval 15 hour
                    when (date_part('hour', ts) = 9 and date_part('minute', ts) >= 30) or date_part('hour', ts) = 10 or (date_part('hour', ts) = 11 and date_part('minute', ts) < 30)
                        then date_trunc('minute', ts) + interval 1 minute
                    when date_part('hour', ts) = 13 or (date_part('hour', ts) = 14 and date_part('minute', ts) < 58)
                        then date_trunc('minute', ts) + interval 1 minute
                    else null
                end
            """
        elif period == "60m":
            label_expr = """
                case
                    when ts::time >= time '09:30:00' and ts::time < time '10:30:00'
                        then date_trunc('day', ts) + interval 10 hour + interval 30 minute
                    when ts::time >= time '10:30:00' and ts::time < time '11:30:00'
                        then date_trunc('day', ts) + interval 11 hour + interval 30 minute
                    when ts::time >= time '13:00:00' and ts::time < time '14:00:00'
                        then date_trunc('day', ts) + interval 14 hour
                    when ts::time >= time '14:00:00' and ts::time <= time '15:00:59'
                        then date_trunc('day', ts) + interval 15 hour
                    else null
                end
            """
        else:
            raise ValueError(period)
        df = self.con.execute(
            f"""
            with labeled as (
                select
                    {label_expr} as bar_ts,
                    ts,
                    source_row,
                    last_price,
                    volume_lots,
                    amount_delta
                from raw_tick_v3
                where code = ?
                  and ts >= ?
                  and ts <= ?
                  and last_price > 0
            ),
            filtered as (
                select *
                from labeled
                where bar_ts is not null
                  and bar_ts >= ?
                  and bar_ts <= ?
            )
            select
                bar_ts as ts,
                cast(strftime(bar_ts, '%Y%m%d%H%M%S') as bigint) as time,
                first(last_price order by ts, source_row) as open,
                max(last_price) as high,
                min(last_price) as low,
                last(last_price order by ts, source_row) as close,
                sum(volume_lots) as volume,
                sum(amount_delta) as amount
            from filtered
            group by bar_ts
            having sum(volume_lots) > 0
            order by bar_ts
            """,
            [code, start.to_pydatetime(), source_end.to_pydatetime(), start.to_pydatetime(), end.to_pydatetime()],
        ).fetchdf()
        if df.empty:
            return _empty_bar()
        df["time"] = df["ts"].map(qmt_millis)
        df["preClose"] = np.nan
        return df

    def _apply_intraday_dividend(self, code: str, df: pd.DataFrame, dividend_type: str | None) -> pd.DataFrame:
        if dividend_type in {None, "", "none"}:
            return df
        if dividend_type in {"back_ratio", "back"}:
            raise NotImplementedError("mock intraday back_ratio is not implemented; use raw or front_ratio")
        if dividend_type not in {"front_ratio", "front"}:
            raise ValueError(f"Unsupported mock intraday dividend_type: {dividend_type!r}")
        if df.empty:
            return _empty_adjusted_bar()
        out = df.copy()
        out["bar_date"] = pd.to_datetime(out["ts"]).dt.normalize().astype("datetime64[ns]")
        anchor_date = out["bar_date"].max()
        factors = self.con.execute(
            """
            select factor_date, foreAdjustFactor
            from baostock_adjust_factor_events
            where code = ?
              and factor_date <= ?
            order by factor_date
            """,
            [normalize_qmt_code(code), anchor_date.to_pydatetime()],
        ).fetchdf()
        if factors.empty:
            out["foreAdjustFactor"] = 1.0
            out["anchor_factor"] = 1.0
        else:
            factors["factor_date"] = pd.to_datetime(factors["factor_date"]).dt.normalize().astype("datetime64[ns]")
            factor_by_date = pd.merge_asof(
                pd.DataFrame({"bar_date": pd.Series(sorted(out["bar_date"].dropna().unique()), dtype="datetime64[ns]")}),
                factors,
                left_on="bar_date",
                right_on="factor_date",
                direction="backward",
            )[["bar_date", "foreAdjustFactor"]]
            factor_by_date["foreAdjustFactor"] = factor_by_date["foreAdjustFactor"].fillna(1.0)
            anchor_factor = factors.loc[factors["factor_date"].le(anchor_date), "foreAdjustFactor"].iloc[-1]
            out = out.merge(factor_by_date, on="bar_date", how="left")
            out["foreAdjustFactor"] = out["foreAdjustFactor"].fillna(1.0)
            out["anchor_factor"] = float(anchor_factor)
        ratio = out["foreAdjustFactor"].astype(float) / out["anchor_factor"].astype(float)
        for col in ["open", "high", "low", "close", "preClose"]:
            if col in out.columns:
                out[f"raw_{col}"] = out[col]
                out[col] = out[col].astype(float) * ratio
        return out.drop(columns=["bar_date"])

    @staticmethod
    def _array5(row: pd.Series, prefix: str) -> list[float]:
        return [float(row.get(f"{prefix}{i}", np.nan)) for i in range(1, 6)]

    def _tick_record_to_qmt(self, r: pd.Series) -> dict[str, Any]:
        ts = pd.Timestamp(r["ts"])
        return {
            "time": qmt_millis(ts),
            "ts": ts,
            "lastPrice": float(r["last_price"]),
            "volume": float(r.get("cum_volume", r.get("volume_lots", np.nan))),
            "amount": float(r.get("cum_amount", r.get("amount_delta", np.nan))),
            "open": float(r.get("open_price", r["last_price"])),
            "askPrice": self._array5(r, "ask_price"),
            "bidPrice": self._array5(r, "bid_price"),
            "askVol": self._array5(r, "ask_vol"),
            "bidVol": self._array5(r, "bid_vol"),
        }
