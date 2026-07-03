from __future__ import annotations

import os
from typing import Any, Callable

import pandas as pd

from qmt_data_layer.duckdb_data import DuckDbMarketData, parse_qmt_time

_market_data: DuckDbMarketData | None = None
_mock_now: pd.Timestamp | None = None


def _data() -> DuckDbMarketData:
    global _market_data
    if _market_data is None:
        _market_data = DuckDbMarketData(os.environ.get("QMT_MOCK_DUCKDB"))
    return _market_data


def close() -> None:
    global _market_data
    if _market_data is not None:
        _market_data.close()
        _market_data = None


def set_mock_now(value: Any) -> None:
    global _mock_now
    _mock_now = parse_qmt_time(value)


def get_mock_now() -> pd.Timestamp | None:
    return _mock_now


def download_history_data(
    stock_code: str,
    period: str,
    start_time: str = "",
    end_time: str = "",
    incrementally: bool = False,
    **kwargs: Any,
) -> bool:
    if incrementally:
        raise NotImplementedError("mock download_history_data incrementally=True is not implemented")
    if kwargs:
        raise NotImplementedError(f"mock download_history_data keyword arguments are not implemented: {sorted(kwargs)}")
    start = parse_qmt_time(start_time) if start_time else pd.Timestamp("1900-01-01")
    end = parse_qmt_time(end_time) if end_time else pd.Timestamp("2100-01-01")
    if not _data().has_history_data(stock_code, period, start, end):
        raise FileNotFoundError(f"mock DuckDB has no local data for {stock_code} period={period!r} start={start_time!r} end={end_time!r}")
    return True


def download_history_data2(
    stock_list: list[str],
    period: str,
    start_time: str = "",
    end_time: str = "",
    incrementally: bool = False,
    **kwargs: Any,
) -> bool:
    if incrementally:
        raise NotImplementedError("mock download_history_data2 incrementally=True is not implemented")
    if kwargs:
        raise NotImplementedError(f"mock download_history_data2 keyword arguments are not implemented: {sorted(kwargs)}")
    for stock_code in stock_list:
        download_history_data(stock_code, period, start_time, end_time, incrementally=False)
    return True


def get_market_data_ex(
    field_list: list[str] | None = None,
    stock_list: list[str] | None = None,
    period: str = "1d",
    start_time: str = "",
    end_time: str = "",
    count: int = -1,
    fill_data: bool = False,
    dividend_type: str | None = None,
    **kwargs: Any,
) -> dict[str, pd.DataFrame]:
    return _data().get_market_data_ex(
        field_list=field_list or [],
        stock_list=stock_list or [],
        period=period,
        start_time=start_time,
        end_time=end_time,
        count=count,
        fill_data=fill_data,
        dividend_type=dividend_type,
        **kwargs,
    )


def get_full_tick(stock_list: list[str]) -> dict[str, dict[str, Any]]:
    return _data().get_full_tick(stock_list, now=_mock_now)


def subscribe_quote(
    stock_code: str,
    period: str = "tick",
    start_time: str = "",
    end_time: str = "",
    count: int = 0,
    callback: Callable[..., Any] | None = None,
    **kwargs: Any,
) -> int:
    del stock_code, period, start_time, end_time, count, callback, kwargs
    raise NotImplementedError("mock subscribe_quote push subscription is not implemented")


def subscribe_whole_quote(code_list: list[str], callback: Callable[..., Any] | None = None) -> int:
    del code_list, callback
    raise NotImplementedError("mock subscribe_whole_quote push subscription is not implemented")


def unsubscribe_quote(seq: int) -> None:
    del seq
    raise NotImplementedError("mock unsubscribe_quote is not implemented because subscriptions are not implemented")


def run() -> None:
    raise NotImplementedError("mock xtdata.run event loop is not implemented")
