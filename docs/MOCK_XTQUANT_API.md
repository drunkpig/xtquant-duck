# xtquant_duck API

This document describes the DuckDB-backed mock package installed as:

`xtquant_duck`

Install `xtquant-duck` and import it as an `xtquant`-compatible module:

```python
import xtquant_duck as xtquant
```

The mock is intentionally strict. Unsupported semantics raise `NotImplementedError`; missing local data raises `FileNotFoundError`. It must not silently return placeholder market data.

## Environment

| variable | default | meaning |
| --- | --- | --- |
| `QMT_MOCK_DUCKDB` | `C:\data-tick\duckdb\qmt_mock.duckdb` | DuckDB database path |
| `QMT_MOCK_NOW` | current local time | default clock for `get_full_tick` if `set_mock_now()` is not used |

## Time And Code Rules

Stock codes are normalized to QMT format, for example:

- `sh600000` -> `600000.SH`
- `600000.SH` -> `600000.SH`
- `000001` -> `000001.SZ`

Time parameters accept the formats handled by `parse_qmt_time()`:

- `YYYYMMDD`
- `YYYYMMDDHHMMSS`
- pandas-compatible datetime strings
- Unix seconds or milliseconds as integers

Returned `time` values use QMT-style local-time milliseconds since Unix epoch.

## `xtquant_duck.xtdata`

### `download_history_data(stock_code, period, start_time="", end_time="", incrementally=False, **kwargs) -> bool`

Validates that local DuckDB already has data for the requested code, period, and time range.

Supported parameters:

| parameter | supported value |
| --- | --- |
| `stock_code` | QMT-style or normalizable stock code |
| `period` | `tick`, `1m`, `1min`, `60m`, `60min`, `1h`, `1d`, `day`, `d` |
| `start_time`, `end_time` | optional QMT-style time strings |
| `incrementally` | only `False` |

Returns `True` when local data exists.

Raises:

- `FileNotFoundError` if DuckDB has no matching local data.
- `NotImplementedError` for unsupported period, `incrementally=True`, or extra keyword arguments.

### `download_history_data2(stock_list, period, start_time="", end_time="", incrementally=False, **kwargs) -> bool`

Calls `download_history_data()` for every stock in `stock_list`.

Returns `True` only when every stock has matching local data.

Raises the same exceptions as `download_history_data()`.

### `get_market_data_ex(field_list=None, stock_list=None, period="1d", start_time="", end_time="", count=-1, fill_data=False, dividend_type=None, **kwargs) -> dict[str, pandas.DataFrame]`

Main historical market-data API.

Supported parameters:

| parameter | supported value |
| --- | --- |
| `field_list` | `None`, empty list, or implemented field names for the requested period |
| `stock_list` | list of QMT-style or normalizable stock codes |
| `period` | `tick`, `1m`, `1min`, `60m`, `60min`, `1h`, `1d`, `day`, `d` |
| `start_time`, `end_time` | optional QMT-style time strings |
| `count` | `-1`/`0` for all rows, positive integer for tail rows |
| `fill_data` | only `False` |
| `dividend_type` | see period-specific rules below |

Return value is a dict keyed by normalized QMT code. Each value is a pandas DataFrame sorted by time.

When `field_list` is provided, only those fields are returned. If the DataFrame has a `time` column and `time` was not requested, `time` is still prepended to match strategy expectations. Requesting an unimplemented field raises `NotImplementedError`.

#### Period `tick`

Source: `qmt_tick_v1`.

Price domain: raw, never adjusted.

Supported `dividend_type`: `None`, `""`, `none`.

Fields:

| field | meaning |
| --- | --- |
| `time` | QMT-style millisecond timestamp |
| `ts` | pandas timestamp |
| `lastPrice` | latest raw tick price |
| `volume` | cumulative trading-day volume in lots up to this tick |
| `amount` | cumulative trading-day amount up to this tick |
| `open` | first valid tick price of the trading day up to this tick |
| `askPrice`, `bidPrice` | 5-level price lists |
| `askVol`, `bidVol` | 5-level volume lists |

`dividend_type` other than empty/`none` raises `NotImplementedError`.

#### Period `1m` / `1min`

Source: dynamic aggregation from `qmt_tick_v1`.

Raw bar rules:

- `09:30` bar is the opening auction bar from `09:25` ticks.
- Continuous auction bars are right-labeled.
- `15:00` closing auction tick is labeled `15:00`.
- `volume` is per-bar volume in lots.
- `amount` is per-bar traded amount.

Supported `dividend_type`:

| value | behavior |
| --- | --- |
| `None`, `""`, `none` | raw 1m bars |
| `front_ratio`, `front` | dynamic front-adjusted 1m bars |

Base fields:

| field | meaning |
| --- | --- |
| `time`, `ts` | bar label time |
| `open`, `high`, `low`, `close` | raw or front-adjusted price depending on `dividend_type` |
| `volume`, `amount` | raw per-bar volume/amount, not adjusted |
| `preClose` | currently `NaN` for raw intraday bars |

Additional fields for `front_ratio`/`front`:

| field | meaning |
| --- | --- |
| `raw_open`, `raw_high`, `raw_low`, `raw_close`, `raw_preClose` | raw price fields before adjustment |
| `foreAdjustFactor` | BaoStock factor as of the bar date |
| `anchor_factor` | BaoStock factor as of the latest bar date in the requested result set |

Formula:

`adjusted_price = raw_price * foreAdjustFactor(bar_date) / foreAdjustFactor(anchor_date)`

`back_ratio`/`back` raises `NotImplementedError`.

#### Period `60m` / `60min` / `1h`

Source: dynamic aggregation from `qmt_tick_v1`, not from 1m bars.

Raw bar boundaries:

- `10:30`: `[09:30, 10:30)`
- `11:30`: `[10:30, 11:30)`
- `14:00`: `[13:00, 14:00)`
- `15:00`: `[14:00, 15:00:59]`

Fields and `dividend_type` behavior are the same as `1m`.

The current strategy requests raw 60m bars.

#### Period `1d` / `day` / `d`

Source: `qmt_daily_v1`.

Supported `dividend_type`:

| value | behavior |
| --- | --- |
| `None`, `""`, `none` | raw daily bars |
| `front_ratio`, `front` | dynamic front-adjusted daily bars |
| `back_ratio`, `back` | BaoStock back-adjusted daily bars |

Raw fields:

| field | meaning |
| --- | --- |
| `time`, `ts` | trading date |
| `open`, `high`, `low`, `close` | raw OHLC |
| `volume` | daily volume in lots |
| `amount` | daily traded amount |
| `preClose` | previous raw close within the returned source sequence |

Additional fields for `front_ratio`/`front`:

| field | meaning |
| --- | --- |
| `raw_open`, `raw_high`, `raw_low`, `raw_close`, `raw_preClose` | raw daily fields before adjustment |
| `foreAdjustFactor` | BaoStock factor as of the trading date |
| `anchor_factor` | BaoStock factor as of the latest trading date in the requested result set |

Formula:

`adjusted_price = raw_price * foreAdjustFactor(date) / foreAdjustFactor(anchor_date)`

Additional fields for `back_ratio`/`back`:

| field | meaning |
| --- | --- |
| `raw_open`, `raw_high`, `raw_low`, `raw_close`, `raw_preClose` | raw daily fields before adjustment |
| `backAdjustFactor` | BaoStock back-adjust factor as of the trading date |

Formula:

`adjusted_price = raw_price * backAdjustFactor(date)`

### `get_full_tick(stock_list) -> dict[str, dict]`

Returns the latest raw tick for each stock at the mock clock.

Clock source:

1. `set_mock_now(value)` if called.
2. `QMT_MOCK_NOW` environment variable.
3. current local time.

Returned dict fields match the tick fields used by the strategy:

| field | meaning |
| --- | --- |
| `time`, `ts` | tick time |
| `lastPrice` | latest raw tick price |
| `volume` | cumulative trading-day volume in lots up to the tick |
| `amount` | cumulative trading-day amount up to the tick |
| `open` | first valid tick price of the trading day up to the tick |
| `askPrice`, `bidPrice`, `askVol`, `bidVol` | 5-level lists |

Stocks with no tick at or before the mock clock for that day are omitted from the returned dict.

### `set_mock_now(value) -> None`

Sets the process-local mock clock used by `get_full_tick()`.

### `get_mock_now() -> pandas.Timestamp | None`

Returns the process-local mock clock, or `None` if unset.

### `close() -> None`

Closes the cached DuckDB connection used by the mock module.

### Unsupported push APIs

These APIs are declared only so imports resolve, but push subscriptions are not implemented:

- `subscribe_quote(...)`
- `subscribe_whole_quote(...)`
- `unsubscribe_quote(...)`
- `run()`

All raise `NotImplementedError`.

## `xtquant_duck.xttrader`

The mock package provides import-compatible stubs:

- `XtQuantTraderCallback`
- `XtQuantTrader`

`XtQuantTrader.register_callback(callback)` stores the callback object only.

The following trading/account methods are not implemented and raise `NotImplementedError`:

- `start()`
- `connect()`
- `subscribe(account)`
- `query_stock_asset(account)`
- `query_stock_positions(account)`
- `query_stock_orders(account, cancelable_only=False)`
- `query_stock_trades(account)`

This prevents mock backtests from accidentally treating placeholder account data as real trading state.

## `xtquant_duck.xttype`

### `StockAccount(account_id: str, account_type: str = "STOCK")`

Simple data holder with attributes:

- `account_id`
- `account_type`

## `xtquant_duck.xtconstant`

Defined constants:

| name | value |
| --- | --- |
| `STOCK_BUY` | `23` |
| `STOCK_SELL` | `24` |
| `FIX_PRICE` | `11` |
| `LATEST_PRICE` | `5` |

## Tests

Run:

```powershell
uv run python -m unittest discover -s test -v
```

The tests build a temporary DuckDB database and verify:

- raw tick cumulative fields.
- dynamic front-adjusted 1m and daily data.
- strict `NotImplementedError` behavior.
- mock `download_history_data`, `get_market_data_ex`, `get_full_tick`, and trader stubs.
