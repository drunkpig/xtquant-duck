# xtquant-duck

DuckDB-backed local mock implementation of selected `xtquant` APIs.

The goal is to let research code switch between real QMT `xtquant` and a local
DuckDB-backed mock by changing only the import. QMT semantics are the canonical
reference; unsupported mock behavior raises `NotImplementedError` instead of
silently returning placeholder data.

## Install

```powershell
uv pip install xtquant-duck
```

For local development:

```powershell
git clone git@github.com:drunkpig/xtquant-duck.git
cd xtquant-duck
uv sync --all-extras --dev
```

## Basic Usage

Set the DuckDB database path:

```powershell
$env:QMT_MOCK_DUCKDB = "C:\data-tick\duckdb\qmt_mock.duckdb"
```

Use the mock package as an `xtquant`-compatible module:

```python
import xtquant_duck as xtquant

xtdata = xtquant.xtdata

data = xtdata.get_market_data_ex(
    field_list=["time", "open", "high", "low", "close", "volume"],
    stock_list=["600036.SH"],
    period="1d",
    start_time="20220101",
    end_time="20251231",
    dividend_type="front_ratio",
)
```

The packaged `qmt_data_layer` module is kept for database build and validation
tools. Strategy code should use the `xtquant_duck` API above so switching back
to real miniQMT only changes the import.

Default database path is `C:\data-tick\duckdb\qmt_mock.duckdb`; override it with
`QMT_MOCK_DUCKDB`.

## Documentation

See [docs/MOCK_XTQUANT_API.md](docs/MOCK_XTQUANT_API.md) for supported APIs,
parameters, return shapes, and strict unsupported behavior.

The production DuckDB file uses QMT-canonical tables `qmt_tick_v1` and
`qmt_daily_v1`; legacy `raw_*` tables are retained only as rebuild/audit inputs.

## Runtime Data Boundary

Installed runtime APIs are only allowed to read canonical QMT-semantics tables:

- `qmt_tick_v1`
- `qmt_daily_v1`
- `baostock_adjust_factor_events`
- `choice_sse50_constituents_2022_2025`

`raw_tick_v3` and `raw_daily_v1` are source-lineage tables for offline import,
canonical rebuild, and validation scripts only. Strategy code and
`xtquant_duck.xtdata` must not query them directly. Unit tests scan the runtime
packages and fail if these raw table names appear there.

## Canonical Rebuild

Canonical tables are rebuilt with fixed source boundaries:

- 2022-2025 tick: `D:\ticks\data-tick\sh_v3` and `D:\ticks\data-tick\sz_v3`.
- 2026 tick: `D:\百度盘分钟K10年\A50-2026`.
- 2022-2025 daily: vendor raw daily first, then missing code-days backfilled
  from corrected sh/sz tick.
- 2026 daily: derived from A50 tick.

QMT volume semantics are lots. During rebuild, sh/sz v3 `688*` tick and vendor
daily rows are converted from shares to lots by dividing by `100`; sh/sz non-688
and A50 rows are already lots. A50 startup rows are not allowed into canonical
2022-2025 data.

```powershell
uv run python scripts\rebuild_qmt_canonical_tables.py --db C:\data-tick\duckdb\qmt_mock.duckdb --rebuild --skip-indexes
```

## Tests

```powershell
uv run python -m unittest discover -s test -v
```

The unit tests build a temporary DuckDB database and do not read or write the
default production database.
