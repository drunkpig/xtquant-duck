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

## Tests

```powershell
uv run python -m unittest discover -s test -v
```

The unit tests build a temporary DuckDB database and do not read or write the
default production database.
