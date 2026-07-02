# xtquant-duck tests

Run from `C:\workspace\xtquant-duck`:

```powershell
uv run python -m unittest discover -s test -v
```

The unit tests build a temporary DuckDB database and do not read or write `C:\data-tick\duckdb\qmt_mock.duckdb`.
