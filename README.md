# Fundo — Data Engineer take-home

Local SQL Server → DuckDB pipeline: incremental loads, duplicate-customer resolution, and source-vs-warehouse checks. Design and trade-offs are in [SOLUTION.md](SOLUTION.md).

## Requirements

- Docker with Docker Compose v2.32 or later (tested with 2.32.4) and at least 2 GB of memory for containers (SQL Server minimum).
- `make`.
- Apple Silicon: the SQL Server image is amd64-only, so turn on *Use Rosetta for x86_64/amd64 emulation* in Docker Desktop (Settings → General).

Python is not needed on the host: every command runs inside the `pipeline` container.

## Run

```bash
docker compose up -d   # starts SQL Server only
make demo              # full scenario, ~40 s once SQL Server is up
```

`make demo` stops at the first step that does not return the expected exit code. Running it again gives the same output except timings. `SEED=<n> make demo` seeds different random data with the same catalog of cases; the expected output below is for the default seed.

Expected output (key lines; the rest are per-table rows and Docker messages):

```text
== 2. load (initial, full) ==
rows_read total: 32500 (100.00% of the full load, 32500 rows)
== 2. check ==
check: 0 FAIL                       # the 3 resolution checks show SKIP until dedup runs
== 5. load FAIL_AT=customers (expected exit 1) ==
customers         incremental FAILED: FAIL_AT=customers: simulated failure before COMMIT
== 6. load (recovery; the measured incremental load) ==
rows_read total: 323 (0.99% of the full load, 32500 rows)
== 7. load (expected 0 changed) ==
rows_read total: 308 (0.95% of the full load, 32500 rows)   # "changed" is 0 on every table
== 8. dedup (recreated duplicate becomes merged) ==
  status   survivor=1926 merged=65 needs_review=2 excluded_test=8
== 9. check (expected exit 1, four FAIL) ==
check: 4 FAIL
== 9. check ==
check: 0 FAIL
== 10. test ==
56 passed
== demo: every step returned the expected exit code ==
```

## Commands

| Command | What it does | Exit codes |
|---------|--------------|------------|
| `make demo` | Whole scenario: reset → load → check → dedup → mutate → failed load → recovery load → idempotent load → dedup → check → break → check (fails) → restore → check → test | 0 if every step behaves as expected |
| `make reset` | Waits for SQL Server (up to 120 s), recreates and seeds the source, deletes `data/` | 0; 2 if SQL Server does not answer |
| `make load [FAIL_AT=<table>]` | Loads `customers`, `advances`, `cards`, `transactions`, `customer_history` in that order; full the first time, incremental afterwards. `FAIL_AT` simulates a failure before that table commits | 0; 1 if a table fails |
| `make mutate` | Applies a fixed ~1% change set to the source (updates, inserts, hard deletes, a late-committed row, a recreated duplicate) | 0 |
| `make dedup` | Rebuilds `customer_resolution`, `dim_customer` and `cards_resolved`; prints counts | 0 |
| `make check` | Source vs warehouse (count, missing, extra, content) and resolution invariants | 0 if no `FAIL`; 1 otherwise |
| `make break` | Backs up the warehouse and damages `raw_transactions` on purpose | 0; 2 if a backup already exists |
| `make restore` | Restores and deletes the backup | 0; 2 if there is no backup |
| `make test` | `pytest`: identity rules, and resolution vs `seed_manifest` when `customer_resolution` exists | 0; 1 if a test fails |

Exit codes are those of the Python step. `make` turns any non-zero code into its own exit code 2 and prints `Error N`, where N is the code above.

## Reproduce the failing check

After `make demo` (or at least `make reset` and `make load`):

```bash
make check     # check: 0 FAIL
make break     # backs up data/warehouse.duckdb, then deletes 3 transactions, adds a fake one, changes one amount
make check     # transactions Count, Missing, Extra and Content FAIL; "check: 4 FAIL"; make prints Error 1
make restore   # puts the backup back
make check     # check: 0 FAIL
```

## Where things are

| Path | Content |
|------|---------|
| `seed/` | Source schema and reproducible synthetic data |
| `pipeline/` | Loader, mutator, dedup, checks, break/restore |
| `tests/` | `pytest` suite |
| `data/warehouse.duckdb` | The warehouse (created by `make load`, git-ignored) |

Inspect the warehouse:

```bash
docker compose run --rm pipeline python -c "import duckdb; print(duckdb.connect('data/warehouse.duckdb', read_only=True).sql('SELECT * FROM load_runs ORDER BY run_id, table_name'))"
```

## Clean up

```bash
docker compose down -v
```
