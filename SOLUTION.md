# Solution

Everything runs locally: SQL Server 2022 as the source, DuckDB as the warehouse, Python for the pipeline, `make` as the entry point. How to run it is in the [README](README.md).

## 1. Source assumptions

Every incremental strategy rests on assumptions about the source. This one assumes only what almost every application database already has (a primary key, `updated_at`, a read-only user), so nothing must be enabled on the source (Change Tracking, CDC, replication) and no extra service is needed (AWS DMS, Fivetran). The last column says what changes if an assumption fails; the seed follows all of them.

| ID | Assumption | If it does not hold |
|----|------------|---------------------|
| S1 | Every table has a primary key | Full reload for that table, or exclude it |
| S2 | Tables that change have `updated_at` in UTC, set on every insert and update (by the app or a trigger) | Full reload, or enable Change Tracking |
| S3 | Append-only tables have an ever-increasing id | Treat the table as S2 |
| S4 | Deletes can be hard deletes | — |
| S5 | Only a read-only user is available; the database configuration cannot change | — |
| S6 | The longest application transaction lasts under 5 minutes | Widen the re-read margin |
| S7 | US customers: 9-digit national id, 10-digit phone (11 if it starts with 1) | Change the validity rules |

`load` and `check` connect as `etl_reader` (read-only, S5). Only the seed and `mutate`, which play the application, use `sa`.

## 2. Problem 1 — incremental load

**Strategy per table**

| Table | Strategy | Trade-off |
|-------|----------|-----------|
| `customers`, `advances`, `cards` | `updated_at` cursor + primary-key comparison | Small, updated and hard-deleted; the key list is cheap |
| `transactions` | Same | 61% of the rows and rarely changed, but still hard-deleted, so every load compares its 20,000 ids. At production size: recent keys daily, all keys weekly, or Change Tracking |
| `customer_history` | `history_id` cursor (append-only, S3) | No updates or deletes, so no key comparison: the cheapest strategy |
| `tmp_import_2023` | Not loaded | Orphaned scratch table: nobody reads it |

**Per table, on every load**

1. Read rows with `cursor >= last cursor − margin` (everything on the first load). Margin: 5 minutes for `updated_at` (S6), 100 ids for `history_id`.
2. `updated_at` tables only: read the full primary-key list. Keys present in the source but missing (or marked deleted) in the warehouse, and not read in step 1, are read in full. This catches late-committed inserts and keys that reappear.
3. In one DuckDB transaction: stage rows in bulk, upsert by key only when the row fingerprint (`row_hash`, SHA-256 of the source columns) changed or the row was marked deleted, mark missing keys with `_deleted_at`, save the new cursor (the max of the previous cursor and the max value read; never the pipeline clock), COMMIT.

**Gaps of an `updated_at` cursor and how they are covered**

| Gap | Coverage |
|-----|----------|
| Hard deletes are invisible | Primary-key comparison marks them with `_deleted_at` |
| A row committed late with an `updated_at` older than the cursor is skipped | Updates: the 5-minute re-read margin (`make mutate` seeds such a row). Inserts: the key comparison, at any age |
| The app changes a row without touching `updated_at` | The content check (`make check`) shows the difference |

**ELT, not ETL.** The raw layer (`raw_<table>`) is an exact copy of the source plus `_row_hash`, `_deleted_at` and `_last_changed_run`. No business rule filters rows during the load: a wrong rule would drop rows with no way back, while here it is fixed by re-running it, and the checks compare against a faithful copy.

**Measured (from `make demo`)**

| Metric | Value |
|--------|-------|
| Full load: rows read | 32,500 |
| Incremental load after ~1% change: rows read | 323 (0.99% of the full load; target ≤ 5%) |
| Incremental load: primary keys compared | 27,501 (key list only, not full rows) |
| Next load: rows read / rows changed | 308 / 0 (re-read margin, nothing rewritten) |
| `make demo` duration | ~40 s with SQL Server already running (Apple Silicon, Docker Desktop with 8 GB) |

**Cost impact**

| Driver | Today: full copy every day | After (measured) |
|--------|----------------------------|------------------|
| Rows read from SQL Server and sent to the warehouse | Every row | 323 of 32,500 (0.99%), plus the key list of 4 tables (27,501 ids, no other columns) |
| Rows written in the warehouse | Every row | 218 of 32,500; a re-read row with the same fingerprint is not rewritten |
| Work on the source | Full scan of every table | A range read on `updated_at` plus a key-only scan; needs an index on `updated_at`, otherwise every read scans the table |

*Estimated:* the bill falls roughly in proportion to rows moved. How today's copy is priced (per row, per GB transferred, per daily snapshot) is unknown to us, so it is not converted to money.

**Measured before deciding**

- The cursor and the fingerprint need `DATETIME2(6)` to arrive with its microseconds. The connection pins TDS protocol 7.4 and we measured full microseconds coming back (`03:31:52.245919`), so no rounding workaround was added; the Content check passing on every table confirms it.
- The 5-minute re-read margin costs 308 rows on a load with no changes (0.95%, measured), well under the 5% target, so it stays.

**Alternatives considered**

| Option | Why not here |
|--------|--------------|
| SQL Server Change Tracking (what Fivetran uses) | Must be enabled on the database and on each table, which breaks S5. Next step if `updated_at` is unreliable or comparing keys gets expensive |
| Log-based CDC (AWS DMS, Debezium, Fivetran) | Needs CDC or replication enabled, full recovery model and high privileges; the load is daily and intermediate changes are not needed |
| dlt as the loader | Cursor and lag are built in, but delete detection by key comparison would still be custom code |

## 3. Problem 2 — duplicate customers

Rules run after the load, in Python, on active rows of the raw layer. They write `customer_resolution` (one row per active customer) and two views: `dim_customer` (status `survivor` or `needs_review`) and `cards_resolved`. The source and the raw layer are never modified; `customer_resolution` is rebuilt in full on every `make dedup`.

**Only one field proves identity:** `document_key`, the document without spaces, dots or hyphens, upper-cased. A valid document has 9 digits and is not a single repeated digit. Email, phone and name + birth date are shared (families, company addresses) or repeated (namesakes), so they only produce a suggestion (`suggested_match_id`, `suggestion_basis`) and never change the status.

**Evaluation order** (first match wins):

| # | Condition | Status | Survivor |
|---|-----------|--------|----------|
| 1 | Test account (test email pattern) with no activity | `excluded_test` | — |
| 2 | Invalid document | `survivor` | itself |
| 3 | Only customer with that document | `survivor` | itself |
| 4 | Group with 2+ protected customers | `needs_review` for all | — |
| 5 | Group with 1 protected customer | protected one survives, the rest `merged` | the protected one |
| 6 | Group without protected customers | oldest `created_at` survives (lowest id on ties), the rest `merged` | the oldest |

- **Protected:** has an active advance whose status, without spaces and lower-cased, is `funded` or `paid_off` (catches ` Funded`, `PAID_OFF`).
- **Activity:** at least one active advance, card or transaction.
- **Test pattern:** the email local part (without `+alias`) is `test`, `testing`, `qa` or `dummy` at `fundo.com` or `example.com`. The last name is never used, so "Testerman" is not excluded, and staff with `@fundo.com` addresses are not excluded. A test-pattern account with activity is kept and flagged `is_test_pattern = true`.
- **Malformed contacts** are flagged (`email_is_valid`, `phone_is_valid`) and counted, never corrected: correcting would invent data.
- **Cards** follow their final owner in `cards_resolved`: `COALESCE(survivor_customer_id, customer_id)`. Advances and transactions are attributed to the survivor by joining `customer_resolution`. If a card stayed on the merged customer, it would drop out of every report joined to `dim_customer`, spend included; if cards moved on a suggestive field, they would land on another person. `Cards without owner` catches the first case.
- **Two protected customers in one group** (`needs_review`): nothing is merged and both stay in `dim_customer`. What I would do: send the group to operations with the evidence (each member's document format, advances and contact suggestions), and store the decision in an override table (`customer_id` → survivor) that `make dedup` applies before the rules.
- **A recreated duplicate** falls into its group on the next `make dedup` and becomes `merged`. A group's survivor can change if a protected customer appears later.
- **Other data:** `SEED=<n> make demo` seeds different random data with the same catalog of hard cases. Measured on seeds 1, 42 and 777: 68, 72 and 64 merged, `check: 0 FAIL` and 56 tests passing on each. The catalog itself is mine, so formats nobody seeded only show up with real data.

**Preventing duplicates in the source (proposal).** `document_number` is `NVARCHAR(MAX)`, which cannot be indexed. Change it to `NVARCHAR(32)`, add a persisted computed column with `document_key`, and a filtered unique index on it. That needs a schema and application change, so it is out of scope here.

## 4. Problem 3 — checks

`make check` prints table, check, source value, warehouse value and status, and exits 1 on any `FAIL`.

| Check | Compares |
|-------|----------|
| Count | Source rows vs active warehouse rows |
| Missing | Source keys without an active warehouse row |
| Extra | Active warehouse keys absent from the source |
| Content | `row_hash` recomputed from the columns on both sides, for keys present on both |
| Cards without owner | Active cards whose final owner is not an active customer |
| Invalid survivors | `survivor_customer_id` pointing to a `merged` or `excluded_test` customer |
| Resolution coverage | Active customers without a `customer_resolution` row |

The three resolution checks report `SKIP` before the first `make dedup`. `make break` backs up the warehouse and damages `raw_transactions` (deletes 3 rows, inserts a fake one, adds 1 to an amount without touching `updated_at`); `make check` then fails exactly Count, Missing, Extra and Content, and `make restore` brings it back.

**Reliability.** Each table commits together with its cursor, so a failure only rolls back the table in progress. `make load FAIL_AT=customers` fails after writing customers and before COMMIT, records `failed` in `load_runs`, and exits with an error; the next `make load` recovers, `make check` passes and the following load changes 0 rows.

## 5. Skipped, and why

| Skipped | Why |
|---------|-----|
| Preventing duplicates in the source (proposal in section 3) | Schema and application change; the pipeline is read-only (S5). In the warehouse, a recreated duplicate is merged on the next `make dedup` |
| Persisting `needs_review` decisions | Needs an operations owner and a review flow; the override table in section 3 is the design |
| Purging `customer_history`, dropping `tmp_import_2023` | Destructive in the source; recommended, not done |
| Loading into BigQuery or an emulator | DuckDB is the local stand-in the challenge allows; in BigQuery the upsert would be a `MERGE` |
| Demonstrating a hard-deleted key that comes back | Same key-comparison path as late inserts, which the demo runs; the reappearance itself is not run |
| A test asserting "0 changed" on the repeated load | Shown in the `make demo` output and covered by `make check`; no test asserts it |

## 6. Production

- **Orchestration:** Airflow or Dagster instead of `make`; one task per table, retries, and a run log like `load_runs`.
- **Alerts:** on any `FAIL` from the checks, on `load_runs.status = 'failed'`, and on a sudden jump of `rows_read`.
- **Checks at scale:** the content check reads every source row. In production run it on samples or date ranges, or use dbt tests / Datafold data-diff.
- **Security:** a secret manager instead of default passwords; access control on the raw layer, which holds unmasked personal and card data; PCI DSS scope for cards.
- **Source:** an index on `updated_at`; Change Tracking if `updated_at` turns out unreliable; an inventory of source tables in the checks, to detect new tables nobody configured.
- **One-time vs permanent.** One-time: the initial full load (the same `load`, run with no cursor), the source clean-up (document column, scratch table, a retention rule for `customer_history`), and the review of the `needs_review` groups that exist today. Permanent, daily and in this order: incremental load, `dedup`, checks.
- **First delivery: the incremental load of `transactions` with its checks.** It is most of the rows, so it removes most of the daily transfer, and its checks prove the cheaper load loses nothing. The other tables reuse the code; `dedup` comes last because `needs_review` needs an operations owner.
