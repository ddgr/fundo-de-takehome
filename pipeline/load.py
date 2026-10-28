"""Incremental load from SQL Server into the raw layer of the DuckDB warehouse."""

import argparse
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from pipeline import db
from pipeline.row_hash import row_hash

TIMESTAMPS = {"created_at": "TIMESTAMP", "updated_at": "TIMESTAMP"}


@dataclass(frozen=True)
class Table:
    name: str
    key: str
    cursor: str
    margin: timedelta | int  # re-read below the cursor, for rows committed late
    columns: dict[str, str]  # source column -> DuckDB type, in fingerprint order

    @property
    def tracks_deletes(self) -> bool:
        """Append-only tables are never deleted from, so they skip the key comparison."""
        return self.cursor == "updated_at"


# Load order is fixed: parents before children.
TABLES = [
    Table("customers", "customer_id", "updated_at", timedelta(minutes=5), {
        "customer_id": "INTEGER", "document_number": "VARCHAR", "first_name": "VARCHAR",
        "last_name": "VARCHAR", "email": "VARCHAR", "phone": "VARCHAR", "date_of_birth": "DATE",
        **TIMESTAMPS}),
    Table("advances", "advance_id", "updated_at", timedelta(minutes=5), {
        "advance_id": "INTEGER", "customer_id": "INTEGER", "amount": "DECIMAL(12,2)",
        "status": "VARCHAR", **TIMESTAMPS}),
    Table("cards", "card_id", "updated_at", timedelta(minutes=5), {
        "card_id": "INTEGER", "customer_id": "INTEGER", "last_four": "VARCHAR", "brand": "VARCHAR",
        "status": "VARCHAR", **TIMESTAMPS}),
    Table("transactions", "transaction_id", "updated_at", timedelta(minutes=5), {
        "transaction_id": "BIGINT", "customer_id": "INTEGER", "card_id": "INTEGER",
        "amount": "DECIMAL(12,2)", "occurred_at": "TIMESTAMP", **TIMESTAMPS}),
    Table("customer_history", "history_id", "history_id", 100, {
        "history_id": "BIGINT", "customer_id": "INTEGER", "changed_at": "TIMESTAMP",
        "snapshot_json": "VARCHAR"}),
]


def ensure_schema(wh) -> None:
    """Idempotent, so every load runs it. Deletes are soft, so a key that comes back is revived in place."""
    for t in TABLES:
        columns = ", ".join(f"{name} {sql_type}" for name, sql_type in t.columns.items())
        wh.execute(
            f"CREATE TABLE IF NOT EXISTS raw_{t.name} ({columns}, _row_hash VARCHAR NOT NULL, "
            f"_deleted_at TIMESTAMP, _last_changed_run INTEGER NOT NULL, PRIMARY KEY ({t.key}))"
        )
    wh.execute("CREATE TABLE IF NOT EXISTS load_state (table_name VARCHAR PRIMARY KEY, cursor_value VARCHAR NOT NULL)")
    wh.execute(
        "CREATE TABLE IF NOT EXISTS load_runs (run_id INTEGER, table_name VARCHAR, mode VARCHAR, "
        "rows_read INTEGER, rows_changed INTEGER, rows_marked_deleted INTEGER, keys_compared INTEGER, "
        "seconds DOUBLE, status VARCHAR, error VARCHAR, PRIMARY KEY (run_id, table_name))"
    )


def _read_cursor(wh, table: Table):
    """Cursors are stored as text, so one table holds both timestamps and ids."""
    row = wh.execute("SELECT cursor_value FROM load_state WHERE table_name = ?", [table.name]).fetchone()
    if row is None:
        return None
    return datetime.fromisoformat(row[0]) if table.cursor == "updated_at" else int(row[0])


def _fetch_by_keys(cur, select: str, key: str, keys: list) -> list[tuple]:
    """Chunks of 500 keys, well under the 2,100 parameters SQL Server allows per query."""
    rows = []
    for start in range(0, len(keys), 500):
        chunk = keys[start:start + 500]
        cur.execute(f"{select} WHERE {key} IN ({', '.join(['%s'] * len(chunk))})", tuple(chunk))
        rows += cur.fetchall()
    return rows


def _load_table(src, wh, table: Table, cursor, run_id: int, started_at: datetime, fail_at: str | None) -> dict:
    """One transaction per table, cursor included: on any error the table keeps its previous state."""
    t0 = time.monotonic()
    names = list(table.columns)
    key_i, cursor_i = names.index(table.key), names.index(table.cursor)
    select = f"SELECT {', '.join(names)} FROM dbo.{table.name}"
    cur = src.cursor()

    # Step 1: rows at or after (cursor - margin); everything on a full load.
    if cursor is None:
        cur.execute(select)
    else:
        cur.execute(f"{select} WHERE {table.cursor} >= %s", (db.sql_param(cursor - table.margin),))
    rows = cur.fetchall()

    # Step 2: compare primary keys to find deletes and keys the warehouse lacks.
    keys_compared, deleted = 0, []
    if table.tracks_deletes:
        cur.execute(f"SELECT {table.key} FROM dbo.{table.name}")
        source_keys = {r[0] for r in cur.fetchall()}
        keys_compared = len(source_keys)
        active = {r[0] for r in wh.execute(f"SELECT {table.key} FROM raw_{table.name} WHERE _deleted_at IS NULL").fetchall()}
        already_read = {r[key_i] for r in rows}
        rows += _fetch_by_keys(cur, select, table.key, sorted(source_keys - active - already_read))
        deleted = sorted(active - source_keys)

    # Step 3: one DuckDB transaction per table, cursor included.
    staged = [(*row, row_hash(row, table.columns.values())) for row in rows]
    updates = ", ".join(f"{n} = excluded.{n}" for n in names if n != table.key)
    wh.execute("BEGIN")
    try:
        wh.execute(f"CREATE OR REPLACE TEMP TABLE stage AS SELECT {', '.join(names)}, _row_hash FROM raw_{table.name} LIMIT 0")
        db.insert_rows(wh, "stage", {**table.columns, "_row_hash": "VARCHAR"}, staged)
        # Rewrite only rows whose fingerprint changed or that come back from a delete.
        changed = wh.execute(
            f"INSERT INTO raw_{table.name} ({', '.join(names)}, _row_hash, _deleted_at, _last_changed_run) "
            f"SELECT {', '.join(names)}, _row_hash, NULL, {run_id} FROM stage "
            f"ON CONFLICT ({table.key}) DO UPDATE SET {updates}, _row_hash = excluded._row_hash, "
            f"_deleted_at = NULL, _last_changed_run = excluded._last_changed_run "
            f"WHERE raw_{table.name}._row_hash <> excluded._row_hash OR raw_{table.name}._deleted_at IS NOT NULL"
        ).fetchone()[0]
        if deleted:
            wh.execute(
                f"UPDATE raw_{table.name} SET _deleted_at = ?, _last_changed_run = ? "
                f"WHERE {table.key} IN (SELECT UNNEST(?))",
                [started_at, run_id, deleted],
            )
        # Source values only, never the pipeline clock: the two clocks can differ.
        new_cursor = max([c for c in [cursor, *(r[cursor_i] for r in rows)] if c is not None], default=None)
        if new_cursor is not None:
            wh.execute(
                "INSERT INTO load_state VALUES (?, ?) ON CONFLICT (table_name) DO UPDATE SET cursor_value = excluded.cursor_value",
                [table.name, new_cursor.isoformat() if isinstance(new_cursor, datetime) else str(new_cursor)],
            )
        if fail_at == table.name:
            raise RuntimeError(f"FAIL_AT={fail_at}: simulated failure before COMMIT")
        stats = {"rows_read": len(rows), "rows_changed": changed, "rows_marked_deleted": len(deleted),
                 "keys_compared": keys_compared, "seconds": round(time.monotonic() - t0, 3)}
        wh.execute(
            "INSERT INTO load_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'ok', NULL)",
            [run_id, table.name, "full" if cursor is None else "incremental", *stats.values()],
        )
        wh.execute("COMMIT")
    except Exception:
        wh.execute("ROLLBACK")
        raise
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fail-at", choices=[t.name for t in TABLES], help="simulate a failure before this table commits")
    fail_at = parser.parse_args().fail_at

    wh = db.warehouse()
    ensure_schema(wh)
    run_id = wh.execute("SELECT COALESCE(MAX(run_id), 0) + 1 FROM load_runs").fetchone()[0]
    started_at = datetime.now(UTC).replace(tzinfo=None)
    src = db.mssql("etl_reader")
    total_read = 0
    print(f"{'table':<17} {'mode':<11} {'read':>6} {'changed':>7} {'deleted':>7} {'keys':>6}  seconds")
    for table in TABLES:
        cursor = _read_cursor(wh, table)
        mode = "full" if cursor is None else "incremental"
        try:
            s = _load_table(src, wh, table, cursor, run_id, started_at, fail_at)
        except Exception as exc:
            wh.execute(
                "INSERT INTO load_runs VALUES (?, ?, ?, NULL, NULL, NULL, NULL, NULL, 'failed', ?)",
                [run_id, table.name, mode, str(exc)],
            )
            print(f"{table.name:<17} {mode:<11} FAILED: {exc}")
            # Stop: later tables reference earlier ones; the next load resumes from each saved cursor.
            return 1
        total_read += s["rows_read"]
        print(f"{table.name:<17} {mode:<11} {s['rows_read']:>6} {s['rows_changed']:>7} "
              f"{s['rows_marked_deleted']:>7} {s['keys_compared']:>6}  {s['seconds']:.2f}")
    full = wh.execute("SELECT SUM(rows_read) FROM load_runs WHERE mode = 'full' AND status = 'ok'").fetchone()[0]
    if full:
        print(f"rows_read total: {total_read} ({100 * total_read / full:.2f}% of the full load, {full} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
