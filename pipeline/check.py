"""Compare the source with the warehouse and check the resolution invariants."""

import os
import sys

from pipeline import db
from pipeline.load import TABLES
from pipeline.row_hash import row_hash

# (table, check, query that counts violations): PASS when the count is 0.
RESOLUTION_CHECKS = [
    ("cards", "Cards without owner", """
        SELECT COUNT(*) FROM cards_resolved k
        LEFT JOIN raw_customers c ON c.customer_id = k.owner_customer_id AND c._deleted_at IS NULL
        WHERE c.customer_id IS NULL"""),
    ("customers", "Invalid survivors", """
        SELECT COUNT(*) FROM customer_resolution r
        JOIN customer_resolution s ON s.customer_id = r.survivor_customer_id
        WHERE s.status IN ('merged', 'excluded_test')"""),
    ("customers", "Resolution coverage", """
        SELECT COUNT(*) FROM raw_customers c
        WHERE c._deleted_at IS NULL
          AND NOT EXISTS (SELECT 1 FROM customer_resolution r WHERE r.customer_id = c.customer_id)"""),
]


def _hashes(rows, table) -> dict:
    """Recompute from the columns: the stored _row_hash would hide an edit made in the warehouse."""
    key_i = list(table.columns).index(table.key)
    return {r[key_i]: row_hash(r, table.columns.values()) for r in rows}


def source_vs_warehouse(src, wh) -> list[tuple]:
    """Content compares only keys on both sides, so a missing or extra row is not counted twice."""
    results = []
    for t in TABLES:
        select = ", ".join(t.columns)
        cur = src.cursor()
        cur.execute(f"SELECT {select} FROM dbo.{t.name}")
        source = _hashes(cur.fetchall(), t)
        warehouse = _hashes(wh.execute(f"SELECT {select} FROM raw_{t.name} WHERE _deleted_at IS NULL").fetchall(), t)
        both = source.keys() & warehouse.keys()
        missing, extra = len(source.keys() - warehouse.keys()), len(warehouse.keys() - source.keys())
        equal = sum(source[k] == warehouse[k] for k in both)
        results += [
            (t.name, "Count", len(source), len(warehouse), len(source) == len(warehouse)),
            (t.name, "Missing", missing, 0, missing == 0),
            (t.name, "Extra", 0, extra, extra == 0),
            (t.name, "Content", len(both), equal, equal == len(both)),
        ]
    return results


def resolution(wh) -> list[tuple]:
    """SKIP (None) until the first dedup creates customer_resolution."""
    exists = wh.execute("SELECT COUNT(*) FROM duckdb_tables() WHERE table_name = 'customer_resolution'").fetchone()[0]
    results = []
    for table, name, sql in RESOLUTION_CHECKS:
        if not exists:
            results.append((table, name, "-", "-", None))
            continue
        found = wh.execute(sql).fetchone()[0]
        results.append((table, name, 0, found, found == 0))
    return results


def main() -> int:
    if not os.path.exists(db.WAREHOUSE_PATH):
        print(f"{db.WAREHOUSE_PATH} not found: run `make load` first", file=sys.stderr)
        return 1
    wh = db.warehouse(read_only=True)
    results = source_vs_warehouse(db.mssql("etl_reader"), wh) + resolution(wh)
    print(f"{'table':<17} {'check':<20} {'source':>8} {'warehouse':>9}  status")
    for table, name, source, warehouse, ok in results:
        status = "SKIP" if ok is None else "PASS" if ok else "FAIL"
        print(f"{table:<17} {name:<20} {source:>8} {warehouse:>9}  {status}")
    failed = sum(ok is False for *_, ok in results)
    print(f"check: {failed} FAIL")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
