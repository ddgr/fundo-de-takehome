"""Apply a ~1% change set to the source, as the application would; the same SEED gives the same changes."""

import json
import os
import random
import sys

from pipeline import db

SEED = int(os.environ.get("SEED", "20260101")) + 1  # its own sequence, distinct from the seed's
# table -> (changed, inserted, deleted)
CHANGES = {"customers": (10, 5, 5), "advances": (15, 8, 8), "cards": (12, 6, 6), "transactions": (100, 50, 50)}
# Seeded hard cases are never touched, so the manifest test still holds after mutate.
MANIFEST = "SELECT customer_id FROM dbo.seed_manifest"
NOW = "SYSUTCDATETIME()"  # server clock in UTC, as the application sets updated_at


def _ids(cur, sql: str, params: tuple = ()) -> list[int]:
    cur.execute(sql, params)
    return [r[0] for r in cur.fetchall()]


def _next_id(cur, table: str, key: str) -> int:
    return _ids(cur, f"SELECT COALESCE(MAX({key}), 0) + 1 FROM dbo.{table}")[0]


def _split(rng: random.Random, deletable: list[int], pool: list[int], n_change: int, n_delete: int):
    """Changed and deleted ids never overlap, so the printed counts are exact."""
    to_delete = rng.sample(deletable, n_delete)
    to_change = rng.sample(sorted(set(pool) - set(to_delete)), n_change)
    return to_change, to_delete


def _delete(cur, table: str, key: str, ids: list[int]) -> None:
    cur.execute(f"DELETE FROM dbo.{table} WHERE {key} IN ({', '.join(['%s'] * len(ids))})", tuple(ids))


def late_row(cur) -> int:
    """Commit a change dated 2 minutes before the last loaded cursor: only the re-read margin catches it."""
    late_id = _ids(cur, f"SELECT MIN(transaction_id) FROM dbo.transactions WHERE customer_id NOT IN ({MANIFEST})")[0]
    cur.execute(
        "UPDATE dbo.transactions SET amount = amount + 1, "
        "updated_at = DATEADD(MINUTE, -2, (SELECT MAX(updated_at) FROM dbo.transactions)) "
        "WHERE transaction_id = %s", (late_id,))
    return late_id


def customers(cur, rng: random.Random) -> int:
    n_change, n_new, n_delete = CHANGES["customers"]
    pool = _ids(cur, f"SELECT customer_id FROM dbo.customers WHERE customer_id NOT IN ({MANIFEST}) ORDER BY customer_id")
    # Only customers without dependents: the foreign keys reject deleting the rest.
    deletable = _ids(cur, f"""
        SELECT c.customer_id FROM dbo.customers c
        WHERE c.customer_id NOT IN ({MANIFEST})
          AND NOT EXISTS (SELECT 1 FROM dbo.advances a WHERE a.customer_id = c.customer_id)
          AND NOT EXISTS (SELECT 1 FROM dbo.cards k WHERE k.customer_id = c.customer_id)
          AND NOT EXISTS (SELECT 1 FROM dbo.transactions t WHERE t.customer_id = c.customer_id)
        ORDER BY c.customer_id""")
    to_change, to_delete = _split(rng, deletable, pool, n_change, n_delete)

    history_id = _next_id(cur, "customer_history", "history_id")
    for cid in to_change:
        phone = f"557{cid:07d}"
        cur.execute(f"UPDATE dbo.customers SET phone = %s, updated_at = {NOW} WHERE customer_id = %s", (phone, cid))
        cur.execute(f"INSERT INTO dbo.customer_history VALUES (%s, %s, {NOW}, %s)",
                    (history_id, cid, json.dumps({"phone": phone})))
        history_id += 1

    new_id = _next_id(cur, "customers", "customer_id")
    for i in range(n_new):
        cid = new_id + i
        doc = str(rng.randrange(900_000_000, 1_000_000_000))
        cur.execute(f"INSERT INTO dbo.customers VALUES (%s, %s, %s, %s, %s, %s, %s, {NOW}, {NOW})",
                    (cid, doc, "New", f"Customer{i}", f"new.customer.{cid}@mail.com", f"558{cid:07d}", "1990-01-01"))

    # Recreate a duplicate of an already merged person: same document, another format.
    survivor_id = _ids(cur, "SELECT MIN(expected_survivor_id) FROM dbo.seed_manifest WHERE case_name = 'dup_group'")[0]
    cid = new_id + n_new
    cur.execute(
        f"INSERT INTO dbo.customers SELECT %s, CONCAT(SUBSTRING(d, 1, 3), ' ', SUBSTRING(d, 4, 3), '-', SUBSTRING(d, 7, 3)), "
        f"first_name, last_name, UPPER(email), phone, date_of_birth, {NOW}, {NOW} "
        f"FROM dbo.customers CROSS APPLY (SELECT REPLACE(REPLACE(REPLACE(document_number, ' ', ''), '.', ''), '-', '') AS d) k "
        f"WHERE customer_id = %s", (cid, survivor_id))
    cur.execute("INSERT INTO dbo.seed_manifest VALUES (%s, 'recreated_duplicate', 'merged', %s)", (cid, survivor_id))

    _delete(cur, "customers", "customer_id", to_delete)
    return n_new + 1


def advances(cur, rng: random.Random) -> None:
    n_change, n_new, n_delete = CHANGES["advances"]
    pool = _ids(cur, f"SELECT advance_id FROM dbo.advances WHERE customer_id NOT IN ({MANIFEST}) ORDER BY advance_id")
    to_change, to_delete = _split(rng, pool, pool, n_change, n_delete)
    for aid in to_change:
        cur.execute(f"UPDATE dbo.advances SET amount = amount + 10, updated_at = {NOW} WHERE advance_id = %s", (aid,))
    owners = _ids(cur, f"SELECT customer_id FROM dbo.customers WHERE customer_id NOT IN ({MANIFEST}) ORDER BY customer_id")
    new_id = _next_id(cur, "advances", "advance_id")
    for i, cid in enumerate(rng.sample(owners, n_new)):
        cur.execute(f"INSERT INTO dbo.advances VALUES (%s, %s, %s, 'requested', {NOW}, {NOW})",
                    (new_id + i, cid, f"{rng.randrange(1_000, 100_000) / 100:.2f}"))
    _delete(cur, "advances", "advance_id", to_delete)


def cards(cur, rng: random.Random) -> None:
    n_change, n_new, n_delete = CHANGES["cards"]
    pool = _ids(cur, f"SELECT card_id FROM dbo.cards WHERE customer_id NOT IN ({MANIFEST}) ORDER BY card_id")
    # Only cards without transactions: the foreign key rejects deleting the rest.
    deletable = _ids(cur, f"""
        SELECT k.card_id FROM dbo.cards k
        WHERE k.customer_id NOT IN ({MANIFEST})
          AND NOT EXISTS (SELECT 1 FROM dbo.transactions t WHERE t.card_id = k.card_id)
        ORDER BY k.card_id""")
    to_change, to_delete = _split(rng, deletable, pool, n_change, n_delete)
    for card_id in to_change:
        cur.execute("UPDATE dbo.cards SET status = CASE status WHEN 'active' THEN 'blocked' ELSE 'active' END, "
                    f"updated_at = {NOW} WHERE card_id = %s", (card_id,))
    owners = _ids(cur, f"SELECT customer_id FROM dbo.customers WHERE customer_id NOT IN ({MANIFEST}) ORDER BY customer_id")
    new_id = _next_id(cur, "cards", "card_id")
    for i, cid in enumerate(rng.sample(owners, n_new)):
        cur.execute(f"INSERT INTO dbo.cards VALUES (%s, %s, %s, 'visa', 'active', {NOW}, {NOW})",
                    (new_id + i, cid, f"{rng.randrange(10_000):04d}"))
    _delete(cur, "cards", "card_id", to_delete)


def transactions(cur, rng: random.Random, late_id: int) -> None:
    n_change, n_new, n_delete = CHANGES["transactions"]
    pool = _ids(cur, f"SELECT transaction_id FROM dbo.transactions WHERE customer_id NOT IN ({MANIFEST}) "
                     "AND transaction_id <> %s ORDER BY transaction_id", (late_id,))
    to_change, to_delete = _split(rng, pool, pool, n_change, n_delete)
    for tid in to_change:
        cur.execute(f"UPDATE dbo.transactions SET amount = amount + 1, updated_at = {NOW} WHERE transaction_id = %s", (tid,))
    owners = _ids(cur, f"SELECT customer_id FROM dbo.customers WHERE customer_id NOT IN ({MANIFEST}) ORDER BY customer_id")
    new_id = _next_id(cur, "transactions", "transaction_id")
    for i, cid in enumerate(rng.sample(owners, n_new)):
        cur.execute(f"INSERT INTO dbo.transactions VALUES (%s, %s, NULL, %s, {NOW}, {NOW}, {NOW})",
                    (new_id + i, cid, f"{rng.randrange(100, 50_000) / 100:.2f}"))
    _delete(cur, "transactions", "transaction_id", to_delete)


def main() -> int:
    rng = random.Random(SEED)
    conn = db.mssql("sa")
    cur = conn.cursor()
    late_id = late_row(cur)
    new_customers = customers(cur, rng)
    advances(cur, rng)
    cards(cur, rng)
    transactions(cur, rng, late_id)
    conn.commit()
    conn.close()
    print(f"mutate: late row transaction_id={late_id}; inserted customers={new_customers} (incl. 1 recreated duplicate)")
    for table, (n_change, n_new, n_delete) in CHANGES.items():
        n_new += 1 if table == "customers" else 0
        print(f"  {table:<13} changed={n_change:>3} inserted={n_new:>3} deleted={n_delete:>3}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
