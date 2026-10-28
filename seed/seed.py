"""Recreate the source database and fill it with reproducible synthetic data."""

import json
import os
import random
import sys
import time
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pymssql

from pipeline import db

SEED = int(os.environ.get("SEED", "20260101"))
YEAR_START = datetime(2025, 1, 1)
MINUTES_IN_YEAR = 365 * 24 * 60
TOTAL_CUSTOMERS = 2000
INACTIVE_CUSTOMERS = 100  # plain customers without dependents: mutate deletes from these
TOTAL_ADVANCES, TOTAL_CARDS, IDLE_CARDS, TOTAL_TRANSACTIONS, TOTAL_HISTORY = 3000, 2500, 100, 20000, 5000

FIRST_NAMES = ["Ana", "Luis", "Maria", "John", "Emily", "Carlos", "Sofia", "James", "Olivia", "Daniel",
               "Laura", "Miguel", "Grace", "Peter", "Lucia", "Henry", "Paula", "Diego", "Nora", "Samuel"]
LAST_NAMES = ["Smith", "Garcia", "Johnson", "Lopez", "Brown", "Martinez", "Davis", "Rodriguez", "Miller",
              "Hernandez", "Wilson", "Gonzalez", "Moore", "Perez", "Taylor", "Sanchez", "Anderson",
              "Ramirez", "Thomas", "Torres", "Jackson", "Flores", "White", "Rivera", "Harris", "Gomez",
              "Martin", "Diaz", "Thompson", "Reyes"]
TEST_EMAILS = ["test@fundo.com", "testing@fundo.com", "qa@example.com", "dummy@example.com",
               "test+1@example.com", " QA+signup@Fundo.com ", "Testing+abc@example.com", "dummy+2@fundo.com"]
STAFF_EMAILS = ["ana.ruiz@fundo.com", "mike.chen@fundo.com", "sara.lee@fundo.com",
                "tom.baker@fundo.com", "eva.cruz@fundo.com"]
# "000000000" twice: equal invalid documents must still not be grouped.
INVALID_DOCUMENTS = [None, "", "   ", "NA", "NULL", "unknown", "None", "000000000", "000000000", "12-34"]
MALFORMED_EMAILS = ["user{}@", "user{}.mail.com", "user{}@mail", "user {}@mail.com", "@mail{}.com"]
MALFORMED_PHONES = ["12345", "555-01{:02d}", "+44 20 7946 0{:03d}", "2{:010d}", "N/A"]
ADVANCE_STATUSES = ["requested", "funded", "paid_off", "rejected"]


def _timestamps(rng: random.Random, n: int, start: datetime = YEAR_START) -> list[datetime]:
    """n sorted timestamps in one year, at most one per minute, with microseconds."""
    minutes = sorted(rng.sample(range(MINUTES_IN_YEAR), n))
    return [start + timedelta(minutes=m, microseconds=rng.randrange(60_000_000)) for m in minutes]


def _amount(rng: random.Random) -> Decimal:
    return Decimal(rng.randrange(1_000, 100_000)) / 100


def _doc_formats(doc: str) -> list[str]:
    """One document in the four formats the source mixes, so grouping must normalize it."""
    return [doc, f"{doc[:3]}-{doc[3:5]}-{doc[5:]}", f"{doc[:3]}.{doc[3:6]}.{doc[6:]}", f" {doc[:3]} {doc[3:5]} {doc[5:]} "]


def build(rng: random.Random) -> dict[str, list[dict]]:
    """All source rows plus seed_manifest, deterministic for a given rng."""
    # Seed documents stay below 900000000; mutate draws new ones above it, so they never collide.
    documents = iter(d for d in map(str, rng.sample(range(100_000_000, 900_000_000), 3 * TOTAL_CUSTOMERS)) if len(set(d)) > 1)
    customers: list[dict] = []

    def person(**fields) -> dict:
        cid = len(customers) + 1
        first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        c = {"customer_id": cid, "first_name": first, "last_name": last,
             "email": f"{first}.{last}.{cid}@mail.com".lower(), "phone": f"555{cid:07d}",
             "date_of_birth": date(1950, 1, 1) + timedelta(days=rng.randrange(20_000)),
             "document_number": next(documents), "case": None, "activity": "random",
             "protected_status": None, "no_protect": False, "expected": ("survivor", cid)}
        c.update(fields)
        customers.append(c)
        return c

    def group(case: str, statuses: list[str | None]) -> None:
        """Duplicates of one person; a non-None status gives that member a protected advance."""
        doc, n = next(documents), len(customers)
        shared = {"first_name": rng.choice(FIRST_NAMES), "last_name": rng.choice(LAST_NAMES),
                  "date_of_birth": date(1950, 1, 1) + timedelta(days=rng.randrange(20_000))}
        email, phone = f"{shared['first_name']}.{shared['last_name']}.g{n}@mail.com".lower(), f"556{n:07d}"
        emails = [email, email.upper(), f"  {email} "]
        phones = [phone, f"({phone[:3]}) {phone[3:6]}-{phone[6:]}", f"+1 {phone[:3]} {phone[3:6]} {phone[6:]}"]
        members = [person(case=case, document_number=_doc_formats(doc)[i % 4], email=emails[i % 3],
                          phone=phones[i % 3], protected_status=s, no_protect=s is None, **shared)
                   for i, s in enumerate(statuses)]
        protected = [m for m in members if m["protected_status"]]
        if len(protected) >= 2:
            for m in members:
                m["expected"] = ("needs_review", None)
            return
        survivor = protected[0] if protected else members[0]  # members[0] is the oldest
        for m in members:
            m["expected"] = ("survivor" if m is survivor else "merged", survivor["customer_id"])

    for _ in range(40):
        group("dup_group", [None] * rng.choice([2, 3]))
    group("group_one_protected", [None, " Funded", None])
    group("group_two_protected", ["PAID_OFF", "funded"])
    for status in ["Funded ", "PAID_OFF", " paid_off"]:
        group("dirty_status_protected", [None, status])
    for email in TEST_EMAILS:
        person(case="test_no_activity", email=email, activity="none", expected=("excluded_test", None))
    person(case="test_with_activity", email="test+ops@fundo.com", activity="required")
    for _ in range(3):
        person(case="testerman", last_name="Testerman", activity="required")
    for email in STAFF_EMAILS:
        person(case="staff", email=email, activity="required")
    for i in range(6):
        a, b = person(case="shared_contact"), person(case="shared_contact")
        if i < 3:
            b["email"] = a["email"]
        else:
            b["phone"] = a["phone"]
    for raw in INVALID_DOCUMENTS:
        person(case="invalid_document", document_number=raw)
    for i in range(30):
        person(case="malformed_email", email=MALFORMED_EMAILS[i % 5].format(i))
    for i in range(30):
        person(case="malformed_phone", phone=MALFORMED_PHONES[i % 5].format(i))
    for _ in range(INACTIVE_CUSTOMERS):
        person(activity="none")
    while len(customers) < TOTAL_CUSTOMERS:
        person()

    eligible = [c for c in customers if c["activity"] != "none"]
    required = [c for c in customers if c["activity"] == "required"]

    advances: list[dict] = []
    for c in customers:
        if c["protected_status"]:
            advances.append({"customer_id": c["customer_id"], "status": c["protected_status"]})
    while len(advances) < TOTAL_ADVANCES:
        c = rng.choice(eligible)
        # A random protected advance would change the expected survivor of a duplicate group.
        statuses = ["requested", "rejected"] if c["no_protect"] else ADVANCE_STATUSES
        advances.append({"customer_id": c["customer_id"], "status": rng.choice(statuses)})

    # Cards: 15 on customers that will be merged, one per required customer, the rest random.
    # The last IDLE_CARDS cards get no transactions, so mutate can delete cards without dependents.
    merged = [c for c in customers if c["case"] == "dup_group" and c["expected"][0] == "merged"]
    owners = merged[:15] + required
    owners += [rng.choice(eligible) for _ in range(TOTAL_CARDS - len(owners))]
    cards = [{"customer_id": c["customer_id"], "last_four": f"{rng.randrange(10_000):04d}",
              "brand": rng.choice(["visa", "mastercard"]), "status": rng.choice(["active", "active", "blocked"])}
             for c in owners]
    usable = list(enumerate(cards[:-IDLE_CARDS], start=1))

    transactions: list[dict] = []
    for c in required:
        card_id = next(i for i, card in usable if card["customer_id"] == c["customer_id"])
        transactions.append({"customer_id": c["customer_id"], "card_id": card_id})
    while len(transactions) < TOTAL_TRANSACTIONS:
        if rng.random() < 0.9:
            card_id, card = rng.choice(usable)
            transactions.append({"customer_id": card["customer_id"], "card_id": card_id})
        else:
            transactions.append({"customer_id": rng.choice(eligible)["customer_id"], "card_id": None})

    for rows, key in [(customers, None), (advances, "advance_id"), (cards, "card_id"), (transactions, "transaction_id")]:
        for i, (row, ts) in enumerate(zip(rows, _timestamps(rng, len(rows))), start=1):
            if key:
                row[key] = i
            row["created_at"] = row["updated_at"] = ts
    for row in advances + transactions:
        row["amount"] = _amount(rng)
    for row in transactions:
        row["occurred_at"] = row["created_at"]

    history = []
    for i, ts in enumerate(_timestamps(rng, TOTAL_HISTORY), start=1):
        c = rng.choice(customers)
        history.append({"history_id": i, "customer_id": c["customer_id"], "changed_at": ts,
                        "snapshot_json": json.dumps({"email": c["email"], "phone": c["phone"]})})
    scratch = [{"import_row": i, "legacy_ref": f"LEG-{i:05d}", "payload": json.dumps({"batch": i // 10}),
                "imported_at": ts} for i, ts in enumerate(_timestamps(rng, 50, datetime(2023, 1, 1)), start=1)]
    manifest = [{"customer_id": c["customer_id"], "case_name": c["case"], "expected_status": c["expected"][0],
                 "expected_survivor_id": c["expected"][1]} for c in customers if c["case"]]
    return {"customers": customers, "advances": advances, "cards": cards, "transactions": transactions,
            "customer_history": history, "tmp_import_2023": scratch, "seed_manifest": manifest}


COLUMNS = {
    "customers": ["customer_id", "document_number", "first_name", "last_name", "email", "phone",
                  "date_of_birth", "created_at", "updated_at"],
    "advances": ["advance_id", "customer_id", "amount", "status", "created_at", "updated_at"],
    "cards": ["card_id", "customer_id", "last_four", "brand", "status", "created_at", "updated_at"],
    "transactions": ["transaction_id", "customer_id", "card_id", "amount", "occurred_at", "created_at", "updated_at"],
    "customer_history": ["history_id", "customer_id", "changed_at", "snapshot_json"],
    "tmp_import_2023": ["import_row", "legacy_ref", "payload", "imported_at"],
    "seed_manifest": ["customer_id", "case_name", "expected_status", "expected_survivor_id"],
}


def insert(cur, table: str, columns: list[str], rows: list[dict]) -> None:
    """Multi-row INSERT in batches of 1000, the SQL Server limit for a VALUES list."""
    placeholders = "(" + ", ".join(["%s"] * len(columns)) + ")"
    for start in range(0, len(rows), 1000):
        chunk = rows[start:start + 1000]
        cur.execute(
            f"INSERT INTO dbo.{table} ({', '.join(columns)}) VALUES " + ", ".join([placeholders] * len(chunk)),
            tuple(db.sql_param(row[c]) for row in chunk for c in columns),
        )


def _wait_for_server(timeout_seconds: int = 120):
    """Retry for up to 120 s: SQL Server accepts logins a while after its container starts."""
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            return db.mssql("sa", database="master", autocommit=True)
        except pymssql.Error:
            if time.monotonic() > deadline:
                return None
            time.sleep(2)


def main() -> int:
    master = _wait_for_server()
    if master is None:
        print("SQL Server did not respond within 120 s", file=sys.stderr)
        return 2
    cur = master.cursor()
    # SINGLE_USER closes open connections, which would otherwise block DROP DATABASE.
    cur.execute(f"IF DB_ID('{db.DATABASE}') IS NOT NULL BEGIN "
                f"ALTER DATABASE {db.DATABASE} SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE {db.DATABASE}; END")
    cur.execute(f"CREATE DATABASE {db.DATABASE}")
    # Logins live in master and outlive the dropped database, so create it only once.
    reader_password = os.environ["ETL_READER_PASSWORD"].replace("'", "''")
    cur.execute(f"IF SUSER_ID('etl_reader') IS NULL CREATE LOGIN etl_reader WITH PASSWORD = '{reader_password}'")
    master.close()

    conn = db.mssql("sa")
    cur = conn.cursor()
    cur.execute(Path(__file__).with_name("schema.sql").read_text())
    cur.execute("CREATE USER etl_reader FOR LOGIN etl_reader; ALTER ROLE db_datareader ADD MEMBER etl_reader;")
    data = build(random.Random(SEED))
    for table, columns in COLUMNS.items():
        insert(cur, table, columns, data[table])
    conn.commit()
    conn.close()
    print("seed: " + ", ".join(f"{table}={len(rows)}" for table, rows in data.items()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
