"""Rebuild customer_resolution from the active rows of the raw layer."""

import re
import sys
from collections import Counter, defaultdict

from pipeline import db

DOCUMENT_NOISE = re.compile(r"[\s.\-]")
EMAIL = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
TEST_LOCAL_PARTS = {"test", "testing", "qa", "dummy"}
TEST_DOMAINS = {"fundo.com", "example.com"}
PROTECTED_STATUSES = {"funded", "paid_off"}

RESOLUTION_COLUMNS = {
    "customer_id": "INTEGER", "status": "VARCHAR", "survivor_customer_id": "INTEGER", "reason": "VARCHAR",
    "document_key": "VARCHAR", "is_test_pattern": "BOOLEAN", "email_is_valid": "BOOLEAN",
    "phone_is_valid": "BOOLEAN", "suggested_match_id": "INTEGER", "suggestion_basis": "VARCHAR",
}


def document_key(raw: str | None) -> str | None:
    """Document without spaces, dots or hyphens, upper-cased; None when nothing is left."""
    if raw is None:
        return None
    return DOCUMENT_NOISE.sub("", raw).upper() or None


def is_valid_document(key: str | None) -> bool:
    """Nine digits (US national id), not a single repeated digit."""
    return key is not None and re.fullmatch(r"\d{9}", key) is not None and len(set(key)) > 1


def _blank(raw: str | None) -> bool:
    return raw is None or not raw.strip()


def email_is_valid(raw: str | None) -> bool | None:
    """None when empty, otherwise whether the trimmed, lower-cased email has a valid shape."""
    return None if _blank(raw) else EMAIL.fullmatch(raw.strip().lower()) is not None


def phone_is_valid(raw: str | None) -> bool | None:
    """None when empty; valid with 10 digits, or 11 starting with 1."""
    if _blank(raw):
        return None
    digits = re.sub(r"\D", "", raw)
    return len(digits) == 10 or (len(digits) == 11 and digits.startswith("1"))


def normalized_phone(raw: str) -> str:
    """Last 10 digits, so +1 555 123 4567 and (555) 123-4567 compare equal."""
    return re.sub(r"\D", "", raw)[-10:]


def is_test_pattern(raw: str | None) -> bool:
    """Local part (without +alias) is a test word and the domain is fundo.com or example.com."""
    if _blank(raw):
        return False
    local, _, domain = raw.strip().lower().rpartition("@")
    return local.split("+", 1)[0] in TEST_LOCAL_PARTS and domain in TEST_DOMAINS


def is_protected_status(status: str | None) -> bool:
    """Ignores spaces and case: the source mixes ' Funded', 'PAID_OFF' and 'paid_off '."""
    return status is not None and "".join(status.split()).lower() in PROTECTED_STATUSES


def resolve(customers: list[dict], protected_ids: set[int], active_ids: set[int]) -> list[dict]:
    """Rules in order, first match wins; one row per customer. No I/O, so the tests call it directly."""
    rows, groups = {}, defaultdict(list)
    for c in customers:
        cid, key = c["customer_id"], document_key(c["document_number"])
        row = {"customer_id": cid, "status": "survivor", "survivor_customer_id": cid, "reason": None,
               "document_key": key, "is_test_pattern": is_test_pattern(c["email"]),
               "email_is_valid": email_is_valid(c["email"]), "phone_is_valid": phone_is_valid(c["phone"]),
               "suggested_match_id": None, "suggestion_basis": None}
        rows[cid] = row
        if row["is_test_pattern"] and cid not in active_ids:
            row.update(status="excluded_test", survivor_customer_id=None, reason="test_account")
        elif not is_valid_document(key):
            row["reason"] = "no_valid_document"
        else:
            groups[key].append(c)

    for members in groups.values():
        protected = [m for m in members if m["customer_id"] in protected_ids]
        if len(members) == 1:
            rows[members[0]["customer_id"]]["reason"] = "single"
        elif len(protected) >= 2:
            for m in members:
                rows[m["customer_id"]].update(status="needs_review", survivor_customer_id=None, reason="multiple_protected")
        else:
            if protected:
                survivor, kept, merged = protected[0], "protected", "merged_into_protected"
            else:
                survivor = min(members, key=lambda m: (m["created_at"], m["customer_id"]))
                kept, merged = "oldest_created_at", "merged_into_oldest"
            for m in members:
                is_survivor = m is survivor
                rows[m["customer_id"]].update(status="survivor" if is_survivor else "merged",
                                              survivor_customer_id=survivor["customer_id"],
                                              reason=kept if is_survivor else merged)

    _suggest(customers, rows)
    return list(rows.values())


def _suggest(customers: list[dict], rows: dict[int, dict]) -> None:
    """Lowest customer_id in another group sharing email, phone or name + birth date; a hint, never a merge."""
    # Members of one group are already resolved together; an invalid document is a group of one.
    def group_of(c):
        key = rows[c["customer_id"]]["document_key"]
        return key if is_valid_document(key) else f"#{c['customer_id']}"

    candidates = [c for c in customers if rows[c["customer_id"]]["status"] != "excluded_test"]
    values = {
        "email": lambda c: c["email"].strip().lower() if email_is_valid(c["email"]) else None,
        "phone": lambda c: normalized_phone(c["phone"]) if phone_is_valid(c["phone"]) else None,
        "name_dob": lambda c: (c["first_name"].strip().lower(), c["last_name"].strip().lower(), c["date_of_birth"])
        if c["date_of_birth"] else None,
    }
    index = {basis: defaultdict(list) for basis in values}
    for c in candidates:
        for basis, value_of in values.items():
            if (value := value_of(c)) is not None:
                index[basis][value].append(c)
    for c in candidates:
        for basis, value_of in values.items():
            value = value_of(c)
            others = [o["customer_id"] for o in index[basis].get(value, []) if group_of(o) != group_of(c)]
            if value is not None and others:
                rows[c["customer_id"]].update(suggested_match_id=min(others), suggestion_basis=basis)
                break


def _ids(wh, sql: str) -> set[int]:
    return {r[0] for r in wh.execute(sql).fetchall()}


def main() -> int:
    wh = db.warehouse()
    cols = ["customer_id", "document_number", "first_name", "last_name", "email", "phone", "date_of_birth", "created_at"]
    customers = [dict(zip(cols, r)) for r in wh.execute(
        f"SELECT {', '.join(cols)} FROM raw_customers WHERE _deleted_at IS NULL ORDER BY customer_id").fetchall()]
    protected = {cid for cid, status in wh.execute(
        "SELECT customer_id, status FROM raw_advances WHERE _deleted_at IS NULL").fetchall() if is_protected_status(status)}
    active = _ids(wh, "SELECT customer_id FROM raw_advances WHERE _deleted_at IS NULL "
                      "UNION SELECT customer_id FROM raw_cards WHERE _deleted_at IS NULL "
                      "UNION SELECT customer_id FROM raw_transactions WHERE _deleted_at IS NULL")
    rows = resolve(customers, protected, active)

    # One transaction: readers never see a half-built resolution or views out of step with it.
    wh.execute("BEGIN")
    columns = ", ".join(f"{n} {t}" for n, t in RESOLUTION_COLUMNS.items())
    wh.execute(f"CREATE OR REPLACE TABLE customer_resolution ({columns}, PRIMARY KEY (customer_id))")
    db.insert_rows(wh, "customer_resolution", RESOLUTION_COLUMNS, [tuple(r[c] for c in RESOLUTION_COLUMNS) for r in rows])
    wh.execute("""
        CREATE OR REPLACE VIEW dim_customer AS
        SELECT c.* EXCLUDE (_row_hash, _deleted_at, _last_changed_run), r.status, r.is_test_pattern,
               r.email_is_valid, r.phone_is_valid, r.suggested_match_id, r.suggestion_basis
        FROM raw_customers c JOIN customer_resolution r USING (customer_id)
        WHERE c._deleted_at IS NULL AND r.status IN ('survivor', 'needs_review')""")
    # A card moves to its owner's survivor; with no survivor (needs_review, excluded_test) it stays put.
    wh.execute("""
        CREATE OR REPLACE VIEW cards_resolved AS
        SELECT k.card_id, k.customer_id AS original_customer_id,
               COALESCE(r.survivor_customer_id, k.customer_id) AS owner_customer_id,
               k.last_four, k.brand, k.status
        FROM raw_cards k LEFT JOIN customer_resolution r ON r.customer_id = k.customer_id
        WHERE k._deleted_at IS NULL""")
    wh.execute("COMMIT")

    def tally(field):
        c = Counter(r[field] for r in rows)
        return f"valid={c[True]} malformed={c[False]} empty={c[None]}"

    statuses = Counter(r["status"] for r in rows)
    bases = Counter(r["suggestion_basis"] for r in rows if r["suggestion_basis"])
    print(f"customer_resolution: {len(rows)} active customers")
    print("  status   " + " ".join(f"{s}={statuses[s]}" for s in ["survivor", "merged", "needs_review", "excluded_test"]))
    print(f"  email    {tally('email_is_valid')}")
    print(f"  phone    {tally('phone_is_valid')}")
    print(f"  test pattern with activity (kept for review): {sum(r['is_test_pattern'] and r['status'] != 'excluded_test' for r in rows)}")
    print("  suggestions " + " ".join(f"{b}={bases[b]}" for b in ["email", "phone", "name_dob"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
