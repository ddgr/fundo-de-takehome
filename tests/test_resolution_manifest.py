"""customer_resolution against seed_manifest; needs `make load` and `make dedup` first."""

import os

import pytest

from pipeline import db


@pytest.fixture(scope="module")
def resolution():
    if not os.path.exists(db.WAREHOUSE_PATH):
        pytest.skip("warehouse not found: run make load and make dedup")
    wh = db.warehouse(read_only=True)
    if not wh.execute("SELECT COUNT(*) FROM duckdb_tables() WHERE table_name = 'customer_resolution'").fetchone()[0]:
        pytest.skip("customer_resolution not found: run make dedup")
    cols = ["customer_id", "status", "survivor_customer_id", "email_is_valid", "phone_is_valid", "suggested_match_id"]
    rows = {r[0]: dict(zip(cols, r)) for r in wh.execute(f"SELECT {', '.join(cols)} FROM customer_resolution").fetchall()}
    wh.close()
    return rows


@pytest.fixture(scope="module")
def manifest():
    conn = db.mssql("etl_reader")
    cur = conn.cursor()
    cur.execute("SELECT customer_id, case_name, expected_status, expected_survivor_id FROM dbo.seed_manifest")
    rows = cur.fetchall()
    conn.close()
    return rows


def test_every_case_matches_expected_status_and_survivor(resolution, manifest):
    wrong = [(cid, case, status, survivor, resolution.get(cid))
             for cid, case, status, survivor in manifest
             if cid not in resolution
             or (resolution[cid]["status"], resolution[cid]["survivor_customer_id"]) != (status, survivor)]
    assert wrong == []


def test_shared_contacts_are_suggested_not_merged(resolution, manifest):
    shared = [cid for cid, case, *_ in manifest if case == "shared_contact"]
    assert len(shared) == 12
    assert all(resolution[cid]["suggested_match_id"] is not None for cid in shared)


def test_malformed_contacts_are_flagged(resolution, manifest):
    for field, case in [("email_is_valid", "malformed_email"), ("phone_is_valid", "malformed_phone")]:
        ids = [cid for cid, c, *_ in manifest if c == case]
        assert len(ids) == 30
        assert all(resolution[cid][field] is False for cid in ids)
        # Only the seeded cases are malformed, so any other False is a false positive.
        assert sum(r[field] is False for r in resolution.values()) == 30
