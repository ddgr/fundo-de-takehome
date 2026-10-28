"""Identity rules on hand-built customers; no database needed."""

from datetime import date, datetime

import pytest

from pipeline.dedup import (document_key, email_is_valid, is_protected_status, is_test_pattern,
                            is_valid_document, phone_is_valid, resolve)


@pytest.mark.parametrize("raw, key", [
    ("123-45-6789", "123456789"), (" 123 45 6789 ", "123456789"), ("123.456.789", "123456789"),
    ("ab-c", "ABC"), ("", None), ("   ", None), (None, None),
])
def test_document_key(raw, key):
    assert document_key(raw) == key


@pytest.mark.parametrize("key, valid", [
    ("123456789", True), ("000000000", False), ("12345", False), ("NA", False), ("NULL", False), (None, False),
])
def test_is_valid_document(key, valid):
    assert is_valid_document(key) is valid


@pytest.mark.parametrize("raw, valid", [
    ("a@b.com", True), ("  A@B.COM ", True), ("user@", False), ("user@mail", False),
    ("user 1@mail.com", False), ("@mail.com", False), ("", None), (None, None),
])
def test_email_is_valid(raw, valid):
    assert email_is_valid(raw) is valid


@pytest.mark.parametrize("raw, valid", [
    ("5551234567", True), ("(555) 123-4567", True), ("+1 555 123 4567", True), ("25551234567", False),
    ("12345", False), ("N/A", False), (" ", None), (None, None),
])
def test_phone_is_valid(raw, valid):
    assert phone_is_valid(raw) is valid


@pytest.mark.parametrize("raw, pattern", [
    ("test@fundo.com", True), (" QA+signup@Fundo.com ", True), ("dummy+2@example.com", True),
    ("ana.ruiz@fundo.com", False), ("test@gmail.com", False), ("testerman@mail.com", False), (None, False),
])
def test_is_test_pattern(raw, pattern):
    assert is_test_pattern(raw) is pattern


@pytest.mark.parametrize("status, protected", [
    ("funded", True), (" Funded", True), ("PAID_OFF", True), ("paid_off ", True),
    ("requested", False), ("rejected", False), (None, False),
])
def test_is_protected_status(status, protected):
    assert is_protected_status(status) is protected


def _customer(cid, doc, email=None, last="Smith", created=None, phone=None):
    """Defaults for every field, so each test sets only what it checks."""
    return {"customer_id": cid, "document_number": doc, "first_name": "Ana", "last_name": last,
            "email": email or f"c{cid}@mail.com", "phone": phone or f"555{cid:07d}",
            "date_of_birth": date(1990, 1, cid % 28 + 1), "created_at": created or datetime(2025, 1, cid)}


def _by_id(rows):
    return {r["customer_id"]: r for r in rows}


def test_group_without_protected_keeps_oldest():
    rows = _by_id(resolve([_customer(1, "123456789"), _customer(2, "123-45-6789")], set(), set()))
    assert (rows[1]["status"], rows[1]["reason"]) == ("survivor", "oldest_created_at")
    assert (rows[2]["status"], rows[2]["survivor_customer_id"], rows[2]["reason"]) == ("merged", 1, "merged_into_oldest")


def test_oldest_tie_breaks_on_lowest_id():
    same = datetime(2025, 3, 1)
    rows = _by_id(resolve([_customer(5, "123456789", created=same), _customer(4, "123456789", created=same)], set(), set()))
    assert rows[5]["survivor_customer_id"] == 4


def test_protected_member_survives_even_if_newer():
    rows = _by_id(resolve([_customer(1, "123456789"), _customer(2, "123456789")], {2}, set()))
    assert (rows[2]["status"], rows[2]["reason"]) == ("survivor", "protected")
    assert (rows[1]["status"], rows[1]["survivor_customer_id"]) == ("merged", 2)


def test_two_protected_send_whole_group_to_review():
    rows = resolve([_customer(1, "123456789"), _customer(2, "123456789"), _customer(3, "123456789")], {1, 2}, set())
    assert {(r["status"], r["survivor_customer_id"]) for r in rows} == {("needs_review", None)}


def test_invalid_documents_are_never_grouped():
    rows = resolve([_customer(1, "000000000"), _customer(2, "000000000"), _customer(3, None)], set(), set())
    assert {(r["status"], r["reason"]) for r in rows} == {("survivor", "no_valid_document")}


def test_test_account_without_activity_is_excluded():
    rows = _by_id(resolve([_customer(1, "123456789", email="test@fundo.com")], set(), set()))
    assert (rows[1]["status"], rows[1]["survivor_customer_id"], rows[1]["reason"]) == ("excluded_test", None, "test_account")


def test_test_pattern_with_activity_is_kept_and_flagged():
    rows = _by_id(resolve([_customer(1, "123456789", email="test@fundo.com")], set(), {1}))
    assert (rows[1]["status"], rows[1]["is_test_pattern"]) == ("survivor", True)


def test_testerman_and_staff_are_not_test_accounts():
    rows = resolve([_customer(1, "123456789", last="Testerman"), _customer(2, "987654321", email="ana.ruiz@fundo.com")],
                   set(), set())
    assert {r["status"] for r in rows} == {"survivor"}


def test_shared_email_only_suggests():
    rows = _by_id(resolve([_customer(1, "123456789", email="x@mail.com"), _customer(2, "987654321", email=" X@mail.com")],
                          set(), set()))
    assert (rows[1]["status"], rows[1]["suggested_match_id"], rows[1]["suggestion_basis"]) == ("survivor", 2, "email")
    assert (rows[2]["status"], rows[2]["suggested_match_id"]) == ("survivor", 1)


def test_malformed_contacts_do_not_suggest():
    rows = resolve([_customer(1, "123456789", email="user@", phone="12345"),
                    _customer(2, "987654321", email="user@", phone="12345")], set(), set())
    assert all(r["suggestion_basis"] is None for r in rows)
