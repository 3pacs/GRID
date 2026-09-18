"""Tests for governance.promotion_ledger.

Uses an in-memory sqlite engine (no local Postgres per this
workstream's constraints) — the DDL in
governance.promotion_ledger.PROMOTION_LEDGER_DDL is dialect-agnostic
(no JSONB/NOW() dependency; all timestamps are supplied by Python), so
this exercises the real SQL, not a mock.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from governance import promotion_ledger as ledger


@pytest.fixture()
def engine() -> Engine:
    eng = create_engine("sqlite:///:memory:")
    ledger.ensure_schema(eng)
    return eng


def test_recommend_writes_unapproved_record(engine):
    rec = ledger.recommend(
        engine,
        kind="weight_override",
        subject_hash="abc123",
        evaluation_version="v1",
        evidence_ref="s3://evidence/report.md",
        recommended_by="researcher@example",
    )
    assert rec["approved_by"] is None
    assert rec["approved_at"] is None
    assert rec["recommendation_id"] is None

    assert not ledger.is_approved(
        engine, kind="weight_override", subject_hash="abc123", evaluation_version="v1"
    )


def test_approve_creates_new_record_and_does_not_mutate_recommendation(engine):
    rec = ledger.recommend(
        engine,
        kind="model",
        subject_hash="hash-1",
        evaluation_version="v1",
        evidence_ref="evidence",
        recommended_by="researcher@example",
    )
    before = ledger.list_records(engine, subject_hash="hash-1")
    assert len(before) == 1

    approval = ledger.approve(engine, recommendation_id=rec["id"], approved_by="operator@example")

    assert approval["id"] != rec["id"]
    assert approval["recommendation_id"] == rec["id"]
    assert approval["approved_by"] == "operator@example"
    assert approval["approved_at"] is not None

    all_records = ledger.list_records(engine, subject_hash="hash-1")
    assert len(all_records) == 2, "approve() must INSERT, not UPDATE"

    # The original recommendation row is byte-for-byte unmutated.
    original_after = next(r for r in all_records if r["id"] == rec["id"])
    assert original_after["approved_by"] is None
    assert original_after["approved_at"] is None

    assert ledger.is_approved(
        engine, kind="model", subject_hash="hash-1", evaluation_version="v1"
    )


def test_ledger_is_append_only_no_update_delete_api_exists(engine):
    """There is no update/delete function in the public API at all —
    this test documents that invariant so a future addition of one
    trips a reviewer's attention."""
    public_names = {n for n in dir(ledger) if not n.startswith("_")}
    forbidden = {"update", "delete", "mutate", "modify"}
    assert not (public_names & forbidden), (
        f"promotion_ledger gained a mutating function: {public_names & forbidden}"
    )


def test_approve_rejects_unknown_recommendation(engine):
    with pytest.raises(ValueError):
        ledger.approve(engine, recommendation_id="does-not-exist", approved_by="operator")


def test_approve_rejects_approving_an_approval(engine):
    rec = ledger.recommend(
        engine,
        kind="signal_policy",
        subject_hash="hash-2",
        evaluation_version="v1",
        evidence_ref="evidence",
        recommended_by="researcher",
    )
    approval = ledger.approve(engine, recommendation_id=rec["id"], approved_by="operator")

    with pytest.raises(ValueError):
        ledger.approve(engine, recommendation_id=approval["id"], approved_by="someone_else")


def test_recommend_rejects_invalid_kind(engine):
    with pytest.raises(ValueError):
        ledger.recommend(
            engine,
            kind="not_a_real_kind",
            subject_hash="h",
            evaluation_version="v1",
            evidence_ref="e",
            recommended_by="r",
        )


def test_is_approved_requires_exact_match_on_all_three_keys(engine):
    rec = ledger.recommend(
        engine,
        kind="weight_override",
        subject_hash="hash-3",
        evaluation_version="v1",
        evidence_ref="evidence",
        recommended_by="researcher",
    )
    ledger.approve(engine, recommendation_id=rec["id"], approved_by="operator")

    assert ledger.is_approved(
        engine, kind="weight_override", subject_hash="hash-3", evaluation_version="v1"
    )
    # Wrong kind
    assert not ledger.is_approved(
        engine, kind="model", subject_hash="hash-3", evaluation_version="v1"
    )
    # Wrong subject_hash
    assert not ledger.is_approved(
        engine, kind="weight_override", subject_hash="hash-OTHER", evaluation_version="v1"
    )
    # Wrong evaluation_version
    assert not ledger.is_approved(
        engine, kind="weight_override", subject_hash="hash-3", evaluation_version="v2"
    )


def test_compute_subject_hash_deterministic():
    h1 = ledger.compute_subject_hash({"a": 1, "b": 2})
    h2 = ledger.compute_subject_hash({"b": 2, "a": 1})  # key order shouldn't matter
    assert h1 == h2

    h3 = ledger.compute_subject_hash({"a": 1, "b": 3})
    assert h3 != h1
