"""
Tests for the user_intel cooperative contribution router.

All DB access is mocked with an in-memory fake so tests never hit a live DB.
"""

from __future__ import annotations

import asyncio
import os
from types import SimpleNamespace
from unittest.mock import patch

import pytest

os.environ.setdefault("GRID_JWT_SECRET", "test-secret-key-for-testing-only")

from api.routers import user_intel
from api.routers.user_intel import (
    IntelSubmission,
    VALID_TYPES,
    _validate_submission,
    flag_intel,
    get_actor_intel,
    submit_intel,
    verify_intel,
    vote_intel,
)


# ── In-memory fake engine ─────────────────────────────────────────────────


class _FakeResult:
    def __init__(self, rows):
        self._rows = list(rows)

    def first(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def scalar(self):
        if not self._rows:
            return None
        r = self._rows[0]
        if hasattr(r, "_mapping"):
            vals = list(r._mapping.values())
            return vals[0] if vals else None
        if isinstance(r, (tuple, list)):
            return r[0]
        return r


class _Row:
    """Tiny row that mimics both ``row._mapping`` and attribute access."""

    def __init__(self, d):
        self._mapping = dict(d)
        for k, v in d.items():
            setattr(self, k, v)


class _FakeConn:
    def __init__(self, store):
        self.store = store

    def execute(self, stmt, *args, **kwargs):
        # SQLAlchemy TextClause with bindparams keeps params on the compiled form.
        sql = str(getattr(stmt, "text", stmt)).strip().lower()
        params = {}
        compiled = getattr(stmt, "_bindparams", None) or {}
        for name, bp in compiled.items():
            params[name] = getattr(bp, "value", None)

        store = self.store

        # ── COUNT for rate limit ─────────────────────────────────────
        if sql.startswith("select count(*) from user_intel"):
            u = params.get("u")
            count = sum(1 for r in store["intel"] if r["submitted_by"] == u)
            return _FakeResult([_Row({"count": count})])

        # ── UPSERT vote (check before generic insert into user_intel) ──
        if sql.startswith("insert into user_intel_votes"):
            found = False
            for v in store["votes"]:
                if v["intel_id"] == params["i"] and v["user_id"] == params["u"]:
                    v["vote"] = params["v"]
                    found = True
                    break
            if not found:
                store["votes"].append(
                    {"intel_id": params["i"], "user_id": params["u"], "vote": params["v"]}
                )
            return _FakeResult([])

        # ── INSERT user_intel ────────────────────────────────────────
        if sql.startswith("insert into user_intel"):
            new_id = len(store["intel"]) + 1
            row = {
                "id": new_id,
                "actor_id": params["actor_id"],
                "intel_type": params["intel_type"],
                "note": params["note"],
                "source_url": params.get("source_url"),
                "confidence": params.get("confidence"),
                "submitted_by": params["submitted_by"],
                "submitted_at": None,
                "verified_by": None,
                "verified_at": None,
                "verification_status": "pending",
                "upvotes": 0,
                "downvotes": 0,
                "flags": 0,
            }
            store["intel"].append(row)
            return _FakeResult([_Row(row)])

        # ── SELECT intel for actor ───────────────────────────────────
        if "from user_intel" in sql and "where actor_id = :a" in sql:
            rows = [
                r for r in store["intel"]
                if r["actor_id"] == params["a"]
                and r["verification_status"] != "rejected"
            ]
            rows.sort(
                key=lambda r: (-(r["upvotes"] - r["downvotes"]),),
            )
            return _FakeResult([_Row(r) for r in rows[: params.get("lim", 50)]])

        # ── SELECT vote for dedup ────────────────────────────────────
        if sql.startswith("select vote from user_intel_votes"):
            for v in store["votes"]:
                if v["intel_id"] == params["i"] and v["user_id"] == params["u"]:
                    return _FakeResult([_Row({"vote": v["vote"]})])
            return _FakeResult([])

        # ── Aggregate vote counts ────────────────────────────────────
        if sql.startswith("select") and "sum(case when vote" in sql:
            up = sum(1 for v in store["votes"] if v["intel_id"] == params["i"] and v["vote"] == 1)
            dn = sum(1 for v in store["votes"] if v["intel_id"] == params["i"] and v["vote"] == -1)
            return _FakeResult([_Row({"up": up, "dn": dn})])

        # ── UPDATE aggregate on user_intel ───────────────────────────
        if sql.startswith("update user_intel set upvotes"):
            for r in store["intel"]:
                if r["id"] == params["i"]:
                    r["upvotes"] = params["up"]
                    r["downvotes"] = params["dn"]
            return _FakeResult([])

        # ── UPDATE flags ─────────────────────────────────────────────
        if sql.startswith("update user_intel set flags"):
            for r in store["intel"]:
                if r["id"] == params["i"]:
                    r["flags"] += 1
                    return _FakeResult([_Row({"id": r["id"], "flags": r["flags"]})])
            return _FakeResult([])

        # ── UPDATE verification ──────────────────────────────────────
        if sql.startswith("update user_intel"):
            for r in store["intel"]:
                if r["id"] == params["i"]:
                    r["verification_status"] = params["status"]
                    r["verified_by"] = params["verifier"]
                    r["verified_at"] = None
                    return _FakeResult([_Row(r)])
            return _FakeResult([])

        # ── Pending list ─────────────────────────────────────────────
        if "verification_status = 'pending'" in sql:
            rows = [r for r in store["intel"] if r["verification_status"] == "pending"]
            return _FakeResult([_Row(r) for r in rows[: params.get("lim", 100)]])

        return _FakeResult([])

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _FakeEngine:
    def __init__(self):
        self.store = {"intel": [], "votes": []}

    def connect(self):
        return _FakeConn(self.store)

    def begin(self):
        return _FakeConn(self.store)


@pytest.fixture
def fake_engine():
    eng = _FakeEngine()
    with patch.object(user_intel, "get_db_engine", return_value=eng):
        yield eng


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro) if not asyncio.iscoroutine(coro) else asyncio.run(coro)


# ── Tests ────────────────────────────────────────────────────────────────


def test_submission_shape(fake_engine):
    """A valid submission returns id + fields echoed back."""
    body = IntelSubmission(
        intel_type="loyalty",
        note="Ally of 3G Capital on KHC board",
        confidence="medium",
    )
    result = asyncio.run(submit_intel("nelson_peltz", body, "alice"))
    assert result["id"] == 1
    assert result["actor_id"] == "nelson_peltz"
    assert result["intel_type"] == "loyalty"
    assert result["submitted_by"] == "alice"
    assert result["verification_status"] == "pending"
    assert "score" in result
    assert result["score"] == 0


def test_validate_rejects_bad_type():
    """Invalid intel_type raises 400."""
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as exc:
        _validate_submission({"intel_type": "garbage", "note": "x"})
    assert exc.value.status_code == 400


def test_vote_dedup(fake_engine):
    """Same user voting twice updates, doesn't double-count."""
    asyncio.run(
        submit_intel(
            "tim_cook",
            IntelSubmission(intel_type="fact", note="CEO of Apple"),
            "alice",
        )
    )
    r1 = asyncio.run(vote_intel(1, 1, "bob"))
    assert r1["upvotes"] == 1 and r1["downvotes"] == 0
    r2 = asyncio.run(vote_intel(1, 1, "bob"))  # same user, same vote
    assert r2["upvotes"] == 1, "duplicate vote should not double-count"
    r3 = asyncio.run(vote_intel(1, -1, "bob"))  # flip
    assert r3["upvotes"] == 0 and r3["downvotes"] == 1


def test_verify_permission_path(fake_engine):
    """verify_intel marks status = 'verified'."""
    asyncio.run(
        submit_intel(
            "jane_fraser",
            IntelSubmission(intel_type="biography", note="Citi CEO since 2021"),
            "alice",
        )
    )
    result = asyncio.run(verify_intel(1, "verified", "admin_user"))
    assert result["verification_status"] == "verified"
    assert result["verified_by"] == "admin_user"


def test_sort_order_by_score(fake_engine):
    """get_actor_intel returns highest-score first."""
    for i in range(3):
        asyncio.run(
            submit_intel(
                "larry_fink",
                IntelSubmission(intel_type="tip", note=f"note {i}"),
                f"user{i}",
            )
        )
    # Upvote id=2 heavily
    asyncio.run(vote_intel(2, 1, "voter1"))
    asyncio.run(vote_intel(2, 1, "voter2"))
    # Downvote id=3
    asyncio.run(vote_intel(3, -1, "voter1"))

    items = asyncio.run(get_actor_intel("larry_fink", 10, "alice"))
    assert len(items) == 3
    # Highest score first
    assert items[0]["id"] == 2
    assert items[0]["score"] == 2
    assert items[-1]["id"] == 3
    assert items[-1]["score"] == -1


def test_rate_limit_enforced(fake_engine):
    """21st submission in an hour raises 429."""
    from fastapi import HTTPException
    body = IntelSubmission(intel_type="fact", note="x")
    limit = user_intel.RATE_LIMIT_PER_HOUR
    for _ in range(limit):
        asyncio.run(submit_intel("ray_dalio", body, "spammer"))
    with pytest.raises(HTTPException) as exc:
        asyncio.run(submit_intel("ray_dalio", body, "spammer"))
    assert exc.value.status_code == 429


def test_sql_injection_guard(fake_engine):
    """Injection attempts in actor_id/note are passed as bind params, not interpolated."""
    payload = "'; DROP TABLE user_intel; --"
    body = IntelSubmission(intel_type="rumor", note=payload)
    result = asyncio.run(submit_intel(payload, body, "mallory"))
    # The actor_id and note are stored verbatim — no execution, no truncation.
    assert result["actor_id"] == payload
    assert result["note"] == payload
    # Fetching back the actor should still return that item.
    items = asyncio.run(get_actor_intel(payload, 10, "mallory"))
    assert len(items) == 1
    assert items[0]["note"] == payload


def test_flag_increments(fake_engine):
    """Flagging increments the flags counter."""
    asyncio.run(
        submit_intel(
            "warren_buffett",
            IntelSubmission(intel_type="rumor", note="owns X"),
            "alice",
        )
    )
    r1 = asyncio.run(flag_intel(1, "bob"))
    assert r1["flags"] == 1
    r2 = asyncio.run(flag_intel(1, "carol"))
    assert r2["flags"] == 2


def test_valid_types_constant():
    """Required intel types are present."""
    for t in ("biography", "connection", "loyalty", "stance", "rumor", "tip", "fact"):
        assert t in VALID_TYPES
