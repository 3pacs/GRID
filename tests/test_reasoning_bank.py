"""Tests for intelligence.reasoning_bank.

These tests use a FakeEngine instead of mock_engine so we can capture
the SQL string and bound parameters and verify ranking semantics. PG-
specific constructs (jsonb, pgvector) are not exercised against a real
database; an integration test is marked separately and skipped unless
PostgreSQL with pgvector is reachable.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

import pytest

from intelligence.reasoning_bank import (
    DEFAULT_OVERLAP_KEYS,
    ReasoningLesson,
    build_fingerprint_from_decision_data,
    lesson_count,
    retrieve_lessons,
    write_reasoning_lesson,
)


# ── Fake engine plumbing ─────────────────────────────────────────────────


class _FakeRow:
    """Row that supports both attribute and index access (mirrors SQLA Row)."""

    def __init__(self, **fields: Any) -> None:
        self.__dict__.update(fields)
        # Preserve order for index access — Python 3.7+ dicts are ordered.
        self._values = list(fields.values())

    def __getitem__(self, idx: int) -> Any:
        return self._values[idx]


@dataclass
class _FakeResult:
    rows: list[_FakeRow] = field(default_factory=list)

    def fetchone(self) -> _FakeRow | None:
        return self.rows[0] if self.rows else None

    def fetchall(self) -> list[_FakeRow]:
        return list(self.rows)


@dataclass
class _FakeConn:
    captured: list[tuple[str, dict[str, Any]]] = field(default_factory=list)
    next_result: _FakeResult = field(default_factory=_FakeResult)
    raise_on_execute: Exception | None = None

    def execute(self, stmt: Any, params: dict[str, Any] | None = None) -> _FakeResult:
        if self.raise_on_execute is not None:
            raise self.raise_on_execute
        sql = str(getattr(stmt, "text", stmt))
        self.captured.append((sql, dict(params or {})))
        return self.next_result


class _CtxMgr:
    def __init__(self, conn: _FakeConn) -> None:
        self.conn = conn

    def __enter__(self) -> _FakeConn:
        return self.conn

    def __exit__(self, *_a: Any) -> bool:
        return False


@dataclass
class _FakeDialect:
    name: str = "postgresql"


class _FakeEngine:
    def __init__(
        self,
        next_result: _FakeResult | None = None,
        dialect_name: str = "postgresql",
        raise_on_execute: Exception | None = None,
    ) -> None:
        self.dialect = _FakeDialect(name=dialect_name)
        self.conn = _FakeConn(
            next_result=next_result or _FakeResult(),
            raise_on_execute=raise_on_execute,
        )

    def begin(self) -> _CtxMgr:
        return _CtxMgr(self.conn)

    def connect(self) -> _CtxMgr:
        return _CtxMgr(self.conn)


# ── write_reasoning_lesson ───────────────────────────────────────────────


def _lesson(**overrides: Any) -> ReasoningLesson:
    base: dict[str, Any] = dict(
        title="TSM long wrong_signal",
        description="iv_skew flipped post-entry",
        content="we missed earnings whisper drop | fix: gate on whisper",
        outcome_class="failure",
        condition_fingerprint={
            "ticker": "TSM",
            "direction": "long",
            "regime": "GROWTH",
            "fci_bucket": "EASY",
            "vol_bucket": "NORMAL",
            "horizon_bucket": "1w",
        },
        source_type="postmortem_failure",
        source_id="123",
    )
    base.update(overrides)
    return ReasoningLesson(**base)


def test_write_reasoning_lesson_round_trip_returns_id() -> None:
    engine = _FakeEngine(next_result=_FakeResult(rows=[_FakeRow(id=42)]))
    new_id = write_reasoning_lesson(engine, _lesson())
    assert new_id == 42

    sql, params = engine.conn.captured[0]
    # Parameterized — never interpolate user data into the SQL string.
    assert "INSERT INTO reasoning_lessons" in sql
    assert ":title" in sql and ":fingerprint" in sql
    assert params["title"] == "TSM long wrong_signal"
    assert params["outcome_class"] == "failure"
    assert params["source_id"] == "123"
    # Embedding should NOT be bound when none was supplied.
    assert "embedding" not in params
    assert "CAST(:embedding AS vector)" not in sql


def test_write_reasoning_lesson_with_embedding_uses_vector_cast() -> None:
    engine = _FakeEngine(next_result=_FakeResult(rows=[_FakeRow(id=7)]))
    vec = [0.1, 0.2, 0.3]
    new_id = write_reasoning_lesson(engine, _lesson(), embedding=vec)
    assert new_id == 7

    sql, params = engine.conn.captured[0]
    assert "CAST(:embedding AS vector)" in sql
    assert params["embedding"].startswith("[") and params["embedding"].endswith("]")
    # Float formatting check
    assert "0.100000" in params["embedding"]


def test_write_reasoning_lesson_returns_none_on_db_error() -> None:
    engine = _FakeEngine(raise_on_execute=RuntimeError("connection refused"))
    new_id = write_reasoning_lesson(engine, _lesson())
    assert new_id is None


def test_write_reasoning_lesson_skips_vector_cast_on_sqlite() -> None:
    # When the engine is non-PG, embedding is dropped (no pgvector).
    engine = _FakeEngine(
        next_result=_FakeResult(rows=[_FakeRow(id=99)]),
        dialect_name="sqlite",
    )
    new_id = write_reasoning_lesson(engine, _lesson(), embedding=[0.1] * 8)
    assert new_id == 99
    sql, params = engine.conn.captured[0]
    assert "CAST(:embedding AS vector)" not in sql
    assert "embedding" not in params


def test_write_reasoning_lesson_handles_unserializable_fingerprint() -> None:
    class _NotJson:
        pass

    bad = _lesson(condition_fingerprint={"weird": _NotJson()})
    engine = _FakeEngine(next_result=_FakeResult(rows=[_FakeRow(id=1)]))
    # Default-coercion via `default=str` keeps this serializable, so the
    # write should succeed end-to-end.
    new_id = write_reasoning_lesson(engine, bad)
    assert new_id == 1


# ── retrieve_lessons ─────────────────────────────────────────────────────


def _make_row(
    *, lesson_id: int = 1,
    title: str = "t",
    description: str = "d",
    content: str = "c",
    outcome_class: str = "failure",
    fingerprint: dict[str, Any] | None = None,
    source_type: str = "postmortem_failure",
    source_id: str | None = "s1",
    overlap_score: int = 0,
) -> _FakeRow:
    return _FakeRow(
        id=lesson_id,
        title=title,
        description=description,
        content=content,
        outcome_class=outcome_class,
        condition_fingerprint=fingerprint or {},
        source_type=source_type,
        source_id=source_id,
        created_at="2026-04-29T00:00:00Z",
        overlap_score=overlap_score,
    )


def test_retrieve_lessons_no_embedding_uses_overlap_ranking() -> None:
    rows = [
        _make_row(lesson_id=1, title="a", overlap_score=3),
        _make_row(lesson_id=2, title="b", overlap_score=1),
    ]
    engine = _FakeEngine(next_result=_FakeResult(rows=rows))
    out = retrieve_lessons(
        engine,
        fingerprint={
            "regime": "GROWTH",
            "fci_bucket": "EASY",
            "vol_bucket": "NORMAL",
            "horizon_bucket": "1w",
        },
        top_k=5,
    )
    assert [l.title for l in out] == ["a", "b"]

    sql, params = engine.conn.captured[0]
    # No vector ordering when no embedding supplied.
    assert "embedding <=>" not in sql
    assert "ORDER BY overlap_score DESC, created_at DESC" in sql
    # All four overlap keys should produce bind params.
    assert params["fpk_0"] == "regime"
    assert params["fpv_0"] == "GROWTH"
    assert params["top_k"] == 5


def test_retrieve_lessons_with_embedding_orders_by_cosine() -> None:
    rows = [_make_row(lesson_id=10, title="vec-hit", overlap_score=2)]
    engine = _FakeEngine(next_result=_FakeResult(rows=rows))
    out = retrieve_lessons(
        engine,
        fingerprint={"regime": "FRAGILE"},
        query_embedding=[0.5] * 8,
        top_k=3,
        outcome_class="failure",
    )
    assert len(out) == 1 and out[0].title == "vec-hit"

    sql, params = engine.conn.captured[0]
    assert "embedding <=> CAST(:qvec AS vector) ASC" in sql
    assert "embedding IS NOT NULL" in sql
    assert "outcome_class = :outcome_class" in sql
    assert params["outcome_class"] == "failure"
    assert params["qvec"].startswith("[")


def test_retrieve_lessons_with_embedding_falls_back_on_sqlite() -> None:
    rows = [_make_row(lesson_id=20, title="non-pg")]
    engine = _FakeEngine(
        next_result=_FakeResult(rows=rows),
        dialect_name="sqlite",
    )
    out = retrieve_lessons(
        engine,
        fingerprint={"regime": "CRISIS"},
        query_embedding=[0.1] * 8,
        top_k=2,
    )
    assert len(out) == 1 and out[0].title == "non-pg"
    sql, params = engine.conn.captured[0]
    # No pgvector path when dialect != postgresql.
    assert "embedding <=>" not in sql
    assert "qvec" not in params


def test_retrieve_lessons_returns_empty_on_db_error() -> None:
    engine = _FakeEngine(raise_on_execute=RuntimeError("boom"))
    out = retrieve_lessons(engine, fingerprint={"regime": "X"})
    assert out == []


def test_retrieve_lessons_zero_top_k_short_circuits() -> None:
    engine = _FakeEngine(next_result=_FakeResult(rows=[]))
    out = retrieve_lessons(engine, fingerprint={"regime": "X"}, top_k=0)
    assert out == []
    # Nothing should have been executed.
    assert engine.conn.captured == []


def test_retrieve_lessons_omits_outcome_filter_when_any() -> None:
    engine = _FakeEngine(next_result=_FakeResult(rows=[]))
    retrieve_lessons(
        engine,
        fingerprint={"regime": "GROWTH"},
        outcome_class="any",
    )
    sql, params = engine.conn.captured[0]
    assert "outcome_class" not in params
    assert "outcome_class = :outcome_class" not in sql


# ── lesson_count ─────────────────────────────────────────────────────────


def test_lesson_count_basic() -> None:
    engine = _FakeEngine(next_result=_FakeResult(rows=[_FakeRow(c=17)]))
    n = lesson_count(engine)
    assert n == 17
    sql, params = engine.conn.captured[0]
    assert "SELECT COUNT(*)" in sql
    assert params == {}


def test_lesson_count_with_outcome_filter() -> None:
    engine = _FakeEngine(next_result=_FakeResult(rows=[_FakeRow(c=4)]))
    n = lesson_count(engine, outcome_class="success")
    assert n == 4
    sql, params = engine.conn.captured[0]
    assert "WHERE outcome_class = :outcome_class" in sql
    assert params["outcome_class"] == "success"


def test_lesson_count_returns_zero_on_db_error() -> None:
    engine = _FakeEngine(raise_on_execute=RuntimeError("nope"))
    assert lesson_count(engine) == 0


# ── build_fingerprint_from_decision_data ─────────────────────────────────


def test_build_fingerprint_extracts_postmortem_shape() -> None:
    fp = build_fingerprint_from_decision_data(
        ticker="tsm",
        direction="LONG",
        data_at_decision={
            "regime": {"state": "GROWTH", "confidence": 0.8},
            "fci_regime": "EASY",
            "vix_level": 14.2,
        },
        horizon_days=7,
    )
    assert fp["ticker"] == "TSM"
    assert fp["direction"] == "long"
    assert fp["regime"] == "GROWTH"
    assert fp["liquidity_regime"] == "GROWTH"
    assert fp["fci_bucket"] == "EASY"
    assert fp["vix_level"] == 14.2
    assert fp["horizon_bucket"] == "1w"
    assert fp["horizon_days"] == 7


def test_build_fingerprint_handles_empty_input() -> None:
    fp = build_fingerprint_from_decision_data(
        ticker=None,
        direction=None,
        data_at_decision=None,
    )
    assert fp == {}


def test_build_fingerprint_horizon_buckets() -> None:
    cases = [
        (1, "intraday"),
        (3, "1w"),
        (15, "1m"),
        (60, "3m"),
        (200, "long"),
    ]
    for days, expected in cases:
        fp = build_fingerprint_from_decision_data(
            ticker="X", direction="long",
            data_at_decision={}, horizon_days=days,
        )
        assert fp["horizon_bucket"] == expected, (days, expected)


# ── Module-level invariants ──────────────────────────────────────────────


def test_default_overlap_keys_includes_required_fields() -> None:
    for required in ("regime", "fci_bucket", "vol_bucket", "horizon_bucket"):
        assert required in DEFAULT_OVERLAP_KEYS


# ── memory_lesson_conviction_multiplier ──────────────────────────────────


from intelligence.reasoning_bank import (  # noqa: E402  (intentional after fixture defs)
    MEMORY_LESSON_MULT_MAX,
    MEMORY_LESSON_MULT_MIN,
    memory_lesson_conviction_multiplier,
)


def _stub_retrieve(monkeypatch: pytest.MonkeyPatch, lessons: list[ReasoningLesson]) -> None:
    """Patch retrieve_lessons at the call site inside reasoning_bank."""
    monkeypatch.setattr(
        "intelligence.reasoning_bank.retrieve_lessons",
        lambda *a, **k: list(lessons),
    )


def _mk_lesson(outcome: str, direction: str = "long") -> ReasoningLesson:
    return ReasoningLesson(
        title="t", description="d", content="c",
        outcome_class=outcome,  # type: ignore[arg-type]
        condition_fingerprint={"direction": direction, "regime": "GROWTH"},
        source_type=f"postmortem_{outcome}",
        source_id=None,
    )


def test_memory_lesson_multiplier_empty_bank_returns_neutral(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_retrieve(monkeypatch, [])
    mult = memory_lesson_conviction_multiplier(
        _FakeEngine(), fingerprint={"ticker": "TSM", "direction": "long"},
    )
    assert mult == 1.0


def test_memory_lesson_multiplier_all_success_caps_at_upper_bound(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_retrieve(monkeypatch, [_mk_lesson("success") for _ in range(5)])
    mult = memory_lesson_conviction_multiplier(
        _FakeEngine(), fingerprint={"direction": "long"},
    )
    # Pure success ⇒ score = 1 ⇒ multiplier = 1 + 0.15 = 1.15 (upper bound)
    assert mult == pytest.approx(MEMORY_LESSON_MULT_MAX, abs=1e-6)


def test_memory_lesson_multiplier_all_failure_caps_at_lower_bound(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_retrieve(monkeypatch, [_mk_lesson("failure") for _ in range(5)])
    mult = memory_lesson_conviction_multiplier(
        _FakeEngine(), fingerprint={"direction": "long"},
    )
    # Pure failure ⇒ score = -1 ⇒ multiplier = 1 - 0.15 = 0.85 (lower bound)
    assert mult == pytest.approx(MEMORY_LESSON_MULT_MIN, abs=1e-6)


def test_memory_lesson_multiplier_balanced_returns_neutral(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_retrieve(
        monkeypatch,
        [_mk_lesson("success"), _mk_lesson("failure")],
    )
    mult = memory_lesson_conviction_multiplier(
        _FakeEngine(), fingerprint={"direction": "long"},
    )
    # 1 success - 1 failure = 0 ⇒ neutral
    assert mult == pytest.approx(1.0, abs=1e-6)


def test_memory_lesson_multiplier_neutral_lessons_apply_mild_haircut(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_retrieve(monkeypatch, [_mk_lesson("neutral") for _ in range(4)])
    mult = memory_lesson_conviction_multiplier(
        _FakeEngine(), fingerprint={"direction": "long"},
    )
    # All-neutral ⇒ score = -0.3 ⇒ multiplier = 1 - 0.045 = 0.955
    assert mult < 1.0
    assert mult > MEMORY_LESSON_MULT_MIN
    assert mult == pytest.approx(1.0 - 0.15 * 0.3, abs=1e-6)


def test_memory_lesson_multiplier_filters_direction_when_required(monkeypatch: pytest.MonkeyPatch) -> None:
    # 3 successes on the OPPOSITE direction should be ignored when
    # require_direction_match=True (the default).
    _stub_retrieve(
        monkeypatch,
        [_mk_lesson("success", direction="short") for _ in range(3)],
    )
    mult = memory_lesson_conviction_multiplier(
        _FakeEngine(), fingerprint={"direction": "long"},
    )
    assert mult == 1.0


def test_memory_lesson_multiplier_includes_all_when_match_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_retrieve(
        monkeypatch,
        [_mk_lesson("success", direction="short") for _ in range(3)],
    )
    mult = memory_lesson_conviction_multiplier(
        _FakeEngine(),
        fingerprint={"direction": "long"},
        require_direction_match=False,
    )
    assert mult == pytest.approx(MEMORY_LESSON_MULT_MAX, abs=1e-6)


def test_memory_lesson_multiplier_defensive_on_retrieve_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(*_a: Any, **_k: Any) -> list[ReasoningLesson]:
        raise RuntimeError("connection refused")

    monkeypatch.setattr("intelligence.reasoning_bank.retrieve_lessons", _boom)
    mult = memory_lesson_conviction_multiplier(
        _FakeEngine(), fingerprint={"direction": "long"},
    )
    assert mult == 1.0


def test_memory_lesson_multiplier_clamps_within_range(monkeypatch: pytest.MonkeyPatch) -> None:
    # Even pathological mixes must stay within [0.85, 1.15].
    _stub_retrieve(
        monkeypatch,
        [_mk_lesson("success") for _ in range(20)] + [_mk_lesson("neutral")],
    )
    mult = memory_lesson_conviction_multiplier(
        _FakeEngine(), fingerprint={"direction": "long"},
    )
    assert MEMORY_LESSON_MULT_MIN <= mult <= MEMORY_LESSON_MULT_MAX


def test_memory_lesson_multiplier_no_direction_in_fingerprint_skips_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_retrieve(
        monkeypatch,
        [_mk_lesson("failure", direction="short") for _ in range(2)],
    )
    # No 'direction' in fingerprint ⇒ filter no-ops, both lessons counted.
    mult = memory_lesson_conviction_multiplier(
        _FakeEngine(), fingerprint={"ticker": "TSM"},
    )
    assert mult == pytest.approx(MEMORY_LESSON_MULT_MIN, abs=1e-6)


# ── Integration (skipped unless real PG is reachable) ────────────────────


@pytest.mark.integration
def test_pg_round_trip_integration() -> None:
    """End-to-end against a live PostgreSQL with pgvector + the migration applied."""
    if not os.environ.get("GRID_PG_INTEGRATION"):
        pytest.skip("set GRID_PG_INTEGRATION=1 to run")
    from sqlalchemy import create_engine
    url = os.environ.get(
        "GRID_DB_URL",
        "postgresql://grid_user:changeme@localhost:5432/grid",
    )
    eng = create_engine(url)
    new_id = write_reasoning_lesson(
        eng,
        ReasoningLesson(
            title="integration-smoke",
            description="d",
            content="c",
            outcome_class="neutral",
            condition_fingerprint={"ticker": "XYZ"},
            source_type="test_smoke",
            source_id=None,
        ),
    )
    assert new_id is not None
    found = retrieve_lessons(
        eng,
        fingerprint={"ticker": "XYZ"},
        top_k=5,
        outcome_class="neutral",
    )
    assert any(l.title == "integration-smoke" for l in found)
