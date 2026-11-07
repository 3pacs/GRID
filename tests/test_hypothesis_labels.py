"""Legacy hypothesis_registry labels and the consumers that read through them.

``governance.hypothesis_labels`` never rewrites a registry row: it appends
labels, and every PASSED consumer treats a pre-cutoff or labelled row as
unvalidated. ``discovered_hypotheses`` readers show v1 verdicts as
``legacy_v1_unsound`` and count only ``scoring_version IS NOT NULL`` as
trial-confirmed. The real-PostgreSQL proof is
``tests/test_hypothesis_registry_labels_migration_pg.py``.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from governance import hypothesis_labels as hl

ROOT = Path(__file__).resolve().parents[1]
AFTER = hl.LEGACY_CUTOFF + timedelta(days=1)
BEFORE = hl.LEGACY_CUTOFF - timedelta(days=1)


def _conn(row):
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = row
    return conn


@pytest.mark.parametrize("row,validated,reason", [
    (None, False, "not found"),
    (("TESTING", AFTER, None), False, "must be PASSED"),
    (("PASSED", AFTER, "LEGACY_UNVALIDATED"), False, "labelled LEGACY_UNVALIDATED"),
    (("PASSED", BEFORE, None), False, "legacy"),
    (("PASSED", BEFORE.replace(tzinfo=None), None), False, "legacy"),
    (("PASSED", AFTER, None), True, "validated"),
])
def test_passed_validation(row, validated, reason) -> None:
    out = hl.passed_validation(_conn(row), 7)
    assert out["validated"] is validated
    assert reason in out["reason"]


def test_passed_validation_without_hypothesis() -> None:
    conn = MagicMock()
    assert hl.passed_validation(conn, None)["validated"] is False
    conn.execute.assert_not_called()


def _label_engine(planned, existing, inserted=0):
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    results = []

    def execute(stmt, params=None):
        sql = str(stmt)
        res = MagicMock()
        if "INSERT INTO hypothesis_registry_labels" in sql:
            res.rowcount = inserted
        elif "FROM hypothesis_registry_labels" in sql and "GROUP BY 1 ORDER BY 1" in sql and "hypothesis_registry h" not in sql:
            res.__iter__ = lambda self: iter(existing)
        elif "FROM hypothesis_registry h" in sql:
            res.__iter__ = lambda self: iter(planned)
        results.append((sql, params))
        return res

    conn.execute.side_effect = execute
    engine = MagicMock()
    engine.connect.return_value = conn
    return engine, conn, results


def test_dry_run_is_read_only_and_writes_nothing() -> None:
    engine, conn, calls = _label_engine([("LEGACY_UNVALIDATED", 10609), ("LEGACY_LLM_FLAGGED", 30814),
                                         ("LEGACY_INCONCLUSIVE", 7)], [])
    out = hl.label_legacy_registry(engine)
    assert out["dry_run"] is True and out["inserted"] == 0
    assert out["planned"] == {"LEGACY_UNVALIDATED": 10609, "LEGACY_LLM_FLAGGED": 30814,
                              "LEGACY_INCONCLUSIVE": 7}
    assert calls[0][0] == "SET TRANSACTION READ ONLY"
    assert not any("INSERT" in sql for sql, _ in calls)
    conn.begin.return_value.rollback.assert_called_once()
    conn.begin.return_value.commit.assert_not_called()


def test_apply_needs_decided_by() -> None:
    engine, _, _ = _label_engine([], [])
    with pytest.raises(ValueError):
        hl.label_legacy_registry(engine, dry_run=False, decided_by="  ")


def test_apply_inserts_with_bound_params_and_commits() -> None:
    engine, conn, calls = _label_engine([("LEGACY_UNVALIDATED", 2)], [("LEGACY_LLM_FLAGGED", 1)], inserted=2)
    out = hl.label_legacy_registry(engine, dry_run=False, decided_by="anik")
    assert out["inserted"] == 2 and out["already_labelled"]["LEGACY_LLM_FLAGGED"] == 1
    sql, params = next(c for c in calls if "INSERT" in c[0])
    assert "ON CONFLICT (registry_id, label_version) DO NOTHING" in sql
    assert params["decided_by"] == "anik" and params["cutoff"] == hl.LEGACY_CUTOFF
    assert params["label_version"] == hl.LABEL_VERSION
    assert not any(sql == "SET TRANSACTION READ ONLY" for sql, _ in calls)
    conn.begin.return_value.commit.assert_called_once()


def test_label_module_never_mutates_the_registry() -> None:
    source = (ROOT / "governance" / "hypothesis_labels.py").read_text().upper()
    assert not re.search(r"UPDATE\s+HYPOTHESIS_REGISTRY\b", source)
    assert not re.search(r"DELETE\s+FROM\s+HYPOTHESIS_REGISTRY", source)
    assert "INSERT INTO HYPOTHESIS_REGISTRY_LABELS" in source


# ── PASSED consumers read through the labels ────────────────────────────────


@pytest.mark.parametrize("path", [
    "trading/paper_engine.py",
    "scripts/notify.py",
    "scripts/train_regime_model.py",
])
def test_list_consumers_exclude_legacy_passed(path: str) -> None:
    source = (ROOT / path).read_text()
    assert "hypothesis_registry_labels" in source
    assert "legacy_cutoff" in source


@pytest.mark.parametrize("path", ["validation/gates.py", "api/routers/discovery.py"])
def test_gate_consumers_use_passed_validation(path: str) -> None:
    assert "passed_validation" in (ROOT / path).read_text()


def test_paper_engine_binds_the_cutoff() -> None:
    from trading.paper_engine import PaperTradingEngine

    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.fetchall.return_value = []
    paper = PaperTradingEngine.__new__(PaperTradingEngine)
    paper.engine = engine
    assert paper.register_all_passed() == 0
    stmt, params = conn.execute.call_args.args
    assert "NOT EXISTS" in str(stmt) and params == {"legacy_cutoff": hl.LEGACY_CUTOFF}


def test_gate_refuses_a_legacy_passed_hypothesis(monkeypatch) -> None:
    from validation.gates import GateChecker

    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.fetchone.return_value = (11, 42)
    monkeypatch.setattr(hl, "passed_validation", lambda c, hid: {
        "validated": False, "state": "PASSED", "label": "LEGACY_UNVALIDATED",
        "reason": "hypothesis is labelled LEGACY_UNVALIDATED"})
    checker = GateChecker.__new__(GateChecker)
    checker.engine = engine
    out = checker.check_candidate_to_shadow(1)
    assert out["passed"] is False
    assert any("LEGACY_UNVALIDATED" in d for d in out["details"])


def test_gate_accepts_a_validated_passed_hypothesis(monkeypatch) -> None:
    from validation.gates import GateChecker

    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.fetchone.return_value = (11, 42)
    monkeypatch.setattr(hl, "passed_validation", lambda c, hid: {
        "validated": True, "state": "PASSED", "label": None, "reason": "validated PASSED"})
    checker = GateChecker.__new__(GateChecker)
    checker.engine = engine
    assert checker.check_candidate_to_shadow(1)["passed"] is True


# ── discovered_hypotheses readers ───────────────────────────────────────────


@pytest.mark.parametrize("path", [
    "api/routers/canvas_expand.py",
    "api/routers/surfacer.py",
    "api/routers/valuation.py",
])
def test_display_readers_label_v1_verdicts(path: str) -> None:
    source = (ROOT / path).read_text()
    assert "legacy_v1_unsound" in source and "scoring_version IS NULL" in source


def test_model_evolver_counts_only_trial_confirmed() -> None:
    source = (ROOT / "oracle" / "model_evolver.py").read_text()
    assert "status = 'confirmed' AND scoring_version IS NOT NULL" in source
    assert "status IN ('confirmed','active')" not in source


def test_get_stats_splits_v2_from_v1() -> None:
    from intelligence.hypothesis_engine import get_stats

    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    now = datetime(2026, 11, 7, tzinfo=timezone.utc)
    conn.execute.return_value.fetchone.return_value = (
        10, 3, 1, 2, 4, 0.5, 0.5, 3, 1, now, now, 133089, 71476,
    )
    conn.execute.return_value.__iter__ = lambda self: iter([])
    conn.execute.return_value.scalar.return_value = 0
    stats = get_stats(engine)
    assert stats["confirmed"] == 1
    assert stats["legacy_v1_unsound"] == {"confirmed": 133089, "invalidated": 71476}
    first_sql = str(conn.execute.call_args_list[0].args[0])
    assert "status = 'confirmed' AND scoring_version IS NOT NULL" in first_sql
