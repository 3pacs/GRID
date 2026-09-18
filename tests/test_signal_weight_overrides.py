"""Tests for ``intelligence.signal_weight_overrides``.

Covers the 2026-09-18 GRID W7b split: the safeguard *capability*
(promotion-ledger enforcement) is now independent of the operational
*policy* of whether it's turned on. Current production behaviour is
preserved by default:

* ``GRID_SIGNAL_OVERRIDES_ENABLED`` defaults to True (legacy default,
  restored).
* ``GRID_SIGNAL_OVERRIDES_REQUIRE_LEDGER`` (new) defaults to False —
  legacy behaviour exactly: overrides apply with no ledger check, but
  one WARNING is logged (naming the knob) the first time they do.
* Setting ``GRID_SIGNAL_OVERRIDES_REQUIRE_LEDGER=true`` gates
  overrides behind an approved ``governance.promotion_ledger`` record,
  exactly like the previous (now-superseded) always-on ledger gate.

See that module's docstring for the full rationale, and
``docs/reference/LEARNING_PROMOTION_PROTOCOL.md`` for the held
operational decision to actually flip the defaults (tracked on
``fable/overrides-policy-20260918``, not here).
"""

from __future__ import annotations

import pytest

from intelligence import signal_weight_overrides


@pytest.fixture(autouse=True)
def _reset_module_state(monkeypatch):
    """Keep tests isolated from env-derived module state and the
    "warned once" latches."""
    monkeypatch.setattr(signal_weight_overrides, "_warned_missing_promotion", False)
    monkeypatch.setattr(signal_weight_overrides, "_warned_legacy_no_ledger", False)
    yield
    monkeypatch.setattr(signal_weight_overrides, "_warned_missing_promotion", False)
    monkeypatch.setattr(signal_weight_overrides, "_warned_legacy_no_ledger", False)


def _force_promoted(monkeypatch, promoted: bool) -> None:
    """Bypass the real ledger lookup (no DB in this test env) and pin
    the promotion check to a fixed answer."""
    monkeypatch.setattr(
        signal_weight_overrides, "_is_override_set_promoted", lambda: promoted
    )


def test_overrides_enabled_by_default():
    """GRID_SIGNAL_OVERRIDES_ENABLED must default to True (the
    pre-existing production default, restored/preserved by W7b)."""
    assert signal_weight_overrides.SIGNAL_OVERRIDES_ENABLED is True


def test_require_ledger_disabled_by_default():
    """GRID_SIGNAL_OVERRIDES_REQUIRE_LEDGER must default to False —
    the new knob does not change behaviour unless explicitly set."""
    assert signal_weight_overrides.SIGNAL_OVERRIDES_REQUIRE_LEDGER is False


def test_disabled_master_switch_applies_nothing_regardless_of_require_ledger(monkeypatch):
    monkeypatch.setattr(signal_weight_overrides, "SIGNAL_OVERRIDES_ENABLED", False)
    monkeypatch.setattr(signal_weight_overrides, "SIGNAL_OVERRIDES_REQUIRE_LEDGER", False)
    assert signal_weight_overrides.get_override("equity") == 1.0
    assert signal_weight_overrides.get_effective_overrides() == {}

    monkeypatch.setattr(signal_weight_overrides, "SIGNAL_OVERRIDES_REQUIRE_LEDGER", True)
    _force_promoted(monkeypatch, True)  # even "promoted" doesn't matter if OFF
    assert signal_weight_overrides.get_override("equity") == 1.0
    assert signal_weight_overrides.get_effective_overrides() == {}


def test_legacy_default_applies_overrides_and_warns_once(monkeypatch):
    """require_ledger=False (default): overrides apply exactly like
    pre-W7 legacy behaviour, but exactly one WARNING is logged the
    first time, naming GRID_SIGNAL_OVERRIDES_REQUIRE_LEDGER."""
    monkeypatch.setattr(signal_weight_overrides, "SIGNAL_OVERRIDES_ENABLED", True)
    monkeypatch.setattr(signal_weight_overrides, "SIGNAL_OVERRIDES_REQUIRE_LEDGER", False)

    warnings = []
    monkeypatch.setattr(
        signal_weight_overrides.log,
        "warning",
        lambda *a, **k: warnings.append((a, k)),
    )

    assert signal_weight_overrides.get_override("equity") == pytest.approx(1.20)
    assert signal_weight_overrides.get_override("vol") == pytest.approx(0.30)
    assert signal_weight_overrides.get_override(" commodity ") == pytest.approx(1.10)

    effective = signal_weight_overrides.get_effective_overrides()
    assert effective["equity"] == pytest.approx(1.20)
    assert effective["news_intel"] == pytest.approx(0.60)  # deferred set included

    assert len(warnings) == 1, "expected exactly one WARNING on first use"
    msg = str(warnings[0])
    assert "GRID_SIGNAL_OVERRIDES_REQUIRE_LEDGER" in msg

    # Further calls must NOT warn again (log once).
    signal_weight_overrides.get_override("equity")
    signal_weight_overrides.get_effective_overrides()
    assert len(warnings) == 1, "must not warn a second time (log once)"


def test_require_ledger_without_approval_applies_nothing_and_warns_once(monkeypatch):
    monkeypatch.setattr(signal_weight_overrides, "SIGNAL_OVERRIDES_ENABLED", True)
    monkeypatch.setattr(signal_weight_overrides, "SIGNAL_OVERRIDES_REQUIRE_LEDGER", True)

    # Exercise the real _is_override_set_promoted() (not stubbed) by
    # making the underlying ledger lookup fail — the real function
    # already treats that as fail-safe = not promoted, which is what
    # "no ledger entry" looks like from this module's point of view.
    import governance.promotion_ledger as ledger_mod

    def _boom(*_a, **_k):
        raise RuntimeError("no DB configured in tests")

    monkeypatch.setattr(ledger_mod, "is_approved", _boom)

    warnings = []
    monkeypatch.setattr(
        signal_weight_overrides.log,
        "warning",
        lambda *a, **k: warnings.append((a, k)),
    )

    assert signal_weight_overrides.get_override("equity") == 1.0
    assert signal_weight_overrides.get_effective_overrides() == {}
    assert len(warnings) == 1, "expected exactly one WARNING on first miss"

    # Second call must NOT warn again (log once).
    assert signal_weight_overrides.get_override("vol") == 1.0
    assert len(warnings) == 1, "must not warn a second time (log once)"


def test_require_ledger_with_matching_approval_applies_overrides(monkeypatch):
    monkeypatch.setattr(signal_weight_overrides, "SIGNAL_OVERRIDES_ENABLED", True)
    monkeypatch.setattr(signal_weight_overrides, "SIGNAL_OVERRIDES_REQUIRE_LEDGER", True)
    _force_promoted(monkeypatch, True)

    assert signal_weight_overrides.get_override("equity") == pytest.approx(1.20)
    assert signal_weight_overrides.get_override("vol") == pytest.approx(0.30)
    assert signal_weight_overrides.get_override(" commodity ") == pytest.approx(1.10)

    effective = signal_weight_overrides.get_effective_overrides()
    assert effective["equity"] == pytest.approx(1.20)
    assert effective["news_intel"] == pytest.approx(0.60)  # deferred set included


def test_require_ledger_mismatched_hash_applies_nothing(monkeypatch):
    """A ledger entry for a *different* override set/version must not
    count as promoting this one — is_approved() is queried with this
    module's exact subject_hash + evaluation_version, so a mismatch on
    either means "not found", not "found but wrong".

    Uses a real sqlite-backed ledger end-to-end (recommend + approve +
    is_approved), with a fake ``db`` module injected into sys.modules
    so ``_is_override_set_promoted()``'s ``from db import get_engine``
    resolves to our in-memory engine instead of the real db.py (which
    requires a configured .env/DB_PASSWORD this test environment does
    not and must not have)."""
    import sys
    import types

    monkeypatch.setattr(signal_weight_overrides, "SIGNAL_OVERRIDES_ENABLED", True)
    monkeypatch.setattr(signal_weight_overrides, "SIGNAL_OVERRIDES_REQUIRE_LEDGER", True)

    import governance.promotion_ledger as ledger_mod
    from sqlalchemy import create_engine

    engine = create_engine("sqlite:///:memory:")
    ledger_mod.ensure_schema(engine)
    rec = ledger_mod.recommend(
        engine,
        kind="weight_override",
        subject_hash="some-other-hash-not-ours",
        evaluation_version=signal_weight_overrides.EVALUATION_VERSION,
        evidence_ref="n/a",
        recommended_by="tester",
    )
    ledger_mod.approve(engine, recommendation_id=rec["id"], approved_by="approver")

    fake_db = types.ModuleType("db")
    fake_db.get_engine = lambda: engine
    monkeypatch.setitem(sys.modules, "db", fake_db)

    # Real subject hash (computed from the actual table) won't match
    # the "some-other-hash-not-ours" entry above.
    assert signal_weight_overrides.get_override("equity") == 1.0
    assert signal_weight_overrides.get_effective_overrides() == {}


def test_require_ledger_with_real_ledger_match_via_sqlite_applies_overrides(monkeypatch):
    """End-to-end: recommend() + approve() against a real (sqlite)
    ledger, using this module's own compute_subject_hash(), then
    verify get_override()/get_effective_overrides() actually apply."""
    import sys
    import types

    monkeypatch.setattr(signal_weight_overrides, "SIGNAL_OVERRIDES_ENABLED", True)
    monkeypatch.setattr(signal_weight_overrides, "SIGNAL_OVERRIDES_REQUIRE_LEDGER", True)

    import governance.promotion_ledger as ledger_mod
    from sqlalchemy import create_engine

    engine = create_engine("sqlite:///:memory:")
    ledger_mod.ensure_schema(engine)
    rec = ledger_mod.recommend(
        engine,
        kind="weight_override",
        subject_hash=signal_weight_overrides.compute_subject_hash(),
        evaluation_version=signal_weight_overrides.EVALUATION_VERSION,
        evidence_ref="docs/reference/LEARNING_PROMOTION_PROTOCOL.md",
        recommended_by="researcher@example",
    )
    ledger_mod.approve(engine, recommendation_id=rec["id"], approved_by="operator@example")

    fake_db = types.ModuleType("db")
    fake_db.get_engine = lambda: engine
    monkeypatch.setitem(sys.modules, "db", fake_db)

    assert signal_weight_overrides.get_override("equity") == pytest.approx(1.20)
    assert signal_weight_overrides.get_effective_overrides()["news_intel"] == pytest.approx(0.60)


def test_get_override_returns_neutral_for_unknown_or_unusable_signals(monkeypatch):
    monkeypatch.setattr(signal_weight_overrides, "SIGNAL_OVERRIDES_ENABLED", True)
    monkeypatch.setattr(signal_weight_overrides, "SIGNAL_OVERRIDES_REQUIRE_LEDGER", True)
    _force_promoted(monkeypatch, True)

    assert signal_weight_overrides.get_override("unknown_signal") == 1.0
    assert signal_weight_overrides.get_override("feature:equity") == 1.0
    assert signal_weight_overrides.get_override("") == 1.0
    assert signal_weight_overrides.get_override(None) == 1.0


def test_compute_subject_hash_is_deterministic_and_sensitive_to_table():
    h1 = signal_weight_overrides.compute_subject_hash()
    h2 = signal_weight_overrides.compute_subject_hash()
    assert h1 == h2

    h3 = signal_weight_overrides.compute_subject_hash(
        overrides={"equity": 1.20}, evaluation_version="different-version"
    )
    assert h3 != h1
