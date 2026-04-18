"""Tests for SYNTH Wave E consolidation (SYNTH-43, 44, 45).

Wave E is governance — no new signal families, no new detectors. The
three deliverables are:

* SYNTH-43 — make ``ModelRegistry.update_from_contract`` the primary
  weight-evolution path with full Bayesian parity to the batch scan in
  ``OracleEngine.evolve_weights``. ``evolve_weights(event_driven=True)``
  is now a reconciliation pass that only flags drift.
* SYNTH-44 — schema-integrity guard for ``intelligence.trust_scorer``:
  every ``SIGNAL_TRUST_DELTA`` key must have matching entries in
  ``SIGNAL_HALF_LIFE_DAYS`` AND ``EVALUATION_WINDOWS``.
* SYNTH-45 — router-integrity guard: every contract type with a real
  producer ``emit(ContractName(`` call site must have at least one
  handler in ``contracts.router.ROUTES``.

All tests use the ``mock_engine`` fixture so they run in <1s without a
live PostgreSQL instance.
"""
from __future__ import annotations

import inspect
import os
import random
import re
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from contracts.router import ROUTES, resolve_handler
from contracts.schemas import (
    ALL_CONTRACTS,
    BaseContract,
    PredictionScored,
    SignalRef,
)


# ── Shared fixtures ────────────────────────────────────────────────────────


@pytest.fixture
def signal_refs() -> list[SignalRef]:
    return [
        SignalRef(
            signal_id=uuid4(),
            source="insider",
            trust_at_prediction=0.6,
            weight_at_prediction=1.0,
        ),
    ]


def _make_evt(verdict: str, weights: dict[str, float], *, signal_refs):
    return PredictionScored(
        producer_module="oracle.engine",
        correlation_id=uuid4(),
        prediction_id=uuid4(),
        decision_id=1,
        ticker="AAPL",
        verdict=verdict,
        expected_direction="UP",
        realized_direction="UP" if verdict == "HIT" else "DOWN",
        confidence=0.7,
        brier_component=0.09,
        signals_used=signal_refs,
        model_weights_at_prediction=weights,
    )


def _set_db_row(mock_engine, row: tuple | None) -> None:
    """Wire the mock_engine so SELECT ... fetchone() returns ``row``."""
    mock_conn = mock_engine.begin.return_value.__enter__.return_value
    fetch_result = MagicMock()
    fetch_result.fetchone.return_value = row
    mock_conn.execute.return_value = fetch_result


# ──────────────────────────────────────────────────────────────────────────
# 1. SYNTH-43 — ModelRegistry event path (Bayesian parity with batch scan)
# ──────────────────────────────────────────────────────────────────────────


class TestModelRegistryEventPath:
    """Per-event Bayesian nudge must mirror the batch path's math.

    The mock_engine fixture returns ``fetchone() == None`` by default, so
    the registry takes the fallback branch and applies the nudge against
    the prior weight carried on the contract. We assert on the post-nudge
    weight by computing it analytically from the same closed form the
    code uses: ``new = prior + LR * (target - prior)`` where
    ``target = 0.5 + adj * 2.0``.
    """

    LR = 0.05
    MIN_W = 0.1
    MAX_W = 5.0

    @staticmethod
    def _expected(prior: float, verdict: str) -> float:
        adj = {"HIT": 1.0, "PARTIAL": 0.5, "MISS": 0.0}[verdict]
        target = 0.5 + adj * 2.0
        new_w = prior + 0.05 * (target - prior)
        return max(0.1, min(5.0, new_w))

    def test_hit_nudges_weight_up_toward_target(
        self, signal_refs, mock_engine
    ):
        from oracle.engine import ModelRegistry

        registry = ModelRegistry(mock_engine)
        prior = 1.0
        evt = _make_evt("HIT", {"flow_momentum": prior}, signal_refs=signal_refs)
        n = registry.update_from_contract(evt)
        assert n == 1
        # HIT target = 2.5, new = 1.0 + 0.05*(2.5 - 1.0) = 1.075
        expected = self._expected(prior, "HIT")
        # Pull the last UPDATE call's bound parameters to verify weight.
        mock_conn = mock_engine.begin.return_value.__enter__.return_value
        last_call_args = mock_conn.execute.call_args_list[-1][0]
        bound = last_call_args[1]
        assert "w" in bound
        assert bound["w"] == pytest.approx(expected, abs=1e-4)
        assert bound["w"] > prior  # nudged up

    def test_miss_nudges_weight_down_toward_half(
        self, signal_refs, mock_engine
    ):
        from oracle.engine import ModelRegistry

        registry = ModelRegistry(mock_engine)
        prior = 1.5
        evt = _make_evt("MISS", {"flow_momentum": prior}, signal_refs=signal_refs)
        n = registry.update_from_contract(evt)
        assert n == 1
        expected = self._expected(prior, "MISS")
        mock_conn = mock_engine.begin.return_value.__enter__.return_value
        bound = mock_conn.execute.call_args_list[-1][0][1]
        assert bound["w"] == pytest.approx(expected, abs=1e-4)
        assert bound["w"] < prior  # nudged down

    def test_partial_nudges_halfway(self, signal_refs, mock_engine):
        from oracle.engine import ModelRegistry

        registry = ModelRegistry(mock_engine)
        prior = 1.0
        evt = _make_evt(
            "PARTIAL", {"flow_momentum": prior}, signal_refs=signal_refs
        )
        n = registry.update_from_contract(evt)
        assert n == 1
        expected = self._expected(prior, "PARTIAL")
        # PARTIAL target = 1.5, new = 1.0 + 0.05*(1.5-1.0) = 1.025
        assert expected == pytest.approx(1.025, abs=1e-4)
        mock_conn = mock_engine.begin.return_value.__enter__.return_value
        bound = mock_conn.execute.call_args_list[-1][0][1]
        assert bound["w"] == pytest.approx(expected, abs=1e-4)

    def test_clamps_to_min_and_max_weight(self, signal_refs, mock_engine):
        from oracle.engine import ModelRegistry

        registry = ModelRegistry(mock_engine)

        # Far below MIN: a MISS at prior=0.05 still clamps to MIN=0.1
        # because the target (0.5) pulls it up — but if we start at 0.05
        # with a HIT the new = 0.05 + 0.05*(2.5-0.05) = 0.1725 — above
        # MIN. Use a far-too-large prior to test MAX clamp.
        evt = _make_evt(
            "HIT", {"big": 100.0}, signal_refs=signal_refs
        )
        registry.update_from_contract(evt)
        mock_conn = mock_engine.begin.return_value.__enter__.return_value
        bound = mock_conn.execute.call_args_list[-1][0][1]
        # 100 + 0.05*(2.5-100) = 100 - 4.875 = 95.125 → clamp to MAX 5.0
        assert bound["w"] == pytest.approx(self.MAX_W, abs=1e-6)

        # Below MIN clamp: prior tiny + MISS → 0.01 + 0.05*(0.5-0.01) ≈
        # 0.0345 → clamps to MIN 0.1
        mock_conn.execute.reset_mock()
        mock_conn.execute.return_value.fetchone.return_value = None
        evt2 = _make_evt(
            "MISS", {"tiny": 0.01}, signal_refs=signal_refs
        )
        registry.update_from_contract(evt2)
        bound2 = mock_conn.execute.call_args_list[-1][0][1]
        assert bound2["w"] == pytest.approx(self.MIN_W, abs=1e-6)

    def test_unknown_model_name_is_graceful(
        self, signal_refs, mock_engine
    ):
        from oracle.engine import ModelRegistry

        # SELECT returns None (mock default), UPDATE silently no-ops on
        # the unknown row but the registry still reports it as touched
        # because the fallback path issues an UPDATE. The point of this
        # test is that no exception is raised even when the model name
        # is unknown.
        registry = ModelRegistry(mock_engine)
        evt = _make_evt(
            "HIT", {"definitely_not_a_real_model_xyz": 1.0},
            signal_refs=signal_refs,
        )
        # Must not raise.
        n = registry.update_from_contract(evt)
        assert n >= 0  # 1 in mock mode, 0 in real DB if row missing

    def test_missing_model_weights_dict_is_noop(
        self, signal_refs, mock_engine
    ):
        from oracle.engine import ModelRegistry

        evt = MagicMock()
        evt.verdict = "HIT"
        evt.confidence = 0.8
        evt.model_weights_at_prediction = None
        registry = ModelRegistry(mock_engine)
        assert registry.update_from_contract(evt) == 0

    def test_db_error_is_non_fatal(self, signal_refs, mock_engine):
        from oracle.engine import ModelRegistry

        # Force the engine.begin() context manager to raise on entry —
        # the registry must catch and return 0 rather than letting the
        # exception bubble into the dispatcher (which would DLQ the
        # entire event).
        mock_engine.begin.side_effect = RuntimeError("connection refused")
        registry = ModelRegistry(mock_engine)
        evt = _make_evt(
            "HIT", {"flow_momentum": 1.0}, signal_refs=signal_refs
        )
        # Must not raise.
        n = registry.update_from_contract(evt)
        assert n == 0

    def test_event_path_parity_with_batch(self, signal_refs, mock_engine):
        """After N random verdicts, the event path's weight is within
        5% of the equivalent closed-form batch result.

        We simulate the same N events both ways:
          1. event path: feed each verdict through update_from_contract
             and read the final weight bound to the last UPDATE.
          2. batch path: apply the same Bayesian formula
             (target = 0.5 + adj*2, weight += LR*(target-weight)) in a
             plain Python loop.

        Because ``mock_engine`` returns no DB rows, the event path falls
        back to the prior-weight branch which uses the same per-event
        formula. The two paths are therefore mathematically identical
        and the parity test is exact (within float rounding). The 5%
        tolerance in the spec is the headroom we'd consume against a
        real DB whose counters drift between the two views.
        """
        from oracle.engine import ModelRegistry

        rng = random.Random(42)
        verdicts = [rng.choice(["HIT", "MISS", "PARTIAL"]) for _ in range(30)]

        # ── Batch / closed-form simulation ──
        batch_w = 1.0
        for v in verdicts:
            adj = {"HIT": 1.0, "PARTIAL": 0.5, "MISS": 0.0}[v]
            target = 0.5 + adj * 2.0
            batch_w = batch_w + 0.05 * (target - batch_w)
            batch_w = max(self.MIN_W, min(self.MAX_W, batch_w))

        # ── Event-driven simulation ──
        event_w = 1.0
        registry = ModelRegistry(mock_engine)
        mock_conn = mock_engine.begin.return_value.__enter__.return_value
        for v in verdicts:
            mock_conn.execute.reset_mock()
            mock_conn.execute.return_value.fetchone.return_value = None
            evt = _make_evt(
                v, {"parity_model": event_w}, signal_refs=signal_refs
            )
            registry.update_from_contract(evt)
            bound = mock_conn.execute.call_args_list[-1][0][1]
            event_w = float(bound["w"])

        # 5% tolerance per the SYNTH-43 spec.
        assert abs(event_w - batch_w) / max(batch_w, 1e-6) < 0.05


# ──────────────────────────────────────────────────────────────────────────
# 2. SYNTH-43 — evolve_weights reconciliation mode
# ──────────────────────────────────────────────────────────────────────────


class TestEvolveWeightsReconciliation:
    """``event_driven=True`` (the new default) must NOT touch model
    weights — it only flags drift between batch and event counters.
    ``event_driven=False`` must still execute the legacy LEARNING_RATE
    loop for offline backfill use.
    """

    def test_event_driven_is_default_and_returns_mode_marker(
        self, mock_engine
    ):
        from oracle.engine import OracleEngine

        # Don't run __init__ — we only need the bound method against
        # the mock engine.
        eng = OracleEngine.__new__(OracleEngine)
        eng.engine = mock_engine
        result = eng.evolve_weights()  # default event_driven=True
        assert result["mode"] == "event_driven"
        assert result["changes"] == {}

    def test_event_driven_does_not_call_legacy_update(self, mock_engine):
        from oracle.engine import OracleEngine

        eng = OracleEngine.__new__(OracleEngine)
        eng.engine = mock_engine
        eng.evolve_weights(event_driven=True)
        # Inspect the SQL passed to execute() — there must be no
        # "SET weight = :w" UPDATE in event_driven mode.
        mock_conn = mock_engine.begin.return_value.__enter__.return_value
        for call in mock_conn.execute.call_args_list:
            sql = str(call[0][0])
            assert "SET weight = :w" not in sql, (
                f"reconciliation pass must not UPDATE weights, got: {sql}"
            )

    def test_legacy_mode_signature_still_accepted(self, mock_engine):
        from oracle.engine import OracleEngine

        eng = OracleEngine.__new__(OracleEngine)
        eng.engine = mock_engine
        eng.models = []  # _load_models is called at the end
        # Stub _load_models so the legacy reload at the bottom of the
        # batch path doesn't try to hit the DB.
        eng._load_models = lambda: []
        # No fetchall() rows → no models updated, but the call must
        # not raise. Confirms the signature still accepts the kwarg.
        result = eng.evolve_weights(event_driven=False)
        assert "changes" in result


# ──────────────────────────────────────────────────────────────────────────
# 3. SYNTH-44 — trust_scorer schema integrity
# ──────────────────────────────────────────────────────────────────────────


# Historical keys that exist in EVALUATION_WINDOWS but were never paired
# with a SIGNAL_TRUST_DELTA. Visibility-only — xfailed so the suite
# stays green while making the gap impossible to ignore.
_HISTORICAL_DELTA_GAPS = frozenset({
    "congressional", "insider", "darkpool", "social", "scanner",
    "prediction_market", "whale_options", "options_flow",
    "foreign_lobbying", "geopolitical", "diplomatic_cable",
    "lobbying", "campaign_finance", "offshore_leak", "ai_trader",
})


class TestTrustScorerIntegrity:
    """Every key in SIGNAL_TRUST_DELTA must have matching entries in
    SIGNAL_HALF_LIFE_DAYS and EVALUATION_WINDOWS. Future SYNTH waves
    will catch missing keys here before they ship.
    """

    def test_every_trust_delta_key_has_half_life(self):
        from intelligence.trust_scorer import (
            SIGNAL_HALF_LIFE_DAYS,
            SIGNAL_TRUST_DELTA,
        )

        missing = sorted(
            k for k in SIGNAL_TRUST_DELTA if k not in SIGNAL_HALF_LIFE_DAYS
        )
        assert not missing, (
            "SIGNAL_TRUST_DELTA keys without SIGNAL_HALF_LIFE_DAYS entries: "
            f"{missing}"
        )

    def test_every_trust_delta_key_has_evaluation_window(self):
        from intelligence.trust_scorer import (
            EVALUATION_WINDOWS,
            SIGNAL_TRUST_DELTA,
        )

        missing = sorted(
            k for k in SIGNAL_TRUST_DELTA if k not in EVALUATION_WINDOWS
        )
        assert not missing, (
            "SIGNAL_TRUST_DELTA keys without EVALUATION_WINDOWS entries: "
            f"{missing}"
        )

    def test_evaluation_window_values_are_sane(self):
        from intelligence.trust_scorer import EVALUATION_WINDOWS

        for k, v in EVALUATION_WINDOWS.items():
            assert isinstance(v, int), (
                f"EVALUATION_WINDOWS[{k!r}] is not an int: {v!r}"
            )
            assert 0 <= v <= 365, (
                f"EVALUATION_WINDOWS[{k!r}] = {v} is out of [0, 365]"
            )

    @pytest.mark.xfail(
        strict=False,
        reason=(
            "Historical EVALUATION_WINDOWS entries (congressional, insider, "
            "darkpool, social, scanner, prediction_market, whale_options, "
            "options_flow, foreign_lobbying, geopolitical, diplomatic_cable, "
            "lobbying, campaign_finance, offshore_leak, ai_trader) have no "
            "matching SIGNAL_TRUST_DELTA. Visibility-only — these source "
            "types use the Bayesian base score and don't need a delta on "
            "top. Tracked but not blocking. SYNTH-44."
        ),
    )
    def test_every_evaluation_window_has_a_delta(self):
        from intelligence.trust_scorer import (
            EVALUATION_WINDOWS,
            SIGNAL_TRUST_DELTA,
        )

        missing = sorted(
            k for k in EVALUATION_WINDOWS if k not in SIGNAL_TRUST_DELTA
        )
        # This is the visibility surface — the assert is here so that if
        # someone fixes one of the historical gaps the xfail flips to
        # XPASS and we notice.
        assert not missing, (
            "EVALUATION_WINDOWS keys without SIGNAL_TRUST_DELTA: "
            f"{missing}"
        )


# ──────────────────────────────────────────────────────────────────────────
# 4. SYNTH-45 — router integrity (producers vs handlers)
# ──────────────────────────────────────────────────────────────────────────


_REPO_ROOT = Path(__file__).resolve().parent.parent
_PY_SOURCE_CACHE: list[tuple[Path, str]] | None = None


def _iter_repo_python_sources() -> list[tuple[Path, str]]:
    global _PY_SOURCE_CACHE
    if _PY_SOURCE_CACHE is not None:
        return _PY_SOURCE_CACHE

    hits: list[Path] = []
    sources: list[tuple[Path, str]] = []
    skipped_dirs = {
        "tests",
        "docs",
        ".venv",
        "venv",
        "node_modules",
        ".git",
        ".grid_backups",
        ".pytest_cache",
    }
    for root, dirs, files in os.walk(_REPO_ROOT):
        dirs[:] = [d for d in dirs if d not in skipped_dirs]
        for filename in files:
            if not filename.endswith(".py"):
                continue
            path = Path(root) / filename
            rel_parts = path.relative_to(_REPO_ROOT).parts
            if not rel_parts:
                continue
            if rel_parts[0] in skipped_dirs:
                continue
            if rel_parts[:2] == ("contracts", "handlers"):
                continue
            if rel_parts[:2] == ("contracts", "emit.py"):
                continue
            if path.name == "emit.py" and rel_parts[0] == "contracts":
                continue
            try:
                src = path.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue
            sources.append((path, src))

    _PY_SOURCE_CACHE = sources
    return sources


def _find_producer_files(contract_name: str) -> list[Path]:
    """Return every .py file in the repo that emits ``contract_name``."""
    pattern = re.compile(
        rf"emit\s*\(\s*{re.escape(contract_name)}\s*\(",
        re.MULTILINE,
    )
    return [path for path, src in _iter_repo_python_sources() if pattern.search(src)]


class TestRouterIntegrity:
    """Every contract type with at least one producer must have at
    least one handler in ROUTES. Every handler must resolve. Every
    handler must use the (evt, *, engine) keyword-only signature.
    """

    def test_every_producer_has_a_handler(self):
        unwired: list[str] = []
        no_producers: list[str] = []
        for ctype in ALL_CONTRACTS:
            producers = _find_producer_files(ctype.__name__)
            if not producers:
                no_producers.append(ctype.__name__)
                continue
            if ctype not in ROUTES or not ROUTES.get(ctype):
                unwired.append(ctype.__name__)

        assert not unwired, (
            f"Contracts with producers but no handlers: {unwired}. "
            f"(Contracts with no producers — skipped: {no_producers})"
        )

    def test_every_handler_path_resolves(self):
        for ctype, paths in ROUTES.items():
            for path in paths:
                # Should not raise ImportError or AttributeError.
                handler = resolve_handler(path)
                assert callable(handler), f"{path} is not callable"

    def test_every_handler_has_keyword_engine_param(self):
        bad: list[str] = []
        for ctype, paths in ROUTES.items():
            for path in paths:
                handler = resolve_handler(path)
                sig = inspect.signature(handler)
                params = sig.parameters
                if "engine" not in params:
                    bad.append(f"{path}: missing 'engine' param")
                    continue
                p = params["engine"]
                if p.kind != inspect.Parameter.KEYWORD_ONLY:
                    bad.append(
                        f"{path}: 'engine' must be KEYWORD_ONLY, got {p.kind}"
                    )
                # Must also have an 'evt' positional param (any kind).
                positional_names = [
                    n for n, p in params.items()
                    if p.kind in (
                        inspect.Parameter.POSITIONAL_OR_KEYWORD,
                        inspect.Parameter.POSITIONAL_ONLY,
                    )
                ]
                if not positional_names:
                    bad.append(f"{path}: missing positional 'evt' param")
        assert not bad, "Handler signature violations:\n" + "\n".join(bad)
