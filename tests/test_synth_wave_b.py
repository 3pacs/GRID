"""Tests for SYNTH Wave B handler wiring (SYNTH-24..33).

Covers the new ``contracts.handlers.oracle_signals`` module, the new
``SignalFired`` route in ``contracts.router.ROUTES``, the anti-signal
extensions on ``oracle.engine.OracleEngine`` (cross-lens supply shocks,
regulatory threats), the sector-health routing helper, and the new
``SIGNAL_TRUST_DELTA``/``SIGNAL_HALF_LIFE_DAYS``/``EVALUATION_WINDOWS``
entries in ``intelligence.trust_scorer``.

All tests use the shared ``mock_engine`` fixture from ``conftest.py`` —
no live DB calls. A handful of patches swap the module-level ``emit``
import so SignalFired emission inside detector paths is observable
without touching the real bus.
"""
from __future__ import annotations

from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from contracts.router import ROUTES, resolve_handler
from contracts.schemas import SignalFired


# ── Fixtures ───────────────────────────────────────────────────────────────


@pytest.fixture
def holder_signal() -> SignalFired:
    return SignalFired(
        producer_module="intelligence.holder_deal_overlap",
        correlation_id=uuid4(),
        signal_id=uuid4(),
        source="holder_deal_overlap:VanguardGroup",
        signal_type="holder_overlap",
        strength=1.0,
        ticker="AAPL",
        actor_hint="Vanguard Group Inc",
        raw_row_ids=[],
    )


@pytest.fixture
def fundamental_signal() -> SignalFired:
    return SignalFired(
        producer_module="intelligence.fundamental_divergence",
        correlation_id=uuid4(),
        signal_id=uuid4(),
        source="fundamental_divergence:Technology",
        signal_type="fundamental_divergence",
        strength=-0.6,  # short_candidate
        ticker="NVDA",
        raw_row_ids=[],
    )


@pytest.fixture
def unrelated_signal() -> SignalFired:
    return SignalFired(
        producer_module="intelligence.other",
        correlation_id=uuid4(),
        signal_id=uuid4(),
        source="other",
        signal_type="something_else",
        strength=1.0,
        ticker="AAPL",
        raw_row_ids=[],
    )


# ── 1. Router integrity (SignalFired route registered) ───────────────────


class TestRouterIntegrity:
    def test_signal_fired_routed(self):
        assert SignalFired in ROUTES
        paths = ROUTES[SignalFired]
        assert paths, "SignalFired must have at least one handler"
        assert "contracts.handlers.oracle_signals.on_signal_fired" in paths

    def test_all_wave_b_handlers_resolve(self):
        for path in ROUTES[SignalFired]:
            handler = resolve_handler(path)
            assert callable(handler), f"{path} is not callable"


# ── 2. oracle_signals handler ────────────────────────────────────────────


class TestOracleSignalsHandler:
    def test_handler_routes_holder_overlap(self, holder_signal, mock_engine):
        from contracts.handlers import oracle_signals

        oracle_signals.on_signal_fired(holder_signal, engine=mock_engine)

        # engine.begin() should have been used and exactly one insert issued
        assert mock_engine.begin.called
        conn = mock_engine.begin.return_value.__enter__.return_value
        assert conn.execute.call_count == 1
        args, kwargs = conn.execute.call_args
        params = args[1] if len(args) > 1 else kwargs
        assert params["st"] == "holder_overlap"
        assert params["t"] == "AAPL"
        assert params["d"] == "BUY"
        assert params["p"] == pytest.approx(1.0)

    def test_handler_routes_fundamental_divergence(
        self, fundamental_signal, mock_engine,
    ):
        from contracts.handlers import oracle_signals

        oracle_signals.on_signal_fired(fundamental_signal, engine=mock_engine)

        conn = mock_engine.begin.return_value.__enter__.return_value
        assert conn.execute.call_count == 1
        args, kwargs = conn.execute.call_args
        params = args[1] if len(args) > 1 else kwargs
        assert params["st"] == "fundamental_divergence"
        assert params["t"] == "NVDA"
        assert params["d"] == "SELL"
        assert params["p"] == pytest.approx(0.6)

    def test_handler_ignores_unrelated_types(
        self, unrelated_signal, mock_engine,
    ):
        from contracts.handlers import oracle_signals

        oracle_signals.on_signal_fired(unrelated_signal, engine=mock_engine)

        # No DB writes for types this handler does not own.
        assert not mock_engine.begin.called

    def test_handler_swallows_db_errors(self, holder_signal, mock_engine):
        from contracts.handlers import oracle_signals

        conn = mock_engine.begin.return_value.__enter__.return_value
        conn.execute.side_effect = RuntimeError("boom")

        # Must not raise — dispatcher relies on that guarantee.
        oracle_signals.on_signal_fired(holder_signal, engine=mock_engine)

    def test_handler_skips_zero_strength(self, mock_engine):
        from contracts.handlers import oracle_signals

        evt = SignalFired(
            producer_module="intelligence.holder_deal_overlap",
            correlation_id=uuid4(),
            signal_id=uuid4(),
            source="x",
            signal_type="holder_overlap",
            strength=0.0,
            ticker="AAPL",
            raw_row_ids=[],
        )
        oracle_signals.on_signal_fired(evt, engine=mock_engine)
        assert not mock_engine.begin.called


# ── 3. Oracle anti-signal extensions ──────────────────────────────────────


def _make_engine_for_oracle(mock_engine):
    """Build a stub engine wrapping ``mock_engine`` so the OracleEngine
    can be partially constructed without the DDL side-effects of
    ``_ensure_tables``.

    Returns an ``OracleEngine`` instance whose ``engine`` attribute is
    the mock and whose ``models`` list is empty (anti-signal methods
    don't depend on them).
    """
    from oracle.engine import OracleEngine

    eng = OracleEngine.__new__(OracleEngine)
    eng.engine = mock_engine
    eng.models = []
    eng._last_guard_verdicts = []
    return eng


class TestCrossLensAntiSignals:
    def test_bullish_call_surfaces_supply_shock(self, mock_engine):
        eng = _make_engine_for_oracle(mock_engine)

        # Fake one confirmed supply_shock row.
        from datetime import date

        row = (
            "brent_crude",          # upstream_id
            date(2026, 4, 1),        # shock_date
            0.12,                    # shock_magnitude
            -0.07,                   # downstream_move_pct
            -0.65,                   # correlation
            "derived",              # confidence
            "1-sigma upstream move", # evidence
        )
        conn = mock_engine.connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchall.return_value = [row]

        out = eng._cross_lens_anti_signals("XOM", "CALL")
        assert len(out) == 1
        anti = out[0]
        assert anti.name == "cross_lens_supply_shock"
        assert anti.family == "supply"
        assert anti.severity == pytest.approx(0.65)

    def test_bearish_call_skips_cross_lens(self, mock_engine):
        eng = _make_engine_for_oracle(mock_engine)

        out = eng._cross_lens_anti_signals("XOM", "PUT")
        assert out == []

    def test_empty_db_returns_nothing(self, mock_engine):
        eng = _make_engine_for_oracle(mock_engine)
        conn = mock_engine.connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchall.return_value = []

        out = eng._cross_lens_anti_signals("XOM", "CALL")
        assert out == []


class TestRegulatoryAntiSignals:
    def test_severity_mapping(self, mock_engine):
        eng = _make_engine_for_oracle(mock_engine)

        from datetime import date

        rows = [
            ("fda", "warning_letter", date(2026, 4, 1), "high",
             "FDA warning", "https://fda.gov/x"),
            ("doj", "indictment", date(2026, 4, 5), "critical",
             "DOJ indictment", "https://doj.gov/y"),
        ]
        conn = mock_engine.connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchall.return_value = rows

        out = eng._regulatory_anti_signals("PFE")
        {a.contradiction[:3]: a.severity for a in out}
        assert any(a.name == "regulatory_threat" for a in out)
        assert len(out) == 2
        # 'HIGH' → 0.7, 'CRITICAL' → 1.0
        mapped = sorted(a.severity for a in out)
        assert mapped == pytest.approx([0.7, 1.0])

    def test_no_rows_returns_empty(self, mock_engine):
        eng = _make_engine_for_oracle(mock_engine)
        conn = mock_engine.connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchall.return_value = []

        out = eng._regulatory_anti_signals("PFE")
        assert out == []


class TestFindAntiSignalsIntegration:
    def test_find_anti_signals_layers_all_three_sources(self, mock_engine):
        """When given a ticker, ``_find_anti_signals`` must fan out to
        both the cross-lens query and the regulatory-events query on top
        of the legacy in-memory z-score loop."""
        eng = _make_engine_for_oracle(mock_engine)

        # Build one in-memory contra signal + one each from the DB.
        from oracle.engine import Signal

        sigs = [
            Signal(
                name="vix_zscore", family="vol", value=22.0, z_score=-2.0,
                direction="bearish", weight=1.0, freshness_hours=0,
            ),
        ]

        from datetime import date

        def _fake_execute(*args, **kwargs):
            # Use SQL shape to distinguish which query was issued.
            result = MagicMock()
            stmt = args[0] if args else None
            sql = getattr(stmt, "text", str(stmt))
            if "supply_shock_attributions" in sql:
                result.fetchall.return_value = [(
                    "brent_crude", date(2026, 4, 1), 0.12, -0.07, -0.8,
                    "derived", "event",
                )]
            elif "regulatory_events" in sql:
                result.fetchall.return_value = [(
                    "fda", "recall", date(2026, 4, 2), "critical",
                    "recall", "https://fda.gov/z",
                )]
            else:
                result.fetchall.return_value = []
            return result

        conn = mock_engine.connect.return_value.__enter__.return_value
        conn.execute.side_effect = _fake_execute

        out = eng._find_anti_signals(sigs, "CALL", ticker="XOM")
        names = {a.name for a in out}
        assert "cross_lens_supply_shock" in names
        assert "regulatory_threat" in names
        # The z-score contra signal should still be there (name=vix_zscore).
        assert "vix_zscore" in names


# ── 4. Sector health routing (SYNTH-30) ──────────────────────────────────


class TestSectorHealthRouting:
    def test_healthy_sector_boosts_equity(self, mock_engine):
        eng = _make_engine_for_oracle(mock_engine)
        conn = mock_engine.connect.return_value.__enter__.return_value
        from datetime import date

        conn.execute.return_value.fetchone.return_value = (82.0, date(2026, 4, 10))

        out = eng._get_sector_health_routing("Technology")
        assert out  # non-empty
        assert out["equity"] > 1.0
        assert out["flows"] > 1.0
        assert out["vol"] < 1.0

    def test_unhealthy_sector_trims_equity(self, mock_engine):
        eng = _make_engine_for_oracle(mock_engine)
        conn = mock_engine.connect.return_value.__enter__.return_value
        from datetime import date

        conn.execute.return_value.fetchone.return_value = (20.0, date(2026, 4, 10))

        out = eng._get_sector_health_routing("Consumer Discretionary")
        assert out
        assert out["equity"] < 1.0
        assert out["flows"] < 1.0
        assert out["vol"] > 1.0

    def test_neutral_score_returns_empty(self, mock_engine):
        eng = _make_engine_for_oracle(mock_engine)
        conn = mock_engine.connect.return_value.__enter__.return_value
        from datetime import date

        conn.execute.return_value.fetchone.return_value = (52.0, date(2026, 4, 10))

        out = eng._get_sector_health_routing("Energy")
        assert out == {}

    def test_missing_sector_returns_empty(self, mock_engine):
        eng = _make_engine_for_oracle(mock_engine)
        conn = mock_engine.connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchone.return_value = None

        assert eng._get_sector_health_routing("NonexistentSector") == {}

    def test_empty_sector_arg_short_circuits(self, mock_engine):
        eng = _make_engine_for_oracle(mock_engine)
        assert eng._get_sector_health_routing("") == {}


# ── 5. Trust-scorer integrity (SYNTH-44 precursor) ───────────────────────


class TestTrustScorerIntegrity:
    """Every new SIGNAL_TRUST_DELTA key must have a matching half-life
    and evaluation window — this is the SYNTH-44 integrity contract put
    in place one wave early so regressions can't slip in."""

    def test_all_new_keys_have_all_three_entries(self):
        from intelligence.trust_scorer import (
            EVALUATION_WINDOWS,
            SIGNAL_HALF_LIFE_DAYS,
            SIGNAL_TRUST_DELTA,
        )

        new_keys = {
            "holder_overlap",
            "fundamental_divergence",
            "cross_lens_supply_shock",
            "regulatory_threat",
        }
        for key in new_keys:
            assert key in SIGNAL_TRUST_DELTA, f"{key} missing from SIGNAL_TRUST_DELTA"
            assert key in SIGNAL_HALF_LIFE_DAYS, f"{key} missing from SIGNAL_HALF_LIFE_DAYS"
            assert key in EVALUATION_WINDOWS, f"{key} missing from EVALUATION_WINDOWS"

    def test_wave_b_trust_delta_signs(self):
        from intelligence.trust_scorer import SIGNAL_TRUST_DELTA

        # Confirmations are positive, contradictions are negative.
        assert SIGNAL_TRUST_DELTA["holder_overlap"] > 0
        assert SIGNAL_TRUST_DELTA["fundamental_divergence"] > 0
        assert SIGNAL_TRUST_DELTA["cross_lens_supply_shock"] < 0
        assert SIGNAL_TRUST_DELTA["regulatory_threat"] < 0

    def test_global_integrity_delta_half_life_windows(self):
        """Every key in SIGNAL_TRUST_DELTA must also appear in the other
        two dicts — guards against partial additions in future waves."""
        from intelligence.trust_scorer import (
            EVALUATION_WINDOWS,
            SIGNAL_HALF_LIFE_DAYS,
            SIGNAL_TRUST_DELTA,
        )

        for key in SIGNAL_TRUST_DELTA:
            assert key in SIGNAL_HALF_LIFE_DAYS
            assert key in EVALUATION_WINDOWS


# ── 6. DEFAULT_MODELS carries the two new heads ──────────────────────────


class TestDefaultModels:
    def test_holder_overlap_model_present(self):
        from oracle.engine import DEFAULT_MODELS

        names = {m.name for m in DEFAULT_MODELS}
        assert "holder_overlap" in names
        assert "fundamental" in names

    def test_holder_overlap_families(self):
        from oracle.engine import DEFAULT_MODELS

        by_name = {m.name: m for m in DEFAULT_MODELS}
        assert by_name["holder_overlap"].signal_families == ["insider", "flows"]
        assert by_name["fundamental"].signal_families == ["macro", "equity"]


# ── 7. Detector emit shape (SYNTH-24 / SYNTH-26) ─────────────────────────


class TestHolderOverlapEmit:
    def test_emit_helper_constructs_signal_fired(self, monkeypatch):
        """``_emit_holder_overlap_signal`` must build a SignalFired with
        signal_type='holder_overlap' and a non-empty ticker."""
        from datetime import date

        from intelligence.holder_deal_overlap import (
            OverlapRow,
            _emit_holder_overlap_signal,
        )

        captured: list[SignalFired] = []

        def _fake_emit(contract):
            captured.append(contract)
            return contract.event_id

        import sys

        _emit_mod = sys.modules["contracts.emit"]
        monkeypatch.setattr(_emit_mod, "emit", _fake_emit)

        row = OverlapRow(
            deal_announcement_date=date(2026, 4, 1),
            acquirer_ticker="MSFT",
            target_ticker="AI",
            filer_name="Vanguard",
            acquirer_position_value_usd=1_000_000.0,
            target_position_value_usd=800_000.0,
            holding_report_date=date(2026, 3, 31),
            days_before_announcement=1,
            pre_position_flag=True,
            quick_exit_flag=True,
            narrative="",
        )

        _emit_holder_overlap_signal(row)

        assert len(captured) == 2  # one per leg
        assert {c.signal_type for c in captured} == {"holder_overlap"}
        assert {c.ticker for c in captured} == {"MSFT", "AI"}
        assert all(c.strength == pytest.approx(1.0) for c in captured)


class TestFundamentalDivergenceEmit:
    def test_short_candidate_emits_negative_strength(self, monkeypatch):
        from intelligence.fundamental_divergence import _emit_divergence_signal

        captured: list[SignalFired] = []

        def _fake_emit(contract):
            captured.append(contract)
            return contract.event_id

        import sys

        _emit_mod = sys.modules["contracts.emit"]
        monkeypatch.setattr(_emit_mod, "emit", _fake_emit)

        _emit_divergence_signal({
            "ticker": "TSLA",
            "sector": "Consumer Discretionary",
            "divergence": -55.0,
            "classification": "short_candidate",
        })

        assert len(captured) == 1
        c = captured[0]
        assert c.signal_type == "fundamental_divergence"
        assert c.ticker == "TSLA"
        assert c.strength < 0
        assert c.strength == pytest.approx(-0.55)

    def test_aligned_classification_does_not_emit(self, monkeypatch):
        from intelligence.fundamental_divergence import _emit_divergence_signal

        captured: list[SignalFired] = []
        import sys

        _emit_mod = sys.modules["contracts.emit"]
        monkeypatch.setattr(
            _emit_mod,
            "emit",
            lambda c: (captured.append(c), c.event_id)[1],
        )

        _emit_divergence_signal({
            "ticker": "AAPL",
            "sector": "Technology",
            "divergence": 5.0,
            "classification": "aligned",
        })
        assert captured == []
