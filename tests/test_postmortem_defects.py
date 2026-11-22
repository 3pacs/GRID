"""Regression tests for verified postmortem, outcome-event, and thesis-monitor defects.

Covers:
1. Unit mixing in intelligence/postmortem.py (option premium vs underlying spot).
2. Staleness check using future expiry instead of decision-time signal age.
3. EXPIRED worthless (-100%) mapping to PARTIAL instead of MISS (loss).
4. PostmortemCompleted stamping producer_module='intelligence.postmortem'
   instead of a key matching ModelRegistry.decay_model_by_source.
5. apply_contagion_feedback using abs(actual_price_move_pct) and reinforcing
   edges on wrong-direction moves.
6. thesis_invalidation_monitor querying non-existent decision_journal columns
   (and ticker_metrics_daily.as_of_date) and swallowing DB errors at log.debug.

All tests are offline (synthetic inputs, mocks, or in-memory SQLite).
"""

from __future__ import annotations

import json
import re
import sys
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, event, text

from contracts.handlers import oracle_weights
from intelligence import postmortem as pm_mod
from intelligence import thesis_invalidation_monitor as tim
from intelligence.postmortem import (
    PostMortem,
    _classify_failure,
    _compute_implied_pct,
    _emit_trade_postmortem,
    _normalize_producer_key,
    _verdict_from_outcome,
    apply_contagion_feedback,
    generate_postmortem,
    generate_prediction_postmortem,
)
from oracle.engine import DEFAULT_MODELS, ModelRegistry
from oracle.trace_evolver import FailurePattern, TargetedMutator, TraceAnalyzer


def _capture_emits(monkeypatch) -> list:
    """Patch ``contracts.emit.emit`` safely (contracts.__init__ re-exports emit)."""
    captured: list = []

    def _fake_emit(contract):
        captured.append(contract)
        return contract.event_id

    emit_mod = sys.modules.get("contracts.emit")
    if emit_mod is None:
        import contracts.emit as _imported  # noqa: F401
        emit_mod = sys.modules["contracts.emit"]
    monkeypatch.setattr(emit_mod, "emit", _fake_emit)
    return captured


# ── Defect 1: Unit mixing (option premium vs underlying spot) ───────────────


class TestDefect1UnitMixing:
    def test_losing_call_does_not_compare_underlying_spot_against_option_premium(self):
        """Option premium $2.50 vs underlying path $180 -> $170 must NOT yield
        +7,100% and mislabel a monotonic losing CALL as right_signal_wrong_timing."""
        cat, root_cause, _wrong, _right, _missed = _classify_failure(
            ticker="AAPL",
            direction="CALL",
            entry_price=2.50,  # option premium paid
            strike=185.0,
            target=5.00,
            expiry=date(2026, 4, 15),
            outcome="EXPIRED",
            actual_return=-1.0,
            price_path=[
                {"date": "2026-04-10", "price": 180.0},
                {"date": "2026-04-13", "price": 178.0},
                {"date": "2026-04-14", "price": 175.0},
                {"date": "2026-04-15", "price": 170.0},
            ],
            data_at_decision={
                "decision_date": "2026-04-10",
                "scanner_signals": {
                    "spot_price": 180.0,
                    "signal_date": "2026-04-10",
                },
            },
            sanity={},
        )
        assert cat == "wrong_signal", f"Expected wrong_signal, got {cat}: {root_cause}"
        assert "7100" not in root_cause

    def test_losing_put_gets_right_signal_wrong_timing_when_underlying_dropped_first(self):
        """PUT bought with underlying at $180, option premium $2.50. Underlying
        dropped -2.22% to $176 before rallying to $185 -> right_signal_wrong_timing."""
        cat, root_cause, _wrong, _right, _missed = _classify_failure(
            ticker="AAPL",
            direction="PUT",
            entry_price=2.50,  # option premium paid
            strike=175.0,
            target=5.00,
            expiry=date(2026, 4, 15),
            outcome="EXPIRED",
            actual_return=-1.0,
            price_path=[
                {"date": "2026-04-10", "price": 180.0},
                {"date": "2026-04-13", "price": 176.0},  # -2.22% favorable for PUT
                {"date": "2026-04-15", "price": 185.0},
            ],
            data_at_decision={
                "decision_date": "2026-04-10",
                "scanner_signals": {
                    "spot_price": 180.0,
                    "signal_date": "2026-04-10",
                },
            },
            sanity={},
        )
        assert cat == "right_signal_wrong_timing", f"Got {cat}: {root_cause}"
        assert "2.2%" in root_cause

    def test_missing_underlying_spot_labels_insufficient_data(self):
        """When no underlying spot at entry is available, fail closed with
        'insufficient_data' rather than dividing by option premium or guessing."""
        cat, root_cause, _wrong, _right, _missed = _classify_failure(
            ticker="AAPL",
            direction="CALL",
            entry_price=2.50,  # option premium only — NOT underlying spot
            strike=185.0,
            target=5.00,
            expiry=date(2026, 4, 15),
            outcome="EXPIRED",
            actual_return=-1.0,
            price_path=[
                {"date": "2026-04-13", "price": 180.0},
                {"date": "2026-04-15", "price": 170.0},
            ],
            data_at_decision={"decision_date": "2026-04-10"},
            sanity={},
        )
        assert cat == "insufficient_data", f"Expected insufficient_data, got {cat}: {root_cause}"

    def test_generate_postmortem_loads_pit_raw_series_close_when_scanner_spot_missing(self, monkeypatch):
        """When options_daily_signals has no row and price path falls back to
        raw_series, generate_postmortem loads the PIT close (obs_date <= generated_at)
        from raw_series rather than dividing by option premium."""
        gen_ts = datetime(2026, 4, 10, 15, 0, tzinfo=timezone.utc)
        closed_ts = datetime(2026, 4, 24, 21, 0, tzinfo=timezone.utc)
        executed_queries: list[tuple[str, dict]] = []

        class _FakeConn:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def execute(self, stmt, params=None):
                sql = " ".join(str(stmt).split())
                p = dict(params or {})
                executed_queries.append((sql, p))
                res = MagicMock()
                if "FROM options_recommendations" in sql:
                    res.fetchone.return_value = (
                        7, "AAPL", "CALL", 185.0, date(2026, 4, 24), 2.50,
                        5.00, 1.25, 0.7, "bullish call", "{}", gen_ts,
                        "EXPIRED", -1.0, closed_ts, "",
                        json.dumps([{"name": "iv_skew", "score": 3.0, "direction": "bullish"}]),
                        "[]",
                    )
                elif "FROM raw_series" in sql and "obs_date >= :s" in sql:
                    # Price path fell monotonically from 180 to 170
                    res.fetchall.return_value = [
                        (date(2026, 4, 10), 180.0),
                        (date(2026, 4, 17), 175.0),
                        (date(2026, 4, 24), 170.0),
                    ]
                elif "FROM raw_series" in sql and "obs_date <= :d" in sql:
                    # PIT underlying close at or before generated_at
                    res.fetchone.return_value = (180.0,)
                else:
                    res.fetchone.return_value = None
                    res.fetchall.return_value = []
                return res

        class _FakeEngine:
            def connect(self):
                return _FakeConn()

            def begin(self):
                return _FakeConn()

        monkeypatch.setattr(pm_mod, "_ensure_tables", lambda _e: None)
        monkeypatch.setattr(pm_mod, "_store_postmortem", lambda *a, **k: None)
        monkeypatch.setattr(pm_mod, "_emit_trade_postmortem", lambda **k: None)
        monkeypatch.setattr(
            pm_mod,
            "_get_llm_postmortem",
            lambda **k: {"recommended_fix": k["root_cause"], "what_we_missed": "", "confidence": 0.5},
        )

        pm = generate_postmortem(_FakeEngine(), trade_id=7)
        assert pm is not None
        assert pm.failure_category == "wrong_signal"
        assert pm.actual_return == pytest.approx(-1.0)
        # Confirm a PIT-bounded raw_series entry close query ran with d == 2026-04-10
        pit_entry_calls = [
            (sql, p) for sql, p in executed_queries
            if "FROM raw_series" in sql and "obs_date <= :d" in sql
        ]
        assert len(pit_entry_calls) == 1
        assert pit_entry_calls[0][1]["d"] == date(2026, 4, 10)


# ── Defect 2: Staleness check uses future expiry instead of decision time ──


class TestDefect2Staleness:
    def test_fresh_signal_with_35_dte_expiry_is_not_bad_data(self):
        """Signal observed on the decision date (0 days old) with a 35-DTE expiry
        must NOT be labelled bad_data."""
        cat, root_cause, _w, _r, _m = _classify_failure(
            ticker="NVDA",
            direction="PUT",
            entry_price=3.00,
            strike=120.0,
            target=6.00,
            expiry=date(2026, 5, 15),  # 35 days after decision
            outcome="EXPIRED",
            actual_return=-1.0,
            price_path=[
                {"date": "2026-04-10", "price": 125.0},
                {"date": "2026-05-15", "price": 130.0},
            ],
            data_at_decision={
                "decision_date": "2026-04-10",
                "scanner_signals": {
                    "spot_price": 125.0,
                    "signal_date": "2026-04-10",
                },
            },
            sanity={},
        )
        assert cat == "wrong_signal", f"Fresh signal misclassified as {cat}: {root_cause}"

    def test_genuinely_stale_signal_at_entry_is_bad_data(self):
        """Scanner signal 16 days older than decision time IS stale -> bad_data,
        and root_cause reports age at decision time (16 days), not expiry gap (51 days)."""
        cat, root_cause, _w, _r, _m = _classify_failure(
            ticker="NVDA",
            direction="PUT",
            entry_price=3.00,
            strike=120.0,
            target=6.00,
            expiry=date(2026, 5, 15),  # 51 days after 2026-03-25
            outcome="EXPIRED",
            actual_return=-1.0,
            price_path=[
                {"date": "2026-04-10", "price": 125.0},
                {"date": "2026-05-15", "price": 130.0},
            ],
            data_at_decision={
                "decision_date": "2026-04-10",
                "scanner_signals": {
                    "spot_price": 125.0,
                    "signal_date": "2026-03-25",  # 16 days before 2026-04-10
                },
            },
            sanity={},
        )
        assert cat == "bad_data", f"Expected bad_data, got {cat}: {root_cause}"
        assert "16 days old" in root_cause


# ── Defect 3: EXPIRED worthless maps to MISS (LOSS), not PARTIAL ───────────


class TestDefect3ExpiredMapping:
    def test_expired_outcome_maps_to_miss(self):
        assert _verdict_from_outcome("EXPIRED") == "MISS"
        assert _verdict_from_outcome("expired") == "MISS"
        assert _verdict_from_outcome("LOSS") == "MISS"
        assert _verdict_from_outcome("PARTIAL") == "PARTIAL"
        assert _verdict_from_outcome("WIN") == "HIT"

    def test_expired_options_trade_emits_miss_and_skips_oracle_decay(self, monkeypatch):
        """An EXPIRED worthless options trade must emit verdict='MISS' and
        producer_module='trading.options_recommender', and oracle_weights must
        skip decay (options trades are not attributable to oracle models)."""
        captured = _capture_emits(monkeypatch)
        info_logs: list[str] = []
        monkeypatch.setattr(oracle_weights, "_SKIP_DECAY_LOGGED_EVENT_TYPES", set())
        monkeypatch.setattr(
            oracle_weights.log,
            "info",
            lambda msg, **kw: info_logs.append(msg.format(**kw)),
        )

        pm = PostMortem(
            trade_id=101,
            ticker="AAPL",
            direction="CALL",
            outcome="EXPIRED",
            actual_return=-1.0,
            data_at_decision={
                "decision_signals": [{"name": "iv_skew", "score": 3.0, "direction": "bullish"}],
            },
            thesis_at_decision="call thesis",
            sanity_results_at_decision={},
            what_actually_happened="expired worthless",
            price_path=[],
            failure_category="wrong_signal",
            root_cause="wrong direction",
            which_signals_were_wrong=["iv_skew_flat"],
            which_signals_were_right=[],
            what_we_missed="",
            recommended_fix="",
            confidence_in_analysis=0.7,
            generated_at=datetime(2026, 4, 25, tzinfo=timezone.utc).isoformat(),
        )
        _emit_trade_postmortem(pm=pm, trade_id=101)
        assert len(captured) == 1
        evt = captured[0]
        assert evt.verdict == "MISS"
        assert evt.producer_module == "trading.options_recommender"

        mock_engine = MagicMock()
        with patch("oracle.engine.ModelRegistry") as MockRegistry:
            oracle_weights.on_postmortem_completed(evt, engine=mock_engine)
            oracle_weights.on_postmortem_completed(evt, engine=mock_engine)
            MockRegistry.assert_not_called()
            MockRegistry.return_value.decay_model_by_source.assert_not_called()
            MockRegistry.return_value.decay_model_by_name.assert_not_called()

        skip_msgs = [
            m for m in info_logs
            if "skip decay: outcome not attributable to an oracle model" in m
        ]
        # Logged at INFO once per event type, not repeated on the second event.
        assert len(skip_msgs) == 1


# ── Defect 4: Targeted oracle-model decay vs non-oracle options outcomes ───


class TestDefect4ProducerModuleMatching:
    def _sqlite_oracle_engine(self, extra_models: list[tuple[str, list[str]]] | None = None):
        eng = create_engine("sqlite://")

        @event.listens_for(eng, "connect")
        def _register_funcs(dbapi_conn, _rec):
            dbapi_conn.create_function("NOW", 0, lambda: "2026-04-25 00:00:00")
            dbapi_conn.create_function("GREATEST", 2, lambda a, b: max(a, b))

        with eng.begin() as conn:
            conn.execute(text("""
                CREATE TABLE oracle_models (
                    name TEXT PRIMARY KEY,
                    signal_families TEXT,
                    weight DOUBLE PRECISION DEFAULT 1.0,
                    last_updated TEXT
                )
            """))
            for m in DEFAULT_MODELS:
                conn.execute(
                    text("INSERT INTO oracle_models (name, signal_families, weight) VALUES (:n, :f, 1.0)"),
                    {"n": m.name, "f": json.dumps(m.signal_families)},
                )
            for name, families in extra_models or []:
                conn.execute(
                    text("INSERT INTO oracle_models (name, signal_families, weight) VALUES (:n, :f, 1.0)"),
                    {"n": name, "f": json.dumps(families)},
                )
        return eng

    def test_options_loss_decays_nothing(self, monkeypatch):
        """An options-trade LOSS derived from options_recommendations/scanner
        must emit producer_module='trading.options_recommender' and leave ALL
        oracle model weights untouched (even models carrying vol/sentiment)."""
        captured = _capture_emits(monkeypatch)
        info_logs: list[str] = []
        monkeypatch.setattr(oracle_weights, "_SKIP_DECAY_LOGGED_EVENT_TYPES", set())
        monkeypatch.setattr(
            oracle_weights.log,
            "info",
            lambda msg, **kw: info_logs.append(msg.format(**kw)),
        )

        pm = PostMortem(
            trade_id=202,
            ticker="TSLA",
            direction="CALL",
            outcome="LOSS",
            actual_return=-0.8,
            data_at_decision={
                "decision_signals": [{"name": "iv_skew", "score": 4.0, "direction": "bullish"}],
            },
            thesis_at_decision="skew play",
            sanity_results_at_decision={},
            what_actually_happened="dropped",
            price_path=[],
            failure_category="wrong_signal",
            root_cause="skew failed",
            which_signals_were_wrong=["iv_skew_flat"],
            which_signals_were_right=[],
            what_we_missed="",
            recommended_fix="",
            confidence_in_analysis=0.6,
            generated_at=datetime(2026, 4, 25, tzinfo=timezone.utc).isoformat(),
        )
        _emit_trade_postmortem(pm=pm, trade_id=202)
        assert len(captured) == 1
        evt = captured[0]
        assert evt.verdict == "MISS"
        assert evt.producer_module == "trading.options_recommender"

        eng = self._sqlite_oracle_engine(
            extra_models=[
                ("options_flow_mut_abc123", ["vol", "sentiment"]),
                ("trace_remove_s_xyz789", ["vol", "sentiment", "equity"]),
            ]
        )
        oracle_weights.on_postmortem_completed(evt, engine=eng)

        with eng.connect() as conn:
            weights = dict(conn.execute(text("SELECT name, weight FROM oracle_models")).fetchall())
        assert all(w == pytest.approx(1.0) for w in weights.values()), (
            f"Options LOSS must decay zero oracle models, got {weights}"
        )
        assert any(
            "skip decay: outcome not attributable to an oracle model" in m
            for m in info_logs
        )

    def test_oracle_prediction_loss_with_model_name_decays_only_that_model(self, monkeypatch):
        """An oracle-prediction LOSS carrying a model_name must decay ONLY that
        exact model (bound WHERE name = :name), leaving every other model
        (including models sharing vol/sentiment signal_families) untouched."""
        captured = _capture_emits(monkeypatch)
        created_ts = datetime(2026, 4, 10, 14, 0, tzinfo=timezone.utc)
        scored_ts = datetime(2026, 4, 17, 21, 0, tzinfo=timezone.utc)

        class _FakeConn:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def execute(self, stmt, params=None):
                sql = " ".join(str(stmt).split())
                res = MagicMock()
                if "FROM oracle_predictions" in sql:
                    res.fetchone.return_value = (
                        "pred-42", "NVDA", "CALL", 140.0, 125.0, date(2026, 4, 17),
                        0.72, 12.0, "options_flow_mut_abc123",
                        json.dumps([{"name": "iv_skew", "direction": "bullish"}]),
                        "[]", "{}", "miss", 118.0, -5.6, -0.056, scored_ts, "missed",
                        created_ts,
                    )
                else:
                    res.fetchone.return_value = None
                    res.fetchall.return_value = []
                return res

        class _FakePredEngine:
            def connect(self):
                return _FakeConn()

            def begin(self):
                return _FakeConn()

        monkeypatch.setattr(pm_mod, "_ensure_tables", lambda _e: None)
        monkeypatch.setattr(pm_mod, "_store_postmortem", lambda *a, **k: None)
        monkeypatch.setattr(
            pm_mod,
            "_get_llm_postmortem",
            lambda **k: {"recommended_fix": k["root_cause"], "what_we_missed": "", "confidence": 0.6},
        )

        pm = generate_prediction_postmortem(_FakePredEngine(), prediction_id="pred-42")
        assert pm is not None
        assert len(captured) == 1
        evt = captured[0]
        assert evt.verdict == "MISS"
        assert evt.producer_module == "oracle.model:options_flow_mut_abc123"

        eng = self._sqlite_oracle_engine(
            extra_models=[
                ("options_flow_mut_abc123", ["vol", "sentiment"]),
                ("trace_remove_s_xyz789", ["vol", "sentiment"]),
            ]
        )
        executed_sql: list[str] = []

        @event.listens_for(eng, "before_cursor_execute")
        def _record_sql(_conn, _cursor, statement, _parameters, _context, _executemany):
            executed_sql.append(" ".join(statement.split()))

        oracle_weights.on_postmortem_completed(evt, engine=eng)

        with eng.connect() as conn:
            weights = dict(conn.execute(text("SELECT name, weight FROM oracle_models")).fetchall())

        assert weights["options_flow_mut_abc123"] == pytest.approx(0.9)
        for name, weight in weights.items():
            if name != "options_flow_mut_abc123":
                assert weight == pytest.approx(1.0), (
                    f"Sibling model {name!r} was unexpectedly decayed to {weight}"
                )

        update_sqls = [s for s in executed_sql if s.upper().startswith("UPDATE ORACLE_MODELS")]
        assert len(update_sqls) == 1
        assert "LIKE" not in update_sqls[0].upper()
        assert "signal_families" not in update_sqls[0].lower()
        assert "WHERE name = ?" in update_sqls[0]

    def test_oracle_prediction_loss_without_model_name_skips_decay(self, monkeypatch):
        """An oracle-prediction LOSS with no model_name must NOT decay anything."""
        captured = _capture_emits(monkeypatch)
        info_logs: list[str] = []
        monkeypatch.setattr(oracle_weights, "_SKIP_DECAY_LOGGED_EVENT_TYPES", set())
        monkeypatch.setattr(
            oracle_weights.log,
            "info",
            lambda msg, **kw: info_logs.append(msg.format(**kw)),
        )
        created_ts = datetime(2026, 4, 10, 14, 0, tzinfo=timezone.utc)
        scored_ts = datetime(2026, 4, 17, 21, 0, tzinfo=timezone.utc)

        class _FakeConn:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def execute(self, stmt, params=None):
                sql = " ".join(str(stmt).split())
                res = MagicMock()
                if "FROM oracle_predictions" in sql:
                    res.fetchone.return_value = (
                        "pred-99", "NVDA", "CALL", 140.0, 125.0, date(2026, 4, 17),
                        0.72, 12.0, None, "[]", "[]", "{}", "miss", 118.0, -5.6,
                        -0.056, scored_ts, "missed", created_ts,
                    )
                else:
                    res.fetchone.return_value = None
                    res.fetchall.return_value = []
                return res

        class _FakePredEngine:
            def connect(self):
                return _FakeConn()

            def begin(self):
                return _FakeConn()

        monkeypatch.setattr(pm_mod, "_ensure_tables", lambda _e: None)
        monkeypatch.setattr(pm_mod, "_store_postmortem", lambda *a, **k: None)
        monkeypatch.setattr(
            pm_mod,
            "_get_llm_postmortem",
            lambda **k: {"recommended_fix": k["root_cause"], "what_we_missed": "", "confidence": 0.6},
        )

        pm = generate_prediction_postmortem(_FakePredEngine(), prediction_id="pred-99")
        assert pm is not None
        assert len(captured) == 1
        evt = captured[0]
        assert evt.producer_module == "unknown"

        eng = self._sqlite_oracle_engine()
        oracle_weights.on_postmortem_completed(evt, engine=eng)
        with eng.connect() as conn:
            weights = dict(conn.execute(text("SELECT name, weight FROM oracle_models")).fetchall())
        assert all(w == pytest.approx(1.0) for w in weights.values())
        assert any(
            "skip decay: outcome not attributable to an oracle model" in m
            for m in info_logs
        )

    def test_unmapped_producer_returns_none_and_is_skipped(self, monkeypatch):
        """_normalize_producer_key must return None for unmapped values instead
        of passing them through, and oracle_weights must skip decay cleanly."""
        for unmapped in (
            "some_unmapped_producer",
            "vol",
            "sentiment",
            "supply",
            "iv_skew",
            "intelligence.postmortem",
            "unknown",
            "oracle.engine",
            "oracle.model:",
            "oracle.model:   ",
            "oracle.model:unknown",
            "",
            None,
        ):
            assert _normalize_producer_key(unmapped) is None, (
                f"Expected None for unmapped key {unmapped!r}"
            )

        assert _normalize_producer_key("trading.options_recommender") == "trading.options_recommender"
        assert _normalize_producer_key("options_scanner") == "trading.options_recommender"
        assert _normalize_producer_key("oracle.model:flow_momentum") == "oracle.model:flow_momentum"

        captured = _capture_emits(monkeypatch)
        warnings: list[str] = []
        info_logs: list[str] = []
        monkeypatch.setattr(pm_mod.log, "warning", lambda msg, **kw: warnings.append(msg.format(**kw)))
        monkeypatch.setattr(oracle_weights, "_SKIP_DECAY_LOGGED_EVENT_TYPES", set())
        monkeypatch.setattr(
            oracle_weights.log,
            "info",
            lambda msg, **kw: info_logs.append(msg.format(**kw)),
        )

        pm = PostMortem(
            trade_id=203,
            ticker="TSLA",
            direction="CALL",
            outcome="LOSS",
            actual_return=-0.8,
            data_at_decision={},
            thesis_at_decision="",
            sanity_results_at_decision={},
            what_actually_happened="",
            price_path=[],
            failure_category="wrong_signal",
            root_cause="unknown",
            which_signals_were_wrong=[],
            which_signals_were_right=[],
            what_we_missed="",
            recommended_fix="",
            confidence_in_analysis=0.5,
            generated_at=datetime(2026, 4, 25, tzinfo=timezone.utc).isoformat(),
        )
        _emit_trade_postmortem(pm=pm, trade_id=203, producer_module="some_unmapped_producer")
        assert len(captured) == 1
        evt = captured[0]
        assert evt.producer_module == "unknown"
        assert warnings, "Expected a warning log when producer_module is unmapped"

        eng = self._sqlite_oracle_engine()
        with patch("oracle.engine.ModelRegistry") as MockRegistry:
            oracle_weights.on_postmortem_completed(evt, engine=eng)
            # Also verify a raw event carrying a family or arbitrary unmapped string is skipped
            raw_family_evt = evt.model_copy(update={"producer_module": "vol"})
            raw_unmapped_evt = evt.model_copy(update={"producer_module": "custom.unmapped"})
            oracle_weights.on_postmortem_completed(raw_family_evt, engine=eng)
            oracle_weights.on_postmortem_completed(raw_unmapped_evt, engine=eng)
            MockRegistry.assert_not_called()

        skip_msgs = [
            m for m in info_logs
            if "skip decay: outcome not attributable to an oracle model" in m
        ]
        assert len(skip_msgs) == 1

    def test_decay_model_by_name_is_exact_and_bounded(self):
        """ModelRegistry.decay_model_by_name decays only the named model and respects floor."""
        eng = self._sqlite_oracle_engine(
            extra_models=[("options_flow_v2", ["vol", "sentiment"])]
        )
        reg = ModelRegistry(eng)
        assert reg.decay_model_by_name("options_flow", 0.9) == 1
        assert reg.decay_model_by_name("", 0.9) == 0
        assert reg.decay_model_by_name("options_flow", 0.0) == 0
        assert reg.decay_model_by_name("nonexistent_model", 0.9) == 0

        with eng.connect() as conn:
            w_exact = conn.execute(
                text("SELECT weight FROM oracle_models WHERE name = 'options_flow'")
            ).scalar()
            w_prefix = conn.execute(
                text("SELECT weight FROM oracle_models WHERE name = 'options_flow_v2'")
            ).scalar()
        assert w_exact == pytest.approx(0.9)
        assert w_prefix == pytest.approx(1.0)


# ── Follow-up: TraceEvolver insufficient_data + POSTMORTEM_WRITES_LESSONS ─


class TestTraceEvolverInsufficientDataAndLessonGuard:
    def test_trace_evolver_treats_insufficient_data_as_noop(self):
        """insufficient_data must map to a no-op remedy (not 'Investigate ...')
        and produce zero mutation proposals even if what_we_missed mentions words
        matching signal names like 'time'."""
        eng = MagicMock()
        analyzer = TraceAnalyzer(eng)
        action = analyzer._recommend_action(
            "insufficient_data", [], [], ["time", "spot", "price"]
        )
        assert "no-op" in action.lower() or "ignore" in action.lower()
        assert "consider adding missed signals" not in action.lower()

        mutator = TargetedMutator(eng)
        pattern = FailurePattern(
            category="insufficient_data",
            frequency=5,
            fraction=0.5,
            signals_commonly_wrong=["feature:sentiment"],
            signals_commonly_right=[],
            signals_commonly_missed=["time", "spot"],
            affected_models=["flow_momentum"],
            regime_context="NEUTRAL",
            recommended_action=action,
        )
        assert mutator.propose([pattern]) == []

    def test_postmortem_writes_lessons_remains_false(self):
        assert pm_mod.POSTMORTEM_WRITES_LESSONS is False


# ── Defect 5: Contagion feedback sign on wrong-direction moves ─────────────


class TestDefect5ContagionFeedbackSign:
    def test_compute_implied_pct_wrong_direction_does_not_reinforce(self):
        """Positive cost shock (+30%) expects negative downstream move; an actual
        move of +10% (wrong direction) must yield implied=0.0, not +0.476."""
        implied = _compute_implied_pct(
            actual_price_move_pct=+0.10,
            shock_magnitude=0.30,
            pass_through=0.70,
        )
        assert implied == pytest.approx(0.0)

    def test_compute_implied_pct_positive_prediction_respects_sign(self):
        # Right direction (+move for +prediction)
        assert _compute_implied_pct(
            actual_price_move_pct=+0.021,
            shock_magnitude=0.30,
            pass_through=0.70,
            predicted_margin_impact_pct=+0.05,
        ) == pytest.approx(0.10, rel=1e-3)
        # Wrong direction (-move for +prediction)
        assert _compute_implied_pct(
            actual_price_move_pct=-0.10,
            shock_magnitude=0.30,
            pass_through=0.70,
            predicted_margin_impact_pct=+0.05,
        ) == pytest.approx(0.0)

    def test_apply_contagion_feedback_decays_edge_on_wrong_direction_move(self):
        """Edge at 0.02 with predicted -5% and actual +10% (wrong direction,
        accuracy=0.0) must DECAY toward 0 (0.02 -> 0.014), NEVER reinforce to 0.04."""
        edge_updates: list[dict] = []

        class _Row:
            def __init__(self, vals):
                self._v = vals

            def __getitem__(self, i):
                return self._v[i]

        class _Conn:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def execute(self, stmt, params=None):
                sql = " ".join(str(stmt).lower().split())
                p = dict(params or {})
                res = MagicMock()
                if "from contagion_backtest_results" in sql:
                    res.fetchall.return_value = [
                        _Row((1, 42, "HSY", -0.05, +0.10, 0.0, "cocoa_beans", 0.30))
                    ]
                elif "from supply_chain_edges" in sql:
                    res.fetchall.return_value = [
                        _Row((101, "cocoa_beans", "HSY", 0.02, "estimated"))
                    ]
                elif "update supply_chain_edges" in sql and "pct_downstream_cogs" in sql:
                    edge_updates.append(p)
                return res

        class _Eng:
            def connect(self):
                return _Conn()

            def begin(self):
                return _Conn()

        summary = apply_contagion_feedback(_Eng(), since_hours=24)
        assert summary["decayed"] == 1
        assert len(edge_updates) == 1
        # 0.7 * 0.02 + 0.3 * 0.0 = 0.014 (strictly decayed, NOT reinforced to 0.04)
        assert edge_updates[0]["v"] == pytest.approx(0.014, abs=1e-6)
        assert edge_updates[0]["v"] < 0.02


# ── Defect 6: thesis_invalidation_monitor schema + error visibility ────────


class TestDefect6ThesisInvalidationMonitor:
    def test_queries_use_real_schema_columns_only(self):
        """decision_journal query must not reference non-existent columns
        (metadata, outcome, logged_at, or decision_journal.ticker), and
        ticker_metrics_daily must order by obs_date (not non-existent as_of_date)."""
        captured_sql: list[tuple[str, dict]] = []
        now = datetime(2026, 4, 13, 16, 0, tzinfo=timezone.utc)

        eng = MagicMock()
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = False

        def _execute(stmt, params=None):
            sql = " ".join(str(stmt).split())
            captured_sql.append((sql, dict(params or {})))
            res = MagicMock()
            if "FROM decision_journal" in sql:
                res.fetchall.return_value = [
                    (
                        42,
                        "AAPL",
                        {"invalidation": {"type": "price_level", "operator": "below", "threshold": 180.0}},
                        now,
                    )
                ]
            elif "FROM ticker_metrics_daily" in sql:
                res.fetchone.return_value = (175.0,)
            else:
                res.fetchall.return_value = []
                res.fetchone.return_value = None
            return res

        conn.execute = _execute
        eng.connect.return_value = conn

        run = tim.run_monitor(eng, as_of=now)
        assert run.predictions_scanned == 1
        assert run.triggered_count == 1

        dj_sql = next(sql for sql, _ in captured_sql if "FROM decision_journal" in sql)
        # Forbidden non-existent columns on decision_journal
        assert not re.search(r"\bmetadata\b", dj_sql), f"Non-existent column 'metadata' in: {dj_sql}"
        assert not re.search(r"\blogged_at\b", dj_sql), f"Non-existent column 'logged_at' in: {dj_sql}"
        assert not re.search(r"\boutcome\s+IS\s+NULL\b", dj_sql, re.IGNORECASE), (
            f"Non-existent column 'outcome' in: {dj_sql}"
        )
        # Real columns present
        assert "contradiction_flags" in dj_sql
        assert "decision_timestamp" in dj_sql
        assert "verdict IS NULL" in dj_sql
        assert "outcome_recorded_at IS NULL" in dj_sql
        assert "agent_runs" in dj_sql

        tmd_sql, tmd_params = next(
            (sql, p) for sql, p in captured_sql if "FROM ticker_metrics_daily" in sql
        )
        assert not re.search(r"(?<!:)\bas_of_date\b", tmd_sql), (
            f"Non-existent column 'as_of_date' in: {tmd_sql}"
        )
        assert "obs_date" in tmd_sql
        assert tmd_params.get("as_of_date") == date(2026, 4, 13)

        assert run.query_failures == 0
        assert run.had_query_error is False

    def test_generate_postmortem_end_to_end_expired_put_fresh_signal(self, monkeypatch):
        """End-to-end trade postmortem on an EXPIRED PUT with 35 DTE, fresh signal,
        option premium $2.50, and underlying dipping from $180 to $176 before
        rallying to $185 -> right_signal_wrong_timing, verdict=MISS,
        producer='trading.options_recommender'."""
        gen_ts = datetime(2026, 4, 10, 14, 30, tzinfo=timezone.utc)
        closed_ts = datetime(2026, 5, 15, 21, 0, tzinfo=timezone.utc)
        captured = _capture_emits(monkeypatch)

        class _FakeConn:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def execute(self, stmt, params=None):
                sql = " ".join(str(stmt).split())
                res = MagicMock()
                if "FROM options_recommendations" in sql:
                    res.fetchone.return_value = (
                        88, "AAPL", "PUT", 175.0, date(2026, 5, 15), 2.50,
                        5.00, 1.25, 0.65, "bearish put", "{}", gen_ts,
                        "EXPIRED", -1.0, closed_ts, "",
                        json.dumps([{"name": "iv_skew", "score": 3.5, "direction": "bearish"}]),
                        "[]",
                    )
                elif "put_call_ratio" in sql:
                    res.fetchone.return_value = (
                        1.4, 0.32, 0.25, 172.0, 180.0, 50000, -0.04, 0.12, date(2026, 4, 10),
                    )
                elif "signal_date >= :s" in sql:
                    res.fetchall.return_value = [
                        (date(2026, 4, 10), 180.0),
                        (date(2026, 4, 20), 176.0),  # -2.22% favorable for PUT
                        (date(2026, 5, 15), 185.0),
                    ]
                else:
                    res.fetchone.return_value = None
                    res.fetchall.return_value = []
                return res

        class _FakeEngine:
            def connect(self):
                return _FakeConn()

            def begin(self):
                return _FakeConn()

        monkeypatch.setattr(pm_mod, "_ensure_tables", lambda _e: None)
        monkeypatch.setattr(pm_mod, "_store_postmortem", lambda *a, **k: None)
        monkeypatch.setattr(
            pm_mod,
            "_get_llm_postmortem",
            lambda **k: {"recommended_fix": k["root_cause"], "what_we_missed": "", "confidence": 0.6},
        )

        pm = generate_postmortem(_FakeEngine(), trade_id=88)
        assert pm is not None
        assert pm.failure_category == "right_signal_wrong_timing"
        assert pm.actual_return == pytest.approx(-1.0)
        assert len(captured) == 1
        assert captured[0].verdict == "MISS"
        assert captured[0].producer_module == "trading.options_recommender"

    def test_broken_query_logs_warning_and_sets_failure_flag(self, monkeypatch):
        """A failing SQL query must log at WARNING (not debug) and surface via
        query_failures / had_query_error / errors in MonitorRun."""
        warnings: list[str] = []
        monkeypatch.setattr(tim.log, "warning", lambda msg, **kw: warnings.append(msg.format(**kw)))

        eng = MagicMock()
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = False
        conn.execute.side_effect = RuntimeError('column "metadata" does not exist')
        eng.connect.return_value = conn

        run = tim.run_monitor(eng, as_of=datetime(2026, 4, 13, tzinfo=timezone.utc))
        assert run.predictions_scanned == 0
        assert warnings, "Expected log.warning on failed DB query"
        assert len(run.errors) == 1
        assert "metadata" in run.errors[0]
        assert run.had_query_error is True
        assert run.query_failures == 1
        d = run.to_dict()
        assert d["had_query_error"] is True
        assert d["query_failures"] == 1
