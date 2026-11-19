"""Tests for the Oracle API router.

Stubs ``api.auth`` and ``api.dependencies`` if their heavy transitive deps
(``psycopg2``, ``jose``, ``passlib``) cannot be imported in the lightweight
CI environment. Mirrors the pattern in ``tests/test_intelligence_search.py``.
"""

from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from types import ModuleType
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Prefer the real api.auth — only stub if heavy deps are unavailable.
# Unconditional stubbing pollutes sys.modules for every later test.
# ---------------------------------------------------------------------------

try:
    import api.auth  # noqa: F401
except Exception:
    _auth_stub = ModuleType("api.auth")
    _auth_stub.require_auth = lambda: None  # type: ignore[attr-defined]
    sys.modules["api.auth"] = _auth_stub

try:
    import api.dependencies  # noqa: F401
except Exception:
    _deps_stub = ModuleType("api.dependencies")
    _deps_stub.get_db_engine = lambda: None  # type: ignore[attr-defined]
    sys.modules["api.dependencies"] = _deps_stub


@dataclass
class _DummyPrediction:
    ticker: str
    direction: str
    score: int
    confidence: float
    strength: float
    coherence: float
    model_count: int
    level: str
    model_votes: list[dict]
    as_of: datetime | None
    horizon: int = 7


def _run(coro):
    """Run a handler result synchronously (routes may be sync or async)."""
    if not asyncio.iscoroutine(coro):
        return coro
    return asyncio.new_event_loop().run_until_complete(coro)


def test_predict_live_returns_serialized_prediction_payload():
    """Live oracle predictions should expose a JSON-safe confidence stack payload."""
    engine = MagicMock()
    prediction = _DummyPrediction(
        ticker="AAPL",
        direction="bullish",
        score=73,
        confidence=0.61,
        strength=0.55,
        coherence=0.8,
        model_count=4,
        level="meta",
        model_votes=[{"model_name": "macro-1", "direction": "bullish"}],
        as_of=datetime(2026, 4, 18, 12, 30, tzinfo=timezone.utc),
        horizon=7,
    )

    with patch("api.routers.oracle.get_db_engine", return_value=engine), patch("api.routers.oracle.EnsemblePredictor") as predictor_cls:
        predictor_cls.return_value.predict.return_value = prediction

        from api.routers.oracle import predict_live

        result = _run(predict_live(ticker="aapl", horizon=7, _token="test"))

    predictor_cls.assert_called_once_with(engine)
    predictor_cls.return_value.predict.assert_called_once()
    assert result["ticker"] == "AAPL"
    assert result["direction"] == "bullish"
    assert result["model_count"] == 4
    assert result["horizon"] == 7
    assert result["as_of"] == "2026-04-18T12:30:00+00:00"


# ── Pagination envelope (no DB needed) ────────────────────────────────────

class TestGetPredictionsPaginationEnvelope:
    """Verify ``GET /api/v1/oracle/predictions`` carries ``limit``/``offset``/
    ``has_more`` per ``.claude/rules/security.md`` list-endpoint contract
    (canonical pattern at ``api/routers/journal.py:77``)."""

    @staticmethod
    def _patched_engine(*, total: int, rows: list | None = None):
        """Mock ``engine.connect()`` context manager for the count + results
        queries. First ``execute()`` is the COUNT (fetchone → ``(total,)``);
        second is the SELECT (fetchall → ``rows or []``)."""
        conn = MagicMock()
        count_result = MagicMock()
        count_result.fetchone.return_value = (total,)
        rows_result = MagicMock()
        rows_result.fetchall.return_value = rows or []
        conn.execute.side_effect = [count_result, rows_result]
        engine = MagicMock()
        engine.connect.return_value.__enter__.return_value = conn
        engine.connect.return_value.__exit__.return_value = False
        return engine

    def test_empty_result_envelope_includes_pagination_fields(self):
        from api.routers import oracle as mod

        with patch.object(mod, "get_db_engine",
                          return_value=self._patched_engine(total=0)):
            result = mod.get_predictions(
                ticker=None, model=None, status=None,
                limit=50, offset=20, _token="test",
            )

        assert result["predictions"] == []
        assert result["total"] == 0
        assert result["limit"] == 50
        assert result["offset"] == 20
        assert result["has_more"] is False

    def test_has_more_true_when_more_pages_exist(self):
        from api.routers import oracle as mod

        # total=100, offset=0, limit=10 → (0+10) < 100 ⇒ has_more=True.
        # Build a single result row with the 24 columns the SELECT projects
        # (23 plus null_write_policy — the historical-NULL provenance
        # boundary stamp, packet 2a item (a)). Use verdict='hit'
        # (non-'pending') so the tracking-pnl branch at oracle.py:124 stays
        # inert and we only need the count + select results queued on the
        # conn.execute side_effect.
        now = datetime(2026, 5, 19, tzinfo=timezone.utc)
        fake_row = (
            "pred-1", now, "AAPL", "options", "CALL", 200.0, 180.0,
            "2026-06-19", 0.7, 5.0, 0.6, 0.8, "model-a", "v1",
            {"items": [], "actor_evidence": {"direct_vote_row_ids": [42]}}, [], {}, "hit", 210.0, 16.6, 16.6, now, "scored",
            "oracle_pred_nullable_0918", None,
        )
        with patch.object(mod, "get_db_engine",
                          return_value=self._patched_engine(total=100, rows=[fake_row])):
            result = mod.get_predictions(
                ticker=None, model=None, status=None,
                limit=10, offset=0, _token="test",
            )

        assert result["predictions"][0]["flow_context"]["actor_evidence"]["direct_vote_row_ids"] == [42]
        assert result["total"] == 100
        assert result["limit"] == 10
        assert result["offset"] == 0
        assert result["has_more"] is True
        assert len(result["predictions"]) == 1
        assert result["predictions"][0]["ticker"] == "AAPL"
        assert result["predictions"][0]["null_write_policy"] == "oracle_pred_nullable_0918"

    def test_has_more_false_on_last_page(self):
        from api.routers import oracle as mod

        # total=20, offset=10, limit=10 → (10+10) < 20 is False, last page.
        # 24 columns — see the comment in test_has_more_true_when_more_
        # pages_exist above.
        now = datetime(2026, 5, 19, tzinfo=timezone.utc)
        fake_row = (
            "pred-2", now, "NVDA", "options", "PUT", 100.0, 120.0,
            "2026-06-19", 0.55, -3.0, 0.4, 0.7, "model-b", "v1",
            [], [], {}, "miss", 130.0, 8.3, -8.3, now, "scored",
            None, None,
        )
        with patch.object(mod, "get_db_engine",
                          return_value=self._patched_engine(total=20, rows=[fake_row])):
            result = mod.get_predictions(
                ticker=None, model=None, status=None,
                limit=10, offset=10, _token="test",
            )

        assert result["total"] == 20
        assert result["limit"] == 10
        assert result["offset"] == 10
        assert result["has_more"] is False
        assert result["predictions"][0]["null_write_policy"] is None


def test_prediction_lifecycle_filters_execute_and_preserve_unscorable_reason():
    """The actual read queries partition rows; unknown states remain reachable."""
    from api.routers import oracle as mod

    engine = create_engine("sqlite://")
    today = date.today()
    with engine.begin() as conn:
        conn.execute(text("""CREATE TABLE oracle_predictions (
            id TEXT, created_at TEXT, ticker TEXT, prediction_type TEXT,
            direction TEXT, target_price REAL, entry_price REAL, expiry DATE,
            confidence REAL, expected_move_pct REAL, signal_strength REAL,
            coherence REAL, model_name TEXT, model_version TEXT, signals TEXT,
            anti_signals TEXT, flow_context TEXT, verdict TEXT, actual_price REAL,
            actual_move_pct REAL, pnl_pct REAL, scored_at TEXT, score_notes TEXT,
            null_write_policy TEXT, dedup_keep BOOLEAN, recording_policy TEXT
        )"""))
        rows = [
            ("active", "pending", today + timedelta(days=1), True),
            ("due", "pending", today, True),
            ("overdue", "pending", today - timedelta(days=1), True),
            ("no-price", "no_data", today, True),
            ("hit", "hit", today, True),
            ("miss", "miss", today, True),
            ("partial", "partial", today, True),
            ("unknown", "void", today, True),
            ("null-verdict", None, today, True),
            ("no-expiry", "pending", None, True),
            ("duplicate", "no_data", today, False),
        ]
        conn.execute(text("""INSERT INTO oracle_predictions
            (id, ticker, model_name, verdict, expiry, dedup_keep, score_notes)
            VALUES (:id, 'SYNTH', 'test-model', :verdict, :expiry, :keep, 'No entry price recorded')"""),
            [dict(zip(("id", "verdict", "expiry", "keep"), row)) for row in rows])

    expected = {
        "active": {"active"}, "expired": {"due", "overdue"},
        "unscorable": {"no-price"}, "scored": {"hit", "miss", "partial"},
        "other": {"unknown", "null-verdict", "no-expiry"},
    }
    with patch.object(mod, "get_db_engine", return_value=engine):
        for status, ids in expected.items():
            result = mod.get_predictions(ticker=None, model=None, status=status, limit=100, offset=0, _token="test")
            assert {p["id"] for p in result["predictions"]} == ids
            assert result["total"] == len(ids)
            assert result["has_more"] is False
            if status == "unscorable":
                assert result["predictions"][0]["score_notes"] == "No entry price recorded"
        filtered = mod.get_predictions(ticker="OTHER", model=None, status="unscorable", limit=100, offset=0, _token="test")
        assert filtered["total"] == 0


DECISION = datetime(2026, 11, 10, 15, 10, tzinfo=timezone.utc)


def _receipt(**changes):
    from oracle.entry_price_policy import QUOTE_REFERENCE_POLICY
    return {
        "status": "measured", "policy": QUOTE_REFERENCE_POLICY,
        "ticker": "SPY", "source": "options_capture_batches", "provider": "cboe",
        "basis": "raw_spot", "price": 100.0, "capture_batch_id": "fixture-batch",
        "capture_source": "options_puller", "backfilled": False,
        "market_at": "2026-11-10T15:05:00+00:00",
        "capture_started_at": "2026-11-10T15:08:00+00:00",
        "capture_completed_at": "2026-11-10T15:09:00+00:00",
        "available_at": "2026-11-10T15:09:01+00:00",
        "read_observed_at": "2026-11-10T15:09:02+00:00",
        "observed_on": "2026-11-10", **changes,
    }


@pytest.mark.parametrize("changes,reason", [
    ({"market_at": None}, "missing or unzoned"),
    ({"market_at": "2026-11-10T15:05:00"}, "missing or unzoned"),
    ({"market_at": "2026-11-10T14:49:59+00:00"}, "20-minute"),
    ({"market_at": "2026-11-10T15:11:00+00:00"}, "future or inconsistent"),
    ({"available_at": "2026-11-10T15:11:00+00:00"}, "future or inconsistent"),
    ({"read_observed_at": None}, "database read"),
    ({"read_observed_at": "2026-11-10T15:10:01+00:00"}, "database read"),
    ({"market_at": "2026-11-09T15:05:00+00:00"}, "different market session"),
    ({"provider": None}, "unqualified"),
    ({"backfilled": True}, "unqualified"),
    ({"price": float("inf")}, "positive and finite"),
    ({"price": 0}, "positive and finite"),
    ({"price": True}, "positive and finite"),
    ({"ticker": "NVDA"}, "does not match"),
])
def test_quote_receipt_fail_closed(changes, reason):
    from oracle.entry_price_policy import validate_quote_reference
    assert reason in validate_quote_reference(_receipt(**changes), as_of=DECISION, ticker="SPY")


def test_exact_age_boundary_and_final_decision_revalidation():
    from oracle.entry_price_policy import validate_quote_reference
    receipt = _receipt(market_at="2026-11-10T14:50:00+00:00")
    assert validate_quote_reference(receipt, as_of=DECISION, ticker="SPY") is None
    assert "20-minute" in validate_quote_reference(receipt, as_of=DECISION + timedelta(seconds=1))


@pytest.mark.parametrize("cutoff", [
    datetime(2026, 11, 10, 14, 29, tzinfo=timezone.utc),  # pre-open
    datetime(2026, 11, 10, 21, 0, tzinfo=timezone.utc),  # regular close
    datetime(2026, 11, 7, 15, 0, tzinfo=timezone.utc),  # Saturday
    datetime(2026, 11, 26, 15, 0, tzinfo=timezone.utc),  # Thanksgiving
    datetime(2026, 11, 27, 18, 0, tzinfo=timezone.utc),  # early close
])
def test_reference_entry_refuses_closed_session(cutoff):
    from oracle.entry_price_policy import validate_quote_reference
    assert "session is closed" in validate_quote_reference(_receipt(), as_of=cutoff)


def test_loader_does_not_read_a_capture_registered_after_cutoff_or_infer_a_missing_clock():
    from oracle.entry_price_policy import load_quote_reference
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text("""CREATE TABLE options_capture_batches (
            ticker TEXT, spot_price REAL, provider_regular_market_at TEXT, spot_provider TEXT,
            capture_batch_id TEXT, capture_source TEXT, capture_started_at TEXT,
            capture_completed_at TEXT, registered_at TEXT, snap_date TEXT,
            backfilled BOOLEAN, capture_ordinal INTEGER)"""))
        for ordinal, registered, quote in [
            (1, DECISION - timedelta(minutes=1), None),
            (2, DECISION + timedelta(seconds=1), DECISION - timedelta(minutes=2)),
        ]:
            conn.execute(text("""INSERT INTO options_capture_batches VALUES
                ('SPY', 100, :quote, 'cboe', :batch, 'options_puller', :started,
                 :completed, :registered, '2026-11-10', FALSE, :ordinal)"""),
                {"quote": quote, "batch": str(ordinal), "started": DECISION - timedelta(minutes=2),
                 "completed": DECISION - timedelta(minutes=1), "registered": registered, "ordinal": ordinal})
        receipt = load_quote_reference(conn, "SPY", as_of=DECISION)
    assert receipt["capture_batch_id"] == "1"
    assert receipt["status"] == "unavailable"
    assert "missing or unzoned" in receipt["reason"]
    assert receipt["executable"] is False
    assert receipt["available_at_basis"] == "registration_lower_bound_not_commit"


def test_tracking_requires_postdecision_quote_and_rebases_recorded_splits():
    from oracle.entry_price_policy import tracking_reference_return
    from store.observations import SplitEvent
    entry = _receipt()
    current = _receipt(price=51.0, observed_on="2026-11-11",
                       market_at="2026-11-11T15:15:00+00:00",
                       capture_started_at="2026-11-11T15:16:00+00:00",
                       capture_completed_at="2026-11-11T15:17:00+00:00",
                       available_at="2026-11-11T15:17:01+00:00",
                       read_observed_at="2026-11-11T15:17:02+00:00")
    as_of = datetime(2026, 11, 11, 15, 20, tzinfo=timezone.utc)
    kwargs = dict(ticker="SPY", direction="LONG", entry_price=100, entry_receipt=entry,
                  decision_at=DECISION, expiry=date(2026, 11, 12), current_receipt=current, as_of=as_of)
    with patch("store.observations.read_splits", return_value=[SplitEvent("SPY", date(2026, 11, 11), 2.0)]) as splits:
        move, reason = tracking_reference_return(MagicMock(), **kwargs)
    assert move == pytest.approx(2.0)
    assert "not an executable return" in reason
    assert splits.call_args.kwargs["as_of_ts"] == as_of
    move, reason = tracking_reference_return(MagicMock(), **(kwargs | {"expiry": date(2026, 11, 10)}))
    assert move is None and "beyond expiry" in reason
    move, reason = tracking_reference_return(MagicMock(), **(kwargs | {"current_receipt": entry, "as_of": DECISION}))
    assert move is None and "after the forecast decision" in reason
    move, reason = tracking_reference_return(MagicMock(), **(kwargs | {"entry_receipt": None}))
    assert move is None and "legacy or unknown" in reason


def test_api_cohort_filters_are_bound_to_named_policies_and_receipt_visible():
    from api.routers import oracle as mod
    receipt = _receipt()
    row = ("new", DECISION, "SPY", "options", "CALL", 110, 100,
           date(2026, 11, 12), .7, 10, .5, .5, "fixture", "v1", [], [],
           {"entry_price_basis": receipt}, "hit", 110, 10, 10, DECISION,
           "fixture", "oracle_pred_nullable_0918", "oracle_forward_v2")
    engine = TestGetPredictionsPaginationEnvelope._patched_engine(total=1, rows=[row])
    with patch.object(mod, "get_db_engine", return_value=engine):
        result = mod.get_predictions(ticker=None, model=None, status="scored", cohort="prospective",
                                     limit=100, offset=0, _token="test")
    assert result["predictions"][0]["entry_price_basis"] == receipt
    assert result["predictions"][0]["recording_policy"] == "oracle_forward_v2"
    queries = [str(call.args[0]) for call in engine.connect.return_value.__enter__.return_value.execute.call_args_list]
    assert all("recording_policy = 'oracle_forward_v2'" in query for query in queries)


@pytest.mark.parametrize("stale", [False, True])
def test_pending_api_exposes_qualified_clock_and_never_uses_undated_spot(stale):
    from api.routers import oracle as mod
    now = DECISION + timedelta(minutes=10)
    current = _receipt(price=102.0, market_at=(DECISION + timedelta(minutes=5)).isoformat(),
                       capture_started_at=(DECISION + timedelta(minutes=6)).isoformat(),
                       capture_completed_at=(DECISION + timedelta(minutes=7)).isoformat(),
                       available_at=(DECISION + timedelta(minutes=8)).isoformat(),
                       read_observed_at=(DECISION + timedelta(minutes=9)).isoformat())
    if stale:
        current["market_at"] = (now - timedelta(minutes=21)).isoformat()
    row = ("new", DECISION, "SPY", "options", "LONG", 110, 100,
           date(2026, 11, 12), .7, 10, .5, .5, "fixture", "v1", [], [],
           {"entry_price_basis": _receipt()}, "pending", None, None, None, None,
           None, "oracle_pred_nullable_0918", "oracle_forward_v2")
    engine = TestGetPredictionsPaginationEnvelope._patched_engine(total=1, rows=[row])
    with patch.object(mod, "get_db_engine", return_value=engine), \
            patch.object(mod, "datetime") as clock, \
            patch.object(mod, "load_quote_reference", return_value=current) as loader, \
            patch("store.observations.read_splits", return_value=[]):
        clock.now.return_value = now
        result = mod.get_predictions(ticker=None, model=None, status="active", cohort="prospective",
                                     limit=100, offset=0, _token="test")
    prediction = result["predictions"][0]
    assert prediction["tracking_quote"] == current
    loader.assert_called_once()
    if stale:
        assert prediction["tracking_pnl"] is None
        assert "20-minute" in prediction["tracking_pnl_basis"]
    else:
        assert prediction["tracking_pnl"] == 2.0
        assert "not an executable return" in prediction["tracking_pnl_basis"]
    queries = [str(call.args[0]) for call in engine.connect.return_value.__enter__.return_value.execute.call_args_list]
    assert not any("options_daily_signals" in query for query in queries)
