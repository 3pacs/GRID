"""regime_history writer, backfill and staleness-field tests.

Covers the gap that let ``regime_history`` freeze at 2026-03-29: the table had
readers all over the codebase and no writer anywhere. These tests pin the
writer onto the scheduled path, pin the backfill's point-in-time boundaries,
and pin the additive as_of fields the surfaces read staleness from.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from scripts.auto_regime import (
    LOOKBACK_DAYS,
    REGIME_HISTORY_LABELS,
    REGIME_HISTORY_SOURCE,
    backfill_regime_history,
    compute_regime_at,
    persist_regime_history,
)
from store.pit import PITStore


AS_OF = date(2026, 9, 10)


def _pit_frame(as_of: date, n_days: int = 400, release_offset_days: int = 1) -> pd.DataFrame:
    """Build a long-format PIT frame like ``PITStore.get_pit`` returns."""
    rows = []
    for i in range(n_days):
        obs = as_of - timedelta(days=n_days - i)
        release = obs + timedelta(days=release_offset_days)
        for fid, base in ((1, 20.0), (2, 3.5), (3, 4500.0)):
            rows.append(
                {
                    "feature_id": fid,
                    "obs_date": obs,
                    "value": base + (i % 17) * 0.1,
                    "release_date": release,
                    "vintage_date": release,
                }
            )
    return pd.DataFrame(rows)


def _engine_with_features(
    fids=(
        (1, "vix", 900, date(2026, 9, 9)),
        (2, "hy_spread", 900, date(2026, 9, 9)),
        (3, "sp500", 900, date(2026, 9, 9)),
    )
):
    """Mock engine whose feature query answers the candidate/coverage shape.

    ``_REGIME_CANDIDATE_SQL`` returns (id, name, n_obs, newest_obs) — the
    coverage is what decides whether a candidate can back a concept.
    """
    engine = MagicMock()
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    result = MagicMock()
    result.fetchall.return_value = list(fids)
    result.rowcount = 1
    conn.execute.return_value = result
    engine.connect.return_value = conn
    engine.begin.return_value = conn
    return engine, conn


# ── Vocabulary the readers agree on ─────────────────────────────────────


class TestVocabulary:
    def test_writer_vocabulary_is_the_classifier_state_names(self):
        assert set(REGIME_HISTORY_LABELS) == {"GROWTH", "NEUTRAL", "FRAGILE", "CRISIS"}

    def test_every_label_passes_the_trial_signal_buy_gate_check(self):
        """trial_signal stores the label into a CHECK-constrained column and
        gates BUY on it — a label outside its set silently downgrades every
        BUY to WATCHLIST."""
        from grid.signals.trial_signal import ALLOWED_REGIMES

        assert set(REGIME_HISTORY_LABELS) <= ALLOWED_REGIMES

    def test_every_label_folds_into_the_oracle_five_state_bucket(self):
        from oracle.prediction_context import VALID_REGIMES, canonical_regime

        for label in REGIME_HISTORY_LABELS:
            assert canonical_regime(label) in VALID_REGIMES
        # And the distinction the four-label contract vocabulary would lose.
        assert canonical_regime("CRISIS") != canonical_regime("FRAGILE")

    def test_every_label_survives_the_astrogrid_normalizer(self):
        """Before this change the normalizer returned None for every state but
        NEUTRAL, so AstroGrid dropped the regime on any non-neutral day."""
        from store.astrogrid import _normalize_regime_label

        for label in REGIME_HISTORY_LABELS:
            assert _normalize_regime_label(label) in {
                "risk_on", "risk_off", "neutral", "transition"
            }
        assert _normalize_regime_label("GROWTH") == "risk_on"
        assert _normalize_regime_label("CRISIS") == "risk_off"
        assert _normalize_regime_label("NEUTRAL") == "neutral"
        # Unrelated junk is still rejected.
        assert _normalize_regime_label("banana") is None

    def test_the_legacy_loaded_rows_are_still_valid(self):
        """griddb's 12 existing rows are 'NEUTRAL' with source='decision_journal'."""
        assert "NEUTRAL" in REGIME_HISTORY_LABELS


# ── Persistence guards ──────────────────────────────────────────────────


class TestPersistRegimeHistory:
    def test_writes_parameterized_upsert_on_obs_date(self):
        engine, conn = _engine_with_features()
        assert persist_regime_history(engine, AS_OF, "GROWTH", 0.8) is True

        sql = str(conn.execute.call_args[0][0])
        params = conn.execute.call_args[0][1]
        assert "INSERT INTO regime_history" in sql
        assert "ON CONFLICT (obs_date)" in sql
        # Values are bound, never interpolated.
        assert ":obs_date" in sql and ":regime" in sql and ":confidence" in sql
        assert "GROWTH" not in sql
        assert params == {
            "obs_date": AS_OF,
            "regime": "GROWTH",
            "confidence": 0.8,
            "source": REGIME_HISTORY_SOURCE,
            # Unknown unless the caller establishes it — never guessed.
            "data_as_of": None,
        }

    def test_statements_are_whole_literals_not_assembled(self):
        """.claude/rules/security.md bans f-strings, .format() and concatenation
        in SQL. The repo's own guard (tests/test_regression_20260329.py) only
        scans api/routers and intelligence and only warns, so scripts/ needs its
        own pin."""
        import inspect

        import scripts.auto_regime as auto

        source = inspect.getsource(auto.persist_regime_history)
        # No SQL is built in the function body at all — the statements come from
        # module constants, so there is no fragment to interpolate into. (The
        # f-strings that remain are exception messages, which the rule allows.)
        assert "text(" not in source
        assert ".format(" not in source
        for line in source.splitlines():
            if line.lstrip().startswith(("f\"", "f'")):
                assert not any(
                    kw in line.upper()
                    for kw in ("SELECT", "INSERT", "UPDATE", "DELETE", "CONFLICT")
                ), f"SQL assembled in an f-string: {line.strip()}"
        # The two statements are module-level literals, selected by a flag.
        for stmt in (auto._REGIME_HISTORY_UPSERT_SQL, auto._REGIME_HISTORY_INSERT_IGNORE_SQL):
            sql = str(stmt)
            assert "INSERT INTO regime_history" in sql
            assert ":obs_date" in sql and ":regime" in sql
        assert "DO UPDATE SET" in str(auto._REGIME_HISTORY_UPSERT_SQL)
        assert "DO NOTHING" in str(auto._REGIME_HISTORY_INSERT_IGNORE_SQL)

    def test_stamps_its_own_provenance(self):
        """So a reader can tell these rows from the 2026-03 one-off load,
        which used source='decision_journal'."""
        engine, conn = _engine_with_features()
        persist_regime_history(engine, AS_OF, "NEUTRAL", 0.5)
        assert conn.execute.call_args[0][1]["source"] == "auto_regime"
        assert "source" in str(conn.execute.call_args[0][0])

    def test_overwrite_false_leaves_existing_row_alone(self):
        engine, conn = _engine_with_features()
        persist_regime_history(engine, AS_OF, "NEUTRAL", 0.5, overwrite=False)
        assert "DO NOTHING" in str(conn.execute.call_args[0][0])

    def test_rejects_label_outside_the_vocabulary(self):
        """A label the readers do not recognize is worse than no row —
        trial_signal would store UNKNOWN and downgrade every BUY."""
        engine, _ = _engine_with_features()
        for bad in ("risk_on", "growth", "EXPANSION", ""):
            with pytest.raises(ValueError, match="outside the vocabulary"):
                persist_regime_history(engine, AS_OF, bad, 0.5)

    @pytest.mark.parametrize("bad", [float("nan"), float("inf"), -0.1, 1.5])
    def test_rejects_non_finite_or_out_of_range_confidence(self, bad):
        engine, _ = _engine_with_features()
        with pytest.raises(ValueError, match=r"\[0, 1\]"):
            persist_regime_history(engine, AS_OF, "NEUTRAL", bad)


# ── PIT boundaries ──────────────────────────────────────────────────────


class TestComputeRegimeAtIsPointInTime:
    def test_queries_pit_with_the_decision_date_and_first_release(self):
        engine, _ = _engine_with_features()
        captured = {}

        def fake_get_pit(fids, as_of, vintage_policy="LATEST_AS_OF"):
            captured["fids"] = fids
            captured["as_of"] = as_of
            captured["policy"] = vintage_policy
            return _pit_frame(as_of)

        with patch.object(PITStore, "get_pit", side_effect=fake_get_pit):
            result = compute_regime_at(engine, AS_OF)

        assert captured["as_of"] == AS_OF
        # FIRST_RELEASE, not LATEST_AS_OF: a backfilled day must not see the
        # revised vintage that only exists today.
        assert captured["policy"] == "FIRST_RELEASE"
        assert sorted(captured["fids"]) == [1, 2, 3]
        assert result["regime"] in REGIME_HISTORY_LABELS

    def test_asserts_no_lookahead_before_returning(self):
        engine, _ = _engine_with_features()
        with patch.object(PITStore, "get_pit", side_effect=lambda f, a, vintage_policy=None: _pit_frame(a)), \
             patch.object(PITStore, "assert_no_lookahead") as guard:
            compute_regime_at(engine, AS_OF)
        guard.assert_called_once()
        assert guard.call_args[0][1] == AS_OF

    def test_a_row_released_after_as_of_raises_rather_than_classifying(self):
        engine, _ = _engine_with_features()
        # release_date one day past as_of for the newest observations.
        tainted = _pit_frame(AS_OF, release_offset_days=400)

        with patch.object(PITStore, "get_pit", side_effect=lambda f, a, vintage_policy=None: tainted):
            with pytest.raises(ValueError, match="LOOKAHEAD VIOLATION"):
                compute_regime_at(engine, AS_OF)

    def test_no_observation_is_newer_than_the_decision_date(self):
        engine, _ = _engine_with_features()
        seen = {}

        def capture(df, as_of_date):
            seen["max_release"] = df["release_date"].max()
            seen["max_obs"] = df["obs_date"].max()

        with patch.object(PITStore, "get_pit", side_effect=lambda f, a, vintage_policy=None: _pit_frame(a)), \
             patch.object(PITStore, "assert_no_lookahead", side_effect=capture):
            compute_regime_at(engine, AS_OF)

        assert seen["max_release"] <= AS_OF
        assert seen["max_obs"] <= AS_OF

    def test_window_is_bounded_to_the_lookback(self):
        engine, _ = _engine_with_features()
        captured = {}

        def fake_stress(matrix, names, weights):
            captured["min_index"] = matrix.index.min()
            return np.zeros(len(matrix)), {}

        with patch.object(PITStore, "get_pit", side_effect=lambda f, a, vintage_policy=None: _pit_frame(a, n_days=1200)), \
             patch("scripts.auto_regime._compute_stress_index", side_effect=fake_stress):
            compute_regime_at(engine, AS_OF)

        oldest_allowed = pd.Timestamp(AS_OF - timedelta(days=LOOKBACK_DAYS))
        assert captured["min_index"] >= oldest_allowed

    def test_insufficient_pit_data_reports_error_instead_of_a_label(self):
        engine, _ = _engine_with_features()
        with patch.object(PITStore, "get_pit", side_effect=lambda f, a, vintage_policy=None: _pit_frame(a, n_days=10)):
            result = compute_regime_at(engine, AS_OF)
        assert result["regime"] == "UNKNOWN"
        assert "insufficient data" in result["error"]


# ── Backfill ────────────────────────────────────────────────────────────


class TestBackfill:
    def test_computes_each_day_with_its_own_as_of(self):
        engine, _ = _engine_with_features()
        start, end = date(2026, 9, 1), date(2026, 9, 5)
        seen_as_of: list[date] = []

        def fake_compute(eng, as_of, weights=None, fid_to_name=None):
            seen_as_of.append(as_of)
            return {"regime": "NEUTRAL", "confidence": 0.5}

        with patch("scripts.auto_regime.compute_regime_at", side_effect=fake_compute), \
             patch("scripts.auto_regime.persist_regime_history", return_value=True) as writer:
            result = backfill_regime_history(engine, start, end, overwrite=True)

        assert seen_as_of == [start + timedelta(days=i) for i in range(5)]
        assert result["written"] == 5
        # Each row is stamped with the date it was computed for.
        assert [c[0][1] for c in writer.call_args_list] == seen_as_of

    def test_rejects_an_inverted_range(self):
        engine, _ = _engine_with_features()
        with pytest.raises(ValueError, match="before start"):
            backfill_regime_history(engine, date(2026, 9, 5), date(2026, 9, 1))

    def test_span_is_bounded(self):
        engine, _ = _engine_with_features()
        with pytest.raises(ValueError, match="exceeds max_days"):
            backfill_regime_history(
                engine, date(2020, 1, 1), date(2026, 9, 10), max_days=400
            )

    def test_skips_dates_that_already_have_a_row_unless_overwriting(self):
        engine, conn = _engine_with_features()
        existing = MagicMock()
        existing.fetchall.return_value = [(date(2026, 9, 2),), (date(2026, 9, 3),)]
        feats = MagicMock()
        feats.fetchall.return_value = [
            (1, "vix", 900, date(2026, 9, 9)),
            (2, "hy_spread", 900, date(2026, 9, 9)),
            (3, "sp500", 900, date(2026, 9, 9)),
        ]
        conn.execute.side_effect = [feats, existing]

        with patch("scripts.auto_regime.compute_regime_at",
                   return_value={"regime": "NEUTRAL", "confidence": 0.5}), \
             patch("scripts.auto_regime.persist_regime_history", return_value=True):
            result = backfill_regime_history(
                engine, date(2026, 9, 1), date(2026, 9, 4), overwrite=False
            )

        assert result["skipped"] == 2
        assert result["written"] == 2

    def test_a_day_without_enough_pit_data_is_reported_not_fabricated(self):
        engine, _ = _engine_with_features()
        with patch("scripts.auto_regime.compute_regime_at",
                   return_value={"regime": "UNKNOWN", "error": "insufficient data"}), \
             patch("scripts.auto_regime.persist_regime_history") as writer:
            result = backfill_regime_history(
                engine, date(2026, 9, 1), date(2026, 9, 3), overwrite=True
            )
        assert result["failed"] == 3
        assert result["written"] == 0
        writer.assert_not_called()


# ── Scheduler registration ──────────────────────────────────────────────


class TestSchedulerRegistration:
    def test_daily_pulls_runs_regime_detection(self):
        import ingestion.scheduler as sched
        import inspect

        source = inspect.getsource(sched.run_daily_pulls)
        assert "from scripts.auto_regime import run" in source

    def test_daily_pulls_is_registered_on_the_schedule(self):
        import inspect

        import ingestion.scheduler as sched

        source = inspect.getsource(sched.start_scheduler)
        assert "run_daily_pulls" in source

    def test_scheduled_run_persists_regime_history(self):
        """The scheduled path must reach the regime_history writer, not stop at
        decision_journal — the exact gap that froze the table."""
        import inspect

        import scripts.auto_regime as auto

        source = inspect.getsource(auto.run)
        assert "persist_regime_history" in source


# ── Additive staleness fields ───────────────────────────────────────────


class TestStalenessFields:
    def test_regime_current_carries_as_of_date_and_age(self):
        from api.routers import regime as regime_router

        reading_ts = datetime.now(timezone.utc) - timedelta(days=165)
        engine = MagicMock()
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = False

        prod = MagicMock()
        prod.fetchone.return_value = (7, "stress_index", "1.0")
        latest = MagicMock()
        latest.fetchone.return_value = (
            "FRAGILE", 0.72, 0.1, {}, "DEFENSIVE", "NEUTRAL", reading_ts,
        )
        # Third query: the regime_history row's data_as_of for that date.
        history = MagicMock()
        history.fetchone.return_value = (None,)
        conn.execute.side_effect = [prod, latest, history]
        engine.connect.return_value = conn

        with patch.object(regime_router, "get_db_engine", return_value=engine):
            resp = regime_router.get_current(_token="t")

        assert resp.as_of_date == reading_ts.date().isoformat()
        assert resp.staleness_days == 165
        # Existing fields are untouched — the response stays backward compatible.
        assert resp.state == "FRAGILE"
        assert resp.as_of == reading_ts.isoformat()

    def test_regime_current_defaults_are_backward_compatible(self):
        from api.schemas.regime import RegimeCurrentResponse

        resp = RegimeCurrentResponse(state="NEUTRAL")
        assert resp.as_of_date == ""
        assert resp.staleness_days is None

    def test_chat_regime_context_reports_the_rows_own_date(self):
        from api.routers import chat

        obs_date = date.today() - timedelta(days=165)
        engine = MagicMock()
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = False
        result = MagicMock()
        result.fetchone.return_value = (
            obs_date, "FRAGILE", 0.7, datetime(2026, 3, 29, 20, 10), obs_date,
        )
        conn.execute.return_value = result
        engine.connect.return_value = conn

        with patch.object(chat, "_get_db_engine", return_value=engine):
            framing, source = chat._gather_regime_context()

        assert source == "regime_history"
        assert obs_date.isoformat() in framing
        assert "165 days old" in framing

    def test_chat_regime_context_selects_obs_date(self):
        from api.routers import chat

        engine = MagicMock()
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = False
        result = MagicMock()
        result.fetchone.return_value = None
        conn.execute.return_value = result
        engine.connect.return_value = conn

        with patch.object(chat, "_get_db_engine", return_value=engine):
            chat._gather_regime_context()

        assert "obs_date" in str(conn.execute.call_args[0][0])

    def test_todays_row_carries_no_staleness_warning(self):
        from api.routers import chat

        engine = MagicMock()
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = False
        result = MagicMock()
        result.fetchone.return_value = (
            date.today(), "NEUTRAL", 0.6, datetime.now(), date.today(),
        )
        conn.execute.return_value = result
        engine.connect.return_value = conn

        with patch.object(chat, "_get_db_engine", return_value=engine):
            framing, _ = chat._gather_regime_context()

        assert "days old" not in framing
