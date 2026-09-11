"""Regime data truth: fed features, and the data date that travels with the label.

PR #440 made ``scripts/auto_regime.py`` write ``regime_history`` and backfilled
it. The rows were hollow. The 14 weighted names resolved to feature_registry
rows whose newest ``resolved_series.obs_date`` is 2026-04-03, so every ``as_of``
from 2026-04-15 to 2026-09-10 saw the same point-in-time frame and produced the
same answer — S 0.5425, dS -0.1056, NEUTRAL 0.4287 — written 160 times, each row
stamped with the day it was computed and therefore reported by
``/api/v1/regime/current`` as ``staleness_days: 0``.

Two defects, two halves of this file:

  * the concepts were bound to registry rows nothing feeds, while the live raw
    series map to different rows (``VIXCLS`` → ``vix_spot``, ``BAMLH0A0HYM2`` →
    ``hy_oas_spread``, ``YF:^GSPC:close`` → ``sp500_full``, ``DGS10`` →
    ``yc_10y``);
  * nothing carried the date the data actually came from, so a stale reading
    was indistinguishable from a fresh one on every surface.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from scripts.auto_regime import (
    DEFAULT_FEATURE_WEIGHTS,
    MAX_FRESH_DATA_AGE_DAYS,
    MIN_CANDIDATE_OBSERVATIONS,
    REGIME_FEATURE_SOURCES,
    _candidate_names,
    _frame_data_as_of,
    _frame_observation_dates,
    _resolve_regime_bindings,
    _resolve_regime_features,
    _stale_frame_features,
    backfill_regime_history,
    compute_regime_at,
    persist_regime_history,
)
from store.pit import PITStore

AS_OF = date(2026, 9, 10)
# Where resolver-fed features stop. Ops-exec run 34547630786 read this off
# griddb: no weighted feature has an observation after 2026-04-03.
DATA_CLIFF = date(2026, 4, 3)


def _engine_returning(rows):
    """Engine whose one query returns ``rows`` as (id, name, n_obs, newest)."""
    engine = MagicMock()
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    result = MagicMock()
    result.fetchall.return_value = list(rows)
    result.rowcount = 1
    conn.execute.return_value = result
    engine.connect.return_value = conn
    engine.begin.return_value = conn
    return engine, conn


def _pit_frame(fids, newest_obs: date, n_days: int = 400) -> pd.DataFrame:
    """Long-format PIT frame ending at ``newest_obs``, one row per day per fid."""
    rows = []
    for i in range(n_days):
        obs = newest_obs - timedelta(days=n_days - 1 - i)
        for fid in fids:
            rows.append(
                {
                    "feature_id": fid,
                    "obs_date": obs,
                    "value": 20.0 + fid + (i % 17) * 0.1,
                    "release_date": obs,
                    "vintage_date": obs,
                }
            )
    return pd.DataFrame(rows)


# ── The alias layer ─────────────────────────────────────────────────────


class TestFeatureAliasing:
    def test_every_weighted_concept_has_a_decision_recorded(self):
        """A concept with no entry falls back to its own name — which is the
        binding that produced the hollow rows. Every weighted concept must
        carry an explicit decision, including "nothing feeds this" (an empty
        list), so the omission cannot happen by accident."""
        for concept in DEFAULT_FEATURE_WEIGHTS:
            assert concept in REGIME_FEATURE_SOURCES, concept

    def test_an_explicitly_unfed_concept_offers_no_candidates(self):
        """Empty list, not a fallback to its own frozen registry row."""
        assert REGIME_FEATURE_SOURCES["move_index"] == []
        assert _candidate_names("move_index") == []

    def test_an_unfed_concept_scores_nothing(self):
        engine, conn = _engine_returning([(174, "move_index", 507, DATA_CLIFF)])
        bindings = _resolve_regime_bindings(engine, {"move_index": 0.08})
        assert bindings == {}
        # And its name never even reaches the query.
        assert conn.execute.call_count == 0

    def test_the_vix_concept_never_binds_to_a_different_series(self):
        """vvix is eligible and fed, but VVIX is the volatility *of* VIX — it
        can spike while VIX is quiet, and it would carry the index's largest
        weight (+0.20) under a slider labelled "vix". `vix` id 105 is eligible
        with 505 observations and no seed, so listing it would bind straight
        back to one of the frozen rows this PR is about. Honest zero instead."""
        assert REGIME_FEATURE_SOURCES["vix"] == ["vix_spot"]

    def test_the_credit_concept_never_binds_to_the_etf_price(self):
        """hyg_full resolves further than hy_oas_spread, but it is the HY ETF
        price: it falls as spreads widen. Scoring it under a +0.15 stress
        weight would invert the credit signal."""
        assert "hyg_full" not in REGIME_FEATURE_SOURCES["hy_spread"]
        assert "hyg" not in REGIME_FEATURE_SOURCES["hy_spread"]

    def test_weights_stay_keyed_by_concept(self):
        """outputs/regime_weights.json and the API sliders send concept names;
        renaming the weight keys to feature names would break both."""
        assert "vix" in DEFAULT_FEATURE_WEIGHTS
        assert "vix_spot" not in DEFAULT_FEATURE_WEIGHTS

    def test_a_concept_without_an_entry_falls_back_to_its_own_name(self):
        assert _candidate_names("not_a_concept") == ["not_a_concept"]

    def test_binds_the_first_candidate_that_is_fed(self):
        concept = "dollar_index"
        first, second = REGIME_FEATURE_SOURCES[concept][:2]
        engine, _ = _engine_returning(
            [
                (11, first, 900, DATA_CLIFF),
                (12, second, 900, DATA_CLIFF),
            ]
        )
        bindings = _resolve_regime_bindings(engine, {concept: 0.2})
        assert bindings[concept]["feature"] == first
        assert bindings[concept]["feature_id"] == 11

    def test_falls_through_to_the_next_candidate_when_the_first_is_absent(self):
        concept = "dollar_index"
        second = REGIME_FEATURE_SOURCES[concept][1]
        engine, _ = _engine_returning([(12, second, 900, DATA_CLIFF)])
        bindings = _resolve_regime_bindings(engine, {concept: 0.2})
        assert bindings[concept]["feature"] == second

    def test_skips_a_candidate_with_too_little_history_for_the_zscore(self):
        """The index is a 252-day rolling z-score; a column shorter than that
        contributes mostly its own warm-up."""
        concept = "dollar_index"
        first, second = REGIME_FEATURE_SOURCES[concept][:2]
        engine, _ = _engine_returning(
            [
                (11, first, MIN_CANDIDATE_OBSERVATIONS - 1, DATA_CLIFF),
                (12, second, MIN_CANDIDATE_OBSERVATIONS, DATA_CLIFF),
            ]
        )
        bindings = _resolve_regime_bindings(engine, {concept: 0.05})
        assert bindings[concept]["feature"] == second

    def test_a_registered_but_never_fed_candidate_is_rejected(self):
        """LEFT JOIN gives n_obs = 0 for a registry row with no resolved rows —
        exactly the shape of the names the hollow backfill scored."""
        concept = "vix"
        first = REGIME_FEATURE_SOURCES[concept][0]
        engine, _ = _engine_returning([(11, first, 0, None)])
        assert _resolve_regime_bindings(engine, {concept: 0.2}) == {}

    def test_an_unfed_concept_drops_out_entirely(self):
        engine, _ = _engine_returning([])
        bindings = _resolve_regime_bindings(engine, {"vix": 0.2})
        assert bindings == {}
        assert _resolve_regime_features(engine, {"vix": 0.2}) == {}

    def test_an_unfed_concept_is_logged_not_hidden(self):
        engine, _ = _engine_returning([])
        with patch("scripts.auto_regime.log") as logger:
            _resolve_regime_bindings(engine, {"vix": 0.2})
        warned = " ".join(str(c) for c in logger.warning.call_args_list)
        assert "vix" in warned

    def test_the_feature_backing_each_concept_is_logged(self):
        concept = "vix"
        first = REGIME_FEATURE_SOURCES[concept][0]
        engine, _ = _engine_returning([(11, first, 900, DATA_CLIFF)])
        with patch("scripts.auto_regime.log") as logger:
            _resolve_regime_bindings(engine, {concept: 0.2})
        logged = " ".join(str(c) for c in logger.info.call_args_list)
        assert concept in logged and first in logged

    def test_the_feature_map_is_keyed_by_id_and_valued_by_concept(self):
        """_compute_stress_index looks the weight up by this value, so it has
        to be the concept, not the feature that happens to back it."""
        concept = "vix"
        first = REGIME_FEATURE_SOURCES[concept][0]
        engine, _ = _engine_returning([(11, first, 900, DATA_CLIFF)])
        assert _resolve_regime_features(engine, {concept: 0.2}) == {11: concept}

    def test_two_concepts_never_share_one_column(self):
        engine, _ = _engine_returning([(11, "shared", 900, DATA_CLIFF)])
        with patch.dict(
            REGIME_FEATURE_SOURCES,
            {"alpha": ["shared"], "beta": ["shared"]},
            clear=False,
        ):
            bindings = _resolve_regime_bindings(engine, {"alpha": 0.1, "beta": 0.1})
        assert list(bindings) == ["alpha"]

    def test_candidates_are_queried_with_bound_parameters(self):
        engine, conn = _engine_returning([])
        _resolve_regime_bindings(engine, {"vix": 0.2})
        sql = str(conn.execute.call_args[0][0])
        params = conn.execute.call_args[0][1]
        assert ":names" in sql
        assert "vix" not in sql
        assert REGIME_FEATURE_SOURCES["vix"][0] in params["names"]


# ── data_as_of ──────────────────────────────────────────────────────────


class TestFrameDataAsOf:
    def test_reports_the_newest_real_observation(self):
        frame = pd.DataFrame(
            {1: [1.0, 2.0]},
            index=pd.DatetimeIndex([date(2026, 4, 2), DATA_CLIFF], name="obs_date"),
        )
        assert _frame_data_as_of(frame, AS_OF) == DATA_CLIFF

    def test_never_exceeds_the_decision_date(self):
        """store/pit.py bounds release_date, not obs_date."""
        frame = pd.DataFrame(
            {1: [1.0, 2.0]},
            index=pd.DatetimeIndex(
                [DATA_CLIFF, AS_OF + timedelta(days=30)], name="obs_date"
            ),
        )
        assert _frame_data_as_of(frame, AS_OF) == DATA_CLIFF

    def test_empty_frame_reports_unknown_rather_than_today(self):
        assert _frame_data_as_of(pd.DataFrame(), AS_OF) is None


class TestComputeRegimeCarriesTheDataDate:
    def test_reports_the_cliff_not_the_decision_date(self):
        engine, _ = _engine_returning(
            [
                (1, REGIME_FEATURE_SOURCES["vix"][0], 900, DATA_CLIFF),
                (2, REGIME_FEATURE_SOURCES["hy_spread"][0], 900, DATA_CLIFF),
                (3, REGIME_FEATURE_SOURCES["sp500"][0], 900, DATA_CLIFF),
            ]
        )
        weights = {"vix": 0.2, "hy_spread": 0.15, "sp500": -0.1}

        with patch.object(
            PITStore,
            "get_pit",
            side_effect=lambda f, a, vintage_policy=None: _pit_frame(f, DATA_CLIFF),
        ):
            result = compute_regime_at(engine, AS_OF, weights=weights)

        assert result["data_as_of"] == DATA_CLIFF
        assert result["data_staleness_days"] == (AS_OF - DATA_CLIFF).days
        assert result["data_staleness_days"] > MAX_FRESH_DATA_AGE_DAYS

    def test_is_measured_before_the_forward_fill(self):
        """After ffill every column carries a value on the last row, so a
        measurement taken afterwards reports the same date for a feature that
        stopped in April as for one that updated this morning."""
        index = pd.DatetimeIndex(
            [DATA_CLIFF, DATA_CLIFF + timedelta(days=30), AS_OF], name="obs_date"
        )
        raw = pd.DataFrame({1: [1.0, None, None], 2: [2.0, 3.0, 4.0]}, index=index)

        before = _frame_observation_dates(raw, AS_OF)
        after = _frame_observation_dates(raw.ffill(), AS_OF)

        assert before[1] == DATA_CLIFF
        assert before[2] == AS_OF
        # The forward-filled frame cannot tell them apart — hence the ordering
        # constraint in compute_regime_at.
        assert after[1] == AS_OF and after[2] == AS_OF

    def test_names_the_stale_inputs_behind_a_fresh_looking_frame(self):
        """data_as_of is the newest observation anywhere in the frame, so one
        feature a direct writer keeps current makes the whole frame look
        current. The per-concept view is what shows the rest."""
        index = pd.DatetimeIndex(
            [DATA_CLIFF, DATA_CLIFF + timedelta(days=30), AS_OF], name="obs_date"
        )
        raw = pd.DataFrame({1: [1.0, None, None], 2: [2.0, 3.0, 4.0]}, index=index)
        fid_to_name = {1: "vix", 2: "spy_rsi"}

        assert _frame_data_as_of(raw, AS_OF) == AS_OF
        stale = _stale_frame_features(raw, fid_to_name, AS_OF)
        assert stale == {"vix": (AS_OF - DATA_CLIFF).days}

    def test_a_current_frame_reports_zero_staleness(self):
        engine, _ = _engine_returning(
            [
                (1, REGIME_FEATURE_SOURCES["vix"][0], 900, AS_OF),
                (2, REGIME_FEATURE_SOURCES["hy_spread"][0], 900, AS_OF),
                (3, REGIME_FEATURE_SOURCES["sp500"][0], 900, AS_OF),
            ]
        )
        weights = {"vix": 0.2, "hy_spread": 0.15, "sp500": -0.1}
        with patch.object(
            PITStore,
            "get_pit",
            side_effect=lambda f, a, vintage_policy=None: _pit_frame(f, AS_OF),
        ):
            result = compute_regime_at(engine, AS_OF, weights=weights)
        assert result["data_as_of"] == AS_OF
        assert result["data_staleness_days"] == 0


class TestPersistCarriesTheDataDate:
    def test_writes_the_column_with_a_bound_parameter(self):
        engine, conn = _engine_returning([])
        persist_regime_history(
            engine, AS_OF, "NEUTRAL", 0.5, data_as_of=DATA_CLIFF
        )
        sql = str(conn.execute.call_args[0][0])
        params = conn.execute.call_args[0][1]
        assert "data_as_of" in sql
        assert ":data_as_of" in sql
        assert str(DATA_CLIFF) not in sql
        assert params["data_as_of"] == DATA_CLIFF

    def test_updates_the_column_on_conflict(self):
        """A re-backfill over rows written before the column existed has to
        fill it in, not leave the old NULL behind."""
        engine, conn = _engine_returning([])
        persist_regime_history(engine, AS_OF, "NEUTRAL", 0.5, data_as_of=DATA_CLIFF)
        sql = str(conn.execute.call_args[0][0])
        assert "data_as_of = EXCLUDED.data_as_of" in sql

    def test_unknown_data_date_is_written_as_null_not_guessed(self):
        engine, conn = _engine_returning([])
        persist_regime_history(engine, AS_OF, "NEUTRAL", 0.5)
        assert conn.execute.call_args[0][1]["data_as_of"] is None

    def test_rejects_a_data_date_after_the_row_it_describes(self):
        engine, _ = _engine_returning([])
        with pytest.raises(ValueError, match="after the row's obs_date"):
            persist_regime_history(
                engine, AS_OF, "NEUTRAL", 0.5, data_as_of=AS_OF + timedelta(days=1)
            )


class TestBackfillCarriesTheDataDate:
    def test_each_backfilled_row_gets_its_own_data_date(self):
        engine, _ = _engine_returning(
            [(1, "vix_spot", 900, DATA_CLIFF),
             (2, "hy_oas_spread", 900, DATA_CLIFF),
             (3, "sp500_full", 900, DATA_CLIFF)]
        )

        def fake_compute(eng, as_of, weights=None, fid_to_name=None):
            return {
                "regime": "NEUTRAL",
                "confidence": 0.5,
                "data_as_of": DATA_CLIFF,
                "data_staleness_days": (as_of - DATA_CLIFF).days,
            }

        with patch("scripts.auto_regime.compute_regime_at", side_effect=fake_compute), \
             patch("scripts.auto_regime.persist_regime_history", return_value=True) as writer:
            backfill_regime_history(
                engine, date(2026, 9, 1), date(2026, 9, 3), overwrite=True
            )

        assert writer.call_count == 3
        for call in writer.call_args_list:
            assert call.kwargs["data_as_of"] == DATA_CLIFF


# ── Surfaces ────────────────────────────────────────────────────────────


class TestRegimeCurrentEndpoint:
    def _engine(self, reading_ts, data_as_of):
        engine = MagicMock()
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = False

        prod = MagicMock()
        prod.fetchone.return_value = (7, "stress_index", "1.0")
        latest = MagicMock()
        latest.fetchone.return_value = (
            "NEUTRAL", 0.43, 0.1, {}, "BALANCED", "NEUTRAL", reading_ts,
        )
        history = MagicMock()
        history.fetchone.return_value = (data_as_of,) if data_as_of else (None,)
        conn.execute.side_effect = [prod, latest, history]
        engine.connect.return_value = conn
        return engine

    def test_reports_the_data_date_beside_the_reading_date(self):
        from api.routers import regime as regime_router

        today = date.today()
        reading_ts = datetime(today.year, today.month, today.day, 6, 0)
        engine = self._engine(reading_ts, DATA_CLIFF)

        with patch.object(regime_router, "get_db_engine", return_value=engine):
            resp = regime_router.get_current(_token="t")

        # The row is from today; its data is five months old. Both are said.
        assert resp.as_of_date == today.isoformat()
        assert resp.staleness_days == 0
        assert resp.data_as_of == DATA_CLIFF.isoformat()
        assert resp.data_staleness_days == (today - DATA_CLIFF).days

    def test_a_row_without_a_data_date_reports_unknown(self):
        from api.routers import regime as regime_router

        today = date.today()
        engine = self._engine(datetime(today.year, today.month, today.day, 6, 0), None)

        with patch.object(regime_router, "get_db_engine", return_value=engine):
            resp = regime_router.get_current(_token="t")

        assert resp.data_as_of == ""
        assert resp.data_staleness_days is None

    def test_defaults_stay_backward_compatible(self):
        from api.schemas.regime import RegimeCurrentResponse

        resp = RegimeCurrentResponse(state="NEUTRAL")
        assert resp.data_as_of == ""
        assert resp.data_staleness_days is None


class TestChatRegimeContext:
    def _framing(self, obs_date, data_as_of):
        from api.routers import chat

        engine = MagicMock()
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = False
        result = MagicMock()
        result.fetchone.return_value = (
            obs_date, "NEUTRAL", 0.43, datetime(2026, 9, 10, 20, 10), data_as_of,
        )
        conn.execute.return_value = result
        engine.connect.return_value = conn

        with patch.object(chat, "_get_db_engine", return_value=engine):
            framing, source = chat._gather_regime_context()
        return framing, source, conn

    def test_selects_the_data_date(self):
        _, _, conn = self._framing(date.today(), DATA_CLIFF)
        assert "data_as_of" in str(conn.execute.call_args[0][0])

    def test_says_plainly_that_a_fresh_looking_row_has_stale_inputs(self):
        framing, source, _ = self._framing(date.today(), DATA_CLIFF)
        assert source == "regime_history"
        assert DATA_CLIFF.isoformat() in framing
        assert "NOT today" in framing

    def test_a_current_row_carries_no_stale_warning(self):
        framing, _, _ = self._framing(date.today(), date.today())
        assert "NOT today" not in framing
        assert date.today().isoformat() in framing

    def test_an_unknown_data_date_is_not_presented_as_today(self):
        framing, _, _ = self._framing(date.today(), None)
        assert "unknown" in framing
