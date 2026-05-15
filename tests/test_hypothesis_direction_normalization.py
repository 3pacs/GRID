"""Regression tests for the direction-vocabulary normalization in
``HypothesisGenerator._normalize_direction``.

Before this normalization, the convergence-pattern producer wrote direction
strings as ``CALL``/``PUT``/``opposite``/``neutral`` (because the upstream
options-flow + convergence detector emits those literally) while the
consumer in ``_check_ticker_move`` only matched ``("bullish","up")`` and
``("bearish","down")``. The result: every convergence hypothesis fell
through the if-ladder and returned ``"inconclusive"``, even when the ticker
moved double-digit percent.

We caught this on 2026-05-15 when ``hypothesis_boost_log`` had 4,852 rows
all marked ``'inconclusive'`` while the underlying tickers (INTC, LLY, JNJ,
etc.) had moved 13-78% over their evaluation windows. See
``docs/scoring/boost_log_scoring_v1.md``.
"""

from __future__ import annotations

import pytest

from intelligence.hypothesis_engine import HypothesisGenerator


class TestNormalizeDirectionVocabulary:
    """Every dialect produced anywhere in the pipeline must map to one of
    {"up", "down", "neutral", "ambiguous"}.
    """

    @pytest.mark.parametrize("raw", [
        "up", "bullish", "long",
        "CALL", "call",
        "increase", "increases",
        "rising",
        "UP", "Up", "  bullish  ",  # case + whitespace tolerance
    ])
    def test_up_vocabulary(self, raw: str) -> None:
        assert HypothesisGenerator._normalize_direction(raw) == "up"

    @pytest.mark.parametrize("raw", [
        "down", "bearish", "short",
        "PUT", "put",
        "decrease", "decreases",
        "falling",
        "DOWN", "  bearish ",
    ])
    def test_down_vocabulary(self, raw: str) -> None:
        assert HypothesisGenerator._normalize_direction(raw) == "down"

    @pytest.mark.parametrize("raw", ["neutral", "flat", "sideways", "NEUTRAL"])
    def test_neutral_vocabulary(self, raw: str) -> None:
        assert HypothesisGenerator._normalize_direction(raw) == "neutral"

    @pytest.mark.parametrize("raw", [
        "opposite",   # _anti hypotheses use this — needs parent lookup, not handled here
        "unknown",
        "",
        None,         # type: ignore[arg-type]
        "moonshot",
        "gibberish",
    ])
    def test_ambiguous_vocabulary(self, raw: str) -> None:
        assert HypothesisGenerator._normalize_direction(raw) == "ambiguous"


class TestCheckTickerMoveDirectionConsumption:
    """The previously-broken if-ladder in _check_ticker_move now goes through
    _normalize_direction. The bucket {neutral, ambiguous} short-circuits to
    'inconclusive' before even querying oracle_predictions.

    We don't need a live DB here — the normalize+short-circuit happens at the
    top of the function, before the SQL call. We exercise that path by
    constructing a HypothesisGenerator with a None engine and confirming the
    function returns 'inconclusive' without touching the engine for the
    ambiguous/neutral cases.
    """

    def _make_generator(self):
        # _check_ticker_move only touches self.engine when direction is up/down,
        # so for the short-circuit cases we can pass a sentinel that would
        # raise AttributeError if accidentally touched.
        gen = HypothesisGenerator.__new__(HypothesisGenerator)
        class _ExplodingEngine:
            def connect(self):
                raise AssertionError(
                    "engine.connect() called for a short-circuit direction — "
                    "_normalize_direction is no longer gating early."
                )
        gen.engine = _ExplodingEngine()
        return gen

    def test_neutral_short_circuits_inconclusive(self) -> None:
        from datetime import datetime, timezone
        gen = self._make_generator()
        result = gen._check_ticker_move(
            ticker="INTC",
            direction="neutral",
            since=datetime(2026, 5, 1, tzinfo=timezone.utc),
            window_days=14,
            min_move_pct=2.0,
        )
        assert result == "inconclusive"

    def test_ambiguous_opposite_short_circuits_inconclusive(self) -> None:
        # "opposite" is the direction string written by the _anti suffix path.
        # It needs parent-lookup-and-invert to score correctly (see
        # docs/scoring/boost_log_scoring_v1.md Rule 2). Until that's wired in
        # at the mechanical layer, return inconclusive — the Opus-scored
        # opus_outcome column carries the real verdict for those rows.
        from datetime import datetime, timezone
        gen = self._make_generator()
        result = gen._check_ticker_move(
            ticker="INTC",
            direction="opposite",
            since=datetime(2026, 5, 1, tzinfo=timezone.utc),
            window_days=14,
            min_move_pct=2.0,
        )
        assert result == "inconclusive"
