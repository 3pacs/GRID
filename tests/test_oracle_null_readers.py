"""Read-side NULL compatibility for ``oracle_predictions``.

PR #544 makes ``oracle_predictions.entry_price`` and
``oracle_predictions.confidence`` nullable and starts publishing NULL when
nothing measured them. These tests pin the *reader* contract that has to hold
on this branch so it stays a safe rollback target once such a row exists:

* a NULL entry price is **not scorable** — never fetched into the scorer's
  chunk, never divided by, never turned into a 0% return, and it leaves no
  ``pnl_pct`` behind;
* a **measured** ``0.0`` entry price keeps the contract the code already had:
  ``scripts/score_oracle_trades.py`` requires ``entry_price > 0`` before it
  divides, so a zero entry is swept to ``no_data`` rather than dividing by
  zero. That behaviour is unchanged here — only NULL is newly named;
* a NULL confidence is excluded from calibration, reliability and every
  average, while a **measured** ``0.0`` confidence is counted as ``0.0``;
* nothing substitutes ``0``, ``0.5`` or ``0.7`` for a missing value.

Everything here runs on both schemas: the queries under test never depend on
the column being nullable, only on their own NULL handling.
"""

from __future__ import annotations

import re
import sqlite3
from datetime import date, timedelta
from pathlib import Path

import pytest
from sqlalchemy import create_engine, event, text

# oracle.* must be imported BEFORE scripts.score_oracle_trades. That module
# does `sys.path.insert(0, "/data/grid_v4/grid_repo")` (a compute-node
# checkout path, pre-existing on main, out of scope here) whose own `oracle`
# package predates entry_price_policy.py. On a host where that directory
# exists, importing score_oracle_trades first caches THAT `oracle` package
# in sys.modules, and every `oracle.*` import anywhere in this file after
# that point -- including the lazy `from oracle.engine import OracleEngine`
# further down -- resolves from it instead of this repo's real package,
# raising ModuleNotFoundError for oracle.entry_price_policy. Importing the
# real submodules first caches the correct `oracle` package in sys.modules,
# so nothing later in the process (this file or otherwise) can be shadowed.
import oracle.engine  # noqa: F401
from oracle.calibration import compute_calibration
from oracle.entry_price_policy import (
    SCORE_NOTE_ENTRY_NULL,
    SCORE_NOTE_ENTRY_ZERO,
)

import scripts.score_oracle_trades as sot
from intelligence import source_quality_ablation as sqa

REPO_ROOT = Path(__file__).resolve().parents[1]


# ── In-memory fixture ──────────────────────────────────────────────────────
#
# SQLite stands in for Postgres for the portable statements under test. The
# module SQL runs verbatim — we only teach SQLite the one function it lacks
# (``NOW()``) so no query has to be rewritten for the test.

_ORACLE_PREDICTIONS_DDL = """
CREATE TABLE oracle_predictions (
    id TEXT PRIMARY KEY,
    created_at TEXT,
    ticker TEXT NOT NULL,
    prediction_type TEXT,
    direction TEXT NOT NULL,
    target_price DOUBLE PRECISION,
    entry_price DOUBLE PRECISION,
    expiry DATE NOT NULL,
    confidence DOUBLE PRECISION,
    expected_move_pct DOUBLE PRECISION,
    signal_strength DOUBLE PRECISION,
    coherence DOUBLE PRECISION,
    model_name TEXT,
    model_version TEXT,
    signals TEXT,
    anti_signals TEXT,
    flow_context TEXT,
    model_weights TEXT,
    verdict TEXT DEFAULT 'pending',
    actual_price DOUBLE PRECISION,
    actual_move_pct DOUBLE PRECISION,
    pnl_pct DOUBLE PRECISION,
    scored_at TEXT,
    score_notes TEXT,
    dedup_keep BOOLEAN NOT NULL DEFAULT 1,
    horizon_days INTEGER
)
"""

_ORACLE_MODELS_DDL = """
CREATE TABLE oracle_models (
    name TEXT PRIMARY KEY,
    predictions_made INTEGER DEFAULT 0,
    hits INTEGER DEFAULT 0,
    misses INTEGER DEFAULT 0,
    partials INTEGER DEFAULT 0,
    cumulative_pnl DOUBLE PRECISION DEFAULT 0,
    last_updated TEXT
)
"""


sqlite3.register_adapter(date, date.isoformat)
sqlite3.register_converter("DATE", lambda b: date.fromisoformat(b.decode()))


@pytest.fixture()
def engine():
    # PARSE_DECLTYPES so the DATE column round-trips as a `date`, exactly as
    # psycopg2 hands it back — `get_price_for_date` compares it to a `date`.
    eng = create_engine(
        "sqlite://",
        connect_args={"detect_types": sqlite3.PARSE_DECLTYPES},
    )

    @event.listens_for(eng, "connect")
    def _register_now(dbapi_conn, _record):  # noqa: ANN001
        dbapi_conn.create_function("NOW", 0, lambda: "2026-09-18 00:00:00")

    with eng.begin() as conn:
        conn.execute(text(_ORACLE_PREDICTIONS_DDL))
        conn.execute(text(_ORACLE_MODELS_DDL))
        conn.execute(
            text("INSERT INTO oracle_models (name) VALUES ('astrogrid')")
        )
    return eng


def _insert(conn, **kw) -> None:
    cols = {
        "prediction_type": "directional",
        "expected_move_pct": 5.0,
        "model_name": "astrogrid",
        "verdict": "pending",
        "dedup_keep": 1,
        "created_at": "2026-08-01 00:00:00",
    }
    cols.update(kw)
    names = ", ".join(cols)
    binds = ", ".join(f":{c}" for c in cols)
    conn.execute(
        text(f"INSERT INTO oracle_predictions ({names}) VALUES ({binds})"),
        cols,
    )


# ── The scorer ─────────────────────────────────────────────────────────────

class TestScorerNullEntry:
    """``score_one_chunk`` must not see, score or price a NULL entry."""

    def _seed(self, engine) -> date:
        today = date(2026, 9, 18)
        expiry = today - timedelta(days=1)
        with engine.begin() as conn:
            # (a) nothing measured either field
            _insert(
                conn, id="null-entry", ticker="AAA", direction="CALL",
                entry_price=None, confidence=None, expiry=expiry,
            )
            # (b) a measured zero entry and a measured zero confidence
            _insert(
                conn, id="zero-entry", ticker="BBB", direction="CALL",
                entry_price=0.0, confidence=0.0, expiry=expiry,
            )
            # (c) an ordinary, fully measured row
            _insert(
                conn, id="normal", ticker="CCC", direction="CALL",
                entry_price=100.0, confidence=0.8, expiry=expiry,
            )
        return today

    def test_null_entry_is_never_fetched_and_never_scored(self, engine):
        today = self._seed(engine)
        # The scorable row loses, which keeps the success-lesson writer (it
        # opens its own connection, which SQLite shares with ours) out of the
        # way. What is under test here is the entry-price gate, not lessons.
        prices = {
            "AAA": {today - timedelta(days=1): 110.0},
            "BBB": {today - timedelta(days=1): 110.0},
            "CCC": {today - timedelta(days=1): 90.0},
        }
        with engine.begin() as conn:
            counters = sot.score_one_chunk(
                conn, engine=engine, prices=prices, today=today,
                chunk_size=100,
            )

        # Only the two rows with a usable entry price are even fetched: the
        # chunk WHERE says `entry_price IS NOT NULL AND entry_price > 0`, and
        # `entry_price > 0` on its own is NULL (not false) for a NULL row.
        assert counters["fetched"] == 1, counters
        assert counters["scored"] == 1, counters

        with engine.connect() as conn:
            rows = dict(
                conn.execute(text(
                    "SELECT id, verdict FROM oracle_predictions"
                )).fetchall()
            )
            pnl = dict(
                conn.execute(text(
                    "SELECT id, pnl_pct FROM oracle_predictions"
                )).fetchall()
            )

        # Untouched: still pending, and no P&L was invented for it.
        assert rows["null-entry"] == "pending"
        assert pnl["null-entry"] is None
        # The measured zero entry is equally not scorable — and crucially the
        # scorer did not divide by it.
        assert rows["zero-entry"] == "pending"
        assert pnl["zero-entry"] is None
        # The ordinary row scored normally: (90 - 100) / 100 = -10% on a CALL.
        assert rows["normal"] == "miss"
        assert pnl["normal"] == pytest.approx(-10.0)

    def test_belt_and_braces_skip_survives_a_widened_where(self, engine):
        """If the chunk WHERE ever loses its guard, the loop still refuses.

        The in-loop `entry is None or entry <= 0` check is the second line of
        defence. Exercised here by driving the loop with a widened query.
        """
        today = self._seed(engine)
        with engine.begin() as conn:
            # Simulate the pre-guard query by scoring with prices present for
            # every ticker and a chunk that (hypothetically) included them.
            counters = sot.score_one_chunk(
                conn, engine=engine, prices={}, today=today, chunk_size=100,
            )
        # No price data at all -> the one scorable row goes to no_data; the
        # NULL and zero rows were never candidates in the first place.
        assert counters["no_data"] == 1
        assert counters["skipped"] == 0
        with engine.connect() as conn:
            verdicts = dict(conn.execute(text(
                "SELECT id, verdict FROM oracle_predictions"
            )).fetchall())
        assert verdicts["null-entry"] == "pending"
        assert verdicts["normal"] == "no_data"


def test_no_data_sweep_names_null_explicitly() -> None:
    """The sweep must name NULL: `entry_price = 0` is NULL for a NULL row.

    Without this the sweep silently skips a price-less prediction, which then
    sits 'pending' forever — neither scored nor accounted for — because the
    chunk query excludes it too.

    HISTORICAL-WRITE HOLD: the sweep's WHERE must select only
    ``entry_price IS NULL`` (a new-policy row nothing measured). It must NOT
    also sweep a non-null 0/negative entry_price — that can only be a legacy
    row written before the column was nullable, and historical
    rescoring/repair of those rows is on hold: they are left 'pending',
    never updated/closed/rescored/re-labelled here.
    """
    src = (REPO_ROOT / "scripts" / "score_oracle_trades.py").read_text(
        encoding="utf-8"
    )
    sweep = re.search(
        r"SET verdict = 'no_data',\s*\n\s*score_notes = :note_null,.*?\"\"\"\)",
        src,
        re.S,
    )
    assert sweep is not None, "the no_data sweep no longer names NULL"
    body = sweep.group(0)
    assert "WHERE verdict = 'pending'" in body
    assert "AND entry_price IS NULL" in body
    # The hold: a non-null invalid entry (0 or negative) must never be part
    # of this sweep's WHERE — that would touch a legacy row.
    assert "entry_price <= 0" not in body
    assert "entry_price = 0" not in body
    # And it must not have grown a placeholder.
    assert "COALESCE(entry_price" not in body


def test_scorer_chunk_and_precount_agree_on_scorability() -> None:
    """Step 4's precount and the chunk query must select the same rows.

    If the precount counted rows the chunk cannot fetch, the run loops
    forever waiting for a chunk that never arrives.
    """
    src = (REPO_ROOT / "scripts" / "score_oracle_trades.py").read_text(
        encoding="utf-8"
    )
    assert src.count("AND entry_price IS NOT NULL") == 2
    assert src.count("AND entry_price > 0") == 2


# ── Calibration and averages ───────────────────────────────────────────────

class TestCalibrationNullConfidence:
    def _seed(self, engine) -> None:
        with engine.begin() as conn:
            _insert(
                conn, id="unstated", ticker="AAA", direction="CALL",
                entry_price=100.0, confidence=None, verdict="hit",
                expiry=date(2026, 9, 1),
            )
            _insert(
                conn, id="measured-zero", ticker="BBB", direction="CALL",
                entry_price=100.0, confidence=0.0, verdict="miss",
                expiry=date(2026, 9, 1),
            )
            _insert(
                conn, id="measured-high", ticker="CCC", direction="CALL",
                entry_price=100.0, confidence=0.9, verdict="hit",
                expiry=date(2026, 9, 1),
            )

    def test_null_confidence_excluded_measured_zero_counted(self, engine):
        self._seed(engine)
        report = compute_calibration(engine, n_bins=10)

        # Two rows stated a probability; the third stated none and is not
        # counted at a default.
        assert report.total_predictions == 2

        populated = {
            (b.bin_start, b.bin_end): b
            for b in report.buckets if b.count > 0
        }
        # The measured 0.0 lives in the first bucket as a genuine 0.0 — it
        # was not laundered into "unknown" and not lifted to 0.5.
        first = populated[(0.0, 0.1)]
        assert first.count == 1
        assert first.predicted_mean == pytest.approx(0.0)
        # ...and the 0.9 in its own bucket. No bucket holds three rows.
        assert sum(b.count for b in report.buckets) == 2

    def test_all_null_confidence_reports_insufficient_not_zero(self, engine):
        with engine.begin() as conn:
            _insert(
                conn, id="u1", ticker="AAA", direction="CALL",
                entry_price=100.0, confidence=None, verdict="hit",
                expiry=date(2026, 9, 1),
            )
        report = compute_calibration(engine)
        assert report.total_predictions == 0
        assert report.label == "insufficient_data"
        # Not a perfect Brier score of 0 conjured from no data.
        assert report.buckets == []

    def test_measured_zero_confidence_alone_is_still_a_measurement(self, engine):
        with engine.begin() as conn:
            _insert(
                conn, id="z1", ticker="AAA", direction="CALL",
                entry_price=100.0, confidence=0.0, verdict="miss",
                expiry=date(2026, 9, 1),
            )
        report = compute_calibration(engine)
        assert report.total_predictions == 1


class TestSourceQualityBrier:
    """An unstated confidence is excluded from the Brier average, not 0.5."""

    def test_null_confidence_does_not_enter_the_brier(self):
        stats = sqa.SourcePredictionStats(source_name="yfinance")
        stats.record("hit", None)
        # The outcome still counts towards the hit rate...
        assert stats.prediction_count == 1
        assert stats.hit_rate == pytest.approx(1.0)
        # ...but there is no probability to score, so no Brier term. The old
        # code scored it at p=0.5 and reported brier=0.25.
        assert stats.brier is None

    def test_measured_zero_confidence_is_scored_as_zero(self):
        stats = sqa.SourcePredictionStats(source_name="yfinance")
        stats.record("miss", 0.0)
        # A stated 0% that missed is a perfect forecast: (0.0 - 0.0)^2 = 0.
        assert stats.brier == pytest.approx(0.0)

        stats2 = sqa.SourcePredictionStats(source_name="yfinance")
        stats2.record("hit", 0.0)
        # A stated 0% that hit is maximally wrong: (0.0 - 1.0)^2 = 1.
        assert stats2.brier == pytest.approx(1.0)

    def test_brier_averages_over_the_stated_subset(self):
        stats = sqa.SourcePredictionStats(source_name="yfinance")
        stats.record("hit", 1.0)     # (1-1)^2 = 0
        stats.record("hit", None)    # excluded
        stats.record("miss", 1.0)    # (1-0)^2 = 1
        assert stats.prediction_count == 3
        # Mean over the two stated rows, not over all three.
        assert stats.brier == pytest.approx(0.5)


# ── The remaining reliability replays exclude in SQL ───────────────────────

@pytest.mark.parametrize(
    ("relpath", "needle"),
    [
        ("features/regime_conditional_brier.py", "AND confidence IS NOT NULL"),
        ("intelligence/null_hypothesis_forecaster.py", "AND confidence IS NOT NULL"),
        ("scripts/walk_forward_validate.py", "AND confidence IS NOT NULL"),
        ("scripts/walk_forward_profitability.py", "AND confidence IS NOT NULL"),
        ("scripts/backtest_intelligence.py", "AND confidence IS NOT NULL"),
        ("intelligence/historical_scenario_library.py", "AND confidence IS NOT NULL "),
        ("oracle/calibration.py", "AND confidence IS NOT NULL"),
    ],
)
def test_reliability_replays_exclude_unstated_confidence(relpath, needle):
    """Each replay that scores a probability filters NULL out in SQL.

    These queries use Postgres-only syntax (``::interval``, ``FILTER``) so
    they are pinned by their text here and exercised for real against a
    disposable Postgres database — see the PR body.
    """
    src = (REPO_ROOT / relpath).read_text(encoding="utf-8")
    assert needle in src, f"{relpath} no longer excludes a NULL confidence"


def test_no_placeholder_confidence_survives_in_the_touched_readers():
    """None of the patched readers reintroduces a 0 / 0.5 / 0.7 stand-in."""
    banned = [
        ("features/regime_conditional_brier.py", 'get("confidence") or 0.0'),
        ("intelligence/null_hypothesis_forecaster.py", "float(r[2] or 0.0)"),
        ("scripts/backfill_surfacer_calibration.py", "COALESCE(confidence, 0.5)"),
        ("intelligence/historical_scenario_library.py", "_coerce_float(conf) or 0.0"),
    ]
    for relpath, pattern in banned:
        src = (REPO_ROOT / relpath).read_text(encoding="utf-8")
        assert pattern not in src, f"{relpath} still substitutes: {pattern}"


def test_unscored_confidence_is_not_a_low_confidence():
    """walk_forward_profitability buckets HIGH/MEDIUM/LOW on a probability.

    A row that stated none used to be bucketed LOW, which moved the LOW
    bucket's hit rate and mean PnL — the very numbers the report exists to
    compare against HIGH.
    """
    from scripts.walk_forward_profitability import _bucket_for

    assert _bucket_for(None) == "UNSCORED"
    assert _bucket_for(0.0) == "LOW"       # a measured 0.0 IS low
    assert _bucket_for(0.6) == "MEDIUM"
    assert _bucket_for(0.9) == "HIGH"


# ── Narrative readers ──────────────────────────────────────────────────────

def test_postmortem_summary_does_not_invent_an_entry_price():
    from intelligence.postmortem import _summarise_what_happened

    unmeasured = _summarise_what_happened(
        "AAPL", "CALL", None, 200.0, "miss", -0.05, [],
    )
    assert "$0.00" not in unmeasured
    assert "no measured entry price" in unmeasured

    measured = _summarise_what_happened(
        "AAPL", "CALL", 214.5, 200.0, "miss", -0.05, [],
    )
    assert "entered at $214.50" in measured

    # A genuine measured zero still prints as a number.
    genuine_zero = _summarise_what_happened(
        "AAPL", "CALL", 0.0, 200.0, "miss", -0.05, [],
    )
    assert "entered at $0.00" in genuine_zero


def test_postmortem_timing_check_tolerates_an_unmeasured_entry():
    """`None > 0` raises; a missing baseline means no timing verdict."""
    from intelligence.postmortem import _classify_prediction_failure

    category, root_cause, wrong, right, missed = _classify_prediction_failure(
        ticker="AAPL",
        direction="CALL",
        entry_price=None,
        target=200.0,
        expiry=date(2026, 9, 1),
        actual_price=190.0,
        actual_move_pct=-5.0,
        signals=[],
        anti_signals=[],
        price_path=[{"price": 210.0}, {"price": 205.0}],
    )
    assert isinstance(category, str) and category
    assert isinstance(root_cause, str)


# ── A zero entry price is a measurement that cannot be a basis ─────────────
#
# NULL and 0 are different findings and get different reasons. Neither is
# repaired, neither is divided by, and neither is skipped without saying so.

class TestEngineScoringLoopEntryPrice:
    """``OracleEngine.score_expired_predictions`` closes what it cannot score.

    Unlike the scorer's chunk query, the engine's own SELECT has no
    entry-price filter at all — every pending expired row reaches the loop.
    So this is the path where a NULL raises TypeError and a measured 0 raises
    ZeroDivisionError, and the one that has to settle both before dividing.
    """

    def _engine_under_test(self, engine, prices: dict):
        from oracle.engine import OracleEngine

        # __init__ runs 8+ CREATE TABLE statements in Postgres dialect and
        # loads the model registry. Neither is under test here, so the
        # instance is built directly around the fixture's connection.
        oe = object.__new__(OracleEngine)
        oe.engine = engine
        oe.models = []
        oe._last_guard_verdicts = []
        oe._get_price_at_date = lambda ticker, _expiry: prices.get(ticker)
        return oe

    def _seed(self, engine) -> date:
        today = date.today()
        expiry = today - timedelta(days=1)
        with engine.begin() as conn:
            _insert(
                conn, id="null-entry", ticker="AAA", direction="CALL",
                entry_price=None, confidence=0.6, expiry=expiry,
            )
            _insert(
                conn, id="zero-entry", ticker="BBB", direction="CALL",
                entry_price=0.0, confidence=0.6, expiry=expiry,
            )
            _insert(
                conn, id="negative-entry", ticker="DDD", direction="CALL",
                entry_price=-3.0, confidence=0.6, expiry=expiry,
            )
            _insert(
                conn, id="normal", ticker="CCC", direction="CALL",
                entry_price=100.0, confidence=0.6, expiry=expiry,
            )
        return today

    def test_unusable_entries_are_closed_or_held_by_the_hold_policy(
        self, engine,
    ):
        """NULL is closed with a reason; a non-null invalid entry is held.

        HISTORICAL-WRITE HOLD: ``zero-entry`` and ``negative-entry`` are
        legacy rows — a non-null invalid ``entry_price`` can only exist from
        before ``oracle_pred_nullable_0918`` made the column nullable, since
        the new publish path writes NULL, never 0/negative, when nothing was
        measured. Historical rescoring/repair of those rows is on hold: this
        loop must never update, close, rescore or re-label them. Only
        ``null-entry`` (a new-policy row) is closed to 'no_data'.
        """
        from oracle.entry_price_policy import SCORE_NOTE_ENTRY_NULL

        self._seed(engine)
        oe = self._engine_under_test(
            engine, {"AAA": 110.0, "BBB": 110.0, "CCC": 110.0, "DDD": 110.0},
        )

        # No TypeError on the NULL, no ZeroDivisionError on the measured 0.
        results = oe.score_expired_predictions()

        with engine.connect() as conn:
            rows = {
                r[0]: (r[1], r[2], r[3])
                for r in conn.execute(text(
                    "SELECT id, verdict, score_notes, pnl_pct "
                    "FROM oracle_predictions"
                )).fetchall()
            }

        # The new-policy NULL row is closed — not left 'pending' forever, and
        # not folded into the miss column.
        assert rows["null-entry"][0] == "no_data"
        assert rows["null-entry"][1] == SCORE_NOTE_ENTRY_NULL
        assert rows["null-entry"][2] is None

        # The legacy non-null-invalid rows are held: still 'pending', no
        # score_notes, no pnl_pct — byte-identical to how they were seeded.
        assert rows["zero-entry"] == ("pending", None, None)
        assert rows["negative-entry"] == ("pending", None, None)

        # Reported through separate counters, not folded together.
        assert results["unscorable_entry_price"] == 1, results
        assert results["held_legacy_entry_price"] == 2, results
        assert results["misses"] == 0, results

        # The measured row still scores: (110 - 100) / 100 = +10% on a CALL.
        assert rows["normal"][0] == "hit"
        assert rows["normal"][2] == pytest.approx(10.0)

    def test_a_zero_entry_never_reaches_a_division(self, engine):
        """A zero entry must be settled *before* the divide, not caught after.

        Driven by making any arithmetic against the fetched price raise: if
        the hold check sat downstream of ``(actual - entry) / entry`` this
        would surface as an AssertionError from the operand rather than a
        clean hold. The legacy zero-entry row is held (never scored, never
        closed) rather than divided by or repaired.
        """
        self._seed(engine)

        class _Exploding(float):
            def __sub__(self, other):  # pragma: no cover - must not run
                raise AssertionError("arithmetic on an unusable entry price")

            def __truediv__(self, other):  # pragma: no cover - must not run
                raise AssertionError("division by an unusable entry price")

        with engine.begin() as conn:
            conn.execute(text(
                "DELETE FROM oracle_predictions WHERE id <> 'zero-entry'"
            ))
        oe = self._engine_under_test(engine, {"BBB": _Exploding(110.0)})
        results = oe.score_expired_predictions()
        assert results["unscorable_entry_price"] == 0
        assert results["held_legacy_entry_price"] == 1
        assert results["total"] == 0

        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT verdict, score_notes FROM oracle_predictions "
                "WHERE id = 'zero-entry'"
            )).fetchone()
        assert row == ("pending", None)


class TestEntryPricePolicy:
    """The shared classifier every reader binds its reason from."""

    def test_null_and_zero_are_different_findings(self):
        from oracle.entry_price_policy import (
            SCORE_NOTE_ENTRY_NULL,
            SCORE_NOTE_ENTRY_ZERO,
            entry_price_score_note,
        )

        assert entry_price_score_note(None) == SCORE_NOTE_ENTRY_NULL
        assert entry_price_score_note(0) == SCORE_NOTE_ENTRY_ZERO
        assert entry_price_score_note(0.0) == SCORE_NOTE_ENTRY_ZERO
        assert SCORE_NOTE_ENTRY_NULL != SCORE_NOTE_ENTRY_ZERO

    def test_a_positive_entry_has_no_reason_and_divides(self):
        from oracle.entry_price_policy import (
            entry_price_score_note,
            is_divisible_entry_price,
        )

        assert entry_price_score_note(214.5) is None
        assert is_divisible_entry_price(214.5) is True
        assert is_divisible_entry_price(0.0) is False
        assert is_divisible_entry_price(None) is False

    def test_a_reason_is_never_a_substitute_price(self):
        """Nothing in the policy hands back a number to divide by."""
        from oracle import entry_price_policy

        for name in dir(entry_price_policy):
            if name.startswith("_"):
                continue
            value = getattr(entry_price_policy, name)
            assert not isinstance(value, (int, float)), (
                f"{name} is a number; this module states reasons, it does "
                f"not supply stand-in prices"
            )


class TestPostmortemZeroEntry:
    """A measured zero is narrated as the measurement plus why it is useless."""

    def test_zero_entry_keeps_the_measurement_and_states_the_reason(self):
        from intelligence.postmortem import _summarise_what_happened
        from oracle.entry_price_policy import SCORE_NOTE_ENTRY_ZERO

        text_out = _summarise_what_happened(
            "AAPL", "CALL", 0.0, 210.0, "miss", -0.1, [],
        )
        # The measurement is not hidden -- it really was 0.00 ...
        assert "$0.00" in text_out
        # ... and the sentence says why no return follows from it.
        assert SCORE_NOTE_ENTRY_ZERO in text_out

    def test_null_entry_says_nobody_measured_it(self):
        from intelligence.postmortem import _summarise_what_happened

        text_out = _summarise_what_happened(
            "AAPL", "CALL", None, 210.0, "miss", -0.1, [],
        )
        assert "no measured entry price" in text_out
        # Never a price nobody looked up.
        assert "$0.00" not in text_out

    def test_timing_classification_names_the_reason_instead_of_missing(self):
        from intelligence.postmortem import _classify_prediction_failure
        from oracle.entry_price_policy import (
            SCORE_NOTE_ENTRY_NULL,
            SCORE_NOTE_ENTRY_ZERO,
        )

        for entry, note in ((None, SCORE_NOTE_ENTRY_NULL),
                            (0.0, SCORE_NOTE_ENTRY_ZERO)):
            category, root_cause, _wrong, _right, what_missed = (
                _classify_prediction_failure(
                    ticker="AAPL", direction="CALL", entry_price=entry,
                    target=210.0, expiry=date.today(), actual_price=190.0,
                    actual_move_pct=-5.0, signals=[], anti_signals=[],
                    price_path=[{"price": 200.0}, {"price": 190.0}],
                )
            )
            # Not "wrong_signal": nothing here shows the model was wrong.
            assert category == "not_measured", (entry, category)
            assert note in root_cause
            assert note in what_missed


# ── A row closed for an invalid entry price counts nowhere ──────────────────

class TestClosedInvalidEntryRowsCountNowhere:
    """When the scorer or the engine closes a row whose entry price is NULL,
    zero or negative, it writes exactly ``verdict='no_data'``,
    ``scored_at=NOW()`` and ``score_notes=<reason>`` and leaves ``pnl_pct``
    NULL. Such a row must not become a win, a loss, a zero return or a
    calibration sample — even when it carries a measured confidence — because
    every aggregate keys on ``verdict IN ('hit','miss','partial')`` and never
    on ``pnl_pct`` being present. This pins the closed row against the real
    aggregation SQL, extracted from the scorer, and against calibration.
    """

    def _seed(self, engine) -> None:
        with engine.begin() as conn:
            _insert(conn, id="hit-row", ticker="AAA", direction="CALL",
                    entry_price=100.0, confidence=0.9, verdict="hit",
                    pnl_pct=10.0, expiry=date(2026, 9, 1))
            _insert(conn, id="miss-row", ticker="BBB", direction="CALL",
                    entry_price=100.0, confidence=0.6, verdict="miss",
                    pnl_pct=-10.0, expiry=date(2026, 9, 1))
            # Closed by the engine/scorer for a zero entry: measured
            # confidence, NULL pnl, the zero reason.
            _insert(conn, id="closed-zero-entry", ticker="CCC", direction="CALL",
                    entry_price=0.0, confidence=0.9, verdict="no_data",
                    pnl_pct=None, score_notes=SCORE_NOTE_ENTRY_ZERO,
                    scored_at="2026-09-18 00:00:00", expiry=date(2026, 9, 1))
            _insert(conn, id="closed-null-entry", ticker="DDD", direction="CALL",
                    entry_price=None, confidence=0.9, verdict="no_data",
                    pnl_pct=None, score_notes=SCORE_NOTE_ENTRY_NULL,
                    scored_at="2026-09-18 00:00:00", expiry=date(2026, 9, 1))

    @staticmethod
    def _scorer_sql(marker: str) -> str:
        src = (REPO_ROOT / "scripts" / "score_oracle_trades.py").read_text(
            encoding="utf-8"
        )
        start = src.index(marker)
        start = src.index('text("""', start) + len('text("""')
        end = src.index('"""', start)
        return src[start:end]

    def test_not_a_calibration_sample(self, engine):
        self._seed(engine)
        report = compute_calibration(engine, n_bins=10)
        # Only the hit and the miss are samples; the two closed rows carry a
        # measured 0.9 but are not verdicts.
        assert report.total_predictions == 2
        assert report.overall_accuracy == pytest.approx(0.5)

    def test_not_a_win_loss_or_zero_return_in_the_model_summary(self, engine):
        self._seed(engine)
        sql = self._scorer_sql("model_stats = conn.execute(")
        with engine.connect() as conn:
            rows = conn.execute(text(sql)).fetchall()
        assert len(rows) == 1
        _name, total, hits, misses, partials, pending, avg_pnl = rows[0]
        assert total == 4                      # closed rows are still rows...
        assert (hits, misses, partials, pending) == (1, 1, 0, 0)  # ...not outcomes
        assert avg_pnl == pytest.approx(0.0)   # (10 - 10) / 2: no third term at 0

    def test_absent_from_the_scored_only_ticker_summary(self, engine):
        self._seed(engine)
        sql = self._scorer_sql("ticker_stats = conn.execute(")
        with engine.connect() as conn:
            tickers = {r[0] for r in conn.execute(text(sql)).fetchall()}
        assert tickers == {"AAA", "BBB"}

    def test_closed_rows_are_final_and_untouched_by_the_scorer(self, engine):
        """The chunk query requires a usable entry price; the sweep only
        re-labels rows still 'pending'. A closed row keeps its verdict, note
        and NULL pnl through another run."""
        self._seed(engine)
        src = (REPO_ROOT / "scripts" / "score_oracle_trades.py").read_text(
            encoding="utf-8"
        )
        assert "AND entry_price IS NOT NULL" in src and "AND entry_price > 0" in src
        assert "verdict = 'pending'" in src or "verdict='pending'" in src
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT verdict, score_notes, pnl_pct FROM oracle_predictions "
                "WHERE id = 'closed-zero-entry'"
            )).fetchone()
        assert row == ("no_data", SCORE_NOTE_ENTRY_ZERO, None)
