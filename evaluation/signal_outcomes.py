"""Versioned, honest per-signal outcome evaluator (workstream W3b).

This module is a **from-scratch replacement candidate** for the "signal win
rate" numbers produced by ``intelligence/trust_scorer.py``. It does not
import, call, or modify that module, ``intelligence/postmortem.py``, or any
Hermes/scheduler code. It is not wired into anything production-facing.

Why this exists (see module docstring in the PR/report for the full list):
the old meter (`intelligence/trust_scorer.py::score_prediction_signals` /
its `_price`-based scoring loop around ``MOVE_THRESHOLD_PCT``) collapses
every non-CORRECT case into ``WRONG`` — including signals it never had a
price for, signals whose direction it could not classify, and signals whose
underlying instrument later delisted. This module instead has a dedicated,
never-a-guess ``INELIGIBLE`` outcome with an explicit reason, a genuinely
inconclusive ``UNRESOLVED`` outcome for signals still inside their horizon,
and a dead-band ``NO_MOVE`` outcome that is explicitly NOT counted as WRONG.

Everything here is versioned via ``EVALUATION_VERSION``. This branch only
evaluates in memory; it has no persistence entry point.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from math import isfinite
from zoneinfo import ZoneInfo
from typing import Callable, Iterable, Optional

# ---------------------------------------------------------------------------
# Versioning
# ---------------------------------------------------------------------------

EVALUATION_VERSION: str = "sig-eval-3-dryrun"
MARKET_TZ = ZoneInfo("America/New_York")

# ---------------------------------------------------------------------------
# Vocabulary
# ---------------------------------------------------------------------------

DIRECTIONS = ("BUY", "SELL", "NEUTRAL", "UNKNOWN")
OUTCOMES = ("CORRECT", "WRONG", "NO_MOVE", "UNRESOLVED", "INELIGIBLE")
ORIGIN_TAGS = ("unknown",)  # no row-level origin provenance in this dry run

# eligibility_reason vocabulary (INELIGIBLE only) — not exhaustive by design,
# callers may pass through other short snake_case reasons, but these are the
# ones this module itself produces.
REASON_UNKNOWN_DIRECTION = "unknown_direction"
REASON_MISSING_ENTRY = "missing_entry_price"
REASON_MISSING_EXIT = "missing_exit_price"
REASON_UNSUPPORTED_INSTRUMENT = "unsupported_instrument_history"
REASON_PRICE_OUT_OF_BOUNDS = "price_outside_sanity_bounds"
REASON_STALE_ENTRY = "stale_entry_bar"
REASON_STALE_EXIT = "stale_exit_bar"
REASON_UNVERIFIED_KNOWN_AT = "known_at_unverified"
REASON_AMBIGUOUS_PRICE = "ambiguous_raw_close_multiple_values"


def validate_scoring_parameters(dead_band_pct: float, cost_bps: float) -> None:
    """Refuse values that make directional comparisons or returns meaningless."""
    if not isfinite(dead_band_pct) or not 0 <= dead_band_pct <= 100:
        raise ValueError("dead_band_pct must be finite and between 0 and 100")
    if not isfinite(cost_bps) or not 0 <= cost_bps <= 10_000:
        raise ValueError("cost_bps must be finite and between 0 and 10000")


class UnsupportedInstrumentError(Exception):
    """Raise from a price accessor for a delisted / unsupported instrument.

    This lets an accessor distinguish "this instrument's history is not
    supported at all" (-> INELIGIBLE(unsupported_instrument_history)) from a
    plain "no price for this specific date yet" (return ``None``, which this
    module treats as UNRESOLVED-or-missing depending on horizon elapsed).
    """


class AmbiguousPriceError(UnsupportedInstrumentError):
    """Raise when the raw-close series has more than one distinct value for a date.

    ``raw_series``'s uniqueness constraint is on ``(series_id, source_id,
    obs_date, pull_timestamp)`` — a second writer under the *same* series_id
    and source_id (for example an adjusted-close puller sharing the
    ``yfinance`` source row) can insert a second, differently-valued row for
    the same ``obs_date`` at a later ``pull_timestamp``. Historical
    contamination of this kind is confirmed for ``YF:{ticker}:close`` (see
    ``docs/reference/PRICE_SERIES_CONTRACT.md``). An accessor must refuse the
    date outright rather than silently pick "the latest pull wins" -> a
    dedicated subclass of ``UnsupportedInstrumentError`` so callers can label
    the refusal distinctly from a generic unsupported-instrument refusal.
    """


# ---------------------------------------------------------------------------
# Input record
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SignalRecord:
    """One signal to evaluate.

    ``signal_source_id`` is provenance only in this dry-run implementation.

    Origin is deliberately omitted: the selected source rows carry no
    verified row-level origin provenance.
    """

    source_type: str
    instrument: str
    signal_date: date
    direction: str  # one of DIRECTIONS
    horizon_days: int
    signal_source_id: Optional[int] = None
    metadata: dict = field(default_factory=dict)
    known_at: Optional[datetime] = None  # source publication timestamp, if proven
    created_at: Optional[datetime] = None  # conservative ingestion-time proxy

    def dedup_key(self) -> tuple:
        """Identity used to collapse duplicate records before counting."""
        return (self.source_type, self.instrument, self.signal_date, self.horizon_days)


# ---------------------------------------------------------------------------
# Price accessor contract
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PricePoint:
    """A single priced bar used as an entry or exit basis."""

    price: float
    bar_date: date
    basis: str = "raw_close"


# A price accessor takes (instrument, as_of) and returns the PIT-safe price
# bar to use as of that date (i.e. the latest bar with date <= as_of), or
# None if no such bar exists yet. It may raise UnsupportedInstrumentError.
PriceAccessor = Callable[[str, date], Optional[PricePoint]]


def make_default_price_accessor(engine) -> PriceAccessor:
    """Build the exact raw-series accessor; no resolved-feature inference."""
    from evaluation.prices import PITPriceAccessor

    return PITPriceAccessor(engine)


# ---------------------------------------------------------------------------
# Output record
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SignalOutcomeRecord:
    """Versioned outcome record for one (deduplicated) signal."""

    signal_source_id: Optional[int]
    source_type: str
    instrument: str
    signal_date: date
    direction: str
    horizon_days: int
    evaluation_version: str
    origin_tag: str

    dead_band_pct: float
    cost_bps: float

    entry_price: Optional[float]
    entry_price_date: Optional[date]
    entry_price_basis: Optional[str]

    exit_price: Optional[float]
    exit_price_date: Optional[date]
    exit_price_basis: Optional[str]

    raw_return: Optional[float]
    cost_adjusted_return: Optional[float]

    outcome: str
    eligibility_reason: Optional[str] = None


# ---------------------------------------------------------------------------
# Core evaluation
# ---------------------------------------------------------------------------


def _ineligible(record: SignalRecord, reason: str, *, dead_band_pct: float, cost_bps: float,
                 entry: Optional[PricePoint] = None) -> SignalOutcomeRecord:
    return SignalOutcomeRecord(
        signal_source_id=record.signal_source_id,
        source_type=record.source_type,
        instrument=record.instrument,
        signal_date=record.signal_date,
        direction=record.direction,
        horizon_days=record.horizon_days,
        evaluation_version=EVALUATION_VERSION,
        origin_tag="unknown",
        dead_band_pct=dead_band_pct,
        cost_bps=cost_bps,
        entry_price=entry.price if entry else None,
        entry_price_date=entry.bar_date if entry else None,
        entry_price_basis=entry.basis if entry else None,
        exit_price=None,
        exit_price_date=None,
        exit_price_basis=None,
        raw_return=None,
        cost_adjusted_return=None,
        outcome="INELIGIBLE",
        eligibility_reason=reason,
    )


def _classify_direction(direction: str, raw_return: float, band_pct: float) -> str:
    """Direction-aware classification against the dead band.

    BUY  -> CORRECT if return >  +band; WRONG if return < -band; else NO_MOVE.
    SELL -> CORRECT if return <  -band; WRONG if return > +band; else NO_MOVE.
    NEUTRAL -> the *prediction itself* is "no big move": CORRECT if the move
       stayed inside the band, WRONG if it broke out either direction. There
       is no NO_MOVE case for NEUTRAL — staying inside the band is exactly
       what was predicted, not an inconclusive result.
    """
    band = abs(band_pct)
    if direction == "BUY":
        if raw_return > band:
            return "CORRECT"
        if raw_return < -band:
            return "WRONG"
        return "NO_MOVE"
    if direction == "SELL":
        if raw_return < -band:
            return "CORRECT"
        if raw_return > band:
            return "WRONG"
        return "NO_MOVE"
    if direction == "NEUTRAL":
        return "CORRECT" if abs(raw_return) <= band else "WRONG"
    raise ValueError(f"_classify_direction called with non-directional value: {direction!r}")


def evaluate_signal(
    record: SignalRecord,
    price_accessor: PriceAccessor,
    *,
    dead_band_pct: float = 1.0,
    cost_bps: float = 0.0,
    today: Optional[date] = None,
    sanity_bounds: tuple[float, float] = (0.0, 1_000_000.0),
) -> SignalOutcomeRecord:
    """Evaluate a single signal into a versioned outcome record.

    Parameters:
        record: the signal to evaluate.
        price_accessor: PIT-safe callable ``(instrument, as_of) -> PricePoint | None``.
            The exit lookup is always called with
            ``as_of = record.signal_date + horizon_days`` — never beyond it.
        dead_band_pct: absolute-return threshold (in percent, e.g. ``1.0`` ==
            1%) below which a directional move is NO_MOVE rather than
            CORRECT/WRONG. Recorded on the output record.
        cost_bps: round-trip transaction cost in basis points, applied as a
            simple drag on the raw return and recorded (does not change the
            CORRECT/WRONG/NO_MOVE classification, which is about whether the
            *market* moved as predicted, not about net P&L).
        today: current date, used only to distinguish UNRESOLVED (still
            inside horizon) from INELIGIBLE(missing_exit_price) (horizon has
            elapsed and there is still no price). Defaults to
            ``date.today()``.
        sanity_bounds: (min, max) inclusive price sanity bounds. A price
            outside these bounds is treated as bad data, never guessed at.

    Returns:
        A SignalOutcomeRecord. UNKNOWN direction always yields
        INELIGIBLE(unknown_direction) — it is never scored as WRONG, and
        evaluating the same record again does not retry a price lookup
        that will never resolve the direction.
    """
    if today is None:
        today = datetime.now(MARKET_TZ).date()

    validate_scoring_parameters(dead_band_pct, cost_bps)

    if isinstance(record.signal_date, datetime) or not isinstance(record.signal_date, date):
        raise TypeError("signal_date must be a normalized DATE")

    if record.horizon_days <= 0:
        raise ValueError("horizon_days must be positive")
    if record.direction == "UNKNOWN":
        return _ineligible(record, REASON_UNKNOWN_DIRECTION, dead_band_pct=dead_band_pct, cost_bps=cost_bps)
    if record.direction not in DIRECTIONS:
        raise ValueError(f"Unrecognized direction: {record.direction!r}")

    # A DATE is midnight in the market timezone. A timestamp must carry its
    # timezone; naive timestamps cannot establish point-in-time availability.
    # A congressional transaction date precedes disclosure and cannot be
    # made point-in-time safe by a database insertion timestamp alone.
    if "congress" in record.source_type.lower() and record.known_at is None:
        return _ineligible(record, REASON_UNVERIFIED_KNOWN_AT, dead_band_pct=dead_band_pct, cost_bps=cost_bps)
    stamps = [stamp for stamp in (record.known_at, record.created_at) if stamp is not None]
    if not stamps or any(stamp.tzinfo is None or stamp.utcoffset() is None for stamp in stamps):
        return _ineligible(record, REASON_UNVERIFIED_KNOWN_AT, dead_band_pct=dead_band_pct, cost_bps=cost_bps)
    stamp = max(stamps)
    known_date = stamp.astimezone(MARKET_TZ).date()
    if stamp.astimezone(MARKET_TZ).time() >= time(16, 0):
        known_date += timedelta(days=1)
    entry_as_of = max(record.signal_date, known_date)
    if entry_as_of > today:
        return _ineligible(record, REASON_UNVERIFIED_KNOWN_AT, dead_band_pct=dead_band_pct, cost_bps=cost_bps)

    try:
        entry = price_accessor(record.instrument, entry_as_of)
    except AmbiguousPriceError:
        return _ineligible(record, REASON_AMBIGUOUS_PRICE, dead_band_pct=dead_band_pct, cost_bps=cost_bps)
    except UnsupportedInstrumentError:
        return _ineligible(record, REASON_UNSUPPORTED_INSTRUMENT, dead_band_pct=dead_band_pct, cost_bps=cost_bps)

    if entry is None:
        return _ineligible(record, REASON_MISSING_ENTRY, dead_band_pct=dead_band_pct, cost_bps=cost_bps)

    if entry.basis != "raw_close":
        return _ineligible(record, REASON_UNSUPPORTED_INSTRUMENT, dead_band_pct=dead_band_pct, cost_bps=cost_bps, entry=entry)

    # The accessor returns the latest bar at or before the target. An older
    # close cannot be an entry after the signal became available. Without a
    # verified next-session calendar, only an exact-date bar is eligible.
    if not isinstance(entry.bar_date, date) or isinstance(entry.bar_date, datetime) or entry.bar_date != entry_as_of:
        return _ineligible(record, REASON_STALE_ENTRY, dead_band_pct=dead_band_pct, cost_bps=cost_bps, entry=entry)
    if entry.price <= 0:
        return _ineligible(record, REASON_PRICE_OUT_OF_BOUNDS, dead_band_pct=dead_band_pct, cost_bps=cost_bps, entry=entry)

    lo, hi = sanity_bounds
    if not (lo <= entry.price <= hi):
        return _ineligible(record, REASON_PRICE_OUT_OF_BOUNDS, dead_band_pct=dead_band_pct, cost_bps=cost_bps, entry=entry)

    exit_as_of = entry_as_of + timedelta(days=record.horizon_days)

    # A bar at the nominal horizon is not an outcome until that horizon has
    # elapsed. Do not fetch or grade an immature signal even if backfilled data
    # happens to be visible to an accessor.
    if today < exit_as_of:
        return SignalOutcomeRecord(
            signal_source_id=record.signal_source_id, source_type=record.source_type,
            instrument=record.instrument, signal_date=record.signal_date,
            direction=record.direction, horizon_days=record.horizon_days,
            evaluation_version=EVALUATION_VERSION, origin_tag="unknown",
            dead_band_pct=dead_band_pct, cost_bps=cost_bps,
            entry_price=entry.price, entry_price_date=entry.bar_date,
            entry_price_basis=entry.basis, exit_price=None, exit_price_date=None,
            exit_price_basis=None, raw_return=None, cost_adjusted_return=None,
            outcome="UNRESOLVED", eligibility_reason=None,
        )

    try:
        exit_ = price_accessor(record.instrument, exit_as_of)
    except AmbiguousPriceError:
        return _ineligible(record, REASON_AMBIGUOUS_PRICE, dead_band_pct=dead_band_pct, cost_bps=cost_bps, entry=entry)
    except UnsupportedInstrumentError:
        return _ineligible(record, REASON_UNSUPPORTED_INSTRUMENT, dead_band_pct=dead_band_pct, cost_bps=cost_bps, entry=entry)

    if exit_ is None:
        return _ineligible(record, REASON_MISSING_EXIT, dead_band_pct=dead_band_pct, cost_bps=cost_bps, entry=entry)

    if exit_.basis != "raw_close":
        return _ineligible(record, REASON_UNSUPPORTED_INSTRUMENT, dead_band_pct=dead_band_pct, cost_bps=cost_bps, entry=entry)

    # An earlier bar cannot stand in for the nominal horizon. An exact-date
    # exit is the only eligible case until session rules are established.
    if not isinstance(exit_.bar_date, date) or isinstance(exit_.bar_date, datetime) or exit_.bar_date != exit_as_of or exit_.bar_date <= entry.bar_date:
        return _ineligible(record, REASON_STALE_EXIT, dead_band_pct=dead_band_pct, cost_bps=cost_bps, entry=entry)

    if not (lo <= exit_.price <= hi):
        return _ineligible(record, REASON_PRICE_OUT_OF_BOUNDS, dead_band_pct=dead_band_pct, cost_bps=cost_bps, entry=entry)

    raw_return_pct = (exit_.price - entry.price) / entry.price * 100.0
    cost_drag_pct = cost_bps / 100.0  # 1 bp == 0.01%; cost_bps/100 -> percent
    cost_adjusted_return_pct = raw_return_pct - cost_drag_pct

    outcome = _classify_direction(record.direction, raw_return_pct, dead_band_pct)

    return SignalOutcomeRecord(
        signal_source_id=record.signal_source_id,
        source_type=record.source_type,
        instrument=record.instrument,
        signal_date=record.signal_date,
        direction=record.direction,
        horizon_days=record.horizon_days,
        evaluation_version=EVALUATION_VERSION,
        origin_tag="unknown",
        dead_band_pct=dead_band_pct,
        cost_bps=cost_bps,
        entry_price=entry.price,
        entry_price_date=entry.bar_date,
        entry_price_basis=entry.basis,
        exit_price=exit_.price,
        exit_price_date=exit_.bar_date,
        exit_price_basis=exit_.basis,
        raw_return=raw_return_pct,
        cost_adjusted_return=cost_adjusted_return_pct,
        outcome=outcome,
        eligibility_reason=None,
    )


# ---------------------------------------------------------------------------
# Deduplication + cohort summary
# ---------------------------------------------------------------------------


def dedupe_records(records: Iterable[SignalRecord]) -> tuple[list[SignalRecord], int]:
    """Collapse duplicate SignalRecords (same source_type/instrument/date/horizon).

    Returns (deduplicated_records, n_duplicates_dropped). First occurrence
    of each key wins; order of the input otherwise preserved.
    """
    seen: dict[tuple, SignalRecord] = {}
    dropped = 0
    for r in records:
        key = r.dedup_key()
        if key in seen:
            dropped += 1
            continue
        seen[key] = r
    return list(seen.values()), dropped


def dedupe_outcomes(outcomes: Iterable[SignalOutcomeRecord]) -> tuple[list[SignalOutcomeRecord], int]:
    """Same dedup logic, applied post-evaluation to a list of outcome records."""
    seen: dict[tuple, SignalOutcomeRecord] = {}
    dropped = 0
    for o in outcomes:
        key = (o.source_type, o.instrument, o.signal_date, o.horizon_days)
        if key in seen:
            dropped += 1
            continue
        seen[key] = o
    return list(seen.values()), dropped


@dataclass(frozen=True)
class CohortSummary:
    """Cohort-level rollup with explicit denominators. No significance claims."""

    n_total_input: int
    n_duplicates_dropped: int
    n_total: int  # == n_total_input - n_duplicates_dropped
    n_eligible: int  # everything except INELIGIBLE
    n_resolved: int  # CORRECT + WRONG + NO_MOVE (i.e. eligible and not UNRESOLVED)
    n_correct: int
    n_wrong: int
    n_no_move: int
    n_unresolved: int
    n_ineligible: int
    n_ineligible_by_reason: dict
    move_base_rate: Optional[float]  # of n_resolved, fraction with |return| > band
    baseline_by_horizon: dict  # horizon_days -> matched baseline (see below)
    origin_tag_counts: dict  # origin_tag -> n_total for that tag (post-dedup)


def summarize_outcomes(outcomes: Iterable[SignalOutcomeRecord]) -> CohortSummary:
    """Roll a list of outcome records up into a CohortSummary.

    Deduplicates first (same identity as ``SignalRecord.dedup_key``), reports
    how many were dropped, and computes counts with explicit denominators.

    The "matched baseline per horizon" is *not* an assumed 50/50 — it is
    each horizon's own realised outcome distribution among that horizon's
    resolved (CORRECT/WRONG) population: baseline_correct_rate =
    n_correct / (n_correct + n_wrong) for that horizon. This answers "what
    fraction of resolved calls at this horizon were CORRECT," which a
    genuinely uninformative signal matching the population's own realised
    direction mix would be expected to hit — it is deliberately not a
    50% coin-flip assumption, and no p-value/significance claim is made
    here or anywhere in this module.
    """
    all_outcomes = list(outcomes)
    n_total_input = len(all_outcomes)
    deduped, n_dropped = dedupe_outcomes(all_outcomes)
    n_total = len(deduped)

    n_correct = sum(1 for o in deduped if o.outcome == "CORRECT")
    n_wrong = sum(1 for o in deduped if o.outcome == "WRONG")
    n_no_move = sum(1 for o in deduped if o.outcome == "NO_MOVE")
    n_unresolved = sum(1 for o in deduped if o.outcome == "UNRESOLVED")
    n_ineligible = sum(1 for o in deduped if o.outcome == "INELIGIBLE")
    n_eligible = n_total - n_ineligible
    n_resolved = n_correct + n_wrong + n_no_move

    reasons: dict = {}
    for o in deduped:
        if o.outcome == "INELIGIBLE":
            reasons[o.eligibility_reason] = reasons.get(o.eligibility_reason, 0) + 1

    move_denominator = n_correct + n_wrong + n_no_move
    move_base_rate = ((n_correct + n_wrong) / move_denominator) if move_denominator > 0 else None

    by_horizon: dict = {}
    for o in deduped:
        if o.outcome not in ("CORRECT", "WRONG"):
            continue
        by_horizon.setdefault(o.horizon_days, {"correct": 0, "wrong": 0})
        by_horizon[o.horizon_days]["correct" if o.outcome == "CORRECT" else "wrong"] += 1

    baseline_by_horizon = {}
    for horizon, counts in by_horizon.items():
        denom = counts["correct"] + counts["wrong"]
        baseline_by_horizon[horizon] = {
            "n_correct": counts["correct"],
            "n_wrong": counts["wrong"],
            "n_resolved_directional": denom,
            "baseline_correct_rate": (counts["correct"] / denom) if denom > 0 else None,
        }

    origin_tag_counts: dict = {}
    for o in deduped:
        origin_tag_counts[o.origin_tag] = origin_tag_counts.get(o.origin_tag, 0) + 1

    return CohortSummary(
        n_total_input=n_total_input,
        n_duplicates_dropped=n_dropped,
        n_total=n_total,
        n_eligible=n_eligible,
        n_resolved=n_resolved,
        n_correct=n_correct,
        n_wrong=n_wrong,
        n_no_move=n_no_move,
        n_unresolved=n_unresolved,
        n_ineligible=n_ineligible,
        n_ineligible_by_reason=reasons,
        move_base_rate=move_base_rate,
        baseline_by_horizon=baseline_by_horizon,
        origin_tag_counts=origin_tag_counts,
    )
