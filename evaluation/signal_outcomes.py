"""Versioned, honest per-signal outcome evaluator (workstream W3b, wired S8).

The legacy meter (``intelligence/trust_scorer.py::score_pending_signals``)
collapses every non-CORRECT case into ``WRONG`` -- including moves the right
way that fell short of its 1% threshold, signals it never had a price for,
and signals priced off a close from before the signal was known. This module
has a dedicated, never-a-guess ``INELIGIBLE`` outcome with an explicit
reason, an inconclusive ``UNRESOLVED`` outcome for signals still inside
their horizon, and a dead-band ``NO_MOVE`` outcome that is NOT counted as
WRONG.

Everything is versioned. ``EVALUATION_VERSION`` is the original in-memory
rule set (exact calendar-day bars); ``SESSION_EVALUATION_VERSION`` adds
NYSE-session rolling and is what ``replay_signal_sources`` (offline, dry run
by default; CLI ``scripts/evaluate_signals.py replay``) and ``score_forward``
(the trust cycle, behind ``GRID_ENABLE_SIGNAL_OUTCOME_FORWARD_SCORING``)
append to ``signal_source_outcomes``. ``signal_sources.outcome`` is never
written here; consumers still read it until they are switched deliberately.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date, datetime, time, timedelta
from math import isfinite
from zoneinfo import ZoneInfo
from typing import Callable, Iterable, Optional

# ---------------------------------------------------------------------------
# Versioning
# ---------------------------------------------------------------------------

EVALUATION_VERSION: str = "sig-eval-3-dryrun"
# Same rules plus NYSE-session rolling of the entry and exit dates (see
# ``evaluate_signal(session_calendar=...)``). This is the version persisted
# to ``signal_source_outcomes`` by the replay and the forward scorer.
SESSION_EVALUATION_VERSION: str = "sig-eval-4-sessions"
# A session is searched for at most this many calendar days forward.
MAX_SESSION_ROLL_DAYS: int = 7
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
REASON_UNVERIFIED_CUTOVER = "price_basis_cutover_unverified"


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


class UnverifiedCutoverError(UnsupportedInstrumentError):
    """Raise when no verified raw-close ingestion cutover was supplied.

    Without an externally verified ``verified_raw_close_since`` timestamp,
    the accessor cannot tell a genuinely raw close from a later, differently
    -basised value written under the same series/source identity -- so it
    refuses every row before running any query at all, including the
    ambiguous-date check (``AmbiguousPriceError``), which can only run once a
    cutover narrows the visible pull window. This is a distinct, dedicated
    reason from a generic "instrument unsupported" refusal: the instrument
    may be perfectly fine, the cutover is simply absent (e.g. the manual CLI
    never supplies one).
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


def _roll_to_session(d: date, session_calendar: Callable[[date], bool]) -> Optional[date]:
    """First session on or after ``d`` within ``MAX_SESSION_ROLL_DAYS``."""
    for k in range(MAX_SESSION_ROLL_DAYS + 1):
        candidate = d + timedelta(days=k)
        if session_calendar(candidate):
            return candidate
    return None


def evaluate_signal(
    record: SignalRecord,
    price_accessor: PriceAccessor,
    *,
    dead_band_pct: float = 1.0,
    cost_bps: float = 0.0,
    today: Optional[date] = None,
    sanity_bounds: tuple[float, float] = (0.0, 1_000_000.0),
    session_calendar: Optional[Callable[[date], bool]] = None,
) -> SignalOutcomeRecord:
    """Evaluate a single signal into a versioned outcome record.

    Without ``session_calendar`` this is ``EVALUATION_VERSION``: the entry
    and exit must be bars dated exactly on the (calendar-day) entry date and
    entry + ``horizon_days``. With it (``SESSION_EVALUATION_VERSION``), the
    entry date rolls forward to the first session on or after the date the
    signal became known, and the exit to the first session on or after entry
    + ``horizon_days``; both bars must still be dated exactly on those
    sessions. See ``_evaluate_signal`` for the rest of the contract.
    """
    out = _evaluate_signal(
        record, price_accessor, dead_band_pct=dead_band_pct, cost_bps=cost_bps,
        today=today, sanity_bounds=sanity_bounds, session_calendar=session_calendar,
    )
    if session_calendar is not None:
        out = replace(out, evaluation_version=SESSION_EVALUATION_VERSION)
    return out


def _evaluate_signal(
    record: SignalRecord,
    price_accessor: PriceAccessor,
    *,
    dead_band_pct: float = 1.0,
    cost_bps: float = 0.0,
    today: Optional[date] = None,
    sanity_bounds: tuple[float, float] = (0.0, 1_000_000.0),
    session_calendar: Optional[Callable[[date], bool]] = None,
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
    if session_calendar is not None:
        rolled = _roll_to_session(entry_as_of, session_calendar)
        if rolled is None:
            return _ineligible(record, REASON_STALE_ENTRY, dead_band_pct=dead_band_pct, cost_bps=cost_bps)
        entry_as_of = rolled
    if entry_as_of > today:
        return _ineligible(record, REASON_UNVERIFIED_KNOWN_AT, dead_band_pct=dead_band_pct, cost_bps=cost_bps)

    try:
        entry = price_accessor(record.instrument, entry_as_of)
    except AmbiguousPriceError:
        return _ineligible(record, REASON_AMBIGUOUS_PRICE, dead_band_pct=dead_band_pct, cost_bps=cost_bps)
    except UnverifiedCutoverError:
        return _ineligible(record, REASON_UNVERIFIED_CUTOVER, dead_band_pct=dead_band_pct, cost_bps=cost_bps)
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
    if session_calendar is not None:
        rolled_exit = _roll_to_session(exit_as_of, session_calendar)
        if rolled_exit is None:
            return _ineligible(record, REASON_STALE_EXIT, dead_band_pct=dead_band_pct, cost_bps=cost_bps, entry=entry)
        exit_as_of = rolled_exit

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
    except UnverifiedCutoverError:
        return _ineligible(record, REASON_UNVERIFIED_CUTOVER, dead_band_pct=dead_band_pct, cost_bps=cost_bps, entry=entry)
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


# ---------------------------------------------------------------------------
# known_at resolution (shared with scripts/evaluate_signals.py)
# ---------------------------------------------------------------------------

KNOWN_AT_SOURCE_CONGRESSIONAL_DISCLOSURE = "congressional_disclosure_date"
KNOWN_AT_SOURCE_FALLBACK_MISSING_DISCLOSURE = "created_at_fallback_missing_disclosure_date"
KNOWN_AT_SOURCE_FALLBACK_UNPARSEABLE_DISCLOSURE = "created_at_fallback_unparseable_disclosure_date"
KNOWN_AT_SOURCE_FALLBACK_UNVERIFIED_SOURCE_TYPE = "created_at_fallback_unverified_source_type"


def resolve_known_at(source_type: str, signal_value: Optional[dict]) -> tuple[Optional[datetime], str]:
    """Return ``(known_at, known_at_source)`` for one signal row.

    Only congressional rows carry a proven publication field
    (``disclosure_date`` in ``signal_value``, written by
    ``ingestion/altdata/congressional.py``). It has no time of day, so it is
    read as the end of that New York day, which ``evaluate_signal``'s
    after-16:00 rule pushes to the next session. Every other source type
    returns ``None`` and ``evaluate_signal`` falls back to ``created_at``.
    """
    if "congress" not in (source_type or "").lower():
        return None, KNOWN_AT_SOURCE_FALLBACK_UNVERIFIED_SOURCE_TYPE
    raw = (signal_value or {}).get("disclosure_date")
    if not isinstance(raw, str):
        return None, KNOWN_AT_SOURCE_FALLBACK_MISSING_DISCLOSURE
    try:
        disclosure_date = date.fromisoformat(raw)
    except ValueError:
        return None, KNOWN_AT_SOURCE_FALLBACK_UNPARSEABLE_DISCLOSURE
    return datetime.combine(disclosure_date, time.max, MARKET_TZ), KNOWN_AT_SOURCE_CONGRESSIONAL_DISCLOSURE


# ---------------------------------------------------------------------------
# Persistence: offline replay and forward scoring of signal_sources
# ---------------------------------------------------------------------------
#
# ``intelligence/trust_scorer.py::score_pending_signals`` writes the legacy
# ``signal_sources.outcome``: CORRECT when the move beat 1% in the signal's
# direction, WRONG for everything else -- a 0.2% drift, a missing exit, a
# move the right way that fell short. Those rows are never rewritten here.
# The corrected outcome goes to ``signal_source_outcomes`` (append-only, one
# row per ``(signal_source_id, evaluation_version)``), so legacy readers are
# untouched and a later rule change is a new version, not an overwrite.

OUTCOMES_TABLE = "signal_source_outcomes"
# A price-dependent refusal is only final once the puller has had this long
# past the nominal exit to catch up; until then the row is not written.
FINAL_GRACE_DAYS = 7
PRICE_HISTORY_PAD_DAYS = 10
_WAIT_REASONS = frozenset({
    REASON_MISSING_ENTRY, REASON_MISSING_EXIT, REASON_STALE_ENTRY,
    REASON_STALE_EXIT, REASON_UNVERIFIED_KNOWN_AT,
})
_LEGACY_DIRECTION = {"bullish": "BUY", "bearish": "SELL"}


@dataclass(frozen=True)
class SignalSourceRow:
    """The ``signal_sources`` columns the evaluator needs."""

    id: int
    source_type: str
    ticker: Optional[str]
    signal_date: date
    signal_type: Optional[str]
    signal_value: object
    created_at: Optional[datetime]
    legacy_outcome: Optional[str] = None


def parse_signal_payload(value: object) -> Optional[dict]:
    """JSONB ``signal_value`` as a dict (drivers return dicts or JSON text)."""
    import json

    if isinstance(value, dict):
        return value
    if isinstance(value, (str, bytes, bytearray)):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return None
        return parsed if isinstance(parsed, dict) else None
    return None


def record_from_row(row: SignalSourceRow) -> SignalRecord:
    """A ``SignalRecord`` with the legacy scorer's direction and horizon.

    Direction comes from ``trust_scorer._infer_signal_direction`` and the
    horizon from ``trust_scorer.EVALUATION_WINDOWS`` so the replay differs
    from the legacy outcome only in how a priced move is classified and in
    which prices are admissible.
    """
    from intelligence.trust_scorer import EVALUATION_WINDOWS, _infer_signal_direction, yf_symbol

    payload = parse_signal_payload(row.signal_value)
    inferred = _infer_signal_direction(row.signal_type, payload if payload is not None else row.signal_value)
    known_at, known_at_source = resolve_known_at(row.source_type, payload)
    signal_date = row.signal_date.date() if isinstance(row.signal_date, datetime) else row.signal_date
    return SignalRecord(
        source_type=row.source_type,
        instrument=yf_symbol(row.ticker or ""),
        signal_date=signal_date,
        direction=_LEGACY_DIRECTION.get(inferred, "UNKNOWN"),
        horizon_days=int(EVALUATION_WINDOWS.get(row.source_type, 7)),
        signal_source_id=row.id,
        metadata={"known_at_source": known_at_source, "legacy_outcome": row.legacy_outcome},
        known_at=known_at,
        created_at=row.created_at,
    )


def _available_date(record: SignalRecord) -> date:
    stamps = [s for s in (record.known_at, record.created_at) if s is not None and s.tzinfo is not None]
    known = max(stamps).astimezone(MARKET_TZ).date() if stamps else record.signal_date
    return max(record.signal_date, known)


def is_final(outcome: SignalOutcomeRecord, record: SignalRecord, today: date) -> bool:
    """Whether ``outcome`` may be persisted (the table is append-only).

    UNRESOLVED never is. A refusal that a later pull could cure (missing or
    off-session bar, signal not yet known) is final only ``FINAL_GRACE_DAYS``
    after the nominal exit; every other outcome is final at once.
    """
    if outcome.outcome == "UNRESOLVED":
        return False
    if outcome.outcome == "INELIGIBLE" and outcome.eligibility_reason in _WAIT_REASONS:
        return today >= _available_date(record) + timedelta(days=record.horizon_days + FINAL_GRACE_DAYS)
    return True


_SELECT_TICKER_ROWS_SQL = """
    SELECT s.id, s.source_type, s.ticker, s.signal_date, s.signal_type,
           s.signal_value, s.created_at, s.outcome
      FROM signal_sources s
     WHERE s.ticker = :ticker
       AND (:since IS NULL OR s.signal_date >= :since)
       AND MOD(s.id, :mod) = 0
     ORDER BY s.id
"""

_SELECT_TICKER_ROWS_UNSCORED_SQL = """
    SELECT s.id, s.source_type, s.ticker, s.signal_date, s.signal_type,
           s.signal_value, s.created_at, s.outcome
      FROM signal_sources s
     WHERE s.ticker = :ticker
       AND (:since IS NULL OR s.signal_date >= :since)
       AND MOD(s.id, :mod) = 0
       AND NOT EXISTS (SELECT 1 FROM signal_source_outcomes o
                        WHERE o.signal_source_id = s.id
                          AND o.evaluation_version = :version)
     ORDER BY s.id
"""

_SELECT_TICKERS_SQL = """
    SELECT DISTINCT ticker FROM signal_sources
     WHERE ticker IS NOT NULL AND (:since IS NULL OR signal_date >= :since)
     ORDER BY ticker
"""

_SELECT_FORWARD_ROWS_SQL = """
    SELECT s.id, s.source_type, s.ticker, s.signal_date, s.signal_type,
           s.signal_value, s.created_at, s.outcome
      FROM signal_sources s
     WHERE s.signal_date >= :since
       AND s.ticker IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM signal_source_outcomes o
                        WHERE o.signal_source_id = s.id
                          AND o.evaluation_version = :version)
     ORDER BY s.id
     LIMIT :n
"""

_INSERT_OUTCOME_SQL = """
    INSERT INTO signal_source_outcomes (
        signal_source_id, evaluation_version, source_type, ticker, signal_date,
        direction, horizon_days, outcome, eligibility_reason, dead_band_pct,
        cost_bps, entry_price, entry_price_date, exit_price, exit_price_date,
        raw_return, cost_adjusted_return, price_basis, known_at_source,
        legacy_outcome, evaluated_at
    ) VALUES (
        :signal_source_id, :evaluation_version, :source_type, :ticker, :signal_date,
        :direction, :horizon_days, :outcome, :eligibility_reason, :dead_band_pct,
        :cost_bps, :entry_price, :entry_price_date, :exit_price, :exit_price_date,
        :raw_return, :cost_adjusted_return, :price_basis, :known_at_source,
        :legacy_outcome, :evaluated_at
    )
    ON CONFLICT (signal_source_id, evaluation_version) DO NOTHING
"""


def outcome_params(outcome: SignalOutcomeRecord, record: SignalRecord, evaluated_at: datetime) -> dict:
    """Bind parameters for one ``signal_source_outcomes`` insert."""
    return {
        "signal_source_id": outcome.signal_source_id,
        "evaluation_version": outcome.evaluation_version,
        "source_type": outcome.source_type,
        "ticker": outcome.instrument,
        "signal_date": outcome.signal_date,
        "direction": outcome.direction,
        "horizon_days": outcome.horizon_days,
        "outcome": outcome.outcome,
        "eligibility_reason": outcome.eligibility_reason,
        "dead_band_pct": outcome.dead_band_pct,
        "cost_bps": outcome.cost_bps,
        "entry_price": outcome.entry_price,
        "entry_price_date": outcome.entry_price_date,
        "exit_price": outcome.exit_price,
        "exit_price_date": outcome.exit_price_date,
        "raw_return": outcome.raw_return,
        "cost_adjusted_return": outcome.cost_adjusted_return,
        "price_basis": f"YF:{outcome.instrument}:close yfinance+TIINGO raw close pulled >= 17:00 ET",
        "known_at_source": record.metadata.get("known_at_source"),
        "legacy_outcome": record.metadata.get("legacy_outcome"),
        "evaluated_at": evaluated_at,
    }


@dataclass
class OutcomeTally:
    """Projection counters for a replay or forward pass."""

    rows: int = 0
    final: int = 0
    written: int = 0
    tickers: int = 0
    price_reads: int = 0
    by_outcome: dict = field(default_factory=dict)
    by_reason: dict = field(default_factory=dict)
    legacy_vs_new: dict = field(default_factory=dict)
    by_source_type: dict = field(default_factory=dict)

    def add(self, outcome: SignalOutcomeRecord, legacy: Optional[str], final: bool) -> None:
        self.rows += 1
        self.final += int(final)
        self.by_outcome[outcome.outcome] = self.by_outcome.get(outcome.outcome, 0) + 1
        if outcome.outcome == "INELIGIBLE":
            reason = outcome.eligibility_reason or "unspecified"
            self.by_reason[reason] = self.by_reason.get(reason, 0) + 1
        key = f"{legacy or 'PENDING'}->{outcome.outcome}"
        self.legacy_vs_new[key] = self.legacy_vs_new.get(key, 0) + 1
        per = self.by_source_type.setdefault(outcome.source_type, {})
        per[outcome.outcome] = per.get(outcome.outcome, 0) + 1

    def as_dict(self) -> dict:
        def _sorted(d: dict) -> dict:
            return dict(sorted(d.items(), key=lambda kv: (-kv[1], kv[0])))
        return {
            "rows": self.rows, "final": self.final, "written": self.written,
            "tickers": self.tickers, "price_reads": self.price_reads,
            "by_outcome": _sorted(self.by_outcome), "by_reason": _sorted(self.by_reason),
            "legacy_vs_new": _sorted(self.legacy_vs_new),
            "by_source_type": {k: _sorted(v) for k, v in sorted(self.by_source_type.items())},
        }


def _begin_read(conn, *, read_only: bool, timeout_ms: int) -> None:
    """Open a (read-only, time-bounded) transaction on PostgreSQL connections."""
    from sqlalchemy import text

    if getattr(getattr(conn, "dialect", None), "name", None) != "postgresql":
        return
    if read_only:
        conn.execute(text("SET TRANSACTION READ ONLY"))
    conn.execute(text("SELECT set_config('statement_timeout', :t, true)"), {"t": str(int(timeout_ms))})


def _rows(raw_rows) -> list[SignalSourceRow]:
    return [SignalSourceRow(*tuple(r)[:8]) for r in raw_rows]


def evaluate_ticker_rows(
    conn,
    rows: list[SignalSourceRow],
    *,
    evaluated_at: datetime,
    tally: OutcomeTally,
) -> list[dict]:
    """Evaluate one ticker's rows; return insert params for the final ones."""
    from evaluation.prices import CompletedCloseAccessor
    from ingestion.market_calendar import is_market_open
    from intelligence.trust_scorer import MOVE_THRESHOLD_PCT

    if not rows:
        return []
    today = evaluated_at.astimezone(MARKET_TZ).date()
    records = [record_from_row(r) for r in rows]
    start = min(_available_date(rec) for rec in records) - timedelta(days=PRICE_HISTORY_PAD_DAYS)
    accessor = CompletedCloseAccessor(conn, start=start, evaluated_at=evaluated_at)
    writes: list[dict] = []
    for row, rec in zip(rows, records):
        out = evaluate_signal(
            rec, accessor, dead_band_pct=MOVE_THRESHOLD_PCT, cost_bps=0.0,
            today=today, session_calendar=is_market_open,
        )
        final = is_final(out, rec, today)
        tally.add(out, row.legacy_outcome, final)
        if final:
            writes.append(outcome_params(out, rec, evaluated_at))
    tally.tickers += 1
    tally.price_reads += accessor.reads
    return writes


def write_outcomes(engine, params: list[dict]) -> int:
    """Append final outcomes; an existing ``(id, version)`` row is left alone."""
    from sqlalchemy import text

    if not params:
        return 0
    with engine.begin() as conn:
        result = conn.execute(text(_INSERT_OUTCOME_SQL), params)
    count = getattr(result, "rowcount", None)
    return int(count) if isinstance(count, int) and count >= 0 else len(params)


def outcomes_table_exists(engine) -> bool:
    from sqlalchemy import text

    try:
        with engine.connect() as conn:
            return conn.execute(text("SELECT to_regclass('public.signal_source_outcomes')")).scalar() is not None
    except Exception:  # noqa: BLE001 -- SQLite / missing function: treat as absent
        return False


def replay_signal_sources(
    engine,
    *,
    apply: bool = False,
    evaluated_at: Optional[datetime] = None,
    sample_mod: int = 1,
    tickers: Optional[list[str]] = None,
    max_tickers: Optional[int] = None,
    since: Optional[date] = None,
    timeout_ms: int = 120_000,
    progress_every: int = 250,
) -> dict:
    """Recompute ``SESSION_EVALUATION_VERSION`` outcomes for existing rows.

    Dry run by default: reads in read-only transactions and writes nothing.
    ``apply=True`` appends the final outcomes to ``signal_source_outcomes``
    (rows already present for this version are skipped, so a rerun resumes).
    Work is done one ticker at a time -- one bounded price-series read and
    one ``signal_sources`` read per ticker -- so memory stays flat.
    ``sample_mod=k`` evaluates only rows with ``id % k = 0``.
    """
    from sqlalchemy import text

    from config import log

    if sample_mod < 1:
        raise ValueError("sample_mod must be >= 1")
    if apply and not outcomes_table_exists(engine):
        raise RuntimeError("signal_source_outcomes does not exist; run the alembic migration first")
    evaluated_at = evaluated_at or datetime.now(MARKET_TZ)
    tally = OutcomeTally()
    with engine.connect() as conn:
        if tickers is None:
            _begin_read(conn, read_only=True, timeout_ms=timeout_ms)
            tickers = [r[0] for r in conn.execute(text(_SELECT_TICKERS_SQL), {"since": since}).fetchall()]
            conn.rollback()
        if max_tickers is not None:
            tickers = tickers[:max_tickers]
        skip_existing = apply
        for i, ticker in enumerate(tickers, 1):
            _begin_read(conn, read_only=True, timeout_ms=timeout_ms)
            sql = _SELECT_TICKER_ROWS_UNSCORED_SQL if skip_existing else _SELECT_TICKER_ROWS_SQL
            rows = _rows(conn.execute(text(sql), {
                "ticker": ticker, "since": since, "mod": int(sample_mod),
                "version": SESSION_EVALUATION_VERSION,
            }).fetchall())
            writes = evaluate_ticker_rows(conn, rows, evaluated_at=evaluated_at, tally=tally)
            conn.rollback()
            if apply and writes:
                tally.written += write_outcomes(engine, writes)
            if progress_every and i % progress_every == 0:
                log.info("signal outcome replay: {i}/{n} tickers, {r} rows", i=i, n=len(tickers), r=tally.rows)
    result = tally.as_dict()
    result.update({
        "evaluation_version": SESSION_EVALUATION_VERSION, "dry_run": not apply,
        "evaluated_at": evaluated_at.isoformat(), "sample_mod": sample_mod,
        "since": since.isoformat() if since else None,
    })
    return result


def score_forward(
    engine,
    *,
    lookback_days: int = 60,
    max_rows: int = 20_000,
    evaluated_at: Optional[datetime] = None,
    timeout_ms: int = 120_000,
) -> dict:
    """Append ``SESSION_EVALUATION_VERSION`` outcomes for recent unscored rows.

    Called from ``trust_scorer.run_trust_cycle`` when
    ``GRID_ENABLE_SIGNAL_OUTCOME_FORWARD_SCORING`` is set. Selects up to
    ``max_rows`` rows dated within ``lookback_days`` with no outcome row for
    this version, oldest first, and writes the ones whose outcome is final.
    Returns ``{"skipped": reason}`` when the table is absent.
    """
    from sqlalchemy import text

    if not outcomes_table_exists(engine):
        return {"skipped": "signal_source_outcomes missing"}
    evaluated_at = evaluated_at or datetime.now(MARKET_TZ)
    since = evaluated_at.astimezone(MARKET_TZ).date() - timedelta(days=lookback_days)
    tally = OutcomeTally()
    with engine.connect() as conn:
        _begin_read(conn, read_only=True, timeout_ms=timeout_ms)
        rows = _rows(conn.execute(text(_SELECT_FORWARD_ROWS_SQL), {"since": since, "version": SESSION_EVALUATION_VERSION, "n": int(max_rows)}).fetchall())
        by_ticker: dict[str, list[SignalSourceRow]] = {}
        for r in rows:
            by_ticker.setdefault(r.ticker or "", []).append(r)
        writes: list[dict] = []
        for ticker_rows in by_ticker.values():
            writes.extend(evaluate_ticker_rows(conn, ticker_rows, evaluated_at=evaluated_at, tally=tally))
        conn.rollback()
    tally.written = write_outcomes(engine, writes)
    result = tally.as_dict()
    result.update({"evaluation_version": SESSION_EVALUATION_VERSION, "since": since.isoformat()})
    return result
