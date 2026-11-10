"""Oracle entry null reasons and timestamped observed-reference admission.

``oracle_predictions.entry_price`` is the spot that was measured at publish
time, or NULL when nothing measured it (D-M32). Neither NULL nor a
non-positive value can produce a return: ``(actual - entry) / entry`` raises
TypeError on the first and ZeroDivisionError on the second.

So every reader that wants a return has to decide *before* it divides, and
say which case it hit. The cases are different findings and never share a
string:

* **NULL** — nobody took the measurement. Not scorable; not a 0% return.
* **0 or negative** — a measurement exists and it cannot serve as a basis. A
  percentage return off a zero basis is not a big number, it is undefined.

The quote reader additionally binds a raw spot to its exact provider/capture
clocks. Its 20-minute regular-session policy admits an observed benchmark,
not an executable fill. Nothing repairs a row or backfills provenance. The
refusal reason remains visible in ``score_notes`` or ``tracking_pnl_basis``.
"""

from __future__ import annotations

import math
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import text

# ── score_notes (persisted by the scorers) ─────────────────────────────────
SCORE_NOTE_ENTRY_NULL = "No entry price was measured at publish time"
SCORE_NOTE_ENTRY_ZERO = "Entry price is 0, return not computable"
SCORE_NOTE_ENTRY_NEGATIVE = "Entry price is negative, return not computable"

# ── tracking_pnl_basis (returned by /api/v1/oracle/predictions) ────────────
PNL_BASIS_ENTRY_NULL = "unavailable: no entry price was measured at publish time"
PNL_BASIS_ENTRY_ZERO = "unavailable: entry price is 0, return not computable"
PNL_BASIS_ENTRY_NEGATIVE = "unavailable: entry price is negative, return not computable"
PNL_BASIS_NO_SPOT = "unavailable: no spot price has been observed for this ticker"
PNL_BASIS_SPOT_LOOKUP_FAILED = "unavailable: spot price lookup failed"
PNL_BASIS_MEASURED = "measured: (spot - entry_price) / entry_price"

_NULL_PAIR = (SCORE_NOTE_ENTRY_NULL, PNL_BASIS_ENTRY_NULL)
_ZERO_PAIR = (SCORE_NOTE_ENTRY_ZERO, PNL_BASIS_ENTRY_ZERO)
_NEGATIVE_PAIR = (SCORE_NOTE_ENTRY_NEGATIVE, PNL_BASIS_ENTRY_NEGATIVE)

# ── historical-NULL provenance boundary ─────────────────────────────────────
#
# Before ``oracle_pred_nullable_0918``, ``entry_price``/``confidence`` were
# NOT NULL, so today every NULL in either column is provably a post-migration
# honest-NULL row -- but that proof lives in migration history, not on the
# row. A future restore from an older backup, a re-imported dataset, or a
# reader working from the row alone has no way to tell "unmeasured, written
# under this policy" apart from some other, unanticipated source of NULL
# without re-deriving the constraint's history.
#
# Both writers (``oracle/publish.py``, ``oracle/engine.py::_store_predictions``)
# stamp every new row with this value, once, at INSERT -- never on UPDATE, and
# never backfilled onto a row that predates it. NULL in this column means
# "written before this policy existed, or provenance unknown"; this exact
# string means "this row's entry_price/confidence NULL, if either is NULL, is
# the honest-measurement policy's NULL." A new policy would need a new
# string, never a rewrite of this one.
NULL_WRITE_POLICY = "oracle_pred_nullable_0918"


def _classify(entry_price: Any) -> tuple[str, str] | None:
    """Return the (score note, pnl basis) pair, or None if the entry divides.

    A value that is not a number at all is treated as unmeasured rather than
    coerced: ``float("")`` raising is not evidence that the entry was zero.
    """
    if entry_price is None:
        return _NULL_PAIR
    try:
        value = float(entry_price)
    except (TypeError, ValueError):
        return _NULL_PAIR
    if math.isnan(value) or math.isinf(value):
        # A measurement that says nothing, and an infinity is not a price.
        return _NULL_PAIR
    if value == 0:
        return _ZERO_PAIR
    if value < 0:
        return _NEGATIVE_PAIR
    return None


def entry_price_score_note(entry_price: Any) -> str | None:
    """The ``score_notes`` reason this entry is unscorable, else None.

    ``None`` means the entry price is a positive number and the caller may
    divide by it.
    """
    pair = _classify(entry_price)
    return None if pair is None else pair[0]


def entry_price_pnl_basis(entry_price: Any) -> str | None:
    """The ``pnl_basis`` reason no return is computable, else None."""
    pair = _classify(entry_price)
    return None if pair is None else pair[1]


def is_divisible_entry_price(entry_price: Any) -> bool:
    """True only for a positive, finite, numeric entry price."""
    return _classify(entry_price) is None


# A delayed, observed benchmark is not an executable fill. Legacy prices are
# never admitted by inferring a quote clock from their date or capture rows.
QUOTE_REFERENCE_POLICY = "oracle_observed_reference_v1"
QUOTE_MAX_AGE_SECONDS = 20 * 60
_NY = ZoneInfo("America/New_York")


def _aware_clock(value: Any) -> datetime | None:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError:
            return None
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        return None
    return value.astimezone(timezone.utc)


def validate_quote_reference(
    receipt: Any, *, as_of: datetime, ticker: str | None = None,
    _require_read_observation: bool = True,
) -> str | None:
    """Return the refusal reason, or None for a qualified observed reference.

    Call again at the final decision instant: a receipt can age out while
    models/guards run. This validates a frozen receipt, never replaces it.
    """
    from ingestion.market_calendar import is_market_open, market_close_time

    cutoff = _aware_clock(as_of)
    if cutoff is None:
        return "decision clock is missing or unzoned"
    local = cutoff.astimezone(_NY)
    if (not is_market_open(local.date())
            or not time(9, 30) <= local.time() < market_close_time(local.date())):
        return "regular US equity session is closed"
    if not isinstance(receipt, dict) or receipt.get("policy") != QUOTE_REFERENCE_POLICY:
        return "no qualified entry quote receipt (legacy or unknown provenance)"
    if receipt.get("status") != "measured":
        return receipt.get("reason") or "no qualified quote is available"
    if ticker is not None and receipt.get("ticker") != ticker:
        return "quote ticker does not match prediction"
    if (receipt.get("source") != "options_capture_batches"
            or receipt.get("provider") not in {"cboe", "yahoo"}
            or receipt.get("basis") != "raw_spot"
            or not receipt.get("capture_batch_id") or not receipt.get("capture_source")
            or receipt.get("backfilled") is not False):
        return "quote source, raw basis or capture identity is unqualified"
    if isinstance(receipt.get("price"), bool) or not is_divisible_entry_price(receipt.get("price")):
        return "quote price is not positive and finite"
    clocks = {key: _aware_clock(receipt.get(key)) for key in (
        "market_at", "capture_started_at", "capture_completed_at", "available_at",
    )}
    if any(value is None for value in clocks.values()):
        return "exact quote or capture clock is missing or unzoned"
    market, started, completed, available = (clocks[key] for key in (
        "market_at", "capture_started_at", "capture_completed_at", "available_at",
    ))
    if not (market <= completed <= available <= cutoff and started <= completed):
        return "quote or availability clock is future or inconsistent"
    if _require_read_observation:
        read_at = _aware_clock(receipt.get("read_observed_at"))
        if read_at is None or not available <= read_at <= cutoff:
            return "database read was not observed by the decision clock"
    if market.astimezone(_NY).date() != local.date() or receipt.get("observed_on") != local.date().isoformat():
        return "quote belongs to a different market session"
    if market.astimezone(_NY).time() < time(9, 30):
        return "quote predates the regular session"
    if cutoff - market > timedelta(seconds=QUOTE_MAX_AGE_SECONDS):
        return "quote is older than the 20-minute reference limit"
    return None


def load_quote_reference(conn: Any, ticker: str, *, as_of: datetime) -> dict[str, Any]:
    """Load the latest exact first-quote receipt visible by ``as_of``.

    The batch's registered_at is only a lower bound on availability, not its
    commit time. The live post-fetch read clock proves observation by the
    final decision; final admission must revalidate this returned receipt.
    A missing migration/lookup fails closed at the caller, never to daily spot.
    """
    receipt: dict[str, Any] = {
        "status": "unavailable", "policy": QUOTE_REFERENCE_POLICY,
        "ticker": ticker, "source": "options_capture_batches", "basis": "raw_spot",
        "evidence_class": "prospective_observed_reference", "executable": False,
        "max_age_seconds": QUOTE_MAX_AGE_SECONDS,
        "available_at_basis": "registration_lower_bound_not_commit",
    }
    cutoff = _aware_clock(as_of)
    if cutoff is None:
        return receipt | {"reason": "decision clock is missing or unzoned"}
    row = conn.execute(text("""
        SELECT spot_price, provider_regular_market_at, spot_provider,
               capture_batch_id, capture_source, capture_started_at,
               capture_completed_at, registered_at, snap_date, backfilled
        FROM options_capture_batches
        WHERE ticker = :ticker AND capture_completed_at <= :as_of
          AND registered_at <= :as_of
        ORDER BY capture_ordinal DESC LIMIT 1
    """), {"ticker": ticker, "as_of": cutoff}).fetchone()
    read_observed_at = datetime.now(timezone.utc)
    if row is None:
        return receipt | {"reason": "no timestamped options quote was available at decision time"}
    def iso(value: Any) -> Any:
        return value.isoformat() if isinstance(value, (date, datetime)) else value
    receipt.update({
        "status": "measured", "price": float(row[0]) if row[0] is not None else None,
        "market_at": iso(row[1]), "provider": row[2], "capture_batch_id": row[3],
        "capture_source": row[4], "capture_started_at": iso(row[5]),
        "capture_completed_at": iso(row[6]), "available_at": iso(row[7]),
        "registered_at": iso(row[7]), "read_observed_at": read_observed_at.isoformat(),
        "observed_on": iso(row[8]), "backfilled": bool(row[9]),
    })
    reason = validate_quote_reference(receipt, as_of=cutoff, ticker=ticker,
                                      _require_read_observation=False)
    if reason:
        receipt.update(status="unavailable", reason=reason)
    return receipt


def tracking_reference_return(
    conn: Any, *, ticker: str, direction: str, entry_price: Any,
    entry_receipt: Any, decision_at: Any, expiry: date | None,
    current_receipt: dict[str, Any], as_of: datetime,
) -> tuple[float | None, str]:
    """Gross signed reference move, with PIT split rebasing; never trade P&L."""
    reason = entry_price_pnl_basis(entry_price)
    if reason:
        return None, reason
    decision = _aware_clock(decision_at)
    if decision is None:
        return None, "unavailable: prediction decision clock is missing"
    reason = validate_quote_reference(entry_receipt, as_of=decision, ticker=ticker)
    if reason:
        return None, "unavailable: " + reason
    if float(entry_price) != float(entry_receipt["price"]):
        return None, "unavailable: entry price does not match its frozen quote"
    reason = validate_quote_reference(current_receipt, as_of=as_of, ticker=ticker)
    if reason:
        return None, "unavailable: " + reason
    current_at = _aware_clock(current_receipt["market_at"])
    if current_at <= decision or current_at <= _aware_clock(entry_receipt["market_at"]):
        return None, "unavailable: no quote observed after the forecast decision"
    current_day = current_at.astimezone(_NY).date()
    if expiry is None or current_day > expiry:
        return None, "unavailable: quote is beyond expiry or expiry is unknown; awaiting outcome scoring"
    if direction not in {"CALL", "LONG", "PUT", "SHORT"}:
        return None, "unavailable: prediction has no directional return contract"
    from store.observations import price_in_basis, read_splits

    entry_day = date.fromisoformat(entry_receipt["observed_on"])
    splits = read_splits(conn, ticker, start=entry_day, end=current_day, as_of_ts=as_of)
    # Options spots are raw; do not tag them as Yahoo adjusted daily closes.
    current = price_in_basis(current_receipt["price"], current_day,
                             basis_date=entry_day, splits=splits)
    if not is_divisible_entry_price(current):
        return None, "unavailable: split-adjusted reference is not positive and finite"
    move = (current - float(entry_price)) / float(entry_price) * 100
    return (move if direction in {"CALL", "LONG"} else -move), (
        "measured: gross signed observed-reference move, split-adjusted; "
        "delayed quotes, costs excluded, not an executable return"
    )
