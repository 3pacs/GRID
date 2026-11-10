"""Private, descriptive native-minute volume metrics over retained GRID revisions.

Pure reads, no fetching or writes. Calendar/session provenance comes from the
existing trusted calendar; selection and retention are shared with GRID intervals.
OHLCV-weighted HLC3 is an estimate, never exact trade VWAP or a trade signal.
"""

from datetime import timedelta
import math
import statistics
from ingestion.realtime.native_minutes import select_as_of
from ingestion.realtime.research_intervals import _utc, rollup


def calendar_window(calendar, session_key):
    """Resolve exactly ten preceding sessions, not ten nonempty data days."""
    current = calendar(session_key)
    return current, calendar.prior_sessions(session_key, 10)


def native_volume_view(
    rows,
    *,
    session,
    prior_sessions,
    symbol,
    source,
    price_basis,
    as_of,
    max_receipt_age=120,
    settlement_seconds=5,
):
    """Caller supplies exact calendar_window output, never data-selected sessions.

    Freshness is receipt age AND complete event coverage through the last eligible
    minute. Early-close baselines cannot supply later elapsed minutes; no rescale.
    Backfill retains its retrieval availability and cannot support historical PIT.
    """
    now = _utc(as_of)
    if (
        not source
        or not symbol
        or max_receipt_age <= 0
        or not 0 <= settlement_seconds < 60
    ):
        raise ValueError("invalid identity or freshness policy")
    prior = tuple(prior_sessions)
    if len(prior) != 10 or len({p.key for p in prior}) != 10:
        raise ValueError("exact ten expected prior sessions required")
    previous_close = None
    for p in (*prior, session):
        if p.calendar_version != session.calendar_version or not p.calendar_version:
            raise ValueError("calendar provenance mismatch")
        op, cl = _utc(p.opens), _utc(p.closes)
        if cl <= op or any(x.second or x.microsecond for x in (op, cl)):
            raise ValueError("invalid session bounds")
        if previous_close is not None and op <= previous_close:
            raise ValueError("sessions must be ordered and nonoverlapping")
        previous_close = cl
    data = tuple(rows)
    stop = min(_utc(session.closes), now - timedelta(seconds=settlement_seconds))
    count = max(0, int((stop - _utc(session.opens)).total_seconds() // 60))
    end = _utc(session.opens) + timedelta(minutes=count)
    result = dict(
        schema="grid.native-volume.v1",
        symbol=symbol,
        source=source,
        price_basis=price_basis,
        as_of=now,
        session=session.key,
        calendar_version=session.calendar_version,
        end_of_completed_window=end,
        latest_receipt_at=None,
        receipt_age_seconds=None,
        event_age_seconds=(now - end).total_seconds(),
        completed_minutes=count,
        status="unavailable",
        reason=None,
        cumulative_shares=None,
        bar_vwap_estimate=None,
        rvol=None,
        baseline_sessions=[p.key for p in prior],
        baseline_errors={},
        exact_trade_vwap=False,
        signal_confirmation=False,
        cross_source_reconciled=False,
        live=False,
        vwap_basis="Volume-weighted HLC3 of native one-minute OHLCV; not trade VWAP",
        rvol_basis="Cumulative shares / median same-elapsed shares of exact prior ten sessions",
    )
    if not count:
        result["reason"] = "no_completed_minute"
        return result

    def selected(p):
        selected = select_as_of(
            data,
            symbol=symbol,
            source=source,
            price_basis=price_basis,
            as_of=now,
            opens=p.opens,
            closes=p.closes,
        )
        # Reuse shared validation, including point-in-time provenance and OHLCV.
        checked = rollup(selected, p, "1m", as_of=now)
        return {b.start: b for b in checked}

    if (now - end).total_seconds() > max_receipt_age:
        result["reason"] = "event_stale"
        return result
    current = selected(session)
    expected = [_utc(session.opens) + timedelta(minutes=i) for i in range(count)]
    candles = [current.get(t) for t in expected]
    if any(b is None or not b.complete or b.volume is None for b in candles):
        result["reason"] = "current_session_gap"
        return result
    if any(
        type(b.volume) not in (int, float)
        or b.volume != int(b.volume)
        or b.volume > 2**53 - 1
        for b in candles
    ):
        raise ValueError("integer share volumes required")
    latest = candles[-1].last_source_available_at
    age = (now - latest).total_seconds()
    result.update(
        latest_receipt_at=latest,
        receipt_age_seconds=age,
        source_revisions=tuple(r for b in candles for r in b.source_revisions),
    )
    if not 0 <= age <= max_receipt_age:
        result["reason"] = "receipt_stale"
        return result
    volume = sum(b.volume for b in candles)
    result.update(status="degraded", cumulative_shares=volume)
    if volume:
        result["bar_vwap_estimate"] = (
            sum((b.high + b.low + b.close) / 3 * b.volume for b in candles) / volume
        )
    else:
        result["reason"] = "zero_volume_no_weighted_estimate"
    totals = []
    for p in prior:
        full_count = int((_utc(p.closes) - _utc(p.opens)).total_seconds() // 60)
        checked = selected(p)
        full = [
            checked.get(_utc(p.opens) + timedelta(minutes=i)) for i in range(full_count)
        ]
        if any(b is None or not b.complete or b.volume is None for b in full):
            result["baseline_errors"][p.key] = "baseline_session_gap"
        elif full_count < count:
            result["baseline_errors"][p.key] = (
                "early_close_insufficient_elapsed_minutes"
            )
        elif any(
            type(b.volume) not in (int, float)
            or b.volume != int(b.volume)
            or b.volume > 2**53 - 1
            for b in full
        ):
            result["baseline_errors"][p.key] = "invalid_share_volume"
        else:
            totals.append(sum(b.volume for b in full[:count]))
    if len(totals) == 10:
        median = statistics.median(totals)
        result["baseline_median_shares"] = median
        if median > 0 and math.isfinite(median):
            result["rvol"] = volume / median
            if volume > 0:
                result["status"] = "ready"
        else:
            result["baseline_errors"]["denominator"] = "zero_or_invalid"
    return result


def retained_volume_view(
    loader, calendar, *, session_key, symbol, source, price_basis, as_of, **policy
):
    """Read-only adapter for existing SQLite/JSONL loader; no network fallback."""
    session, prior = calendar_window(calendar, session_key)
    rows = []
    for s in (*prior, session):
        rows.extend(loader(symbol, source, s, as_of))
    return native_volume_view(
        rows,
        session=session,
        prior_sessions=prior,
        symbol=symbol,
        source=source,
        price_basis=price_basis,
        as_of=as_of,
        **policy,
    )
