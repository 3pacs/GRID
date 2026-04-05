"""
GRID Intelligence -- Pattern Detection Engine.

Finds recurring event sequences ("every time X happens, Y follows") and
tracks which patterns are currently in progress so you know what is about
to happen next.

Core workflow:
  1. discover_patterns     -- scan historical events, mine recurring sequences
  2. match_active_patterns -- which discovered patterns are in progress RIGHT NOW?
  3. score_pattern_accuracy -- backtest each pattern and promote / kill it
  4. get_patterns_for_ticker -- patterns observed or active for a single ticker

Storage:
  event_patterns table (see _ensure_tables).

Wired into the API via:
  GET  /api/v1/intelligence/patterns
  GET  /api/v1/intelligence/patterns/active
  GET  /api/v1/intelligence/patterns/{ticker}
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass, asdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ---------------------------------------------------------------------------
# Data class
# ---------------------------------------------------------------------------

@dataclass
class Pattern:
    """A discovered recurring event sequence."""

    id: str
    sequence: list[str]        # e.g. ["insider:bearish", "dark_pool:bearish", "price_move:bearish"]
    occurrences: int
    hit_rate: float            # what % of the time does the full sequence complete?
    avg_lead_time_hours: float
    avg_return_after: float
    tickers_seen: list[str]
    first_seen: str
    last_seen: str
    confidence: float
    actionable: bool           # is this tradeable?
    description: str           # human-readable

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Max gap between consecutive events in a sequence to consider them linked
PATTERN_WINDOW_HOURS: float = 168.0  # 7 days

# Lookback for historical scan
SCAN_LOOKBACK_DAYS: int = 365

# Minimum hit rate for a pattern to survive discovery filtering
MIN_HIT_RATE: float = 0.50

# Accuracy thresholds for promote / kill
PROMOTE_ACCURACY: float = 0.70
KILL_ACCURACY: float = 0.40

# Active pattern matching: how many days back to look for "step 1"
ACTIVE_LOOKBACK_DAYS: int = 14

# Price return window after a pattern completes (trading days)
RETURN_WINDOW_DAYS: int = 5


# ---------------------------------------------------------------------------
# Table setup
# ---------------------------------------------------------------------------

_tables_ensured = False


def _ensure_tables(engine: Engine) -> None:
    global _tables_ensured
    if _tables_ensured:
        return
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS event_patterns (
                id TEXT PRIMARY KEY,
                sequence JSONB NOT NULL,
                occurrences INT DEFAULT 0,
                hit_rate NUMERIC,
                avg_lead_time_hours NUMERIC,
                avg_return_after NUMERIC,
                tickers_seen JSONB,
                confidence NUMERIC,
                actionable BOOLEAN DEFAULT FALSE,
                description TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_event_patterns_confidence
                ON event_patterns (confidence DESC)
        """))
    _tables_ensured = True


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _safe_float(val: Any) -> float:
    if val is None:
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def _event_key(event: Any) -> str:
    """Produce a normalised key like 'insider:bearish' from an Event."""
    etype = getattr(event, "event_type", "") or ""
    direction = getattr(event, "direction", "") or ""
    return f"{etype}:{direction}"


def _parse_event_ts(event: Any) -> datetime | None:
    ts = getattr(event, "timestamp", None)
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


def _pattern_id(sequence: list[str]) -> str:
    """Deterministic ID for a sequence so the same sequence always maps to
    the same row in event_patterns."""
    raw = "->".join(sequence)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _human_description(sequence: list[str], hit_rate: float, avg_return: float) -> str:
    """Generate a readable description for a pattern."""
    step_labels = []
    for step in sequence:
        parts = step.split(":")
        etype = parts[0] if parts else step
        direction = parts[1] if len(parts) > 1 else ""
        label = etype.replace("_", " ")
        if direction and direction != "neutral":
            label += f" ({direction})"
        step_labels.append(label)

    chain = " -> ".join(step_labels)
    outcome = "gain" if avg_return > 0 else "loss"
    return (
        f"When {chain} occurs in sequence, "
        f"the pattern completes {hit_rate:.0%} of the time "
        f"with avg {abs(avg_return):.2%} {outcome} within {RETURN_WINDOW_DAYS}d."
    )


def _get_watchlist_tickers(engine: Engine) -> list[str]:
    """Return active watchlist tickers."""
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT DISTINCT ticker FROM watchlist WHERE active = TRUE LIMIT 100"
            )).fetchall()
            return [r[0] for r in rows] if rows else []
    except Exception:
        return []


def _get_price_after(
    engine: Engine, ticker: str, after_date: datetime, days: int = RETURN_WINDOW_DAYS,
) -> float | None:
    """Get the price return in the N days after a given date.

    Returns fractional return (e.g. 0.03 for +3%) or None if data missing.
    """
    start = after_date.date() if isinstance(after_date, datetime) else after_date
    end = start + timedelta(days=days + 5)  # extra buffer for weekends

    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT signal_date, spot_price
                FROM options_daily_signals
                WHERE ticker = :t
                  AND signal_date >= :s
                  AND signal_date <= :e
                  AND spot_price > 0
                ORDER BY signal_date
                LIMIT 20
            """), {"t": ticker, "s": start, "e": end}).fetchall()

            if not rows:
                rows = conn.execute(text("""
                    SELECT obs_date, value
                    FROM raw_series
                    WHERE series_id = :sid
                      AND obs_date >= :s
                      AND obs_date <= :e
                      AND pull_status = 'SUCCESS'
                    ORDER BY obs_date
                    LIMIT 20
                """), {"sid": f"YF:{ticker}:close", "s": start, "e": end}).fetchall()

            if len(rows) < 2:
                return None

            p0 = float(rows[0][1])
            p1 = float(rows[-1][1])
            if p0 <= 0:
                return None
            return (p1 - p0) / p0
    except Exception:
        return None


# ---------------------------------------------------------------------------
# 1. discover_patterns
# ---------------------------------------------------------------------------

def discover_patterns(
    engine: Engine,
    min_occurrences: int = 3,
    max_sequence_length: int = 4,
) -> list[Pattern]:
    """Scan all event sequences across watchlist tickers to find recurring
    2-, 3-, and 4-event sequences.

    Only returns patterns with hit_rate > 50%.
    Sorted by confidence * abs(avg_return_after) descending.

    Parameters
    ----------
    engine : SQLAlchemy engine
    min_occurrences : minimum times a full sequence must appear
    max_sequence_length : longest sequence to search for (2, 3, or 4)

    Returns
    -------
    List of Pattern dataclasses.
    """
    from intelligence.event_sequence import build_sequence

    _ensure_tables(engine)
    tickers = _get_watchlist_tickers(engine)

    if not tickers:
        # Fallback: grab tickers from signal_sources
        try:
            with engine.connect() as conn:
                cutoff = datetime.now(timezone.utc) - timedelta(days=SCAN_LOOKBACK_DAYS)
                rows = conn.execute(text(
                    "SELECT DISTINCT ticker FROM signal_sources "
                    "WHERE signal_date >= :c LIMIT 100"
                ), {"c": cutoff}).fetchall()
                tickers = [r[0] for r in rows] if rows else []
        except Exception:
            tickers = []

    if not tickers:
        log.info("No tickers available for pattern discovery")
        return []

    log.info(
        "Pattern discovery: scanning {n} tickers, seq len 2-{m}, min occ {o}",
        n=len(tickers), m=max_sequence_length, o=min_occurrences,
    )

    # Build event timelines per ticker
    events_by_ticker: dict[str, list] = {}
    for ticker in tickers:
        try:
            seq = build_sequence(engine, ticker, days=SCAN_LOOKBACK_DAYS)
            if seq:
                events_by_ticker[ticker] = seq
        except Exception as exc:
            log.debug("Sequence build failed for {t}: {e}", t=ticker, e=str(exc))

    if not events_by_ticker:
        return []

    # ── Mine n-grams (2 through max_sequence_length) ─────────────────
    # ngram_key -> list of occurrence dicts
    ngram_occurrences: dict[str, list[dict]] = defaultdict(list)

    for ticker, events in events_by_ticker.items():
        # Pre-compute keys and timestamps
        keyed: list[tuple[str, datetime | None]] = []
        for ev in events:
            keyed.append((_event_key(ev), _parse_event_ts(ev)))

        n_events = len(keyed)
        for i in range(n_events):
            key_i, dt_i = keyed[i]
            if dt_i is None:
                continue

            for seq_len in range(2, min(max_sequence_length, n_events - i) + 1):
                steps: list[str] = [key_i]
                timestamps: list[datetime] = [dt_i]
                valid = True

                for offset in range(1, seq_len):
                    j = i + offset
                    if j >= n_events:
                        valid = False
                        break
                    key_j, dt_j = keyed[j]
                    if dt_j is None:
                        valid = False
                        break
                    gap = (dt_j - timestamps[-1]).total_seconds() / 3600.0
                    if gap < 0 or gap > PATTERN_WINDOW_HOURS:
                        valid = False
                        break
                    steps.append(key_j)
                    timestamps.append(dt_j)

                if not valid or len(steps) != seq_len:
                    continue

                # Skip boring sequences (all same event)
                if len(set(steps)) == 1:
                    continue

                ngram_key = "->".join(steps)
                ngram_occurrences[ngram_key].append({
                    "ticker": ticker,
                    "first_ts": timestamps[0].isoformat(),
                    "last_ts": timestamps[-1].isoformat(),
                    "total_gap_hours": (timestamps[-1] - timestamps[0]).total_seconds() / 3600.0,
                })

    # ── Compute hit rate and return for each candidate ───────────────
    # A pattern "hits" when the final step in the sequence is a price_move
    # or when a significant move occurs within RETURN_WINDOW_DAYS after
    # the sequence completes.

    patterns: list[Pattern] = []

    for ngram_key, occurrences in ngram_occurrences.items():
        if len(occurrences) < min_occurrences:
            continue

        steps = ngram_key.split("->")
        last_step = steps[-1]

        # Deduplicate occurrences (same ticker + same day = 1 occurrence)
        seen_dedup: set[str] = set()
        unique_occs: list[dict] = []
        for occ in occurrences:
            dedup_key = f"{occ['ticker']}:{occ['first_ts'][:10]}"
            if dedup_key not in seen_dedup:
                seen_dedup.add(dedup_key)
                unique_occs.append(occ)

        if len(unique_occs) < min_occurrences:
            continue

        # Compute hit rate and return
        hit_count = 0
        returns: list[float] = []
        lead_times: list[float] = []

        for occ in unique_occs:
            ticker = occ["ticker"]
            try:
                last_ts = datetime.fromisoformat(occ["last_ts"])
            except (ValueError, TypeError):
                continue

            # Check if a meaningful price move followed
            ret = _get_price_after(engine, ticker, last_ts, days=RETURN_WINDOW_DAYS)
            if ret is not None:
                returns.append(ret)

                # Determine if the pattern "direction" was confirmed
                last_direction = last_step.split(":")[-1] if ":" in last_step else ""
                if last_direction == "bearish" and ret < -0.005:
                    hit_count += 1
                elif last_direction == "bullish" and ret > 0.005:
                    hit_count += 1
                elif "price_move" in last_step:
                    # The pattern already ends with a price move -- that IS the hit
                    hit_count += 1
                elif abs(ret) > 0.01:
                    # Any significant move counts as a hit for neutral sequences
                    hit_count += 1

            lead_times.append(occ.get("total_gap_hours", 0.0))

        total_scored = max(len(returns), 1)
        hit_rate = hit_count / total_scored if total_scored > 0 else 0.0

        if hit_rate < MIN_HIT_RATE:
            continue

        avg_return = sum(returns) / len(returns) if returns else 0.0
        avg_lead = sum(lead_times) / len(lead_times) if lead_times else 0.0

        unique_tickers = sorted(set(occ["ticker"] for occ in unique_occs))
        ts_list = [occ["first_ts"] for occ in unique_occs if occ.get("first_ts")]
        first_seen = min(ts_list) if ts_list else ""
        last_seen = max(ts_list) if ts_list else ""

        # Confidence: combination of occurrences, hit rate, ticker breadth
        occ_score = min(len(unique_occs) / 20.0, 0.4)
        hit_score = hit_rate * 0.35
        breadth_score = min(len(unique_tickers) / 10.0, 0.15)
        return_score = min(abs(avg_return) * 10, 0.1)
        confidence = min(occ_score + hit_score + breadth_score + return_score, 1.0)

        actionable = (
            hit_rate >= 0.55
            and abs(avg_return) >= 0.005
            and len(unique_occs) >= max(min_occurrences, 3)
        )

        pat = Pattern(
            id=_pattern_id(steps),
            sequence=steps,
            occurrences=len(unique_occs),
            hit_rate=round(hit_rate, 4),
            avg_lead_time_hours=round(avg_lead, 2),
            avg_return_after=round(avg_return, 6),
            tickers_seen=unique_tickers[:30],
            first_seen=first_seen,
            last_seen=last_seen,
            confidence=round(confidence, 4),
            actionable=actionable,
            description=_human_description(steps, hit_rate, avg_return),
        )
        patterns.append(pat)

    # Sort by confidence * abs(return) -- the money sort
    patterns.sort(key=lambda p: -(p.confidence * abs(p.avg_return_after or 0.0001)))

    # Persist
    _store_patterns(engine, patterns)

    log.info(
        "Pattern discovery complete: {n} patterns found ({a} actionable)",
        n=len(patterns),
        a=sum(1 for p in patterns if p.actionable),
    )
    return patterns


# ---------------------------------------------------------------------------
# 2. match_active_patterns
# ---------------------------------------------------------------------------

def match_active_patterns(engine: Engine) -> list[dict[str, Any]]:
    """Check which discovered patterns are currently IN PROGRESS.

    For each stored pattern, look at the recent events for every ticker and
    see if we have matched some but not all steps.  Returns a list of dicts
    describing partial matches and what step comes next.

    This is the prediction engine -- it tells you what is about to happen.
    """
    from intelligence.event_sequence import build_sequence

    _ensure_tables(engine)

    # Load stored patterns
    stored = _load_patterns(engine)
    if not stored:
        log.info("No stored patterns to match against")
        return []

    tickers = _get_watchlist_tickers(engine)
    if not tickers:
        return []

    active: list[dict[str, Any]] = []

    for ticker in tickers:
        try:
            events = build_sequence(engine, ticker, days=ACTIVE_LOOKBACK_DAYS)
        except Exception as exc:
            log.warning("Failed to build sequence for {t}: {e}", t=ticker, e=exc)
            continue
        if not events:
            continue

        # Build keyed timeline for this ticker
        keyed: list[tuple[str, datetime | None]] = []
        for ev in events:
            keyed.append((_event_key(ev), _parse_event_ts(ev)))

        for pat in stored:
            seq = pat.sequence
            seq_len = len(seq)
            if seq_len < 2:
                continue

            # Try to match the first N-1 steps (partial match)
            # Walk backwards through events to find the most recent match start
            best_match_len = 0
            best_match_start_ts: str | None = None
            best_match_last_ts: str | None = None

            for start_idx in range(len(keyed)):
                key_s, dt_s = keyed[start_idx]
                if dt_s is None:
                    continue
                if key_s != seq[0]:
                    continue

                # Try to extend from here
                matched = 1
                last_dt = dt_s
                for step_offset in range(1, seq_len):
                    # Search forward for the next matching event
                    found = False
                    for scan_idx in range(start_idx + step_offset, len(keyed)):
                        key_scan, dt_scan = keyed[scan_idx]
                        if dt_scan is None:
                            continue
                        gap = (dt_scan - last_dt).total_seconds() / 3600.0
                        if gap > PATTERN_WINDOW_HOURS:
                            break
                        if gap < 0:
                            continue
                        if key_scan == seq[step_offset]:
                            matched += 1
                            last_dt = dt_scan
                            found = True
                            break
                    if not found:
                        break

                if matched > best_match_len and matched < seq_len:
                    best_match_len = matched
                    best_match_start_ts = dt_s.isoformat()
                    best_match_last_ts = last_dt.isoformat()

            # Report partial matches where we have matched at least 1 step
            # but have NOT completed the full sequence
            if best_match_len >= 1 and best_match_len < seq_len:
                steps_remaining = seq_len - best_match_len
                next_step = seq[best_match_len]
                next_parts = next_step.split(":")
                next_type = next_parts[0].replace("_", " ")
                next_dir = next_parts[1] if len(next_parts) > 1 else ""

                # Estimate when the next step might occur based on avg lead time
                avg_step_hours = pat.avg_lead_time_hours / max(seq_len - 1, 1)
                est_hours_remaining = avg_step_hours * steps_remaining

                active.append({
                    "pattern_id": pat.id,
                    "pattern_sequence": pat.sequence,
                    "ticker": ticker,
                    "steps_completed": best_match_len,
                    "steps_total": seq_len,
                    "steps_remaining": steps_remaining,
                    "next_expected_step": next_step,
                    "next_step_description": (
                        f"{next_type} ({next_dir})" if next_dir else next_type
                    ),
                    "match_started": best_match_start_ts,
                    "last_matched": best_match_last_ts,
                    "estimated_hours_to_completion": round(est_hours_remaining, 1),
                    "pattern_hit_rate": pat.hit_rate,
                    "pattern_avg_return": pat.avg_return_after,
                    "pattern_confidence": pat.confidence,
                    "actionable": pat.actionable,
                    "description": pat.description,
                    "alert": (
                        f"Pattern {best_match_len}/{seq_len} complete for {ticker}. "
                        f"Next: {next_step}. "
                        f"Expected within ~{est_hours_remaining:.0f}h. "
                        f"Historical hit rate: {pat.hit_rate:.0%}."
                    ),
                })

    # Sort: most complete patterns first, then by confidence
    active.sort(key=lambda a: (
        -a["steps_completed"] / a["steps_total"],
        -a["pattern_confidence"],
    ))

    log.info("Active pattern matching: {n} partial matches found", n=len(active))
    return active


# ---------------------------------------------------------------------------
# 3. score_pattern_accuracy
# ---------------------------------------------------------------------------

def score_pattern_accuracy(engine: Engine) -> dict[str, Any]:
    """Backtest every stored pattern to verify it still works.

    - Patterns with accuracy >= 70% are promoted (confidence boosted).
    - Patterns with accuracy < 40% are killed (removed from storage).

    Returns a summary dict with promoted, killed, and unchanged counts.
    """
    from intelligence.event_sequence import build_sequence

    _ensure_tables(engine)
    stored = _load_patterns(engine)
    if not stored:
        return {"promoted": 0, "killed": 0, "unchanged": 0, "details": []}

    tickers = _get_watchlist_tickers(engine)
    if not tickers:
        return {"promoted": 0, "killed": 0, "unchanged": 0, "details": []}

    promoted = 0
    killed = 0
    unchanged = 0
    details: list[dict] = []

    # Build recent events per ticker (last 90 days for scoring)
    events_by_ticker: dict[str, list] = {}
    for ticker in tickers:
        try:
            seq = build_sequence(engine, ticker, days=90)
            if seq:
                events_by_ticker[ticker] = seq
        except Exception as exc:
            log.warning("Failed to build sequence for {t} in pattern scan: {e}", t=ticker, e=exc)
            continue

    for pat in stored:
        seq = pat.sequence
        seq_len = len(seq)
        if seq_len < 2:
            continue

        total_tests = 0
        correct = 0

        for ticker, events in events_by_ticker.items():
            keyed = [(_event_key(ev), _parse_event_ts(ev)) for ev in events]
            n_events = len(keyed)

            for i in range(n_events):
                key_i, dt_i = keyed[i]
                if dt_i is None or key_i != seq[0]:
                    continue

                # Try to match full sequence
                matched = 1
                last_dt = dt_i
                for step_offset in range(1, seq_len):
                    found = False
                    for j in range(i + step_offset, n_events):
                        key_j, dt_j = keyed[j]
                        if dt_j is None:
                            continue
                        gap = (dt_j - last_dt).total_seconds() / 3600.0
                        if gap > PATTERN_WINDOW_HOURS:
                            break
                        if gap < 0:
                            continue
                        if key_j == seq[step_offset]:
                            matched += 1
                            last_dt = dt_j
                            found = True
                            break
                    if not found:
                        break

                if matched == seq_len:
                    total_tests += 1
                    # Check outcome
                    ret = _get_price_after(engine, ticker, last_dt, days=RETURN_WINDOW_DAYS)
                    if ret is not None:
                        last_direction = seq[-1].split(":")[-1] if ":" in seq[-1] else ""
                        if last_direction == "bearish" and ret < -0.005:
                            correct += 1
                        elif last_direction == "bullish" and ret > 0.005:
                            correct += 1
                        elif "price_move" in seq[-1]:
                            correct += 1
                        elif abs(ret) > 0.01:
                            correct += 1

        accuracy = correct / total_tests if total_tests > 0 else pat.hit_rate
        status = "unchanged"

        if total_tests >= 3:
            if accuracy >= PROMOTE_ACCURACY:
                # Promote: boost confidence
                pat.confidence = min(pat.confidence * 1.15, 1.0)
                pat.hit_rate = round(accuracy, 4)
                status = "promoted"
                promoted += 1
            elif accuracy < KILL_ACCURACY:
                # Kill: remove from storage
                _delete_pattern(engine, pat.id)
                status = "killed"
                killed += 1
            else:
                pat.hit_rate = round(accuracy, 4)
                unchanged += 1
        else:
            unchanged += 1

        details.append({
            "pattern_id": pat.id,
            "sequence": pat.sequence,
            "total_tests": total_tests,
            "correct": correct,
            "accuracy": round(accuracy, 4),
            "status": status,
        })

    # Re-persist promoted / updated patterns
    surviving = [p for p in stored if not any(
        d["pattern_id"] == p.id and d["status"] == "killed" for d in details
    )]
    _store_patterns(engine, surviving)

    result = {
        "promoted": promoted,
        "killed": killed,
        "unchanged": unchanged,
        "total_patterns": len(stored),
        "details": details,
    }
    log.info(
        "Pattern accuracy scoring: {p} promoted, {k} killed, {u} unchanged",
        p=promoted, k=killed, u=unchanged,
    )
    return result


# ---------------------------------------------------------------------------
# 4. get_patterns_for_ticker
# ---------------------------------------------------------------------------

def get_patterns_for_ticker(engine: Engine, ticker: str) -> list[dict[str, Any]]:
    """Return all patterns observed for a specific ticker, plus any that are
    currently in progress.

    Parameters
    ----------
    engine : SQLAlchemy engine
    ticker : Stock ticker symbol

    Returns
    -------
    List of dicts with pattern info and active match status.
    """
    _ensure_tables(engine)
    ticker = ticker.upper()

    stored = _load_patterns(engine)
    if not stored:
        return []

    # Filter to patterns where this ticker has been seen
    ticker_patterns: list[dict[str, Any]] = []
    for pat in stored:
        if ticker in pat.tickers_seen:
            ticker_patterns.append({
                **pat.to_dict(),
                "active_match": None,
            })

    # Check for active (in-progress) matches
    try:
        all_active = match_active_patterns(engine)
        for am in all_active:
            if am["ticker"] != ticker:
                continue
            # See if we already have this pattern in our list
            found = False
            for tp in ticker_patterns:
                if tp["id"] == am["pattern_id"]:
                    tp["active_match"] = am
                    found = True
                    break
            if not found:
                # Pattern wasn't in ticker history but is matching now
                for pat in stored:
                    if pat.id == am["pattern_id"]:
                        ticker_patterns.append({
                            **pat.to_dict(),
                            "active_match": am,
                        })
                        break
    except Exception as exc:
        log.debug("Active pattern matching for {t} failed: {e}", t=ticker, e=str(exc))

    ticker_patterns.sort(key=lambda p: -(p.get("confidence", 0) or 0))
    return ticker_patterns


# ---------------------------------------------------------------------------
# Storage helpers
# ---------------------------------------------------------------------------

def _store_patterns(engine: Engine, patterns: list[Pattern]) -> None:
    """Upsert patterns into event_patterns table."""
    if not patterns:
        return
    _ensure_tables(engine)
    with engine.begin() as conn:
        for pat in patterns:
            conn.execute(text("""
                INSERT INTO event_patterns
                    (id, sequence, occurrences, hit_rate, avg_lead_time_hours,
                     avg_return_after, tickers_seen, confidence, actionable,
                     description, updated_at)
                VALUES
                    (:id, :sequence, :occurrences, :hit_rate, :avg_lead_time_hours,
                     :avg_return_after, :tickers_seen, :confidence, :actionable,
                     :description, NOW())
                ON CONFLICT (id) DO UPDATE SET
                    occurrences = EXCLUDED.occurrences,
                    hit_rate = EXCLUDED.hit_rate,
                    avg_lead_time_hours = EXCLUDED.avg_lead_time_hours,
                    avg_return_after = EXCLUDED.avg_return_after,
                    tickers_seen = EXCLUDED.tickers_seen,
                    confidence = EXCLUDED.confidence,
                    actionable = EXCLUDED.actionable,
                    description = EXCLUDED.description,
                    updated_at = NOW()
            """), {
                "id": pat.id,
                "sequence": json.dumps(pat.sequence),
                "occurrences": pat.occurrences,
                "hit_rate": pat.hit_rate,
                "avg_lead_time_hours": pat.avg_lead_time_hours,
                "avg_return_after": pat.avg_return_after,
                "tickers_seen": json.dumps(pat.tickers_seen),
                "confidence": pat.confidence,
                "actionable": pat.actionable,
                "description": pat.description,
            })


def _load_patterns(engine: Engine) -> list[Pattern]:
    """Load all stored patterns from the database."""
    _ensure_tables(engine)
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT id, sequence, occurrences, hit_rate, avg_lead_time_hours,
                       avg_return_after, tickers_seen, confidence, actionable,
                       description, created_at, updated_at
                FROM event_patterns
                ORDER BY confidence DESC
            """)).fetchall()

        patterns: list[Pattern] = []
        for r in rows:
            seq = r[1] if isinstance(r[1], list) else json.loads(r[1] or "[]")
            tickers = r[6] if isinstance(r[6], list) else json.loads(r[6] or "[]")
            patterns.append(Pattern(
                id=r[0],
                sequence=seq,
                occurrences=r[2] or 0,
                hit_rate=_safe_float(r[3]),
                avg_lead_time_hours=_safe_float(r[4]),
                avg_return_after=_safe_float(r[5]),
                tickers_seen=tickers,
                first_seen=str(r[10]) if r[10] else "",
                last_seen=str(r[11]) if r[11] else "",
                confidence=_safe_float(r[7]),
                actionable=bool(r[8]),
                description=r[9] or "",
            ))
        return patterns
    except Exception as exc:
        log.debug("Failed to load patterns: {e}", e=str(exc))
        return []


def _delete_pattern(engine: Engine, pattern_id: str) -> None:
    """Remove a pattern from storage (it stopped working)."""
    try:
        with engine.begin() as conn:
            conn.execute(text(
                "DELETE FROM event_patterns WHERE id = :id"
            ), {"id": pattern_id})
    except Exception as exc:
        log.debug("Failed to delete pattern {id}: {e}", id=pattern_id, e=str(exc))
