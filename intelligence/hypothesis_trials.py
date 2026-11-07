"""
GRID Intelligence — honest scoring of discovered hypotheses (one-look trials).

Replaces the v1 scorer that ``ACTIVE_HYPOTHESIS_SCORING_HELD`` has held since
2026-09-26. v1 had no trial ledger, no multiple-testing control and no holdout:

* lead-lag hypotheses were "confirmed" by re-scanning the *whole* history (the
  same data that generated them) for the best of ``lag + 5`` lags at an
  uncorrected p < 0.05;
* category watches were "confirmed" when GRID wrote more than five
  ``analytical_snapshots`` rows (its own telemetry) in the window, and actor
  watches when an actor had more than three ``signal_data`` rows;
* convergence hypotheses read ``oracle_predictions.actual_move_pct`` (GRID's own
  prediction rows, mostly ``no_data`` before #976) and took the first of five
  rows that moved 2%.

v2 (this module) issues a verdict only as a declared trial, once:

1. **Eligibility.** A hypothesis whose outcome or leading signal is GRID's own
   output (``snap:*``, ``llm_task_*``, oracle/hypothesis/pipeline series — see
   :func:`is_self_generated`) is ``ineligible``. One that declares no market
   outcome (actor watches; category watches, whose outcome is
   ``analytical_snapshots`` row counts) is ``untestable``. Both are recorded as
   trials with p = 1 and can never confirm.
2. **Point-in-time holdout.** Only data dated after ``created_at`` is tested:
   convergence hypotheses on completed raw closes
   (:func:`store.observations.read_completed_closes`, the #980 reader; entry at
   the first close after creation, exit at the last close inside the declared
   window, both known at ``evaluated_at``) against the ticker's own
   pre-creation distribution of same-horizon excess returns over SPY;
   lead-lag hypotheses at their *declared* lag and sign on the calendar days
   after creation, against a block-permutation null
   (:mod:`analysis.offline_research_proof`).
3. **One look.** A hypothesis enters a batch only when its window has closed
   and its data is settled; its verdict is final. The S11 ledger refuses a
   second test of the same hypothesis.
4. **Trial ledger + FDR.** Every batch is appended to the S11 ledger
   (:func:`analysis.ledger_steered_exploration.record_hypothesis_batch`) before
   any row changes. The ledger computes Benjamini-Hochberg q-values over every
   declared trial of the batch (ineligible/untestable at p = 1) at the ledger's
   frozen level ``q``, and its trials count toward the deflated-Sharpe trial
   count of the S11 overfitting gate.

Pending hypotheses (window still open, or closes not yet captured but within
twice the window) are skipped without a record: data availability is not an
outcome. Past twice the window with no data, they are recorded ``untestable``.

The S11 ledger file lives at ``HYPOTHESIS_TRIAL_LEDGER_PATH`` with its anchor
at ``HYPOTHESIS_TRIAL_LEDGER_ANCHOR_PATH``. Without both, the batch scorer
writes nothing. ``dry_run=True`` touches neither the ledger nor the DB (it
records into an in-memory ledger and returns the would-be verdicts).
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from datetime import time as dtime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
from sqlalchemy import text
from sqlalchemy.engine import Engine

from analysis.offline_research_proof import (
    autocorrelation_block,
    block_permutations,
)
from analysis.offline_research_proof import (
    excluded as _research_excluded,
)

SCORER_VERSION = "hypothesis-trials/2"

ET = ZoneInfo("America/New_York")
US_CLOSE = dtime(16, 0)

# Convergence (ticker-move) test.
MARKET_TICKER = "SPY"
DEFAULT_WINDOW_DAYS = 14
DEFAULT_MIN_MOVE_PCT = 2.0
NULL_LOOKBACK_DAYS = 365 * 5 + 30     # calendar days of pre-creation closes read for the null
MIN_NULL_SAMPLES = 250                # same-horizon null returns needed for a p-value
EDGE_TOLERANCE_DAYS = 4               # entry/exit close may sit this far inside the window edge

# Lead-lag test.
LEAD_LAG_HOLDOUT_DAYS = 90            # calendar days after creation that form the holdout
LEAD_LAG_SETTLE_DAYS = 3              # wait for late-arriving signal rows before the one look
LEAD_LAG_MIN_PAIRS = 30
LEAD_LAG_PERMS = 9999
LEAD_LAG_SEED = 20261106

# Outcome-side tokens that mark a series as GRID's own output. The S11 research
# contract's exclusion (snap:, llm, telemetry, pipeline, astro) is the base.
SELF_GENERATED_TOKENS = (
    "oracle", "hypothes", "prediction", "conviction", "sleuth", "agent",
    "hermes", "heartbeat", "self_", "grid_",
)

INELIGIBLE = "ineligible"
UNTESTABLE = "untestable"
TESTED = "tested"
TRIAL_STATUSES = (TESTED, UNTESTABLE, INELIGIBLE)

# Reasons recorded on ineligible/untestable trials (also kill_reason values).
SELF_TELEMETRY = "SELF_TELEMETRY"
NO_MARKET_OUTCOME = "NO_MARKET_OUTCOME"
NO_DIRECTION = "NO_DIRECTION"
UNSUPPORTED_INSTRUMENT = "UNSUPPORTED_INSTRUMENT"
NO_DATA = "NO_DATA"
INSUFFICIENT_HISTORY = "INSUFFICIENT_HISTORY"
NOT_SIGNIFICANT = "NOT_SIGNIFICANT"


class TrialLedgerNotConfigured(RuntimeError):
    """The S11 trial ledger path/anchor are not configured: no verdicts may be written."""


# ── Eligibility ──────────────────────────────────────────────────────────────


def is_self_generated(signal_key: str | None) -> bool:
    """True when a signal key names GRID's own output rather than the market.

    ``snap:*`` is every ``analytical_snapshots`` category (GRID writes them),
    ``llm_task_*`` its LLM task telemetry; oracle/hypothesis/prediction series
    are GRID's models. Unknown keys are treated as external.
    """
    if not signal_key:
        return False
    key = str(signal_key).lower()
    return _research_excluded(key) or any(token in key for token in SELF_GENERATED_TOKENS)


def normalize_direction(direction: str | None) -> str:
    d = (direction or "").strip().lower()
    if d in ("up", "bullish", "long", "call", "increase", "increases", "rising"):
        return "up"
    if d in ("down", "bearish", "short", "put", "decrease", "decreases", "falling"):
        return "down"
    return "ambiguous"


def _as_dict(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _aware(ts: datetime) -> datetime:
    return ts if ts.tzinfo is not None else ts.replace(tzinfo=timezone.utc)


@dataclass(frozen=True)
class TrialPlan:
    """What one hypothesis is tested for, decided from its row alone (no data)."""

    hypothesis_id: str
    pattern_type: str
    created_at: datetime
    test_kind: str                      # ticker_move | lead_lag | none
    reason: str | None = None           # set for ineligible/untestable plans
    status: str | None = None           # INELIGIBLE / UNTESTABLE for plans that need no data
    params: dict = field(default_factory=dict)
    prior_looks: int = 0                # v1 scorer passes over this row (times_tested)

    @property
    def identity_sha256(self) -> str:
        payload = json.dumps(
            {"kind": self.test_kind, "params": self.params, "created_at": self.created_at.isoformat()},
            sort_keys=True, separators=(",", ":"), default=str,
        )
        return hashlib.sha256(payload.encode()).hexdigest()

    def matures_at(self) -> datetime:
        """Earliest instant the one look may happen."""
        if self.test_kind == "ticker_move":
            exit_day = self.created_at.astimezone(ET).date() + timedelta(days=self.params["window_days"])
            return datetime.combine(exit_day + timedelta(days=1), dtime(0, 0), ET).astimezone(timezone.utc)
        if self.test_kind == "lead_lag":
            return self.created_at + timedelta(days=LEAD_LAG_HOLDOUT_DAYS + LEAD_LAG_SETTLE_DAYS)
        return self.created_at

    def expires_at(self) -> datetime:
        """After this, a test still missing its data is recorded untestable (NO_DATA)."""
        span = self.matures_at() - self.created_at
        return self.created_at + 2 * span + timedelta(days=1)


def plan_trial(
    hypothesis_id: str,
    pattern_type: str | None,
    test_criteria: Any,
    created_at: datetime,
    times_tested: int | None = 0,
    thesis_direction: str | None = None,
) -> TrialPlan:
    """Classify a hypothesis row into its declared test. Reads no data.

    ``thesis_direction``: for an antithesis, its thesis' ``expected_direction``.
    ``_make_antithesis`` had no flip for CALL/PUT, so those antitheses carry
    ``"opposite"``; their claim is the thesis' direction negated.
    """
    criteria = _as_dict(test_criteria)
    created = _aware(created_at)
    ptype = pattern_type or ""
    base = {"hypothesis_id": hypothesis_id, "pattern_type": ptype, "created_at": created,
            "prior_looks": int(times_tested or 0)}

    ticker = criteria.get("ticker")
    if ticker and "expected_direction" in criteria:
        raw = criteria.get("expected_direction")
        direction = normalize_direction(raw)
        if str(raw or "").strip().lower() == "opposite":
            direction = {"up": "down", "down": "up"}.get(normalize_direction(thesis_direction),
                                                         "ambiguous")
        params = {
            "ticker": str(ticker).upper(),
            "direction": direction,
            "window_days": int(criteria.get("window_days") or DEFAULT_WINDOW_DAYS),
            "min_move_pct": float(criteria.get("min_move_pct") or DEFAULT_MIN_MOVE_PCT),
        }
        if direction == "ambiguous":
            return TrialPlan(**base, test_kind="ticker_move", status=UNTESTABLE,
                             reason=NO_DIRECTION, params=params)
        return TrialPlan(**base, test_kind="ticker_move", params=params)

    watch, expect = criteria.get("watch_signal"), criteria.get("expect_signal")
    if watch and expect:
        params = {
            "watch_signal": str(watch),
            "expect_signal": str(expect),
            "lag_days": int(criteria.get("lag_days") or 1),
            "direction": normalize_direction(criteria.get("expected_direction", "increases")),
        }
        if is_self_generated(watch) or is_self_generated(expect):
            return TrialPlan(**base, test_kind="lead_lag", status=INELIGIBLE,
                             reason=SELF_TELEMETRY, params=params)
        if params["direction"] == "ambiguous" or params["lag_days"] < 1:
            return TrialPlan(**base, test_kind="lead_lag", status=UNTESTABLE,
                             reason=NO_DIRECTION, params=params)
        return TrialPlan(**base, test_kind="lead_lag", params=params)

    if criteria.get("watch_category"):
        # The declared outcome is analytical_snapshots row counts: GRID's own writes.
        return TrialPlan(**base, test_kind="none", status=INELIGIBLE, reason=SELF_TELEMETRY,
                         params={"watch_category": str(criteria["watch_category"])})
    if criteria.get("watch_actor"):
        return TrialPlan(**base, test_kind="none", status=UNTESTABLE, reason=NO_MARKET_OUTCOME,
                         params={"watch_actor": str(criteria["watch_actor"])})
    return TrialPlan(**base, test_kind="none", status=UNTESTABLE, reason=NO_MARKET_OUTCOME,
                     params={})


# ── Statistical tests (pure) ─────────────────────────────────────────────────


@dataclass(frozen=True)
class TrialResult:
    status: str                         # TESTED / UNTESTABLE / INELIGIBLE, or "pending"
    p_value: float = 1.0
    effect_ok: bool = False             # economic-size condition for confirmation
    reason: str | None = None
    details: dict = field(default_factory=dict)


PENDING = TrialResult(status="pending")


def entry_session_floor(created_at: datetime) -> date:
    """First session whose close was still in the future at ``created_at``."""
    local = _aware(created_at).astimezone(ET)
    return local.date() if local.time() < US_CLOSE else local.date() + timedelta(days=1)


def ticker_move_test(
    ticker_closes: dict[date, float],
    market_closes: dict[date, float] | None,
    *,
    created_at: datetime,
    direction: str,
    window_days: int,
    min_move_pct: float,
) -> TrialResult:
    """One-sided test that the ticker beat (or lagged) the market as declared.

    Statistic: signed log excess return over SPY from the first close after
    creation to the last close inside the window. Null: every overlapping
    same-horizon signed excess return of the ticker before creation.
    ``p = (1 + #null >= observed) / (n + 1)``. Confirmation also needs the raw
    move to reach ``min_move_pct`` in the declared direction (the thesis'
    own size). Returns ``PENDING`` when the entry or exit close is missing.
    """
    sign = 1.0 if direction == "up" else -1.0
    use_market = market_closes is not None
    days = sorted(d for d in ticker_closes if not use_market or d in market_closes)
    floor = entry_session_floor(created_at)
    ceiling = _aware(created_at).astimezone(ET).date() + timedelta(days=window_days)
    after = [d for d in days if floor <= d <= ceiling]
    if len(after) < 2:
        return PENDING
    entry, exit_ = after[0], after[-1]
    if (entry - floor).days > EDGE_TOLERANCE_DAYS or (ceiling - exit_).days > EDGE_TOLERANCE_DAYS:
        return PENDING

    def log_ret(series: dict[date, float], a: date, b: date) -> float:
        return math.log(series[b] / series[a])

    raw = log_ret(ticker_closes, entry, exit_)
    excess = raw - (log_ret(market_closes, entry, exit_) if use_market else 0.0)
    observed = sign * excess
    horizon = after.index(exit_)  # sessions between entry and exit

    before = [d for d in days if d < _aware(created_at).astimezone(ET).date()]
    t = np.log(np.array([ticker_closes[d] for d in before], dtype=float))
    null = t[horizon:] - t[:-horizon] if len(t) > horizon else np.array([])
    if use_market and len(null):
        m = np.log(np.array([market_closes[d] for d in before], dtype=float))
        null = null - (m[horizon:] - m[:-horizon])
    null = sign * null
    details = {
        "entry": entry.isoformat(), "exit": exit_.isoformat(), "horizon_sessions": horizon,
        "raw_move_pct": round((math.exp(raw) - 1) * 100, 4),
        "excess_log_return": round(excess, 6), "n_null": len(null),
        "benchmark": MARKET_TICKER if use_market else None,
    }
    if len(null) < MIN_NULL_SAMPLES:
        return TrialResult(status=UNTESTABLE, reason=INSUFFICIENT_HISTORY, details=details)
    p = (1 + int((null >= observed - 1e-12).sum())) / (len(null) + 1)
    effect_ok = sign * (math.exp(raw) - 1) * 100 >= min_move_pct and observed > 0
    return TrialResult(status=TESTED, p_value=float(p), effect_ok=bool(effect_ok), details=details)


def lead_lag_test(
    watch: dict[date, float],
    expect: dict[date, float],
    *,
    created_at: datetime,
    lag_days: int,
    direction: str,
    perms: int = LEAD_LAG_PERMS,
    seed: int = LEAD_LAG_SEED,
) -> TrialResult:
    """Holdout test of a declared lead-lag at its declared lag and sign.

    Daily calendar grid over the ``LEAD_LAG_HOLDOUT_DAYS`` after creation
    (missing days are 0, as the generator's counts imply). Correlates
    ``watch[t]`` with ``expect[t + lag]`` inside the holdout only. One-sided
    block-permutation p: the target ``expect`` is permuted in blocks sized from
    its own lag-1 autocorrelation.
    """
    start = _aware(created_at).astimezone(timezone.utc).date() + timedelta(days=1)
    grid = [start + timedelta(days=i) for i in range(LEAD_LAG_HOLDOUT_DAYS)]
    a = np.array([float(watch.get(d, 0.0)) for d in grid])
    b = np.array([float(expect.get(d, 0.0)) for d in grid])
    x, y = a[: len(a) - lag_days], b[lag_days:]
    details = {"holdout_start": start.isoformat(), "holdout_days": LEAD_LAG_HOLDOUT_DAYS,
               "n_pairs": len(x), "lag_days": lag_days}
    if len(x) < LEAD_LAG_MIN_PAIRS:
        return TrialResult(status=UNTESTABLE, reason=NO_DATA, details=details)
    if np.std(x) == 0 or np.std(y) == 0:
        return TrialResult(status=UNTESTABLE, reason=NO_DATA, details=details)
    block, basis = autocorrelation_block(y)
    xc, yc = x - x.mean(), y - y.mean()
    scale = math.sqrt(float(xc @ xc) * float(yc @ yc))
    observed = float(xc @ yc) / scale
    null = (yc[block_permutations(len(yc), block, perms, seed)] @ xc) / scale
    sign = 1.0 if direction == "up" else -1.0
    p = (1 + int((sign * null >= sign * observed - 1e-12).sum())) / (perms + 1)
    details.update({"correlation": round(observed, 6), "block": int(block), "perms": perms,
                    "block_basis": {k: v for k, v in basis.items() if k != "rule"}})
    return TrialResult(status=TESTED, p_value=float(p), effect_ok=sign * observed > 0, details=details)


# ── Data access (read-only) ──────────────────────────────────────────────────


def load_completed_closes(conn: Any, ticker: str, start: date, end: date,
                          evaluated_at: datetime) -> dict[date, float]:
    """Completed raw closes (#980 reader); ambiguous labels are dropped.

    Raises ``evaluation.prices.UnsupportedInstrumentError`` for non-US-listed
    or crypto tickers (their session policy is not NYSE's).
    """
    from evaluation.prices import (
        _CRYPTO_TICKER_PATTERN,
        _US_TICKER_PATTERN,
        COMPLETED_CLOSE_SOURCES,
        UnsupportedInstrumentError,
        us_listed_close_boundary,
    )
    from store.observations import read_completed_closes

    if _CRYPTO_TICKER_PATTERN.match(ticker) or not _US_TICKER_PATTERN.match(ticker):
        raise UnsupportedInstrumentError(f"not a US-listed ticker: {ticker!r}")
    closes = read_completed_closes(
        conn, f"YF:{ticker}:close", start=start, end=end,
        boundary=us_listed_close_boundary, source=COMPLETED_CLOSE_SOURCES,
        as_of_ts=evaluated_at,
    )
    return {d: c.latest.value for d, c in closes.items() if not c.ambiguous}


_SIGNAL_SERIES_SQL = text("""
    SELECT signal_date, COALESCE(SUM(magnitude), COUNT(*)) AS val
    FROM signal_data
    WHERE signal_type = :stype
      AND signal_date >= :start
      AND signal_date <= :end
    GROUP BY signal_date
""")


def load_signal_series(conn: Any, signal_key: str, start: date, end: date) -> dict[date, float]:
    """Daily value of a ``sig:<type>`` key inside ``[start, end]`` only."""
    prefix, _, name = signal_key.partition(":")
    if prefix != "sig" or not name:
        return {}
    rows = conn.execute(_SIGNAL_SERIES_SQL, {"stype": name, "start": start, "end": end}).fetchall()
    return {(r[0].date() if isinstance(r[0], datetime) else r[0]): float(r[1]) for r in rows}


def run_trial(plan: TrialPlan, conn: Any, evaluated_at: datetime,
              price_cache: dict[str, Any] | None = None) -> TrialResult:
    """The one look at a matured plan's holdout data. Read-only."""
    if plan.status is not None:
        return TrialResult(status=plan.status, reason=plan.reason)
    if evaluated_at < plan.matures_at():
        return PENDING
    cache = price_cache if price_cache is not None else {}
    if plan.test_kind == "ticker_move":
        from evaluation.prices import UnsupportedInstrumentError

        p = plan.params
        start = plan.created_at.astimezone(ET).date() - timedelta(days=NULL_LOOKBACK_DAYS)
        end = evaluated_at.astimezone(ET).date()

        def closes(t: str) -> dict[date, float]:
            key = f"{t}|{start}|{end}"
            if key not in cache:
                cache[key] = load_completed_closes(conn, t, start, end, evaluated_at)
            return cache[key]

        try:
            ticker = closes(p["ticker"])
        except UnsupportedInstrumentError:
            return TrialResult(status=UNTESTABLE, reason=UNSUPPORTED_INSTRUMENT)
        market = None if p["ticker"] == MARKET_TICKER else closes(MARKET_TICKER)
        return ticker_move_test(
            ticker, market, created_at=plan.created_at, direction=p["direction"],
            window_days=p["window_days"], min_move_pct=p["min_move_pct"],
        )
    if plan.test_kind == "lead_lag":
        p = plan.params
        start = plan.created_at.astimezone(timezone.utc).date() + timedelta(days=1)
        end = start + timedelta(days=LEAD_LAG_HOLDOUT_DAYS - 1)
        watch = load_signal_series(conn, p["watch_signal"], start, end)
        expect = load_signal_series(conn, p["expect_signal"], start, end)
        return lead_lag_test(watch, expect, created_at=plan.created_at,
                             lag_days=p["lag_days"], direction=p["direction"])
    return TrialResult(status=UNTESTABLE, reason=NO_MARKET_OUTCOME)


# ── Ledger ───────────────────────────────────────────────────────────────────


@contextmanager
def open_trial_ledger(path: str | Path | None = None,
                      anchor: str | Path | None = None) -> Iterator[Any]:
    """The configured S11 ledger, opened under an exclusive file lock.

    Hermes, the generator CLI and any manual run may score concurrently; the
    lock serialises them and the ledger is re-read inside it, so each batch
    sees every earlier one.
    """
    from analysis.ledger_steered_exploration import Ledger

    if path is None or anchor is None:
        from config import settings

        path = path or getattr(settings, "HYPOTHESIS_TRIAL_LEDGER_PATH", "")
        anchor = anchor or getattr(settings, "HYPOTHESIS_TRIAL_LEDGER_ANCHOR_PATH", "")
    if not path or not anchor:
        raise TrialLedgerNotConfigured(
            "HYPOTHESIS_TRIAL_LEDGER_PATH and HYPOTHESIS_TRIAL_LEDGER_ANCHOR_PATH must name "
            "an existing S11 ledger and its anchor"
        )
    lock_path = Path(str(path) + ".lock")
    with lock_path.open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            yield Ledger(path, anchor=anchor)
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def dry_run_ledger(q: float | None = None) -> Any:
    """An in-memory S11 ledger (synthetic catalog) for dry runs; never written to disk."""
    from analysis.ledger_steered_exploration import MAX_Q, Ledger, synthetic_catalog

    return Ledger.create(None, catalog=synthetic_catalog(), ledger_id="hypothesis-dry-run",
                         q=q or MAX_Q)


# ── Batch scorer ─────────────────────────────────────────────────────────────

_ACTIVE_SQL = text("""
    SELECT d.id, d.pattern_type, d.test_criteria, d.created_at,
           COALESCE(d.times_tested, 0), t.test_criteria->>'expected_direction'
    FROM discovered_hypotheses d
    LEFT JOIN discovered_hypotheses t
      ON d.role = 'antithesis' AND t.id = d.pair_id AND t.id <> d.id
    WHERE d.status = 'active'
    ORDER BY d.created_at ASC, d.id ASC
    LIMIT :limit
""")

_STATUS_SQL = text("SELECT id, status FROM discovered_hypotheses WHERE id = ANY(:ids)")

_TRIAL_COLUMNS = {
    "scoring_version": text(
        "ALTER TABLE discovered_hypotheses ADD COLUMN IF NOT EXISTS scoring_version TEXT"),
    "trial_p_value": text(
        "ALTER TABLE discovered_hypotheses ADD COLUMN IF NOT EXISTS trial_p_value DOUBLE PRECISION"),
    "trial_q_value": text(
        "ALTER TABLE discovered_hypotheses ADD COLUMN IF NOT EXISTS trial_q_value DOUBLE PRECISION"),
    "trial_ledger_sha256": text(
        "ALTER TABLE discovered_hypotheses ADD COLUMN IF NOT EXISTS trial_ledger_sha256 TEXT"),
}


def ensure_trial_columns(engine: Engine) -> None:
    """Add the v2 verdict columns, taking the ALTER lock only when one is missing (#992).

    ``scoring_version`` stays NULL on every row v1 decided, which is how
    readers tell a v1 verdict from a ledgered v2 one without rewriting it.
    """
    with engine.connect() as conn:
        have = {r[0] for r in conn.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'discovered_hypotheses'"
        ))}
    missing = [stmt for column, stmt in _TRIAL_COLUMNS.items() if column not in have]
    if not missing:
        return
    with engine.begin() as conn:
        for stmt in missing:
            conn.execute(stmt)


def _kill_reason(trial: dict) -> str | None:
    verdict = trial["verdict"]
    if verdict == "confirmed":
        return None
    if verdict == "invalidated":
        return NOT_SIGNIFICANT
    return trial.get("reason") or NO_DATA


def apply_verdicts(engine: Engine, record: dict, record_sha: str) -> dict[str, int]:
    """Write a ledgered batch's verdicts to rows that are still ``active``.

    Idempotent: a row that already left ``active`` is not touched, so a batch
    whose DB write failed after its ledger append is replayed safely.
    """
    from intelligence.hypothesis_engine import HypothesisGenerator

    counts = {"confirmed": 0, "invalidated": 0, "expired": 0, "unchanged": 0}
    update = text("""
        UPDATE discovered_hypotheses
        SET status = :status,
            times_tested = COALESCE(times_tested, 0) + 1,
            times_correct = COALESCE(times_correct, 0) + :correct,
            last_tested = NOW(),
            kill_reason = :kill_reason,
            killed_at = CASE WHEN :kill_reason IS NOT NULL THEN NOW() ELSE killed_at END,
            scoring_version = :version,
            trial_p_value = :p,
            trial_q_value = :q,
            trial_ledger_sha256 = :sha
        WHERE id = :hid AND status = 'active'
    """)
    generator = HypothesisGenerator(engine)
    for trial in record["trials"]:
        verdict = trial["verdict"]
        kill = _kill_reason(trial)
        with engine.begin() as conn:
            changed = conn.execute(update, {
                "status": verdict, "correct": 1 if verdict == "confirmed" else 0,
                "kill_reason": kill, "version": record["scorer_version"],
                "p": trial["p_value"], "q": trial["q_value"], "sha": record_sha,
                "hid": trial["hypothesis_id"],
            }).rowcount
        if not changed:
            counts["unchanged"] += 1
            continue
        counts[verdict] += 1
        if verdict == "invalidated":
            generator._write_postmortem(trial["hypothesis_id"], kill)
        elif verdict == "confirmed":
            generator._kill_counterpart(trial["hypothesis_id"])
    return counts


def _replay_unapplied(engine: Engine, ledger: Any) -> dict[str, int]:
    """Apply ledgered verdicts whose rows are still active (crash between append and write)."""
    from analysis.ledger_steered_exploration import HYPOTHESIS_BATCH_KIND

    totals: dict[str, int] = {}
    for record in ledger.of_kind(HYPOTHESIS_BATCH_KIND):
        ids = [t["hypothesis_id"] for t in record["trials"]]
        with engine.connect() as conn:
            active = {r[0] for r in conn.execute(_STATUS_SQL, {"ids": ids}) if r[1] == "active"}
        if not active:
            continue
        sha = ledger.line_sha(record["seq"])
        part = {**record, "trials": [t for t in record["trials"] if t["hypothesis_id"] in active]}
        for k, v in apply_verdicts(engine, part, sha).items():
            totals[k] = totals.get(k, 0) + v
    return totals


def evaluate_batch(
    engine: Engine,
    *,
    ledger: Any,
    evaluated_at: datetime | None = None,
    limit: int = 10000,
    max_runtime_s: float | None = None,
) -> dict:
    """Plan and run every matured active hypothesis's one look. Read-only.

    Returns ``{"trials": [...], "pending": n, ...}``; nothing is recorded.
    Hypotheses already tested in ``ledger`` are skipped.
    """
    from analysis.ledger_steered_exploration import hypothesis_batch_trials

    evaluated_at = _aware(evaluated_at or datetime.now(timezone.utc))
    started = time.monotonic()
    seen = hypothesis_batch_trials(ledger)
    with engine.connect() as conn:
        rows = conn.execute(_ACTIVE_SQL, {"limit": limit}).fetchall()
        trials: list[dict] = []
        pending = skipped_seen = 0
        timed_out = False
        cache: dict[str, Any] = {}
        for hid, ptype, criteria, created, tested, thesis_direction in rows:
            if max_runtime_s is not None and time.monotonic() - started >= max_runtime_s:
                timed_out = True
                break
            if hid in seen:
                skipped_seen += 1
                continue
            plan = plan_trial(hid, ptype, criteria, created, tested, thesis_direction)
            result = run_trial(plan, conn, evaluated_at, cache)
            if result.status == "pending":
                if evaluated_at < plan.expires_at() or plan.status is not None:
                    pending += 1
                    continue
                result = TrialResult(status=UNTESTABLE, reason=NO_DATA)
            trials.append({
                "hypothesis_id": hid,
                "pattern_type": plan.pattern_type,
                "test_kind": plan.test_kind,
                "identity_sha256": plan.identity_sha256,
                "created_at": plan.created_at.isoformat(),
                "prior_looks": plan.prior_looks,
                "status": result.status,
                "reason": result.reason,
                "p_value": result.p_value if result.status == TESTED else 1.0,
                "effect_ok": bool(result.effect_ok) if result.status == TESTED else False,
                "params": plan.params,
                "details": result.details,
            })
    return {
        "evaluated_at": evaluated_at.isoformat(),
        "scanned": len(rows),
        "trials": trials,
        "pending": pending,
        "skipped_already_ledgered": skipped_seen,
        "timed_out": timed_out,
    }


def score_batch(
    engine: Engine,
    *,
    ledger: Any | None = None,
    evaluated_at: datetime | None = None,
    limit: int = 10000,
    max_runtime_s: float | None = None,
    dry_run: bool = False,
) -> dict:
    """Ledger every matured hypothesis's one look, then write the verdicts.

    ``dry_run``: an in-memory ledger is used (or the given one) and no row is
    written. Otherwise ``ledger`` must be the configured S11 file ledger
    (normally from :func:`open_trial_ledger`).
    """
    from analysis.ledger_steered_exploration import (
        record_hypothesis_batch,
        summarize_hypothesis_batches,
    )

    if ledger is None:
        if not dry_run:
            raise TrialLedgerNotConfigured("score_batch needs the S11 trial ledger")
        ledger = dry_run_ledger()
    replayed = {} if dry_run else _replay_unapplied(engine, ledger)
    batch = evaluate_batch(engine, ledger=ledger, evaluated_at=evaluated_at,
                           limit=limit, max_runtime_s=max_runtime_s)
    out = {k: v for k, v in batch.items() if k != "trials"}
    out.update({"scorer_version": SCORER_VERSION, "dry_run": dry_run, "replayed": replayed,
                "declared": len(batch["trials"])})
    if not batch["trials"]:
        out["verdicts"] = {}
        out["ledger"] = summarize_hypothesis_batches(ledger)
        return out
    recorded = record_hypothesis_batch(
        ledger, scorer_version=SCORER_VERSION, evaluated_at=batch["evaluated_at"],
        trials=batch["trials"],
    )
    record, sha = recorded["record"], recorded["sha256"]
    tally: dict[str, int] = {}
    for t in record["trials"]:
        tally[t["verdict"]] = tally.get(t["verdict"], 0) + 1
    out.update({
        "verdicts": tally,
        "q": record["q"],
        "trial_count": record["trial_count"],
        "record_sha256": sha,
        "ledger": summarize_hypothesis_batches(ledger),
        "record": record,
    })
    if not dry_run:
        ensure_trial_columns(engine)
        out["applied"] = apply_verdicts(engine, record, sha)
    return out


def summarize_dry_run(result: dict) -> dict:
    """Headline numbers of a ``score_batch(dry_run=True)`` result (no per-trial rows)."""
    trials = (result.get("record") or {}).get("trials", [])
    by: dict[str, dict[str, int]] = {}
    reasons: dict[str, int] = {}
    survivors_fresh = 0
    for t in trials:
        row = by.setdefault(t["pattern_type"] or "?", {})
        row[t["verdict"]] = row.get(t["verdict"], 0) + 1
        if t.get("reason"):
            reasons[t["reason"]] = reasons.get(t["reason"], 0) + 1
        if t["verdict"] == "confirmed" and not t.get("prior_looks"):
            survivors_fresh += 1
    tested = [t for t in trials if t["status"] == TESTED]
    # Sensitivity: BH over the tested trials only (drops the p = 1 denominator).
    from analysis.offline_research_proof import bh_adjusted

    q = result.get("q") or 0.10
    tested_q = bh_adjusted([t["p_value"] for t in tested])
    tested_only = sum(1 for t, qv in zip(tested, tested_q) if qv <= q and t["effect_ok"])
    return {
        "confirmed_bh_over_tested_only": tested_only,
        "min_q_bh_over_tested_only": min(tested_q, default=None),
        "scanned_active": result.get("scanned"),
        "declared_trials": len(trials),
        "tested": len(tested),
        "pending": result.get("pending"),
        "q": result.get("q"),
        "verdicts": result.get("verdicts"),
        "by_pattern_type": by,
        "reasons": reasons,
        "confirmed_with_no_v1_look": survivors_fresh,
        "min_p": min((t["p_value"] for t in tested), default=None),
        "raw_p_below_0_05": sum(1 for t in tested if t["p_value"] < 0.05),
    }


def read_only_engine(engine: Engine) -> Engine:
    """Make every pooled connection of ``engine`` read-only, and prove it.

    Existing pooled connections are dropped; each new DBAPI connection runs
    ``SET SESSION default_transaction_read_only = on`` before first use, so a
    stray write in a dry run fails in Postgres instead of landing.
    """
    from sqlalchemy import event

    def _read_only(dbapi_conn: Any, _record: Any) -> None:
        cursor = dbapi_conn.cursor()
        cursor.execute("SET SESSION default_transaction_read_only = on")
        cursor.close()

    engine.dispose()
    event.listen(engine, "connect", _read_only)
    with engine.connect() as conn:
        if conn.execute(text("SHOW default_transaction_read_only")).scalar() != "on":
            raise RuntimeError("could not make the dry-run session read-only")
    return engine


def main(argv: list[str] | None = None) -> int:
    """``python -m intelligence.hypothesis_trials dry-run [--limit N]`` (read-only)."""
    import argparse

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("command", choices=["dry-run"])
    parser.add_argument("--limit", type=int, default=10000)
    parser.add_argument("--max-runtime-s", type=float, default=None)
    parser.add_argument("--trials-out", default="", help="write per-trial JSONL here")
    args = parser.parse_args(argv)

    from db import get_engine

    engine = read_only_engine(get_engine())
    result = score_batch(engine, dry_run=True, limit=args.limit, max_runtime_s=args.max_runtime_s)
    if args.trials_out:
        with open(args.trials_out, "w", encoding="utf-8") as fh:
            fh.writelines(json.dumps(t, default=str) + "\n" for t in (result.get("record") or {}).get("trials", []))
    print(json.dumps({"summary": summarize_dry_run(result), "ledger": result.get("ledger")},
                     indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
