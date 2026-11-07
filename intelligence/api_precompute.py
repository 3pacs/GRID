"""Background precompute for GRID's slowest read endpoints (W3-2).

A live-site sweep on 2026-11-06 (release 5bafdb0b) measured these GETs at
11 s to well past Cloudflare's 125 s limit: regime analogs, the globe, vote/
trade hypocrisy, the signals snapshot, the smart heatmap, the cross-reference
narrative, the three association views, system architecture, circular flows
and the 100x options scan. Each is now a :class:`Job` here. A job computes on
its cadence and writes one row per (endpoint key, parameter set) to
``api_precomputed_results`` (``store/precomputed.py``); the endpoint reads the
row by primary key (``api/precompute_serving.py``) and never computes inline.

Scheduling: ``intelligence/scheduler.py`` calls :func:`start_background_tick`
every 5 minutes. The tick starts at most one worker thread (so a 3-minute
options scan never stalls the scheduler loop) and the worker runs every due
job in order. Each job holds a PostgreSQL advisory lock while it runs, so a
manual ``scripts/run_api_precompute.py`` run (on grid-svr or gridz4) and the
scheduler never compute the same job at once. Kill switch:
``GRID_API_PRECOMPUTE_ENABLED=0``.

Due rule (:func:`due_reason`): never computed, version changed, or the last
attempt is older than the job's cadence (a failed attempt retries after
``min(cadence, 15 min)``). A job with a ``signature`` (a cheap query over its
inputs, e.g. the latest ``options_daily_signals`` date) skips recomputing
while the signature is unchanged and the payload is younger than
``max_age_s`` -- the 100x scan reruns after the options pull, not hourly.

PIT: builders read through the same PIT paths as the endpoints did, as of
the run date, and every output carries its data ``as_of``. A failed or
``unavailable`` run never replaces a good payload; the old payload keeps its
real ``computed_at`` and is labelled stale once past ``stale_after_s``.
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from store import precomputed as store

MINUTE = 60
HOUR = 3600
FAILURE_RETRY_S = 15 * MINUTE
TICK_MINUTES = 5


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class Output:
    """One computed response body for one (key, params)."""

    key: str
    params: dict[str, Any]
    payload: dict[str, Any] | None
    as_of: date | datetime | None
    inputs: dict[str, Any] = field(default_factory=dict)
    # Set when the build finished but the result is "unavailable" in the
    # AVAILABILITY_CONTRACT sense: recorded as an attempt, never persisted
    # over a good payload.
    unavailable_reason: str | None = None


@dataclass
class BuildContext:
    """What a builder may consult besides the database."""

    engine: Engine
    now: datetime
    signature: str | None = None

    def previous(self, key: str, params: Mapping[str, Any] | None = None) -> store.PrecomputedRow | None:
        try:
            return store.read_result(self.engine, key, params)
        except Exception as exc:  # noqa: BLE001 -- previous result is advisory
            log.warning("api precompute: previous {k} unreadable: {e}", k=key, e=str(exc))
            return None


@dataclass(frozen=True)
class Job:
    """A background computation feeding one or more endpoint keys.

    ``outputs`` declares the (key, params) slots the job always writes; they
    drive the due check and are what a failure is recorded against. A build
    may return extra variants (one per feature family, say).
    ``empty_fields`` maps each key to the response fields an ``unavailable``
    503 body names (as ``null``) so clients keep the documented shape.
    """

    name: str
    cadence_s: int
    stale_after_s: int
    version: str
    build: Callable[[BuildContext], list[Output]]
    outputs: tuple[tuple[str, tuple[tuple[str, Any], ...]], ...]
    empty_fields: Mapping[str, tuple[str, ...]]
    signature: Callable[[Engine], str | None] | None = None
    max_age_s: int | None = None
    description: str = ""

    @property
    def keys(self) -> tuple[str, ...]:
        seen: list[str] = []
        for key, _ in self.outputs:
            if key not in seen:
                seen.append(key)
        for key in self.empty_fields:
            if key not in seen:
                seen.append(key)
        return tuple(seen)

    def declared(self) -> list[tuple[str, dict[str, Any]]]:
        return [(k, dict(p)) for k, p in self.outputs]


def _p(**params: Any) -> tuple[tuple[str, Any], ...]:
    """Hashable params literal for :attr:`Job.outputs`."""
    return tuple(sorted(params.items()))


# ---------------------------------------------------------------------------
# Builders. Heavy imports stay inside the functions: the API process imports
# this module for the endpoint specs only.
# ---------------------------------------------------------------------------


def _max_date(values: list[Any]) -> date | None:
    out: date | None = None
    for v in values:
        if v is None or v == "":
            continue
        try:
            d = v if isinstance(v, date) else date.fromisoformat(str(v)[:10])
        except ValueError:
            continue
        if isinstance(d, datetime):
            d = d.date()
        if out is None or d > out:
            out = d
    return out


def build_regime_analogs(ctx: BuildContext) -> list[Output]:
    """Analog episodes + conditional forecast + TimesFM side-by-side."""
    from api.routers.intelligence_regime import _get_timesfm_comparison
    from intelligence.regime.classifier import classify_regime
    from intelligence.regime.episode_matcher import find_analogous_episodes
    from intelligence.regime.forecast import generate_conditional_forecast
    from intelligence.regime.state_vector import (
        MIN_CACHE_COMPLETENESS,
        get_or_compute_state_vector,
    )

    params = {"n": 20, "min_quality": 0.4}
    engine = ctx.engine
    sv = get_or_compute_state_vector(engine, persist=False)
    if sv.completeness < MIN_CACHE_COMPLETENESS:
        reason = (
            f"state vector for {sv.as_of_date.isoformat()} is only "
            f"{sv.completeness:.0%} complete (need >= {MIN_CACHE_COMPLETENESS:.0%}) "
            "— too thin to match against historical episodes"
        )
        return [Output("intelligence.regime_analogs", params, None, None, unavailable_reason=reason)]

    regime = classify_regime(sv)
    matches = find_analogous_episodes(engine, sv, n=params["n"], min_quality=params["min_quality"])
    forecast = generate_conditional_forecast(engine, matches)
    payload: dict[str, Any] = {
        "available": True,
        "regime": regime.to_dict(),
        "matches": matches.to_dict(),
        "forecast": forecast.to_dict(),
        "cached": sv.cached,
        "as_of_date": sv.as_of_date.isoformat(),
    }
    try:
        payload["timesfm"] = _get_timesfm_comparison(engine)
    except Exception as exc:  # noqa: BLE001 -- second opinion only
        log.info("TimesFM comparison skipped: {e}", e=str(exc))
        payload["timesfm"] = {"available": False, "reason": str(exc)}
    return [Output("intelligence.regime_analogs", params, payload, sv.as_of_date,
                   inputs={"completeness": round(float(sv.completeness), 4)})]


def build_globe(ctx: BuildContext) -> list[Output]:
    from api.routers.intelligence_risk import _build_globe_data

    payload = _build_globe_data()
    as_of = _max_date([payload.get("as_of")])
    return [Output("intelligence.globe", {}, payload, as_of)]


def _congressional_as_of(engine: Engine) -> date | None:
    with engine.connect() as conn:
        v = conn.execute(text(
            "SELECT MAX(signal_date) FROM signal_sources "
            "WHERE source_type = 'congressional' AND signal_date <= CURRENT_DATE"
        )).scalar()
    return _max_date([v])


def build_influence(ctx: BuildContext) -> list[Output]:
    """Vote/trade hypocrisy and circular flows (one job: same source tables).

    ``detect_circular_flows`` persists its loops to ``influence_loops`` --
    that write used to happen inside a GET; it now happens here.
    """
    from intelligence.influence_network import (
        detect_circular_flows,
        vote_trade_hypocrisy,
    )

    engine = ctx.engine
    as_of = _congressional_as_of(engine)
    flags = vote_trade_hypocrisy(engine)
    hyp = {
        "flags": flags,
        "total": len(flags),
        "members_flagged": len({f["member"] for f in flags}),
    }
    loops = detect_circular_flows(engine)
    circ = {
        "loops": [lp.to_dict() for lp in loops],
        "total": len(loops),
        "circular_count": sum(1 for lp in loops if lp.circular_flow_detected),
    }
    return [
        Output("intelligence.influence_hypocrisy", {}, hyp, as_of),
        Output("intelligence.influence_circular_flows", {}, circ, as_of),
    ]


def build_signals_snapshot(ctx: BuildContext) -> list[Output]:
    from api.routers.signals import _compute_feature_snapshot

    payload = _compute_feature_snapshot()
    as_of = _max_date([f.get("obs_date") for f in payload.get("features") or []])
    if not payload.get("features"):
        return [Output("signals.snapshot", {}, None, None,
                       unavailable_reason="feature snapshot is empty (no PIT values for model-eligible features)")]
    return [Output("signals.snapshot", {}, payload, as_of,
                   inputs={"features": payload.get("count")})]


def build_smart_heatmap(ctx: BuildContext) -> list[Output]:
    from api.dependencies import get_pit_store
    from discovery.smart_heatmap import build_smart_heatmap_payloads

    variants = build_smart_heatmap_payloads(ctx.engine, get_pit_store())
    if not variants:
        return [Output("discovery.smart_heatmap", dict(SMART_HEATMAP_DEFAULT), None, None,
                       unavailable_reason="no model-eligible features in feature_registry")]
    return [Output("discovery.smart_heatmap", params, payload, as_of) for params, payload, as_of in variants]


def build_associations(ctx: BuildContext) -> list[Output]:
    """Correlation matrix, anomalies and regime features from ONE PIT read.

    The three views each read a LATEST_AS_OF matrix over every model-eligible
    feature (252 / 252 / 504 days). The job reads the 504-day matrix once and
    slices the shorter windows out of it.
    """
    from api.dependencies import get_pit_store
    from api.routers import associations as assoc

    engine = ctx.engine
    pit = get_pit_store()
    registry = assoc._get_feature_registry(engine)
    raw = assoc.load_raw_feature_matrix(pit, registry["id"].tolist(), ASSOC_MAX_DAYS) if not registry.empty else None

    out: list[Output] = []

    corr_all = assoc._compute_correlation_matrix(252, 0.3, True, raw=raw, registry=registry)
    corr_as_of = _max_date([corr_all.get("window_end")])
    corr_hidden = dict(corr_all)
    corr_hidden["strong_pairs"] = [p for p in corr_all.get("strong_pairs", []) if p.get("kind") != "trivial"]
    out.append(Output("associations.correlation_matrix", {"days": 252, "min_corr": 0.3, "show_trivial": False},
                      corr_hidden, corr_as_of))
    out.append(Output("associations.correlation_matrix", {"days": 252, "min_corr": 0.3, "show_trivial": True},
                      corr_all, corr_as_of))

    anomalies = assoc._compute_anomalies(2.5, 252, raw=raw, registry=registry, alert=False)
    anomaly_window = assoc._build_feature_matrix(pit, registry["id"].tolist(), 252, raw=raw) \
        if raw is not None else None
    anomaly_as_of = (anomaly_window.index.max().date()
                     if anomaly_window is not None and not anomaly_window.empty else None)
    _alert_on_new_extremes(ctx, anomalies)
    out.append(Output("associations.anomalies", {"sigma_threshold": 2.5, "days": 252}, anomalies, anomaly_as_of))

    regime = assoc._compute_regime_features(504, raw=raw, registry=registry)
    regime_as_of = _max_date([raw.index.max()]) if raw is not None and not raw.empty else None
    out.append(Output("associations.regime_features", {"days": 504}, regime, regime_as_of))
    return out


def _alert_on_new_extremes(ctx: BuildContext, anomalies: dict[str, Any]) -> None:
    """E-mail extreme anomalies only when the extreme set changed.

    The GET used to e-mail on every page view with an extreme anomaly; the
    job would e-mail every run. Now: only when the set of extreme features
    differs from the previous stored scan.
    """
    from api.routers.associations import extreme_anomalies, send_extreme_anomaly_alert

    extreme = extreme_anomalies(anomalies.get("anomalies") or [])
    current = sorted(a["feature"] for a in extreme)
    prev_row = ctx.previous("associations.anomalies", {"sigma_threshold": 2.5, "days": 252})
    previous: list[str] = []
    if prev_row is not None and prev_row.has_payload:
        prev_payload = prev_row.payload() or {}
        previous = sorted(a["feature"] for a in extreme_anomalies(prev_payload.get("anomalies") or []))
    if current and current != previous:
        send_extreme_anomaly_alert(extreme)


def _narrative_input_hash(checks: list[Any]) -> str:
    items = sorted(
        (c.name, c.category, c.assessment, round(float(c.actual_divergence or 0.0), 2))
        for c in checks
    )
    return hashlib.sha256(json.dumps(items).encode("utf-8")).hexdigest()[:32]


def build_cross_reference_narrative(ctx: BuildContext) -> list[Output]:
    """LLM narrative over the latest persisted cross-reference checks.

    Hermes runs the checks every 4 h and persists them; this job reads the
    latest batch (no re-run of the slow scans) and calls the REASON tier only
    when the checks changed since the cached narrative. A fallback (LLM
    offline) narrative is stored labelled ``narrative_source: "fallback"``
    and retried on the next run.
    """
    from intelligence.cross_reference import (
        generate_narrative_with_source,
        load_recent_report,
    )

    key = "intelligence.cross_reference_narrative"
    report = load_recent_report(ctx.engine, max_age_seconds=XREF_CHECKS_MAX_AGE_S)
    if report is None or not report.checks:
        return [Output(key, {}, None, None, unavailable_reason=(
            f"no cross-reference checks persisted in the last {XREF_CHECKS_MAX_AGE_S // HOUR} h "
            "(cross_reference_checks; Hermes runs them every 4 h)"))]

    checked_at = _max_date([getattr(c, "checked_at", None) for c in report.checks]) or _max_date(
        [report.generated_at])
    input_hash = _narrative_input_hash(report.checks)
    prev = ctx.previous(key, {})
    if (prev is not None and prev.has_payload and prev.inputs
            and prev.inputs.get("input_hash") == input_hash
            and prev.inputs.get("narrative_source") == "llm"):
        payload = prev.payload() or {}
        return [Output(key, {}, payload, checked_at, inputs=dict(prev.inputs))]

    narrative, source = generate_narrative_with_source(report.checks, report.red_flags)
    payload = {
        "narrative": narrative,
        "generated_at": ctx.now.isoformat(),
        "narrative_source": source,
        "checks_generated_at": report.generated_at,
        "red_flag_count": len(report.red_flags),
        "total_checks": len(report.checks),
    }
    return [Output(key, {}, payload, checked_at,
                   inputs={"input_hash": input_hash, "narrative_source": source})]


def build_architecture(ctx: BuildContext) -> list[Output]:
    """The database half of ``/system/architecture`` (puller stats, counts)."""
    from api.routers.system import _compute_architecture_db_stats

    payload = _compute_architecture_db_stats(ctx.engine)
    # A snapshot of live system state: the data *is* "now".
    return [Output("system.architecture_db", {}, payload, ctx.now)]


PATTERNS_DEFAULT = {"min_occurrences": 3, "max_sequence_length": 4}


def build_patterns(ctx: BuildContext) -> list[Output]:
    """``/intelligence/patterns`` event-sequence mining (~116 s), the PWA's parameters."""
    from api.routers.intelligence_news import _compute_discovered_patterns

    payload = _compute_discovered_patterns(ctx.engine, **PATTERNS_DEFAULT)
    # Mined over the event history up to the build: the data is "now".
    return [Output("intelligence.patterns", dict(PATTERNS_DEFAULT), payload, ctx.now)]


def _options_signature(engine: Engine) -> str | None:
    with engine.connect() as conn:
        r = conn.execute(text(
            "SELECT MAX(signal_date), COUNT(*) FILTER (WHERE signal_date = "
            "(SELECT MAX(signal_date) FROM options_daily_signals WHERE signal_date <= CURRENT_DATE)) "
            "FROM options_daily_signals WHERE signal_date <= CURRENT_DATE"
        )).fetchone()
    if r is None or r[0] is None:
        return None
    return f"{r[0]}:{r[1]}"


OPTIONS_SCAN_DEFAULT = {"min_score": 5.0}
OPTIONS_100X_MIN_SCORE = 6.0  # OptionsScanner.get_100x_opportunities' cut


def _opportunity_dict(o: Any, *, with_signal_date: bool) -> dict[str, Any]:
    out = {
        "ticker": o.ticker,
        "scan_date": str(o.scan_date),
        "score": o.score,
        "estimated_payoff_multiple": o.estimated_payoff_multiple,
        "direction": o.direction,
        "thesis": o.thesis,
        "strikes": o.strikes,
        "expiry": o.expiry,
        "spot_price": o.spot_price,
        "iv_atm": o.iv_atm,
        "confidence": o.confidence,
    }
    if with_signal_date:
        out["signal_date"] = str(o.signal_date) if o.signal_date else None
    else:
        out["is_100x"] = o.is_100x
    return out


def build_options_100x(ctx: BuildContext) -> list[Output]:
    """One SVI scan (~160 s) feeding ``/options/100x`` and ``/options/scan``.

    ``scan_all``'s ``min_score`` only filters, so the 100x list is the
    min_score=5 scan cut at 6 and filtered to ``is_100x`` -- exactly what
    ``get_100x_opportunities`` returns.
    """
    from discovery.options_scanner import OptionsScanner

    scanner = OptionsScanner(ctx.engine)
    opps = scanner.scan_all(min_score=OPTIONS_SCAN_DEFAULT["min_score"])
    hundred_x = [o for o in opps if o.score >= OPTIONS_100X_MIN_SCORE and o.is_100x]
    results = [_opportunity_dict(o, with_signal_date=True) for o in hundred_x]
    sig_date = (ctx.signature or "").split(":", 1)[0] or None
    as_of = _max_date([r["signal_date"] for r in results]) or _max_date([sig_date])
    scan = [_opportunity_dict(o, with_signal_date=False) for o in opps]
    scan_as_of = _max_date([o.signal_date for o in opps]) or _max_date([sig_date])
    inputs = {"signature": ctx.signature}
    return [
        Output("options.100x", {}, {"opportunities": results, "count": len(results)}, as_of, inputs=inputs),
        Output("options.scan", dict(OPTIONS_SCAN_DEFAULT),
               {"opportunities": scan, "count": len(scan), "count_100x": sum(1 for o in opps if o.is_100x)},
               scan_as_of, inputs=inputs),
    ]


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

ASSOC_MAX_DAYS = 504
XREF_CHECKS_MAX_AGE_S = 8 * HOUR
SMART_HEATMAP_DEFAULT = {"family": None, "orthogonal_only": True, "corr_threshold": 0.8}

JOBS: tuple[Job, ...] = (
    Job(
        name="signals_snapshot", cadence_s=30 * MINUTE, stale_after_s=2 * HOUR, version="1",
        build=build_signals_snapshot,
        outputs=(("signals.snapshot", _p()),),
        empty_fields={"signals.snapshot": ("features", "count")},
        description="Latest PIT value + 504-day z-score per model-eligible feature.",
    ),
    Job(
        name="architecture", cadence_s=30 * MINUTE, stale_after_s=3 * HOUR, version="1",
        build=build_architecture,
        outputs=(("system.architecture_db", _p()),),
        empty_fields={"system.architecture_db": ()},
        description="Puller stats and table counts for /system/architecture.",
    ),
    Job(
        name="cross_reference_narrative", cadence_s=30 * MINUTE, stale_after_s=8 * HOUR, version="1",
        build=build_cross_reference_narrative,
        outputs=(("intelligence.cross_reference_narrative", _p()),),
        empty_fields={"intelligence.cross_reference_narrative": ("narrative", "generated_at")},
        description="LLM narrative over the latest persisted checks, cached per input hash.",
    ),
    Job(
        name="globe", cadence_s=HOUR, stale_after_s=6 * HOUR, version="1",
        build=build_globe,
        outputs=(("intelligence.globe", _p()),),
        empty_fields={"intelligence.globe": ("countries", "flows", "hotspots", "fx_map")},
        description="Country activity, trade/capital flows, cross-reference hotspots, FX.",
    ),
    Job(
        name="associations", cadence_s=2 * HOUR, stale_after_s=12 * HOUR, version="1",
        build=build_associations,
        outputs=(
            ("associations.correlation_matrix", _p(days=252, min_corr=0.3, show_trivial=False)),
            ("associations.correlation_matrix", _p(days=252, min_corr=0.3, show_trivial=True)),
            ("associations.anomalies", _p(sigma_threshold=2.5, days=252)),
            ("associations.regime_features", _p(days=504)),
        ),
        empty_fields={
            "associations.correlation_matrix": ("features", "matrix", "strong_pairs"),
            "associations.anomalies": ("anomalies",),
            "associations.regime_features": ("regimes",),
        },
        description="Correlation matrix, anomalies, regime features from one PIT read.",
    ),
    Job(
        name="influence", cadence_s=6 * HOUR, stale_after_s=36 * HOUR, version="1",
        build=build_influence,
        outputs=(
            ("intelligence.influence_hypocrisy", _p()),
            ("intelligence.influence_circular_flows", _p()),
        ),
        empty_fields={
            "intelligence.influence_hypocrisy": ("flags", "total", "members_flagged"),
            "intelligence.influence_circular_flows": ("loops", "total", "circular_count"),
        },
        description="Vote/trade hypocrisy flags and lobbying/PAC circular flows.",
    ),
    Job(
        name="smart_heatmap", cadence_s=6 * HOUR, stale_after_s=36 * HOUR, version="1",
        build=build_smart_heatmap,
        outputs=(("discovery.smart_heatmap", _p(**SMART_HEATMAP_DEFAULT)),),
        empty_fields={"discovery.smart_heatmap": ("features", "matrix", "z_scores")},
        description="Orthogonal-feature correlation submatrix + z-scores, per family.",
    ),
    Job(
        name="regime_analogs", cadence_s=6 * HOUR, stale_after_s=36 * HOUR, version="1",
        build=build_regime_analogs,
        outputs=(("intelligence.regime_analogs", _p(n=20, min_quality=0.4)),),
        empty_fields={"intelligence.regime_analogs": ("regime", "matches", "forecast")},
        description="Analog episodes for today's state vector + conditional forecast + TimesFM.",
    ),
    Job(
        name="patterns", cadence_s=6 * HOUR, stale_after_s=36 * HOUR, version="1",
        build=build_patterns,
        outputs=(("intelligence.patterns", _p(**PATTERNS_DEFAULT)),),
        empty_fields={"intelligence.patterns": ("patterns", "count", "actionable_count")},
        description="Recurring 2-4 event sequences across watchlist tickers (read-only mining).",
    ),
    Job(
        name="options_100x", cadence_s=30 * MINUTE, stale_after_s=36 * HOUR, version="2",
        build=build_options_100x,
        outputs=(("options.100x", _p()), ("options.scan", _p(**OPTIONS_SCAN_DEFAULT))),
        empty_fields={"options.100x": ("opportunities", "count"),
                      "options.scan": ("opportunities", "count", "count_100x")},
        signature=_options_signature, max_age_s=12 * HOUR,
        description="Options mispricing scan (/options/scan) and its 100x cut; reruns when options_daily_signals changes.",
    ),
)

JOBS_BY_NAME: dict[str, Job] = {j.name: j for j in JOBS}
JOB_BY_KEY: dict[str, Job] = {k: j for j in JOBS for k in j.keys}


def job_for_key(key: str) -> Job:
    """The job that feeds an endpoint key (KeyError if unknown)."""
    return JOB_BY_KEY[key]


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def due_reason(job: Job, rows: list[store.PrecomputedRow], now: datetime) -> str | None:
    """Why ``job`` should run now, or ``None`` if it is not due."""
    by_slot = {(r.key, r.params_hash): r for r in rows}
    reasons: list[str] = []
    for key, params in job.declared():
        row = by_slot.get((key, store.params_hash(params)))
        if row is None:
            return f"{key} never attempted"
        if row.version != job.version:
            return f"{key} version {row.version} != {job.version}"
        if row.last_attempt_at is None:
            return f"{key} has no attempt time"
        age = (now - row.last_attempt_at).total_seconds()
        limit = job.cadence_s if row.last_status == store.STATUS_OK else min(job.cadence_s, FAILURE_RETRY_S)
        if age >= limit:
            reasons.append(f"{key} last attempt {int(age)}s ago (limit {limit}s, {row.last_status})")
    return reasons[0] if reasons else None


def _lock_key(job_name: str) -> int:
    digest = hashlib.sha256(f"grid.api_precompute:{job_name}".encode()).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


class _AdvisoryLock:
    """Session advisory lock on a dedicated connection (PostgreSQL only)."""

    def __init__(self, engine: Engine, name: str) -> None:
        self.engine = engine
        self.key = _lock_key(name)
        self.conn: Any = None

    def acquire(self) -> bool:
        if self.engine.dialect.name != "postgresql":
            return True
        self.conn = self.engine.connect()
        got = bool(self.conn.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": self.key}).scalar())
        self.conn.commit()  # session lock outlives the transaction; don't sit idle in one
        if not got:
            self.conn.close()
            self.conn = None
        return got

    def release(self) -> None:
        if self.conn is None:
            return
        try:
            self.conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": self.key})
            self.conn.commit()
            self.conn.close()
        except Exception as exc:  # noqa: BLE001 -- drop the session so the lock dies with it
            log.warning("api precompute: advisory unlock failed ({e}); invalidating connection", e=str(exc))
            try:
                self.conn.invalidate()
            except Exception as inv_exc:  # noqa: BLE001
                log.warning("api precompute: invalidate failed: {e}", e=str(inv_exc))
        finally:
            self.conn = None


def run_job(engine: Engine, job: Job, *, force: bool = False, now: datetime | None = None) -> dict[str, Any]:
    """Run one job (if due, or ``force``) and persist its outputs."""
    now = now or datetime.now(timezone.utc)
    summary: dict[str, Any] = {"job": job.name, "ran": False}
    rows = store.read_key_rows(engine, list(job.keys))
    reason = "forced" if force else due_reason(job, rows, now)
    if reason is None:
        summary["skipped"] = "not due"
        return summary

    signature: str | None = None
    if job.signature is not None:
        try:
            signature = job.signature(engine)
        except Exception as exc:  # noqa: BLE001 -- fall through to a full run
            log.warning("api precompute: {j} signature failed: {e}", j=job.name, e=str(exc))
        if not force and signature is not None and _signature_unchanged(job, rows, signature, now):
            for key, params in job.declared():
                _touch_attempt(engine, key, params)
            summary["skipped"] = f"inputs unchanged ({signature})"
            return summary

    lock = _AdvisoryLock(engine, job.name)
    if not lock.acquire():
        summary["skipped"] = "locked by another runner"
        return summary
    t0 = time.monotonic()
    try:
        ctx = BuildContext(engine=engine, now=now, signature=signature)
        outputs = job.build(ctx)
        duration = time.monotonic() - t0
        written, unavailable = _persist(engine, job, outputs, duration)
        summary.update({"ran": True, "reason": reason, "duration_s": round(duration, 2),
                        "written": written, "unavailable": unavailable})
        log.info("api precompute: {j} done in {d:.1f}s — {w} written, {u} unavailable ({r})",
                 j=job.name, d=duration, w=written, u=unavailable, r=reason)
    except Exception as exc:  # noqa: BLE001 -- one failing job must not stop the others
        duration = time.monotonic() - t0
        err = f"{type(exc).__name__}: {exc}"
        log.warning("api precompute: {j} failed after {d:.1f}s: {e}", j=job.name, d=duration, e=err)
        for key, params in job.declared():
            try:
                store.record_attempt_failure(engine, key=key, params=params, status=store.STATUS_ERROR,
                                             error=err, version=job.version, duration_s=duration)
            except Exception as rec_exc:  # noqa: BLE001
                log.warning("api precompute: could not record {j} failure: {e}", j=job.name, e=str(rec_exc))
        summary.update({"ran": True, "reason": reason, "duration_s": round(duration, 2), "error": err})
    finally:
        lock.release()
    return summary


def _touch_attempt(engine: Engine, key: str, params: Mapping[str, Any]) -> None:
    """Advance ``last_attempt_at`` on a skipped-unchanged run (status stays ok)."""
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE api_precomputed_results SET last_attempt_at = :now "
                "WHERE key = :key AND params_hash = :ph AND last_status = 'ok'"
            ),
            {"now": datetime.now(timezone.utc), "key": key, "ph": store.params_hash(params)},
        )


def _signature_unchanged(job: Job, rows: list[store.PrecomputedRow], signature: str, now: datetime) -> bool:
    by_slot = {(r.key, r.params_hash): r for r in rows}
    for key, params in job.declared():
        row = by_slot.get((key, store.params_hash(params)))
        if row is None or not row.has_payload or row.version != job.version:
            return False
        if (row.inputs or {}).get("signature") != signature:
            return False
        if (job.max_age_s is not None and row.computed_at is not None
                and (now - row.computed_at).total_seconds() >= job.max_age_s):
            return False
    return True


def _persist(engine: Engine, job: Job, outputs: list[Output], duration: float) -> tuple[int, int]:
    from store.availability import cacheable

    written = unavailable = 0
    for out in outputs:
        reason = out.unavailable_reason
        if reason is None and out.payload is None:
            reason = "builder returned no payload"
        if reason is None and out.payload is not None and not cacheable(out.payload).cache:
            reason = str(out.payload.get("reason") or "payload marked unavailable")
        if reason is not None:
            store.record_attempt_failure(engine, key=out.key, params=out.params,
                                         status=store.STATUS_UNAVAILABLE, error=reason,
                                         version=job.version, duration_s=duration)
            unavailable += 1
            continue
        store.write_success(engine, key=out.key, params=out.params, payload=out.payload,
                            as_of=out.as_of, version=job.version, inputs=out.inputs,
                            duration_s=duration)
        written += 1
    return written, unavailable


def run_due(
    engine: Engine,
    *,
    only: list[str] | None = None,
    force: bool = False,
) -> list[dict[str, Any]]:
    """Run every due job (or the named ones), in registry order."""
    results: list[dict[str, Any]] = []
    for job in JOBS:
        if only and job.name not in only:
            continue
        try:
            results.append(run_job(engine, job, force=force))
        except Exception as exc:  # noqa: BLE001 -- e.g. table missing before the migration
            log.warning("api precompute: {j} could not run: {e}", j=job.name, e=str(exc))
            results.append({"job": job.name, "ran": False, "error": str(exc)})
    return results


# ---------------------------------------------------------------------------
# Scheduler hook
# ---------------------------------------------------------------------------

_worker_lock = threading.Lock()
_worker: threading.Thread | None = None


def enabled() -> bool:
    """``GRID_API_PRECOMPUTE_ENABLED`` (config.py), read fresh each tick."""
    from config import Settings

    return bool(Settings().GRID_API_PRECOMPUTE_ENABLED)


def start_background_tick(engine_factory: Callable[[], Engine] | None = None) -> bool:
    """Start one worker thread running :func:`run_due` unless one is running.

    Returns ``True`` if a worker was started. Called every
    :data:`TICK_MINUTES` from ``intelligence/scheduler.py``.
    """
    global _worker
    if not enabled():
        return False
    with _worker_lock:
        if _worker is not None and _worker.is_alive():
            return False

        def _run() -> None:
            try:
                if engine_factory is None:
                    from db import get_engine
                    eng = get_engine()
                else:
                    eng = engine_factory()
                run_due(eng)
            except Exception as exc:  # noqa: BLE001
                log.warning("api precompute tick failed: {e}", e=str(exc))

        _worker = threading.Thread(target=_run, name="api-precompute", daemon=True)
        _worker.start()
        return True


def describe() -> list[dict[str, Any]]:
    """Registry summary for ``scripts/run_api_precompute.py --list``."""
    return [
        {
            "job": j.name,
            "keys": list(j.keys),
            "cadence_s": j.cadence_s,
            "stale_after_s": j.stale_after_s,
            "version": j.version,
            "signature": j.signature is not None,
            "description": j.description,
        }
        for j in JOBS
    ]


__all__ = [
    "JOBS",
    "JOBS_BY_NAME",
    "BuildContext",
    "Job",
    "Output",
    "describe",
    "due_reason",
    "job_for_key",
    "run_due",
    "run_job",
    "start_background_tick",
]
