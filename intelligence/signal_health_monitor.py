"""
GRID Signal Health Monitor — "is the signal even still alive?" check.

Per the user-memory rule: "Always label confirmed/derived/estimated/rumored/
inferred." A stale, NaN-poisoned, or distribution-drifted feed is none of those
— it is *broken*, and the conviction dial must know. This module is the third
confidence amplifier in the batch and the structural complement to
``features/per_signal_brier.py``:

    per_signal_brier  → "Is the signal predictive?"   (calibration / accuracy)
    signal_health     → "Is the signal even alive?"   (freshness / shape / drift)

Both reports flow into ``intelligence/signal_provenance.py`` so the conviction
dial automatically dampens or silences signals that have rotted upstream.

The four checks per ``series_id`` in ``raw_series``:

    1. Freshness    — last observation vs the puller's expected cadence.
    2. Row count    — recent volume vs the cadence-derived expectation.
    3. NaN rate     — fraction of recent observations that are NULL/NaN.
    4. Schema drift — z-score of the latest value against trailing 90d.

Every check rolls up to a four-bucket health: ``green / yellow / orange / red``,
and the worst status determines the overall conviction dampening multiplier
(green=1.0, yellow=0.85, orange=0.6, red=0.0 → silently ignored).

Public surface:
    classify_staleness / classify_nan_rate / classify_drift / combine_status
        — pure helpers, no DB.
    match_cadence
        — longest-prefix lookup over EXPECTED_CADENCE_BY_PREFIX.
    audit_one_series / audit_all_series
        — DB-touching audit functions, every path wrapped in try/except.
    get_signal_dampening
        — the conviction-dial consumer's entry point.
    persist_report / ensure_health_table
        — historical tracking for the dashboard.

DB philosophy: never raise. A failed audit returns a defensive ``red``
SignalHealth so a broken puller is never silently trusted.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, asdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Constants ─────────────────────────────────────────────────────────────

# Expected days between observations, keyed by series_id prefix. Longest
# matching prefix wins via ``match_cadence``. Default cadence = 1 day.
EXPECTED_CADENCE_BY_PREFIX: dict[str, int] = {
    "fed_h8:":              7,    # weekly Fed H.8 commercial bank assets
    "mmf:":                 7,    # weekly money market fund flows
    "treasury_auction:":    1,    # daily Treasury auctions
    "wage_tracker:":        30,   # monthly wage tracker
    "freight_cass_ata:":    30,   # monthly Cass freight + ATA tonnage
    "cot:":                 7,    # weekly Commitments of Traders
    "8k_clusters:":         1,    # daily 8-K filing clusters
    "fci:":                 1,    # daily Financial Conditions Index
    "credit_event_pd:":     1,    # daily credit-event probability of default
    "hmm_regime:":          1,    # daily HMM regime classifier
    "thesis_invalidation:": 1,    # daily thesis invalidation watch
    "credit_novelty:":      1,    # daily credit-novelty detector
    "sector_network:":      1,    # daily sector network refresh
    "refinery_cracks:":     7,    # weekly refinery crack spreads
    "credit_card:":         7,    # weekly credit card spend
    "buybacks:":            90,   # quarterly buyback announcements
    "semi:":                30,   # monthly semiconductor sales
    "ecb_tltro:":           7,    # weekly ECB TLTRO drawdown
    "pboc:":                1,    # daily PBOC OMO and balance sheet
    "taiwan:":              30,   # monthly Taiwan macro
    "freight:":             7,    # weekly freight rates (Drewry / Baltic)
    "lme:":                 1,    # daily LME metal warehouses
    "iron_ore:":            7,    # weekly iron ore
    "taiwan_strait:":       1,    # daily Taiwan Strait incidents
    "credit_proxy:":        1,    # daily credit proxy stack
    "ais:":                 1,    # 4h cadence aggregated to daily
    "social_port:":         1,    # daily social port mentions
    "jodi:":                30,   # monthly JODI oil
    "sge:":                 1,    # daily SGE gold withdrawals
    "reddit_options:":      1,    # daily Reddit options chatter
}

# Staleness multipliers: yellow at 1.5×, orange at 3×, red at 7× expected cadence.
STALENESS_YELLOW_MULT: float = 1.5
STALENESS_ORANGE_MULT: float = 3.0
STALENESS_RED_MULT: float = 7.0

# NaN-rate buckets.
NAN_RATE_YELLOW: float = 0.10
NAN_RATE_ORANGE: float = 0.30
NAN_RATE_RED: float = 0.50

# Drift z-score buckets (no orange — three buckets only).
DRIFT_Z_YELLOW: float = 2.5
DRIFT_Z_RED: float = 4.0

# Status order — used by combine_status (worst-wins).
_STATUS_ORDER: dict[str, int] = {"green": 0, "yellow": 1, "orange": 2, "red": 3}
_ORDER_TO_STATUS: dict[int, str] = {v: k for k, v in _STATUS_ORDER.items()}

# Conviction dampening curve.
_DAMPENING_BY_STATUS: dict[str, float] = {
    "green": 1.0,
    "yellow": 0.85,
    "orange": 0.6,
    "red": 0.0,
}


# ── Data Classes ──────────────────────────────────────────────────────────

@dataclass(frozen=True)
class SignalHealth:
    """Health audit for a single ``series_id`` in raw_series."""

    series_id: str
    last_observation: date | None
    days_since_last: int | None
    expected_cadence_days: int
    staleness_status: str

    recent_row_count: int
    expected_row_count: int

    nan_rate: float
    nan_status: str

    drift_zscore: float | None
    drift_status: str

    overall_status: str
    conviction_dampening: float
    generated_at: str

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        if isinstance(d.get("last_observation"), date):
            d["last_observation"] = d["last_observation"].isoformat()
        return d


@dataclass(frozen=True)
class SignalHealthReport:
    """Aggregated health report across every series_id in raw_series."""

    generated_at: str
    total_series: int
    by_status: dict[str, int]
    by_namespace: dict[str, dict[str, int]]
    unhealthy: list[SignalHealth]
    summary: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "total_series": self.total_series,
            "by_status": dict(self.by_status),
            "by_namespace": {ns: dict(buckets) for ns, buckets in self.by_namespace.items()},
            "unhealthy": [h.to_dict() for h in self.unhealthy],
            "summary": self.summary,
        }


# ── Pure Helpers ──────────────────────────────────────────────────────────

def match_cadence(series_id: str, lookup: dict[str, int]) -> int:
    """Return the expected cadence (in days) for a series_id.

    Resolution rule: pick the LONGEST prefix in ``lookup`` that ``series_id``
    starts with. So for ``pboc:omo:7d`` with only ``pboc:`` registered, this
    returns the ``pboc:`` cadence. If both ``pboc:`` and ``pboc:omo:`` are
    registered, the longer ``pboc:omo:`` wins. Default = 1 day.
    """
    best_prefix = ""
    for prefix in lookup:
        if series_id.startswith(prefix) and len(prefix) > len(best_prefix):
            best_prefix = prefix
    if best_prefix:
        return lookup[best_prefix]
    return 1


def classify_staleness(days_since_last: int | None, expected_cadence: int) -> str:
    """Classify staleness into green/yellow/orange/red.

    None (no observations ever) → red. 0 days fresh → green. Otherwise
    compare to multiples of the expected cadence.
    """
    if days_since_last is None:
        return "red"
    if expected_cadence <= 0:
        expected_cadence = 1
    ratio = days_since_last / expected_cadence
    if ratio >= STALENESS_RED_MULT:
        return "red"
    if ratio >= STALENESS_ORANGE_MULT:
        return "orange"
    if ratio >= STALENESS_YELLOW_MULT:
        return "yellow"
    return "green"


def classify_nan_rate(nan_rate: float) -> str:
    """Classify the NaN/null fraction into green/yellow/orange/red."""
    if nan_rate is None or math.isnan(nan_rate):
        return "red"
    if nan_rate >= NAN_RATE_RED:
        return "red"
    if nan_rate >= NAN_RATE_ORANGE:
        return "orange"
    if nan_rate >= NAN_RATE_YELLOW:
        return "yellow"
    return "green"


def classify_drift(zscore: float | None) -> str:
    """Classify drift z-score into a three-bucket status (no orange)."""
    if zscore is None:
        return "green"
    try:
        if math.isnan(zscore) or math.isinf(zscore):
            return "green"
    except TypeError:
        return "green"
    abs_z = abs(zscore)
    if abs_z >= DRIFT_Z_RED:
        return "red"
    if abs_z >= DRIFT_Z_YELLOW:
        return "yellow"
    return "green"


def combine_status(*statuses: str) -> str:
    """Return the worst status (red > orange > yellow > green)."""
    if not statuses:
        return "green"
    worst_rank = -1
    for s in statuses:
        rank = _STATUS_ORDER.get(s, -1)
        if rank > worst_rank:
            worst_rank = rank
    if worst_rank < 0:
        return "green"
    return _ORDER_TO_STATUS[worst_rank]


def dampening_for_status(status: str) -> float:
    """Map a status to its conviction-dampening multiplier in [0.0, 1.0]."""
    return _DAMPENING_BY_STATUS.get(status, 0.0)


def compose_summary(report: "SignalHealthReport") -> str:
    """Format a one-line summary like '12 green, 3 yellow, 1 orange, 0 red across 16 series'."""
    by = report.by_status
    return (
        f"{by.get('green', 0)} green, "
        f"{by.get('yellow', 0)} yellow, "
        f"{by.get('orange', 0)} orange, "
        f"{by.get('red', 0)} red across {report.total_series} series"
    )


def _namespace_of(series_id: str) -> str:
    """Extract the namespace prefix from a series_id ('pboc:omo:7d' → 'pboc')."""
    if ":" in series_id:
        return series_id.split(":", 1)[0]
    return series_id


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_red_health(series_id: str, *, reason: str) -> SignalHealth:
    """Construct a defensive 'red' SignalHealth for an audit error path."""
    cadence = match_cadence(series_id, EXPECTED_CADENCE_BY_PREFIX)
    return SignalHealth(
        series_id=series_id,
        last_observation=None,
        days_since_last=None,
        expected_cadence_days=cadence,
        staleness_status="red",
        recent_row_count=0,
        expected_row_count=0,
        nan_rate=1.0,
        nan_status="red",
        drift_zscore=None,
        drift_status="green",
        overall_status="red",
        conviction_dampening=0.0,
        generated_at=_now_iso(),
    )


# ── Table Setup ───────────────────────────────────────────────────────────

_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS signal_health_history (
    id                    BIGSERIAL PRIMARY KEY,
    series_id             TEXT NOT NULL,
    namespace             TEXT,
    last_observation      DATE,
    days_since_last       INTEGER,
    expected_cadence_days INTEGER,
    staleness_status      TEXT,
    recent_row_count      INTEGER,
    expected_row_count    INTEGER,
    nan_rate              DOUBLE PRECISION,
    nan_status            TEXT,
    drift_zscore          DOUBLE PRECISION,
    drift_status          TEXT,
    overall_status        TEXT,
    conviction_dampening  DOUBLE PRECISION,
    generated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_signal_health_series
    ON signal_health_history (series_id, generated_at DESC);
CREATE INDEX IF NOT EXISTS idx_signal_health_status
    ON signal_health_history (overall_status, generated_at DESC);
"""


def ensure_health_table(engine: Engine) -> None:
    """Create the ``signal_health_history`` table if it does not exist."""
    try:
        with engine.begin() as conn:
            for stmt in _TABLE_SQL.strip().split(";"):
                stmt = stmt.strip()
                if stmt:
                    conn.execute(text(stmt))
        log.info("signal_health_history table ensured")
    except Exception as exc:
        log.debug("ensure_health_table failed: {e}", e=str(exc))


# ── DB Probes ─────────────────────────────────────────────────────────────

def _fetch_series_stats(
    engine: Engine, series_id: str, *, lookback_days: int
) -> dict[str, Any]:
    """Fetch the four signals needed for an audit in a single round trip.

    Returns:
        last_observation: most recent obs_date (date | None)
        recent_row_count: rows in the lookback window
        nan_count: rows in the lookback window where value IS NULL
        latest_value: most recent non-null value (float | None)
        history_mean / history_std: trailing statistics excluding the latest row
    """
    cutoff = date.today() - timedelta(days=lookback_days)
    out: dict[str, Any] = {
        "last_observation": None,
        "recent_row_count": 0,
        "nan_count": 0,
        "latest_value": None,
        "history_mean": None,
        "history_std": None,
    }

    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT MAX(obs_date) AS last_obs, "
                "COUNT(*) AS row_count, "
                "SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) AS nan_count "
                "FROM raw_series "
                "WHERE series_id = :sid AND obs_date >= :cutoff"
            ),
            {"sid": series_id, "cutoff": cutoff},
        ).fetchone()
        if row is not None:
            out["last_observation"] = row[0]
            out["recent_row_count"] = int(row[1] or 0)
            out["nan_count"] = int(row[2] or 0)

        latest = conn.execute(
            text(
                "SELECT value FROM raw_series "
                "WHERE series_id = :sid AND value IS NOT NULL "
                "ORDER BY obs_date DESC LIMIT 1"
            ),
            {"sid": series_id},
        ).fetchone()
        if latest is not None and latest[0] is not None:
            out["latest_value"] = float(latest[0])

        stats = conn.execute(
            text(
                "SELECT AVG(value) AS mean, STDDEV_SAMP(value) AS std "
                "FROM raw_series "
                "WHERE series_id = :sid "
                "AND obs_date >= :cutoff "
                "AND value IS NOT NULL"
            ),
            {"sid": series_id, "cutoff": cutoff},
        ).fetchone()
        if stats is not None:
            out["history_mean"] = float(stats[0]) if stats[0] is not None else None
            out["history_std"] = float(stats[1]) if stats[1] is not None else None

    return out


def _list_all_series_ids(engine: Engine) -> list[str]:
    """List every distinct ``series_id`` in raw_series."""
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT DISTINCT series_id FROM raw_series ORDER BY series_id")
        ).fetchall()
    return [r[0] for r in rows if r[0]]


# ── Audit ─────────────────────────────────────────────────────────────────

def audit_one_series(
    engine: Engine, series_id: str, *, lookback_days: int = 90
) -> SignalHealth:
    """Run all four health checks against one series_id.

    Defensive: any DB error returns a ``red`` SignalHealth so a broken
    puller is never silently trusted by the conviction dial.
    """
    try:
        cadence = match_cadence(series_id, EXPECTED_CADENCE_BY_PREFIX)
        stats = _fetch_series_stats(engine, series_id, lookback_days=lookback_days)

        last_obs = stats["last_observation"]
        if last_obs is None:
            days_since = None
        else:
            days_since = (date.today() - last_obs).days

        # Expected row count over the lookback window, given the cadence.
        # Floor at 1 to avoid divide-by-zero on tiny windows.
        expected_rows = max(1, lookback_days // max(1, cadence))

        recent_rows = int(stats["recent_row_count"])
        nan_count = int(stats["nan_count"])
        nan_rate = (nan_count / recent_rows) if recent_rows > 0 else (
            0.0 if last_obs is not None else 1.0
        )

        # Drift z-score: latest value vs trailing window mean / std.
        latest = stats["latest_value"]
        mean = stats["history_mean"]
        std = stats["history_std"]
        if (
            latest is not None
            and mean is not None
            and std is not None
            and std > 1e-12
        ):
            drift_z: float | None = (latest - mean) / std
        else:
            drift_z = None

        staleness = classify_staleness(days_since, cadence)
        nan_status = classify_nan_rate(nan_rate)
        drift_status = classify_drift(drift_z)
        overall = combine_status(staleness, nan_status, drift_status)
        dampening = dampening_for_status(overall)

        return SignalHealth(
            series_id=series_id,
            last_observation=last_obs,
            days_since_last=days_since,
            expected_cadence_days=cadence,
            staleness_status=staleness,
            recent_row_count=recent_rows,
            expected_row_count=expected_rows,
            nan_rate=float(nan_rate),
            nan_status=nan_status,
            drift_zscore=drift_z,
            drift_status=drift_status,
            overall_status=overall,
            conviction_dampening=dampening,
            generated_at=_now_iso(),
        )
    except Exception as exc:
        log.debug(
            "audit_one_series({sid}) failed: {e}", sid=series_id, e=str(exc)
        )
        return _build_red_health(series_id, reason=str(exc))


def audit_all_series(engine: Engine) -> SignalHealthReport:
    """Discover every series_id in raw_series and audit each."""
    try:
        series_ids = _list_all_series_ids(engine)
    except Exception as exc:
        log.debug("audit_all_series discovery failed: {e}", e=str(exc))
        series_ids = []

    healths: list[SignalHealth] = []
    by_status: dict[str, int] = {"green": 0, "yellow": 0, "orange": 0, "red": 0}
    by_namespace: dict[str, dict[str, int]] = {}

    for sid in series_ids:
        h = audit_one_series(engine, sid)
        healths.append(h)
        by_status[h.overall_status] = by_status.get(h.overall_status, 0) + 1
        ns = _namespace_of(sid)
        ns_buckets = by_namespace.setdefault(
            ns, {"green": 0, "yellow": 0, "orange": 0, "red": 0}
        )
        ns_buckets[h.overall_status] = ns_buckets.get(h.overall_status, 0) + 1

    unhealthy = [h for h in healths if h.overall_status != "green"]

    report = SignalHealthReport(
        generated_at=_now_iso(),
        total_series=len(series_ids),
        by_status=by_status,
        by_namespace=by_namespace,
        unhealthy=unhealthy,
        summary="",
    )
    # Compose summary against the populated report.
    summary = compose_summary(report)
    return SignalHealthReport(
        generated_at=report.generated_at,
        total_series=report.total_series,
        by_status=report.by_status,
        by_namespace=report.by_namespace,
        unhealthy=report.unhealthy,
        summary=summary,
    )


def get_signal_dampening(
    engine: Engine, series_id: str, *, max_age_hours: int = 4
) -> float:
    """Return the conviction-dial dampening multiplier for a series_id.

    Reads the most recent ``signal_health_history`` audit; if none within
    ``max_age_hours``, runs ``audit_one_series`` on demand. On any error
    returns 1.0 so a broken health monitor doesn't accidentally silence
    every signal.
    """
    try:
        with engine.connect() as conn:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
            row = conn.execute(
                text(
                    "SELECT conviction_dampening, overall_status, generated_at "
                    "FROM signal_health_history "
                    "WHERE series_id = :sid AND generated_at >= :cutoff "
                    "ORDER BY generated_at DESC LIMIT 1"
                ),
                {"sid": series_id, "cutoff": cutoff},
            ).fetchone()
        if row is not None and row[0] is not None:
            return float(row[0])
    except Exception as exc:
        log.debug(
            "get_signal_dampening cache lookup failed for {sid}: {e}",
            sid=series_id, e=str(exc),
        )

    # Probe the underlying source directly so a hard DB failure short-circuits
    # to the safe default (1.0) rather than the defensive red (0.0) that
    # ``audit_one_series`` would emit. Without this, a transient outage of
    # raw_series would silence every signal in the conviction dial.
    try:
        _fetch_series_stats(engine, series_id, lookback_days=90)
    except Exception as exc:
        log.debug(
            "get_signal_dampening probe failed for {sid}: {e}",
            sid=series_id, e=str(exc),
        )
        return 1.0

    try:
        health = audit_one_series(engine, series_id)
        return float(health.conviction_dampening)
    except Exception as exc:
        log.debug(
            "get_signal_dampening on-demand audit failed for {sid}: {e}",
            sid=series_id, e=str(exc),
        )
        return 1.0


def persist_report(engine: Engine, report: SignalHealthReport) -> int:
    """Persist the unhealthy entries from a report. Returns rows written."""
    if not report.unhealthy:
        return 0
    written = 0
    try:
        with engine.begin() as conn:
            for h in report.unhealthy:
                conn.execute(
                    text(
                        "INSERT INTO signal_health_history ("
                        "  series_id, namespace, last_observation, days_since_last, "
                        "  expected_cadence_days, staleness_status, recent_row_count, "
                        "  expected_row_count, nan_rate, nan_status, drift_zscore, "
                        "  drift_status, overall_status, conviction_dampening, generated_at"
                        ") VALUES ("
                        "  :series_id, :namespace, :last_observation, :days_since_last, "
                        "  :expected_cadence_days, :staleness_status, :recent_row_count, "
                        "  :expected_row_count, :nan_rate, :nan_status, :drift_zscore, "
                        "  :drift_status, :overall_status, :conviction_dampening, :generated_at"
                        ")"
                    ),
                    {
                        "series_id": h.series_id,
                        "namespace": _namespace_of(h.series_id),
                        "last_observation": h.last_observation,
                        "days_since_last": h.days_since_last,
                        "expected_cadence_days": h.expected_cadence_days,
                        "staleness_status": h.staleness_status,
                        "recent_row_count": h.recent_row_count,
                        "expected_row_count": h.expected_row_count,
                        "nan_rate": h.nan_rate,
                        "nan_status": h.nan_status,
                        "drift_zscore": h.drift_zscore,
                        "drift_status": h.drift_status,
                        "overall_status": h.overall_status,
                        "conviction_dampening": h.conviction_dampening,
                        "generated_at": h.generated_at,
                    },
                )
                written += 1
    except Exception as exc:
        log.debug("persist_report failed after {n} rows: {e}", n=written, e=str(exc))
    return written


__all__ = [
    "EXPECTED_CADENCE_BY_PREFIX",
    "STALENESS_YELLOW_MULT",
    "STALENESS_ORANGE_MULT",
    "STALENESS_RED_MULT",
    "NAN_RATE_YELLOW",
    "NAN_RATE_ORANGE",
    "NAN_RATE_RED",
    "DRIFT_Z_YELLOW",
    "DRIFT_Z_RED",
    "SignalHealth",
    "SignalHealthReport",
    "match_cadence",
    "classify_staleness",
    "classify_nan_rate",
    "classify_drift",
    "combine_status",
    "dampening_for_status",
    "compose_summary",
    "ensure_health_table",
    "audit_one_series",
    "audit_all_series",
    "get_signal_dampening",
    "persist_report",
]
