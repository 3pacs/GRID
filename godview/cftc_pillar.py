"""godview.cftc_pillar — CFTC positioning God View pillar (W6, first slice).

Source adapter: reuses ``ingestion/altdata/cftc_cot.py`` (read-only) as the
adapter that already pulls CFTC Commitments of Traders (COT) data into
``raw_series`` under ``cftc.<CONTRACT>.<metric>`` series ids. This module does
not re-pull anything from the network -- it only reads what that puller
already wrote, exactly like ``store/pit.py`` reads ``resolved_series``.

Full contract: docs/reference/GODVIEW_PILLAR_CONTRACT.md. The short version:

* ``report_date`` is always a Tuesday (the CFTC's own observation cadence).
* ``release_date`` is set to ``report_date + 3 days`` (the Friday) ONLY when
  ``report_date`` really is a Tuesday, per the CFTC's published release
  schedule (quoted in the contract doc, section 2). Anything else leaves
  ``release_date`` NULL -- quarantined from strict-PIT reads, never deleted.
* Raw positioning fields are ``provenance="measured"``; z-scores and the
  crowding regime are ``provenance="derived"`` over an explicit trailing
  window (52 weekly obs for 1y, 156 for 3y -- the same constants
  ``intelligence/cot_extremes.py`` uses, imported rather than re-picked).
* One materializer run = one ``generation_id``. Publication is one DB
  transaction (see ``godview/generations.py`` and the contract doc section 7).
* Failed or completely-empty upstream never advances the generation.
* Re-running against unchanged upstream data is a no-op (idempotent):
  zero new rows, a fresh ``complete`` generation with ``row_count=0``.

Everything above the "DB wrappers" marker is pure Python: no engine, no I/O,
safe to unit test with constructed fixtures.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Sequence

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from godview.generations import (
    STATUS_COMPLETE,
    STATUS_FAILED,
    latest_attempt as _latest_attempt,
    latest_complete_generation as _latest_complete_generation,
    new_generation_id,
    record_generation,
)
from ingestion.altdata.cftc_cot import _FIELD_MAP  # noqa: SLF001 -- documented public shape, reused not modified
from intelligence.cot_extremes import (
    _PERCENTILE_WINDOW as PERCENTILE_WINDOW_3Y,  # noqa: SLF001
    _Z_SCORE_WINDOW as Z_SCORE_WINDOW_1Y,  # noqa: SLF001
)
from store.availability import measured_or_none

# ---------------------------------------------------------------------------
# Pillar identity and contract constants
# ---------------------------------------------------------------------------

PILLAR_NAME = "cftc_positioning"

#: The four contracts this v1 slice tracks. Chosen to match the contract_code
#: values market_god_view_daily (god_view_market_tables_20260918) already
#: joins by -- "the CFTC contracts God View knows about" is one list, not two.
CFTC_PILLAR_CONTRACTS: dict[str, dict[str, str]] = {
    "SP500": {"contract_code": "ES", "contract_name": "E-mini S&P 500", "asset_class": "equity_index"},
    "NOTE10Y": {"contract_code": "ZN", "contract_name": "10-Year T-Note", "asset_class": "rates"},
    "GOLD": {"contract_code": "GC", "contract_name": "Gold", "asset_class": "metals"},
    "CRUDE_OIL": {"contract_code": "CL", "contract_name": "WTI Crude Oil", "asset_class": "energy"},
}

#: The raw metrics that must ALL be present for a (contract, report_date) to
#: be insertable -- cftc_positioning_daily's raw columns are NOT NULL.
REQUIRED_METRICS: tuple[str, ...] = (
    "commercial_long",
    "commercial_short",
    "noncommercial_long",
    "noncommercial_short",
    "total_open_interest",
)

#: Weekly observations required before a z-score/percentile is even attempted.
_MIN_HISTORY = 8

#: See contract doc section 5 (cadence 7d + up-to-3d publication lag = 10d).
STALE_AFTER_DAYS = 10

#: Crowding-regime thresholds on percentile_3y (0..100), same cut points as
#: intelligence/cot_extremes.py's _PCTILE_ELEVATED/_PCTILE_EXTREME.
_PCTILE_ELEVATED = 85
_PCTILE_EXTREME = 95

RELEASE_RULE_ID = "cftc_release_schedule_v1"
_RELEASE_RULE_URL = "https://www.cftc.gov/MarketReports/CommitmentsofTraders/ReleaseSchedule/index.htm"


def _build_series_id(contract_key: str, metric: str) -> str:
    """Mirror ingestion/altdata/cftc_cot.py's series_id format exactly."""
    return f"cftc.{contract_key}.{metric}"


# ---------------------------------------------------------------------------
# Pure: parsing / validating the source adapter's documented record format
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ParsedCOTRow:
    """One (contract, report_date) worth of raw metrics parsed from a CFTC API record."""

    report_date: date
    market_name: str
    metrics: dict[str, float]  # only the metrics that parsed cleanly


def parse_cot_record(record: dict[str, Any]) -> ParsedCOTRow | None:
    """Parse one CFTC Socrata API record into a ``ParsedCOTRow``.

    Pure re-implementation of the parsing ``ingestion/altdata/cftc_cot.py``'s
    ``CFTCCOTPuller._parse_report_date``/``_extract_metrics`` already do, using
    that module's own ``_FIELD_MAP`` so the two never silently diverge on
    field names. Returns ``None`` when the record has no parseable report
    date -- that record contributes nothing, it is not an error.
    """
    raw_date = record.get("report_date_as_yyyy_mm_dd")
    if raw_date is None:
        return None
    try:
        report_date = date.fromisoformat(str(raw_date)[:10])
    except ValueError:
        return None

    metrics: dict[str, float] = {}
    for metric_name, field_name in _FIELD_MAP.items():
        raw_val = record.get(field_name)
        if raw_val is None:
            continue
        parsed = measured_or_none(raw_val)
        if parsed is not None:
            metrics[metric_name] = parsed

    return ParsedCOTRow(
        report_date=report_date,
        market_name=str(record.get("market_and_exchange_names", "")),
        metrics=metrics,
    )


@dataclass(frozen=True)
class FixtureValidationResult:
    total_records: int
    valid_records: int
    invalid_reasons: dict[str, int]  # reason -> count
    valid_report_dates: list[date]

    @property
    def all_valid(self) -> bool:
        return self.valid_records == self.total_records and self.total_records > 0


def validate_fixture(records: Sequence[dict[str, Any]]) -> FixtureValidationResult:
    """Validate a constructed CFTC COT record set (never a downloaded one).

    A record is valid when: it parses to a report_date, every
    ``REQUIRED_METRICS`` field parsed to a finite number, no long/short/OI
    value is negative, and ``commercial_long+commercial_short`` /
    ``noncommercial_long+noncommercial_short`` do not each individually
    exceed ``total_open_interest`` (a CFTC report never shows one side of one
    trader class alone outnumbering total open interest -- that would mean a
    parsing error, not real positioning).
    """
    invalid_reasons: dict[str, int] = {}
    valid_dates: list[date] = []

    def _bump(reason: str) -> None:
        invalid_reasons[reason] = invalid_reasons.get(reason, 0) + 1

    for record in records:
        parsed = parse_cot_record(record)
        if parsed is None:
            _bump("unparseable_report_date")
            continue

        missing = [m for m in REQUIRED_METRICS if m not in parsed.metrics]
        if missing:
            _bump(f"missing_metric:{missing[0]}")
            continue

        m = parsed.metrics
        if any(m[metric] < 0 for metric in REQUIRED_METRICS):
            _bump("negative_value")
            continue

        oi = m["total_open_interest"]
        if oi <= 0:
            _bump("non_positive_open_interest")
            continue

        if m["commercial_long"] > oi or m["commercial_short"] > oi:
            _bump("commercial_exceeds_open_interest")
            continue
        if m["noncommercial_long"] > oi or m["noncommercial_short"] > oi:
            _bump("noncommercial_exceeds_open_interest")
            continue

        valid_dates.append(parsed.report_date)

    total = len(records)
    valid = total - sum(invalid_reasons.values())
    return FixtureValidationResult(
        total_records=total,
        valid_records=valid,
        invalid_reasons=invalid_reasons,
        valid_report_dates=sorted(valid_dates),
    )


# ---------------------------------------------------------------------------
# Pure: release-date rule (contract doc section 8)
# ---------------------------------------------------------------------------

_TUESDAY = 1  # date.weekday(): Monday=0 ... Sunday=6


def compute_release_date(report_date: date) -> tuple[date | None, str]:
    """Apply the documented CFTC release-schedule rule to one report_date.

    Returns ``(release_date, source_ref)``. ``release_date`` is ``None`` when
    ``report_date`` is not a Tuesday -- the row is still built by the caller,
    just quarantined from strict-PIT reads until/unless a corrected
    report_date arrives (rows are never deleted; see contract doc section 8).
    """
    if report_date.weekday() == _TUESDAY:
        release_date = date.fromordinal(report_date.toordinal() + 3)
        source_ref = (
            f"{RELEASE_RULE_ID}: report_date is Tuesday -> release_date = "
            f"report_date + 3d (Friday 15:30 ET, {_RELEASE_RULE_URL})"
        )
        return release_date, source_ref

    source_ref = (
        f"{RELEASE_RULE_ID}: report_date {report_date.isoformat()} "
        f"({report_date.strftime('%A')}) is not Tuesday; release_date withheld "
        "per quarantine rule"
    )
    return None, source_ref


# ---------------------------------------------------------------------------
# Pure: z-score / percentile / crowding-regime math
# ---------------------------------------------------------------------------


def compute_zscore(history: Sequence[float], window: int) -> float | None:
    """Z-score of ``history[-1]`` against the trailing ``window`` (or fewer, if short).

    ``None`` when there are fewer than ``_MIN_HISTORY`` finite observations,
    or the trailing window's stdev is (numerically) zero.
    """
    finite = [v for v in history if v is not None]
    if len(finite) < _MIN_HISTORY:
        return None
    windowed = finite[-window:] if len(finite) >= window else finite
    current = windowed[-1]
    mean = sum(windowed) / len(windowed)
    if len(windowed) < 2:
        return None
    variance = sum((v - mean) ** 2 for v in windowed) / (len(windowed) - 1)
    std = variance**0.5
    if std <= 1e-9:
        return None
    return (current - mean) / std


def compute_percentile(history: Sequence[float], window: int) -> float | None:
    """Percentile rank (0..100) of ``history[-1]`` within the trailing ``window``."""
    finite = [v for v in history if v is not None]
    if len(finite) < _MIN_HISTORY:
        return None
    windowed = finite[-window:] if len(finite) >= window else finite
    current = windowed[-1]
    below = sum(1 for v in windowed if v < current)
    equal = sum(1 for v in windowed if v == current)
    return (below + 0.5 * equal) / len(windowed) * 100.0


def coverage_fraction_for_window(n_obs: int, window: int = PERCENTILE_WINDOW_3Y) -> float:
    """Fraction of the ideal derived-computation window actually available, capped at 1.0."""
    if window <= 0:
        return 0.0
    return min(1.0, max(0.0, n_obs / window))


def classify_crowding(percentile_3y: float | None) -> str:
    """Crowding regime from the 3y percentile rank. Always returns a value (column is NOT NULL)."""
    if percentile_3y is None:
        return "insufficient_history"
    if percentile_3y >= _PCTILE_EXTREME:
        return "extreme_long_crowding"
    if percentile_3y >= _PCTILE_ELEVATED:
        return "elevated_long_crowding"
    if percentile_3y <= (100 - _PCTILE_EXTREME):
        return "extreme_short_crowding"
    if percentile_3y <= (100 - _PCTILE_ELEVATED):
        return "elevated_short_crowding"
    return "neutral"


# ---------------------------------------------------------------------------
# Materialization result type
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MaterializationResult:
    status: str  # "SUCCESS" | "SUCCESS_NOOP" | "EMPTY" | "FAILED"
    generation_id: str
    rows_written: int = 0
    contracts_with_data: int = 0
    contracts_expected: int = field(default_factory=lambda: len(CFTC_PILLAR_CONTRACTS))
    message: str = ""

    @property
    def coverage_fraction(self) -> float | None:
        if self.contracts_expected == 0:
            return None
        return self.contracts_with_data / self.contracts_expected


# ---------------------------------------------------------------------------
# DB wrappers -- everything below touches a Connection/Engine.
# ---------------------------------------------------------------------------


def _read_contract_history(
    conn: Connection, contract_key: str, as_of: date
) -> dict[date, dict[str, Any]]:
    """PIT-style (LATEST_AS_OF) read of one contract's raw metrics from raw_series.

    Mirrors store/pit.py's DISTINCT ON / ORDER BY vintage-DESC shape, applied
    to raw_series (which has no release_date of its own -- only pull_timestamp)
    instead of resolved_series. For each (series_id, obs_date), the row with
    the latest pull_timestamp wins; only obs_date <= as_of is considered.

    Returns ``{report_date: {metric_name: value, "_pull_ts": {metric_name: dt}}}``.
    """
    series_ids = [_build_series_id(contract_key, metric) for metric in REQUIRED_METRICS]
    rows = conn.execute(
        text(
            """
            SELECT DISTINCT ON (series_id, obs_date)
                series_id, obs_date, value, pull_timestamp
            FROM raw_series
            WHERE series_id = ANY(:sids)
              AND obs_date <= :as_of
              AND pull_status = 'SUCCESS'
            ORDER BY series_id, obs_date, pull_timestamp DESC
            """
        ),
        {"sids": series_ids, "as_of": as_of},
    ).mappings().all()

    metric_by_series = {
        _build_series_id(contract_key, metric): metric for metric in REQUIRED_METRICS
    }

    out: dict[date, dict[str, Any]] = {}
    for row in rows:
        metric = metric_by_series.get(row["series_id"])
        if metric is None:
            continue
        obs_date = row["obs_date"]
        bucket = out.setdefault(obs_date, {"_pull_ts": {}})
        bucket[metric] = float(row["value"])
        bucket["_pull_ts"][metric] = row["pull_timestamp"]
    return out


def _existing_report_dates(conn: Connection, contract_code: str) -> set[date]:
    rows = conn.execute(
        text(
            "SELECT report_date FROM cftc_positioning_daily WHERE contract_code = :code"
        ),
        {"code": contract_code},
    ).fetchall()
    return {r[0] for r in rows}


def _net_speculative_series(history: dict[date, dict[str, Any]], up_to: date) -> list[float]:
    """Chronological net_speculative values (noncommercial_long - short) for dates <= up_to."""
    dates = sorted(d for d in history if d <= up_to)
    out: list[float] = []
    for d in dates:
        m = history[d]
        if "noncommercial_long" in m and "noncommercial_short" in m:
            out.append(m["noncommercial_long"] - m["noncommercial_short"])
    return out


def materialize_cftc_pillar(
    engine: Engine,
    *,
    as_of: date | None = None,
    contracts: dict[str, dict[str, str]] | None = None,
    now: datetime | None = None,
) -> MaterializationResult:
    """Materialize new CFTC positioning rows as one atomic generation.

    See the module docstring and docs/reference/GODVIEW_PILLAR_CONTRACT.md
    section 7 for the exact semantics: one transaction, INSERT-only,
    idempotent re-run, and "failed or empty upstream never advances the
    generation."
    """
    as_of = as_of or date.today()
    contracts = contracts if contracts is not None else CFTC_PILLAR_CONTRACTS
    generation_id = new_generation_id()

    try:
        with engine.begin() as conn:
            rows_to_insert: list[dict[str, Any]] = []
            contracts_with_any_history: set[str] = set()
            contracts_with_new_rows: set[str] = set()

            for contract_key, meta in contracts.items():
                history = _read_contract_history(conn, contract_key, as_of)
                if history:
                    contracts_with_any_history.add(contract_key)

                existing = _existing_report_dates(conn, meta["contract_code"])

                for report_date, metrics in sorted(history.items()):
                    if report_date in existing:
                        continue
                    if not all(m in metrics for m in REQUIRED_METRICS):
                        continue

                    commercial_net = metrics["commercial_long"] - metrics["commercial_short"]
                    noncommercial_net = metrics["noncommercial_long"] - metrics["noncommercial_short"]
                    oi = metrics["total_open_interest"]
                    if oi <= 0:
                        continue
                    spec_net_pct_oi = (noncommercial_net / oi) * 100.0

                    net_spec_history = _net_speculative_series(history, report_date)
                    z1 = compute_zscore(net_spec_history, Z_SCORE_WINDOW_1Y)
                    z3 = compute_zscore(net_spec_history, PERCENTILE_WINDOW_3Y)
                    pct3 = compute_percentile(net_spec_history, PERCENTILE_WINDOW_3Y)
                    crowding = classify_crowding(pct3)
                    coverage = coverage_fraction_for_window(len(net_spec_history))

                    release_date, source_ref = compute_release_date(report_date)

                    available_at = min(metrics["_pull_ts"].values()) if metrics.get("_pull_ts") else None

                    rows_to_insert.append(
                        {
                            "report_date": report_date,
                            "contract_code": meta["contract_code"],
                            "contract_name": meta["contract_name"],
                            "asset_class": meta["asset_class"],
                            "total_open_interest": oi,
                            "commercial_long": metrics["commercial_long"],
                            "commercial_short": metrics["commercial_short"],
                            "commercial_net": commercial_net,
                            "noncommercial_long": metrics["noncommercial_long"],
                            "noncommercial_short": metrics["noncommercial_short"],
                            "noncommercial_net": noncommercial_net,
                            "spec_net_pct_oi": spec_net_pct_oi,
                            "z_score_1y": z1,
                            "z_score_3y": z3,
                            "percentile_3y": pct3,
                            "crowding_regime": crowding,
                            "release_date": release_date,
                            "available_at": available_at,
                            "provenance": "measured",
                            "generation_id": generation_id,
                            "coverage_fraction": coverage,
                            "source_ref": source_ref,
                        }
                    )
                    contracts_with_new_rows.add(contract_key)

            if not contracts_with_any_history:
                # Nothing at all for any tracked contract -- upstream is empty.
                # Do NOT record this attempt as complete; roll back and record
                # the failure from a separate transaction below.
                raise _EmptyUpstream()

            for row in rows_to_insert:
                conn.execute(
                    text(
                        """
                        INSERT INTO cftc_positioning_daily (
                            report_date, contract_code, contract_name, asset_class,
                            total_open_interest, commercial_long, commercial_short,
                            commercial_net, noncommercial_long, noncommercial_short,
                            noncommercial_net, spec_net_pct_oi, z_score_1y, z_score_3y,
                            percentile_3y, crowding_regime, release_date, available_at,
                            provenance, generation_id, coverage_fraction, source_ref
                        ) VALUES (
                            :report_date, :contract_code, :contract_name, :asset_class,
                            :total_open_interest, :commercial_long, :commercial_short,
                            :commercial_net, :noncommercial_long, :noncommercial_short,
                            :noncommercial_net, :spec_net_pct_oi, :z_score_1y, :z_score_3y,
                            :percentile_3y, :crowding_regime, :release_date, :available_at,
                            :provenance, :generation_id, :coverage_fraction, :source_ref
                        )
                        ON CONFLICT (report_date, contract_code) DO NOTHING
                        """
                    ),
                    row,
                )

            coverage_fraction = len(contracts_with_any_history) / len(contracts) if contracts else None
            record_generation(
                conn,
                pillar=PILLAR_NAME,
                generation_id=generation_id,
                status=STATUS_COMPLETE,
                row_count=len(rows_to_insert),
                coverage_fraction=coverage_fraction,
            )

        status = "SUCCESS" if rows_to_insert else "SUCCESS_NOOP"
        return MaterializationResult(
            status=status,
            generation_id=generation_id,
            rows_written=len(rows_to_insert),
            contracts_with_data=len(contracts_with_any_history),
            contracts_expected=len(contracts),
            message=f"{len(rows_to_insert)} new row(s) across {len(contracts_with_new_rows)} contract(s)",
        )

    except _EmptyUpstream:
        _record_failure(engine, generation_id, "empty_upstream")
        return MaterializationResult(
            status="EMPTY",
            generation_id=generation_id,
            contracts_expected=len(contracts),
            message="no raw_series history for any tracked contract",
        )
    except Exception as exc:  # noqa: BLE001 -- must not advance the generation on ANY failure
        _record_failure(engine, generation_id, str(exc))
        return MaterializationResult(
            status="FAILED",
            generation_id=generation_id,
            contracts_expected=len(contracts),
            message=str(exc),
        )


class _EmptyUpstream(Exception):
    """Internal signal: roll back the main transaction, upstream had nothing."""


def _record_failure(engine: Engine, generation_id: str, reason: str) -> None:
    """Record a failed attempt in its own transaction (the main one already rolled back)."""
    try:
        with engine.begin() as conn:
            record_generation(
                conn,
                pillar=PILLAR_NAME,
                generation_id=generation_id,
                status=STATUS_FAILED,
                failure_reason=reason,
            )
    except Exception:  # noqa: BLE001 -- bookkeeping failure must not mask the original error
        pass


# ---------------------------------------------------------------------------
# Read path used by the API router
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PillarReadResult:
    """What the API router needs to build its response. See api/routers/godview_pillars.py."""

    state: str  # "never_configured" | "materializer_failed" | "ok"
    rows: list[dict[str, Any]] = field(default_factory=list)
    contracts_with_data: int = 0
    contracts_expected: int = field(default_factory=lambda: len(CFTC_PILLAR_CONTRACTS))
    newest_release_date: date | None = None
    generation_id: str | None = None
    generation_published_at: Any = None


def read_cftc_pillar(
    conn: Connection,
    as_of: date,
    *,
    contracts: dict[str, dict[str, str]] | None = None,
) -> PillarReadResult:
    """Strict-PIT read: latest qualifying row per tracked contract as of ``as_of``.

    "Qualifying" = release_date IS NOT NULL AND release_date <= as_of (contract
    doc section 8's quarantine rule). Never raises for a missing table -- the
    caller (the API router) is responsible for the to_regclass probe that
    decides whether to call this at all.

    ``contracts`` defaults to the production ``CFTC_PILLAR_CONTRACTS`` (the
    four tracked contracts); tests pass their own isolated contract map so
    they don't collide with each other -- or with a real materializer run --
    on the shared ``cftc_positioning`` pillar name in ``godview_generations``.
    """
    contracts = contracts if contracts is not None else CFTC_PILLAR_CONTRACTS
    generation = _latest_complete_generation(conn, PILLAR_NAME)
    attempt = _latest_attempt(conn, PILLAR_NAME)

    if generation is None:
        if attempt is not None and attempt["status"] == STATUS_FAILED:
            return PillarReadResult(state="materializer_failed", contracts_expected=len(contracts))
        return PillarReadResult(state="never_configured", contracts_expected=len(contracts))

    contract_codes = [meta["contract_code"] for meta in contracts.values()]
    rows = conn.execute(
        text(
            """
            SELECT DISTINCT ON (contract_code)
                contract_code, contract_name, report_date, total_open_interest,
                commercial_long, commercial_short, commercial_net,
                noncommercial_long, noncommercial_short, noncommercial_net,
                spec_net_pct_oi, z_score_1y, z_score_3y, percentile_3y,
                crowding_regime, release_date, available_at, provenance,
                generation_id, coverage_fraction, source_ref
            FROM cftc_positioning_daily
            WHERE contract_code = ANY(:codes)
              AND release_date IS NOT NULL
              AND release_date <= :as_of
            ORDER BY contract_code, report_date DESC
            """
        ),
        {"codes": contract_codes, "as_of": as_of},
    ).mappings().all()

    rows_out = [dict(r) for r in rows]
    newest_release = max((r["release_date"] for r in rows_out), default=None)

    return PillarReadResult(
        state="ok",
        rows=rows_out,
        contracts_with_data=len(rows_out),
        contracts_expected=len(contracts),
        newest_release_date=newest_release,
        generation_id=generation["generation_id"],
        generation_published_at=generation["published_at"],
    )
