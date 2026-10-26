"""godview.cftc_positioning -- CFTC positioning God View writer (materialization plan slice G4).

Writes ``cftc_positioning_daily`` rows, one per ``(report_date, contract_code)``,
from the code-keyed CFTC series G1 (#675) introduced:
``cftc.<cftc_contract_market_code>.<metric>`` (``ingestion/altdata/cftc_markets.py``).
Plan: ``GRID-GODVIEW-MATERIALIZATION-PLAN-20260926.md`` section 2b and slice G4.
Schema: #674 (``godview_writers_20260926``). Reference implementation: G3
(``godview/fed_liquidity.py``, #677); this module mirrors its patterns --
``store.observations`` reads with an explicit source and ``as_of_ts``,
provenance columns on every row, a ``godview_runs`` ledger row in the same
transaction, ``available_at = max(release_at, max pull)``, and a legacy
(``provenance IS NULL``) row is never updated: that key is skipped with
``SKIP_BLOCKED_BY_LEGACY_ROW`` and the run is ``partial_blocked_by_legacy``.

Inputs
------
Only the G1 ids, built with ``cftc_markets.series_id`` (never an f-string, never
a legacy ``cftc.SP500.*`` style key -- guard test
``tests/test_cftc_cot_market_code.py::test_no_analytical_reader_uses_legacy_cftc_ids``
scans this package). Read through ``store.observations.read_window`` with
``source="CFTC_COT"`` (the puller's ``SOURCE_NAME``), SUCCESS rows only, latest
vintage per report date, bounded by ``as_of`` / ``as_of_ts``.

A row needs all five raw legs (``commercial_long``, ``commercial_short``,
``noncommercial_long``, ``noncommercial_short``, ``total_open_interest``) of the
same market code on the same report date. The puller also stores
``net_speculative``; when present it must equal ``noncommercial_long -
noncommercial_short`` or the week is ambiguous and skipped. Legs must be finite,
non-negative whole contract counts, open interest must be positive, and no single
leg may exceed open interest. Anything else is skipped with a reason -- never
zero-filled, rounded or substituted (the incident materializer wrote
``.get(..., 0.0)``).

Metrics
-------
* ``commercial_net``, ``noncommercial_net`` (= net speculative), and
  ``spec_net_pct_oi = noncommercial_net / total_open_interest * 100``.
* ``z_score_1y``, ``z_score_3y``, ``percentile_3y`` of ``noncommercial_net``, over
  the ``intelligence/cot_extremes.py`` windows (52 and 156 weeks), with the same
  formulas (sample standard deviation, current value inside its window;
  percentile = (below + 0.5 * equal) / n).
* ``crowding_regime`` from ``percentile_3y`` with ``cot_extremes``' 85/95
  thresholds. NULL when the percentile is NULL -- never a default ``NEUTRAL``.

Point-in-time windows
---------------------
A window for report date ``R`` holds only the same market's valid reports with
``R - W weeks < report_date <= R``: calendar-bounded, so a sparse series cannot
stretch a "1y" window over several years, and never a later report. That later
reports are excluded is what makes the window PIT at ``R``'s release: the CFTC
publishes reports in report-date order (holiday delays and the 2013 / 2018-19 /
2025 shutdown catch-ups all published the backlog oldest first), so every report
in the window was public no later than ``R`` itself. A statistic is NULL below
its minimum count (``MIN_OBS_1Y`` = ``cot_extremes._MIN_HISTORY`` = 40 of 52
weeks, the density cot_extremes already requires before scoring; ``MIN_OBS_3Y``
= 120 of 156, the same density) or when the window's standard deviation is zero
(cot_extremes returns 0.0 there; a fabricated z is not written here).

Known-at: ``available_at = max(release_at, latest pull_timestamp of R's own legs
and of every report in R's 3y window)``. The row is a function of the whole
window, so it is only knowable once the last input it used was pulled -- the
same "last leg, never first" rule as G3. ``release_at`` comes from
``cftc_markets.compute_release`` (Friday 15:30 ET, federal-holiday shifted, a
floor). A run whose ``as_of_ts`` precedes a row's ``release_at`` skips it
(``SKIP_BEFORE_RELEASE``). ``availability_basis`` is #571's classifier: rows
from the history backfill (pulled long after release) are ``inferred_schedule``;
a row pulled on its release Friday is ``observed_acquisition``.

Report dates with no computable release time
--------------------------------------------
The CFTC moves the position date when the usual Tuesday is a holiday (Monday
data on 2008-12-22, 2018-12-24, 2018-12-31, 2020-12-21, ...; 13 Mondays and
one Wednesday since 2006 across the tracked markets). #682's first
production run skipped 220 rows across these 14 dates -- every one of the
other 15 tracked markets plus 10 of the 14 for VX (VIX futures) -- with
``SKIP_NO_RELEASE_RULE``, because ``compute_release`` only described
Tuesday report dates and returned ``release_at = None`` for every one of
them. Fixed: ``compute_release`` now also resolves Monday/Wednesday report
dates, either from ``cftc_markets.CONFIRMED_HOLIDAY_RELEASES`` (six dates
pinned against a CFTC primary source -- special announcement, press release,
or the archived report file's own timestamp) or a conservative fallback (15:30
ET on the next federal business day after the report's normal Friday, which
is never earlier than the true release -- see that module's docstring). All
14 of the dates above now write a row.

``SKIP_NO_RELEASE_RULE`` remains for the residual case a report date is
neither Tuesday, Monday, nor Wednesday (never observed in real CFTC data):
#674's receipt CHECK requires ``release_at`` on every provenance-marked row
and no rule gives one, so such a date is never written -- skipped, counted in
the ledger's ``reasons`` and listed (per market) in the returned result and a
warning log. Its measured positions still count as history in later windows
(published in order, as above).

Good Friday
-----------
Checked 2026-09-27 against the CFTC's published 2026 release schedule
(cftc.gov/MarketReports/CommitmentsofTraders/ReleaseSchedule): Good Friday
2026-04-03 is a regular release day (no holiday asterisk). Good Friday is not a
federal holiday and the CFTC is open, so G1's federal-holiday rule is right not
to shift it. ``tests/godview/test_cftc_positioning_pure.py`` pins all 52 2026
release dates against ``compute_release``; no correction to G1 was needed.
"""

from __future__ import annotations

import json
import math
import uuid
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from godview.availability_basis import classify_availability_basis
from ingestion.altdata.cftc_markets import (
    MARKETS,
    RAW_FIELD_MAP,
    RELEASE_RULE,
    CFTCMarket,
    compute_release,
    series_id,
)
from intelligence.cot_extremes import (
    _MIN_HISTORY as _COT_EXTREMES_MIN_HISTORY,
)
from intelligence.cot_extremes import (
    _PCTILE_ELEVATED,
    _PCTILE_EXTREME,
    _PERCENTILE_WINDOW,
    _Z_SCORE_WINDOW,
)
from store import observations as obs_store

PILLAR_NAME = "cftc_positioning"
GODVIEW_RUNS_PILLAR = "cftc"  # godview_runs.pillar CHECK domain value

#: ``source_catalog.name`` of ``ingestion.altdata.cftc_cot.CFTCCOTPuller`` --
#: passed to every read so a second writer under the same ids fails closed.
SOURCE_NAME = "CFTC_COT"

RAW_LEGS: tuple[str, ...] = tuple(RAW_FIELD_MAP)  # the five Socrata legs
NET_SPECULATIVE = "net_speculative"
ALL_METRICS: tuple[str, ...] = (*RAW_LEGS, NET_SPECULATIVE)

Z_1Y_WEEKS = _Z_SCORE_WINDOW  # 52
Z_3Y_WEEKS = _PERCENTILE_WINDOW  # 156
MIN_OBS_1Y = _COT_EXTREMES_MIN_HISTORY  # 40 of 52
MIN_OBS_3Y = 3 * _COT_EXTREMES_MIN_HISTORY  # 120 of 156, same density
_STD_EPSILON = 1e-9

#: ``net_speculative`` as stored may differ from ``long - short`` by float noise only.
_NET_SPEC_TOLERANCE = 0.5

#: Asset class per root symbol (``cftc_positioning_daily.asset_class`` is NOT
#: NULL). Legacy vocabulary (EQUITY / COMMODITY / VOLATILITY) plus RATES for
#: the Treasury futures the legacy table never had.
ASSET_CLASS_BY_ROOT: dict[str, str] = {
    "ES": "EQUITY",
    "NQ": "EQUITY",
    "YM": "EQUITY",
    "VX": "VOLATILITY",
    "ZT": "RATES",
    "ZF": "RATES",
    "ZN": "RATES",
    "ZB": "RATES",
    "GC": "COMMODITY",
    "SI": "COMMODITY",
    "HG": "COMMODITY",
    "CL": "COMMODITY",
    "NG": "COMMODITY",
    "ZC": "COMMODITY",
    "ZS": "COMMODITY",
    "ZW": "COMMODITY",
}

REGIME_EXTREME_LONG = "EXTREME_LONG"
REGIME_ELEVATED_LONG = "ELEVATED_LONG"
REGIME_NEUTRAL = "NEUTRAL"
REGIME_ELEVATED_SHORT = "ELEVATED_SHORT"
REGIME_EXTREME_SHORT = "EXTREME_SHORT"
CROWDING_REGIMES: tuple[str, ...] = (
    REGIME_EXTREME_LONG,
    REGIME_ELEVATED_LONG,
    REGIME_NEUTRAL,
    REGIME_ELEVATED_SHORT,
    REGIME_EXTREME_SHORT,
)

STATUS_SUCCESS = "SUCCESS"
STATUS_SUCCESS_NOOP = "SUCCESS_NOOP"
STATUS_PARTIAL_BLOCKED_BY_LEGACY = "PARTIAL_BLOCKED_BY_LEGACY"
STATUS_EMPTY = "EMPTY"
STATUS_FAILED = "FAILED"

# godview_runs.status (#674 GODVIEW_RUN_STATUSES).
RUN_STATUS_COMPLETE = "complete"
RUN_STATUS_NOOP = "noop"
RUN_STATUS_PARTIAL_BLOCKED_BY_LEGACY = "partial_blocked_by_legacy"
RUN_STATUS_INPUTS_MISSING = "inputs_missing"
RUN_STATUS_FAILED = "failed"

SKIP_MISSING_LEG = "missing_raw_leg"
SKIP_INVALID_LEG = "invalid_raw_leg"
SKIP_NON_POSITIVE_OPEN_INTEREST = "non_positive_open_interest"
SKIP_LEG_EXCEEDS_OPEN_INTEREST = "leg_exceeds_open_interest"
SKIP_NET_SPECULATIVE_MISMATCH = "net_speculative_mismatch"
SKIP_MISSING_PULL_TIMESTAMP = "missing_pull_timestamp"
SKIP_NO_RELEASE_RULE = "no_computable_release_time"
SKIP_BEFORE_RELEASE = "before_scheduled_release"
SKIP_BLOCKED_BY_LEGACY_ROW = "blocked_by_legacy_row_requires_a2_archive"
SKIP_MARKET_CODE_CONFLICT = "existing_row_has_other_market_code"


def _ensure_aware_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


# ---------------------------------------------------------------------------
# Pure core (no database) -- tests/godview/test_cftc_positioning_pure.py
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LegObservation:
    """One accepted ``raw_series`` value of one leg (a thin, testable Observation)."""

    series_id: str
    value: float
    pull_timestamp: datetime | None
    source: str | None


@dataclass(frozen=True)
class WeeklyReport:
    """One market's validated positions for one report date."""

    report_date: date
    commercial_long: int
    commercial_short: int
    noncommercial_long: int
    noncommercial_short: int
    total_open_interest: int
    legs: tuple[tuple[str, LegObservation], ...]  # (metric, observation), ALL_METRICS order
    max_pull_timestamp: datetime

    @property
    def commercial_net(self) -> int:
        return self.commercial_long - self.commercial_short

    @property
    def noncommercial_net(self) -> int:
        return self.noncommercial_long - self.noncommercial_short

    @property
    def spec_net_pct_oi(self) -> float:
        return self.noncommercial_net / self.total_open_interest * 100.0


@dataclass(frozen=True)
class RowSkip:
    report_date: date
    reason: str
    detail: str | None = None


def _whole_count(value: float) -> int | None:
    """A non-negative whole contract count, or None (never rounded)."""
    if not math.isfinite(value) or value < 0 or not float(value).is_integer():
        return None
    return int(value)


def assemble_reports(
    legs_by_metric: dict[str, dict[date, LegObservation]],
) -> tuple[list[WeeklyReport], list[RowSkip]]:
    """Validated weekly reports (ascending) and the dates that failed, with reasons.

    ``legs_by_metric`` maps each of ``ALL_METRICS`` to ``{report_date: leg}``
    for ONE market code. Fail-closed rules are listed in the module docstring.
    """
    dates = sorted({d for by_date in legs_by_metric.values() for d in by_date})
    reports: list[WeeklyReport] = []
    skips: list[RowSkip] = []
    for d in dates:
        legs: dict[str, LegObservation] = {}
        missing = [m for m in RAW_LEGS if d not in legs_by_metric.get(m, {})]
        if missing:
            skips.append(RowSkip(d, SKIP_MISSING_LEG, ",".join(missing)))
            continue
        counts: dict[str, int] = {}
        bad = None
        for m in RAW_LEGS:
            legs[m] = legs_by_metric[m][d]
            c = _whole_count(legs[m].value)
            if c is None:
                bad = m
                break
            counts[m] = c
        if bad is not None:
            skips.append(RowSkip(d, SKIP_INVALID_LEG, f"{bad}={legs[bad].value!r}"))
            continue
        oi = counts["total_open_interest"]
        if oi <= 0:
            skips.append(RowSkip(d, SKIP_NON_POSITIVE_OPEN_INTEREST, str(oi)))
            continue
        over = [m for m in RAW_LEGS if m != "total_open_interest" and counts[m] > oi]
        if over:
            skips.append(RowSkip(d, SKIP_LEG_EXCEEDS_OPEN_INTEREST, ",".join(over)))
            continue
        net = counts["noncommercial_long"] - counts["noncommercial_short"]
        stored_net = legs_by_metric.get(NET_SPECULATIVE, {}).get(d)
        if stored_net is not None:
            if not math.isfinite(stored_net.value) or abs(stored_net.value - net) > _NET_SPEC_TOLERANCE:
                skips.append(RowSkip(d, SKIP_NET_SPECULATIVE_MISMATCH, f"stored={stored_net.value!r} legs={net}"))
                continue
            legs[NET_SPECULATIVE] = stored_net
        pulls = [leg.pull_timestamp for leg in legs.values()]
        if any(p is None for p in pulls):
            skips.append(RowSkip(d, SKIP_MISSING_PULL_TIMESTAMP))
            continue
        reports.append(
            WeeklyReport(
                report_date=d,
                commercial_long=counts["commercial_long"],
                commercial_short=counts["commercial_short"],
                noncommercial_long=counts["noncommercial_long"],
                noncommercial_short=counts["noncommercial_short"],
                total_open_interest=oi,
                legs=tuple((m, legs[m]) for m in ALL_METRICS if m in legs),
                max_pull_timestamp=max(_ensure_aware_utc(p) for p in pulls if p is not None),
            )
        )
    return reports, skips


def window_members(reports: Sequence[WeeklyReport], idx: int, weeks: int) -> list[WeeklyReport]:
    """Reports of the same market with ``R - weeks*7d < report_date <= R`` (R = reports[idx]).

    ``reports`` must be sorted ascending by report_date. Never includes a
    report dated after ``R``.
    """
    anchor = reports[idx].report_date
    floor = anchor - timedelta(days=7 * weeks)
    out: list[WeeklyReport] = []
    for r in reports[: idx + 1]:
        if floor < r.report_date <= anchor:
            out.append(r)
    return out


def compute_zscore(values: Sequence[float], min_obs: int) -> float | None:
    """z of ``values[-1]`` against ``values`` (sample std); None if short or flat."""
    if len(values) < max(min_obs, 2):
        return None
    mean = sum(values) / len(values)
    var = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    std = math.sqrt(var)
    if std <= _STD_EPSILON:
        return None
    return (values[-1] - mean) / std


def compute_percentile(values: Sequence[float], min_obs: int) -> float | None:
    """cot_extremes' rank of ``values[-1]``: (below + 0.5 * equal) / n * 100."""
    if len(values) < max(min_obs, 1):
        return None
    current = values[-1]
    below = sum(1 for v in values if v < current)
    equal = sum(1 for v in values if v == current)
    return (below + 0.5 * equal) / len(values) * 100.0


def classify_crowding(percentile_3y: float | None) -> str | None:
    """cot_extremes' 85/95 cut points on the 3y percentile; None when it is None."""
    if percentile_3y is None:
        return None
    if percentile_3y >= _PCTILE_EXTREME:
        return REGIME_EXTREME_LONG
    if percentile_3y >= _PCTILE_ELEVATED:
        return REGIME_ELEVATED_LONG
    if percentile_3y <= 100 - _PCTILE_EXTREME:
        return REGIME_EXTREME_SHORT
    if percentile_3y <= 100 - _PCTILE_ELEVATED:
        return REGIME_ELEVATED_SHORT
    return REGIME_NEUTRAL


@dataclass(frozen=True)
class RowMetrics:
    z_score_1y: float | None
    z_score_3y: float | None
    percentile_3y: float | None
    crowding_regime: str | None
    n_obs_1y: int
    n_obs_3y: int
    window_start_3y: date
    window_max_pull_timestamp: datetime
    coverage_fraction: float


def compute_row_metrics(reports: Sequence[WeeklyReport], idx: int) -> RowMetrics:
    w1 = window_members(reports, idx, Z_1Y_WEEKS)
    w3 = window_members(reports, idx, Z_3Y_WEEKS)
    v1 = [float(r.noncommercial_net) for r in w1]
    v3 = [float(r.noncommercial_net) for r in w3]
    pct = compute_percentile(v3, MIN_OBS_3Y)
    return RowMetrics(
        z_score_1y=compute_zscore(v1, MIN_OBS_1Y),
        z_score_3y=compute_zscore(v3, MIN_OBS_3Y),
        percentile_3y=pct,
        crowding_regime=classify_crowding(pct),
        n_obs_1y=len(w1),
        n_obs_3y=len(w3),
        window_start_3y=w3[0].report_date,
        # w3 always contains reports[idx] itself (its own legs), so this is
        # the latest pull of the row's own legs AND of its whole window.
        window_max_pull_timestamp=max(r.max_pull_timestamp for r in w3),
        coverage_fraction=min(1.0, len(w3) / Z_3Y_WEEKS),
    )


def build_source_ref(market: CFTCMarket, report: WeeklyReport, metrics: RowMetrics, holiday_shifted: bool) -> dict:
    """Deterministic for identical inputs (the upsert's no-op test compares it)."""
    return {
        "market_code": market.code,
        "root": market.root,
        "source": SOURCE_NAME,
        "release_rule": RELEASE_RULE,
        "release_is_floor": True,
        "release_holiday_shifted": holiday_shifted,
        "inputs": [
            {
                "series_id": leg.series_id,
                "obs_date": report.report_date.isoformat(),
                "pull_timestamp": leg.pull_timestamp.isoformat() if leg.pull_timestamp else None,
                "source": leg.source,
            }
            for _, leg in report.legs
        ],
        "window": {
            "series": "noncommercial_net",
            "weeks_1y": Z_1Y_WEEKS,
            "weeks_3y": Z_3Y_WEEKS,
            "min_obs_1y": MIN_OBS_1Y,
            "min_obs_3y": MIN_OBS_3Y,
            "n_obs_1y": metrics.n_obs_1y,
            "n_obs_3y": metrics.n_obs_3y,
            "first_report_date_3y": metrics.window_start_3y.isoformat(),
            "max_pull_timestamp": metrics.window_max_pull_timestamp.isoformat(),
        },
    }


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RowResult:
    contract_code: str
    market_code: str
    report_date: date
    status: str  # "written" | "noop" | "skipped"
    reason: str | None = None
    detail: str | None = None
    z_score_3y: float | None = None
    availability_basis: str | None = None
    release_at: datetime | None = None
    available_at: datetime | None = None


@dataclass(frozen=True)
class MaterializationResult:
    status: str
    rows_written: int = 0
    rows_skipped: int = 0
    rows: tuple[RowResult, ...] = ()
    code_sha: str | None = None
    run_id: str | None = None
    message: str = ""
    markets_without_observations: tuple[str, ...] = ()
    no_release_rule_dates: dict[str, tuple[date, ...]] = field(default_factory=dict)


class _EmptyUpstream(Exception):
    pass


# ---------------------------------------------------------------------------
# DB wrappers
# ---------------------------------------------------------------------------

_EXISTING_ROWS_SQL = text(
    """
    SELECT report_date, provenance, cftc_market_code
    FROM cftc_positioning_daily
    WHERE contract_code = :contract_code
    """
)

# The WHERE is a second, independent guard (G3's B3): a legacy row
# (provenance IS NULL) or a row of another market code is never updated,
# even if the Python pre-check were bypassed or raced.
_UPSERT_SQL = text(
    """
    INSERT INTO cftc_positioning_daily (
        report_date, contract_code, contract_name, asset_class,
        total_open_interest, commercial_long, commercial_short, commercial_net,
        noncommercial_long, noncommercial_short, noncommercial_net,
        spec_net_pct_oi, z_score_1y, z_score_3y, percentile_3y, crowding_regime,
        release_at, available_at, availability_basis, provenance, source_ref,
        run_id, code_sha, updated_at, coverage_fraction, cftc_market_code, market_name
    ) VALUES (
        :report_date, :contract_code, :contract_name, :asset_class,
        :total_open_interest, :commercial_long, :commercial_short, :commercial_net,
        :noncommercial_long, :noncommercial_short, :noncommercial_net,
        :spec_net_pct_oi, :z_score_1y, :z_score_3y, :percentile_3y, :crowding_regime,
        :release_at, :available_at, :availability_basis, 'measured', CAST(:source_ref AS JSONB),
        CAST(:run_id AS UUID), :code_sha, NOW(), :coverage_fraction, :cftc_market_code, :market_name
    )
    ON CONFLICT (report_date, contract_code) DO UPDATE SET
        contract_name = EXCLUDED.contract_name,
        asset_class = EXCLUDED.asset_class,
        total_open_interest = EXCLUDED.total_open_interest,
        commercial_long = EXCLUDED.commercial_long,
        commercial_short = EXCLUDED.commercial_short,
        commercial_net = EXCLUDED.commercial_net,
        noncommercial_long = EXCLUDED.noncommercial_long,
        noncommercial_short = EXCLUDED.noncommercial_short,
        noncommercial_net = EXCLUDED.noncommercial_net,
        spec_net_pct_oi = EXCLUDED.spec_net_pct_oi,
        z_score_1y = EXCLUDED.z_score_1y,
        z_score_3y = EXCLUDED.z_score_3y,
        percentile_3y = EXCLUDED.percentile_3y,
        crowding_regime = EXCLUDED.crowding_regime,
        release_at = EXCLUDED.release_at,
        available_at = EXCLUDED.available_at,
        availability_basis = EXCLUDED.availability_basis,
        provenance = EXCLUDED.provenance,
        source_ref = EXCLUDED.source_ref,
        run_id = EXCLUDED.run_id,
        code_sha = EXCLUDED.code_sha,
        updated_at = NOW(),
        coverage_fraction = EXCLUDED.coverage_fraction,
        market_name = EXCLUDED.market_name
    WHERE cftc_positioning_daily.provenance IS NOT NULL
      AND cftc_positioning_daily.cftc_market_code = EXCLUDED.cftc_market_code
      AND (
        cftc_positioning_daily.source_ref IS DISTINCT FROM EXCLUDED.source_ref
        OR cftc_positioning_daily.total_open_interest IS DISTINCT FROM EXCLUDED.total_open_interest
        OR cftc_positioning_daily.commercial_long IS DISTINCT FROM EXCLUDED.commercial_long
        OR cftc_positioning_daily.commercial_short IS DISTINCT FROM EXCLUDED.commercial_short
        OR cftc_positioning_daily.noncommercial_long IS DISTINCT FROM EXCLUDED.noncommercial_long
        OR cftc_positioning_daily.noncommercial_short IS DISTINCT FROM EXCLUDED.noncommercial_short
        OR cftc_positioning_daily.z_score_1y IS DISTINCT FROM EXCLUDED.z_score_1y
        OR cftc_positioning_daily.z_score_3y IS DISTINCT FROM EXCLUDED.z_score_3y
        OR cftc_positioning_daily.percentile_3y IS DISTINCT FROM EXCLUDED.percentile_3y
        OR cftc_positioning_daily.crowding_regime IS DISTINCT FROM EXCLUDED.crowding_regime
        OR cftc_positioning_daily.release_at IS DISTINCT FROM EXCLUDED.release_at
        OR cftc_positioning_daily.available_at IS DISTINCT FROM EXCLUDED.available_at
        OR cftc_positioning_daily.availability_basis IS DISTINCT FROM EXCLUDED.availability_basis
      )
    """
)

_INSERT_RUN_LEDGER_SQL = text(
    """
    INSERT INTO godview_runs (
        run_id, pillar, started_at, finished_at, status,
        rows_written, rows_skipped, reasons, input_watermarks, error, code_sha
    ) VALUES (
        CAST(:run_id AS UUID), :pillar, :started_at, :finished_at, :status,
        :rows_written, :rows_skipped, CAST(:reasons AS JSONB), CAST(:input_watermarks AS JSONB),
        :error, :code_sha
    )
    """
)


def _insert_run_ledger(
    conn: Connection,
    *,
    run_id: str,
    started_at: datetime,
    finished_at: datetime,
    status: str,
    rows_written: int,
    rows_skipped: int,
    reasons: dict[str, int],
    input_watermarks: dict[str, str | None],
    code_sha: str,
    error: str | None = None,
) -> None:
    conn.execute(
        _INSERT_RUN_LEDGER_SQL,
        {
            "run_id": run_id,
            "pillar": GODVIEW_RUNS_PILLAR,
            "started_at": started_at,
            "finished_at": finished_at,
            "status": status,
            "rows_written": rows_written,
            "rows_skipped": rows_skipped,
            "reasons": json.dumps(reasons, sort_keys=True),
            "input_watermarks": json.dumps(input_watermarks, sort_keys=True),
            "error": error,
            "code_sha": code_sha,
        },
    )


def _read_market_legs(
    conn: Connection,
    code: str,
    *,
    start: date | None,
    as_of: date,
    as_of_ts: datetime | None,
) -> dict[str, dict[date, LegObservation]]:
    out: dict[str, dict[date, LegObservation]] = {}
    for metric in ALL_METRICS:
        sid = series_id(code, metric)
        rows = obs_store.read_window(conn, sid, source=SOURCE_NAME, start=start, as_of=as_of, as_of_ts=as_of_ts)
        out[metric] = {o.obs_date: LegObservation(sid, o.value, o.pull_timestamp, o.source) for o in rows}
    return out


def materialize_cftc_positioning(
    engine: Engine,
    *,
    code_sha: str,
    as_of: date | None = None,
    as_of_ts: datetime | None = None,
    start: date | None = None,
    market_codes: Sequence[str] | None = None,
) -> MaterializationResult:
    """Materialize ``cftc_positioning_daily`` for every tracked market code.

    One transaction for all reads, upserts and the ``godview_runs`` row (the
    rows' run_id FK is DEFERRABLE INITIALLY DEFERRED). An empty-upstream or
    failed run writes only its ledger row, in a separate transaction.

    ``start`` bounds the report dates written; the read reaches back a further
    ``Z_3Y_WEEKS`` so the first written row still sees its full window.
    ``as_of`` bounds report dates (default: the UTC date of ``as_of_ts``, or
    today); ``as_of_ts`` bounds pull timestamps (PIT replay) and is the
    instant the before-release gate compares against (default: now).
    """
    if not code_sha:
        raise ValueError("materialize_cftc_positioning requires a non-empty code_sha")
    codes = list(market_codes) if market_codes is not None else list(MARKETS)
    unknown = [c for c in codes if c not in MARKETS]
    if unknown:
        raise ValueError(f"not tracked CFTC market codes: {unknown}")

    reference_ts = _ensure_aware_utc(as_of_ts) if as_of_ts is not None else datetime.now(timezone.utc)
    as_of = as_of or reference_ts.astimezone(timezone.utc).date()
    read_start = start - timedelta(days=7 * (Z_3Y_WEEKS + 1)) if start is not None else None
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)

    try:
        with engine.begin() as conn:
            written: list[RowResult] = []
            noop: list[RowResult] = []
            skipped: list[RowResult] = []
            watermarks: dict[str, str | None] = {}
            no_obs: list[str] = []
            no_release: dict[str, tuple[date, ...]] = {}

            for code in codes:
                market = MARKETS[code]
                root = market.root
                asset_class = ASSET_CLASS_BY_ROOT[root]
                legs = _read_market_legs(conn, code, start=read_start, as_of=as_of, as_of_ts=as_of_ts)
                all_dates = {d for by_date in legs.values() for d in by_date}
                watermarks[code] = max(all_dates).isoformat() if all_dates else None
                if not all_dates:
                    no_obs.append(code)
                    continue

                reports, bad = assemble_reports(legs)
                for s in bad:
                    if start is None or s.report_date >= start:
                        skipped.append(RowResult(root, code, s.report_date, "skipped", s.reason, s.detail))

                existing = {
                    r[0]: (r[1], r[2])
                    for r in conn.execute(_EXISTING_ROWS_SQL, {"contract_code": root}).fetchall()
                }
                no_release_dates: list[date] = []

                for idx, report in enumerate(reports):
                    rd = report.report_date
                    if start is not None and rd < start:
                        continue  # history only: feeds later windows, not written

                    release = compute_release(rd)
                    if release.release_at is None:
                        no_release_dates.append(rd)
                        skipped.append(RowResult(root, code, rd, "skipped", SKIP_NO_RELEASE_RULE, release.reason))
                        continue
                    if rd in existing:
                        prov, existing_code = existing[rd]
                        if prov is None:
                            skipped.append(RowResult(root, code, rd, "skipped", SKIP_BLOCKED_BY_LEGACY_ROW))
                            continue
                        if existing_code != code:
                            skipped.append(
                                RowResult(root, code, rd, "skipped", SKIP_MARKET_CODE_CONFLICT, str(existing_code))
                            )
                            continue
                    release_at = release.release_at
                    if reference_ts < release_at:
                        skipped.append(RowResult(root, code, rd, "skipped", SKIP_BEFORE_RELEASE))
                        continue

                    metrics = compute_row_metrics(reports, idx)
                    available_at = max(release_at, metrics.window_max_pull_timestamp)
                    basis, _note = classify_availability_basis(release_at.date(), available_at)
                    source_ref = build_source_ref(market, report, metrics, release.holiday_shifted)

                    res = conn.execute(
                        _UPSERT_SQL,
                        {
                            "report_date": rd,
                            "contract_code": root,
                            "contract_name": market.label,
                            "asset_class": asset_class,
                            "total_open_interest": report.total_open_interest,
                            "commercial_long": report.commercial_long,
                            "commercial_short": report.commercial_short,
                            "commercial_net": report.commercial_net,
                            "noncommercial_long": report.noncommercial_long,
                            "noncommercial_short": report.noncommercial_short,
                            "noncommercial_net": report.noncommercial_net,
                            "spec_net_pct_oi": report.spec_net_pct_oi,
                            "z_score_1y": metrics.z_score_1y,
                            "z_score_3y": metrics.z_score_3y,
                            "percentile_3y": metrics.percentile_3y,
                            "crowding_regime": metrics.crowding_regime,
                            "release_at": release_at,
                            "available_at": available_at,
                            "availability_basis": basis,
                            "source_ref": json.dumps(source_ref, sort_keys=True),
                            "run_id": run_id,
                            "code_sha": code_sha,
                            "coverage_fraction": metrics.coverage_fraction,
                            "cftc_market_code": code,
                            "market_name": market.label,
                        },
                    )
                    row = RowResult(
                        root,
                        code,
                        rd,
                        "written" if (res.rowcount or 0) > 0 else "noop",
                        z_score_3y=metrics.z_score_3y,
                        availability_basis=basis,
                        release_at=release_at,
                        available_at=available_at,
                    )
                    (written if row.status == "written" else noop).append(row)

                if no_release_dates:
                    no_release[code] = tuple(no_release_dates)
                    log.warning(
                        "godview.cftc_positioning {code} ({root}): {n} report date(s) with no computable "
                        "release time (not a Tuesday) skipped: {dates}",
                        code=code,
                        root=root,
                        n=len(no_release_dates),
                        dates=[d.isoformat() for d in no_release_dates],
                    )

            if len(no_obs) == len(codes):
                raise _EmptyUpstream()

            legacy_blocked = [r for r in skipped if r.reason == SKIP_BLOCKED_BY_LEGACY_ROW]
            if legacy_blocked:
                status, run_status = STATUS_PARTIAL_BLOCKED_BY_LEGACY, RUN_STATUS_PARTIAL_BLOCKED_BY_LEGACY
                log.warning(
                    "godview.cftc_positioning BLOCKED_BY_LEGACY_ROW {n} key(s) across {c} contract(s) "
                    "run_id={run_id} -- requires the plan's A2 archive before these keys can be written",
                    n=len(legacy_blocked),
                    c=sorted({r.contract_code for r in legacy_blocked}),
                    run_id=run_id,
                )
            elif written:
                status, run_status = STATUS_SUCCESS, RUN_STATUS_COMPLETE
            else:
                status, run_status = STATUS_SUCCESS_NOOP, RUN_STATUS_NOOP

            reasons: dict[str, int] = dict(Counter(r.reason for r in skipped if r.reason))
            if no_obs:
                reasons["market_without_observations"] = len(no_obs)
            _insert_run_ledger(
                conn,
                run_id=run_id,
                started_at=started_at,
                finished_at=datetime.now(timezone.utc),
                status=run_status,
                rows_written=len(written),
                rows_skipped=len(skipped),
                reasons=reasons,
                input_watermarks=watermarks,
                code_sha=code_sha,
            )

        if no_obs:
            log.warning("godview.cftc_positioning: no code-keyed observations for {c}", c=no_obs)
        log.info(
            "godview.cftc_positioning run complete: {w} written, {n} unchanged, {s} skipped, "
            "status={status} run_id={run_id} code_sha={sha}",
            w=len(written),
            n=len(noop),
            s=len(skipped),
            status=status,
            run_id=run_id,
            sha=code_sha,
        )
        return MaterializationResult(
            status=status,
            rows_written=len(written),
            rows_skipped=len(skipped),
            rows=tuple(written + noop + skipped),
            code_sha=code_sha,
            run_id=run_id,
            message=f"{len(written)} written, {len(noop)} unchanged, {len(skipped)} skipped",
            markets_without_observations=tuple(no_obs),
            no_release_rule_dates=no_release,
        )

    except _EmptyUpstream:
        log.warning("godview.cftc_positioning EMPTY: no code-keyed CFTC observations, run_id={r}", r=run_id)
        _try_write_ledger_only(
            engine, run_id=run_id, started_at=started_at, status=RUN_STATUS_INPUTS_MISSING, code_sha=code_sha
        )
        return MaterializationResult(
            status=STATUS_EMPTY,
            code_sha=code_sha,
            run_id=run_id,
            message="no raw_series rows under cftc.<market_code>.<metric> for any tracked market",
            markets_without_observations=tuple(codes),
        )
    except obs_store.MixedSourceError:
        raise
    except Exception as exc:  # noqa: BLE001
        log.error("godview.cftc_positioning FAILED: {e}, run_id={r} code_sha={s}", e=exc, r=run_id, s=code_sha)
        _try_write_ledger_only(
            engine,
            run_id=run_id,
            started_at=started_at,
            status=RUN_STATUS_FAILED,
            code_sha=code_sha,
            error=str(exc),
        )
        return MaterializationResult(status=STATUS_FAILED, code_sha=code_sha, run_id=run_id, message=str(exc))


def _try_write_ledger_only(
    engine: Engine,
    *,
    run_id: str,
    started_at: datetime,
    status: str,
    code_sha: str,
    error: str | None = None,
) -> None:
    """Best-effort ledger row for a run whose data transaction never committed."""
    try:
        with engine.begin() as conn:
            _insert_run_ledger(
                conn,
                run_id=run_id,
                started_at=started_at,
                finished_at=datetime.now(timezone.utc),
                status=status,
                rows_written=0,
                rows_skipped=0,
                reasons={},
                input_watermarks={},
                code_sha=code_sha,
                error=error,
            )
    except Exception as ledger_exc:  # noqa: BLE001
        log.warning("godview.cftc_positioning: could not write godview_runs ledger row: {e}", e=ledger_exc)
