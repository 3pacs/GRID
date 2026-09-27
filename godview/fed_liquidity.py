"""godview.fed_liquidity — Fed net liquidity God View writer (materialization plan slice G3).

    Net Liquidity = WALCL - WTREGEN - RRPONTSYD x 1000   (millions USD)

Source of the plan this module implements:
``GRID-GODVIEW-MATERIALIZATION-PLAN-20260926.md`` section 2a and slice G3
(read at ``C:/Users/owner/Documents/Codex/2026-09-14/wha/outputs/``).

This module is stacked on PR #674 (``feat/godview-provenance-migration-20260926``,
migration ``godview_writers_20260926``), which adds ``release_at``,
``available_at``, ``availability_basis``, ``provenance``, ``source_ref``,
``run_id``, ``code_sha``, ``updated_at``, ``coverage_fraction``,
``delta_1w_m`` and ``delta_4w_m`` to ``fed_net_liquidity_daily``, plus the
``godview_runs`` ledger table. #674 is schema-only (no row written, updated
or deleted); this module is the first writer to use those columns.

Three defects fixed here after an independent review of the first version
of this module (PR #677 @ 79faa3c4):

* **B1 (RRP units).** Production's ``reverse_repo_rrp`` column already
  stores the RRP leg *converted to millions* (verified on the real 09-16
  row: ``6740619 - 877028 - 5375 = 5858216`` -- ``5375`` is 5.375bn
  expressed in millions, not billions). The first version stored the raw
  FRED billions value in that column and then re-multiplied by 1000 when
  reading history back out, which is correct only for its own rows and
  corrupts anything that ever mixed with a legacy-shaped row. This version
  always stores ``reverse_repo_rrp`` already converted to millions, so the
  column's unit is self-consistent with ``fed_assets_walcl`` /
  ``treasury_tga_wtregen`` on every row this writer produces, exactly like
  the legacy convention -- with no separate scale factor needed when the
  three legs are combined or read back.
* **B2 (legacy contamination of deltas/peak).** The history read for
  ``delta_1w_m`` / ``delta_4w_m`` / ``rrp_as_pct_of_peak`` now reads only
  rows with ``provenance IS NOT NULL`` (this writer's own prior rows) that
  actually fall on a Wednesday (``EXTRACT(ISODOW FROM obs_date) = 3``,
  belt-and-suspenders since every row this writer ever produces is a
  Wednesday by construction) -- never a legacy forward-filled or
  fabricated row such as the incident materializer's Friday 05-15 row
  (``WALCL = 6,780,000``).
* **B3 (never touch a legacy row).** ``obs_date`` is still the sole unique
  key on ``fed_net_liquidity_daily`` -- #674 does not relax it -- so a
  legacy (``provenance IS NULL``) row and a new provenance-marked row can
  never coexist for the same date; there is no "add a second row" option.
  This writer therefore never updates or deletes a ``provenance IS NULL``
  row: a candidate Wednesday whose ``obs_date`` already holds a legacy row
  is skipped with reason ``SKIP_BLOCKED_BY_LEGACY_ROW`` and never touched.
  Per the materialization plan's A5/A2, archiving and removing (or
  otherwise clearing) that legacy row is an owner prerequisite for that
  date to ever be written by this pillar -- this module only reports the
  gap, it never resolves it by overwriting evidence. The upsert's own
  ``WHERE`` clause enforces the same rule at the SQL level
  (``provenance IS NOT NULL``) as a second, independent guard.

Inputs and PIT semantics: WALCL and WTREGEN are read straight from
``raw_series`` via ``store.observations`` with ``source="FRED"`` -- the
same rows ``ingestion/fred.py`` writes, never the incident's
``junction_point_readings`` and never ``COMPUTED:fed_net_liquidity`` (still
wrong in production per the plan's finding 4). The RRP->millions scale
factor is imported from ``ingestion.altdata.fed_liquidity`` (the #572
single source of truth for that conversion), not redefined here.

The H.4.1 Wednesday->Thursday release rule also shifts for a Thursday
federal holiday (Thanksgiving, Christmas, etc. landing on a Thursday) to
the next business day, via a small Federal Reserve holiday calendar local
to this module -- deliberately not ``ingestion.market_calendar``, which is
the NYSE *trading* calendar (includes Good Friday, excludes Columbus Day
and Veterans Day) and would misdate the Fed's own release schedule.

No fallback constants, anywhere in this module. A missing or stale
component, a legacy-row collision, or a too-early ``as_of_ts`` all mean the
candidate Wednesday is skipped and the reason is recorded on its
``RowResult`` -- never a hard-coded WALCL/TGA value like the untracked
incident materializer used (see ``godview/__init__.py``).
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from functools import lru_cache
from typing import Sequence
from zoneinfo import ZoneInfo

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from godview.availability_basis import classify_availability_basis
from ingestion.altdata.fed_liquidity import RRPONTSYD_TO_MILLIONS
from ingestion.altdata.fed_liquidity import net_liquidity_millions as _net_liquidity_millions
from store import observations as obs_store

PILLAR_NAME = "fed_net_liquidity"
GODVIEW_RUNS_PILLAR = "fed_liquidity"  # godview_runs.pillar CHECK domain value

#: The only source these three FRED series are read from. Passed explicitly
#: to every store.observations call -- store/observations.py raises
#: MixedSourceError on a bare read of a series_id fed by more than one
#: source, and pinning it here also documents intent.
SOURCE_NAME = "FRED"

WALCL_SERIES_ID = "WALCL"
WTREGEN_SERIES_ID = "WTREGEN"
RRP_SERIES_ID = "RRPONTSYD"

_WEDNESDAY = 2  # date.weekday(): Monday=0 ... Sunday=6
_ISO_WEDNESDAY = 3  # ISODOW: Monday=1 ... Sunday=7
_ET = ZoneInfo("America/New_York")

#: H.4.1 publication lag: Wednesday obs_date -> Thursday ~16:30 ET release,
#: shifted to the next business day when that Thursday is a Federal
#: Reserve holiday. "The H.4.1 statistical release ... is typically
#: published on Thursday afternoon around 4:30 p.m." --
#: federalreserve.gov/releases/h41/about.htm (confirmed 2026-09-18 per
#: #571; both WALCL and WTREGEN are H.4.1 Wednesday levels, not the
#: Treasury's own daily DTS). RRPONTSYD is genuinely daily and published
#: same-day (~13:15 ET), always earlier than the Thursday H.4.1 release, so
#: the Wednesday row's binding release gate is the H.4.1 one.
FED_RELEASE_LAG_DAYS = 1
RELEASE_TIME_ET = time(16, 30)
RELEASE_RULE_ID = "fed_h41_release_schedule_v1"
_H41_URL = "https://www.federalreserve.gov/releases/h41/about.htm"

#: Rolling window (in weekly Wednesday observations) for rrp_as_pct_of_peak,
#: matching the CFTC pillar's 3-year percentile convention.
PEAK_WINDOW_WEEKS = 156
_MIN_HISTORY_FOR_PEAK = 4

#: Delta lookback targets, in calendar days: 1 week and 4 weeks (#674's
#: delta_1w_m / delta_4w_m). Observations are weekly (7-day spaced), so a
#: prior obs must land within tolerance_days of the exact target gap or the
#: delta is left NULL rather than computed against a mismatched horizon.
DELTA_1W_TARGET_DAYS = 7
DELTA_4W_TARGET_DAYS = 28
DELTA_TOLERANCE_DAYS = 3

#: liquidity_regime thresholds on delta_4w_m (millions USD). The column is
#: nullable as of #674, but "insufficient_history" -- a real, honest
#: classification of "no 4w-prior row exists yet" -- is still preferred
#: over NULL here: it is a genuine classification, never a fabricated
#: numeric-midpoint default (store/availability.py's rule).
_REGIME_EXPANDING_THRESHOLD = 50_000.0  # +$50B
_REGIME_CONTRACTING_THRESHOLD = -50_000.0
REGIME_INSUFFICIENT_HISTORY = "insufficient_history"
REGIME_EXPANDING = "expanding"
REGIME_CONTRACTING = "contracting"
REGIME_NEUTRAL = "neutral"

STATUS_SUCCESS = "SUCCESS"
STATUS_SUCCESS_NOOP = "SUCCESS_NOOP"
STATUS_EMPTY = "EMPTY"
STATUS_FAILED = "FAILED"

# godview_runs.status CHECK domain: running/complete/failed/noop/
# inputs_missing/inputs_stale/non_session/no_completed_capture/no_verified_spot.
RUN_STATUS_COMPLETE = "complete"
RUN_STATUS_NOOP = "noop"
RUN_STATUS_INPUTS_MISSING = "inputs_missing"
RUN_STATUS_FAILED = "failed"

SKIP_MISSING_WALCL = "missing_walcl"
SKIP_MISSING_WTREGEN = "missing_wtregen"
SKIP_MISSING_RRP = "missing_rrp"
SKIP_BEFORE_RELEASE = "before_scheduled_release"
SKIP_MISSING_PULL_TIMESTAMP = "missing_pull_timestamp"
SKIP_BLOCKED_BY_LEGACY_ROW = "blocked_by_legacy_row_requires_a2_archive"


# ---------------------------------------------------------------------------
# Federal Reserve holiday calendar (H.4.1 release-schedule shift only)
# ---------------------------------------------------------------------------
#
# Deliberately separate from ingestion/market_calendar.py: that module is
# the NYSE *trading* calendar (observes Good Friday, does not observe
# Columbus Day or Veterans Day). The Federal Reserve follows the federal
# government's own holiday schedule instead -- the two lists differ on
# exactly those three days, and using the wrong one would silently misdate
# the H.4.1 release around Columbus Day, Veterans Day and Good Friday.


def _observed(d: date) -> date:
    """Saturday -> observed Friday; Sunday -> observed Monday (OPM rule)."""
    if d.weekday() == 5:
        return d - timedelta(days=1)
    if d.weekday() == 6:
        return d + timedelta(days=1)
    return d


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    first = date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    last_day = date(year + 1, 1, 1) - timedelta(days=1) if month == 12 else date(year, month + 1, 1) - timedelta(days=1)
    offset = (last_day.weekday() - weekday) % 7
    return last_day - timedelta(days=offset)


@lru_cache(maxsize=32)
def federal_reserve_holidays(year: int) -> frozenset[date]:
    """Federal Reserve / federal-government holiday schedule for one year.

    New Year's Day, MLK Day, Washington's Birthday, Memorial Day, Juneteenth
    (from 2022 -- a federal holiday only since then), Independence Day,
    Labor Day, Columbus Day, Veterans Day, Thanksgiving, Christmas Day.
    """
    holidays: set[date] = {
        _observed(date(year, 1, 1)),  # New Year's Day
        _nth_weekday(year, 1, 0, 3),  # MLK Day: 3rd Monday of January
        _nth_weekday(year, 2, 0, 3),  # Washington's Birthday: 3rd Monday of February
        _last_weekday(year, 5, 0),  # Memorial Day: last Monday of May
        _observed(date(year, 7, 4)),  # Independence Day
        _nth_weekday(year, 9, 0, 1),  # Labor Day: 1st Monday of September
        _nth_weekday(year, 10, 0, 2),  # Columbus Day: 2nd Monday of October
        _observed(date(year, 11, 11)),  # Veterans Day
        _nth_weekday(year, 11, 3, 4),  # Thanksgiving: 4th Thursday of November
        _observed(date(year, 12, 25)),  # Christmas Day
    }
    if year >= 2022:
        holidays.add(_observed(date(year, 6, 19)))  # Juneteenth
    return frozenset(holidays)


def _next_federal_business_day(d: date) -> date:
    """``d`` if it is already a Federal Reserve business day, else the next one."""
    while d.weekday() >= 5 or d in federal_reserve_holidays(d.year):
        d += timedelta(days=1)
    return d


# ---------------------------------------------------------------------------
# Pure functions (no database, no network) -- unit tests exercise these
# directly without a connection or an engine.
# ---------------------------------------------------------------------------


def compute_release_at(obs_date: date) -> tuple[datetime | None, str]:
    """Apply the H.4.1 release rule (with holiday shift) to one obs_date.

    Returns ``(release_at, note)``. ``release_at`` is a timezone-aware
    ``datetime`` in America/New_York, 16:30, the day after ``obs_date`` --
    shifted forward to the next Federal Reserve business day if that lands
    on a holiday -- but only when ``obs_date`` really is a Wednesday. For
    any other weekday ``release_at`` is ``None`` and the row is quarantined.
    """
    if obs_date.weekday() != _WEDNESDAY:
        note = (
            f"{RELEASE_RULE_ID}: obs_date {obs_date.isoformat()} "
            f"({obs_date.strftime('%A')}) is not Wednesday; release withheld"
        )
        return None, note

    naive_release_date = obs_date + timedelta(days=FED_RELEASE_LAG_DAYS)
    release_date = _next_federal_business_day(naive_release_date)
    release_at = datetime.combine(release_date, RELEASE_TIME_ET, tzinfo=_ET)

    if release_date != naive_release_date:
        note = (
            f"{RELEASE_RULE_ID}: obs_date is Wednesday; obs_date + {FED_RELEASE_LAG_DAYS}d "
            f"({naive_release_date.isoformat()}) is a Federal Reserve holiday, shifted to "
            f"the next business day -> release_at = {release_date.isoformat()} "
            f"16:30 America/New_York ({_H41_URL})"
        )
    else:
        note = (
            f"{RELEASE_RULE_ID}: obs_date is Wednesday -> release_at = "
            f"obs_date + {FED_RELEASE_LAG_DAYS}d 16:30 America/New_York ({_H41_URL})"
        )
    return release_at, note


def compute_net_liquidity_millions(
    walcl_raw: float, wtregen_raw: float, rrp_raw_billions: float
) -> float:
    """Net Liquidity = WALCL - WTREGEN - RRPONTSYD, normalised to millions USD.

    ``rrp_raw_billions`` is the raw FRED value (billions, its native unit).
    Thin wrapper over ``ingestion.altdata.fed_liquidity.net_liquidity_millions``
    (the #572 fix, ``RRPONTSYD_TO_MILLIONS = 1000.0``) -- that module is the
    single unit authority per the materialization plan; this pillar does not
    keep its own copy of the scale factor.
    """
    return _net_liquidity_millions(walcl_raw, wtregen_raw, rrp_raw_billions)


def rrp_billions_to_stored_millions(rrp_raw_billions: float) -> float:
    """The value actually stored in ``reverse_repo_rrp`` -- millions, like the other two legs.

    Production's own convention (verified on the real 09-16 row, B1): this
    column is not the raw FRED billions value, it is that value already
    converted to millions, so ``net_liquidity_usd_m`` is a plain subtraction
    of the three stored legs with no separate scale factor at read time.
    """
    return rrp_raw_billions * RRPONTSYD_TO_MILLIONS


def compute_rrp_pct_of_peak(
    rrp_millions_history: Sequence[float], window: int = PEAK_WINDOW_WEEKS
) -> float | None:
    """Current (last) RRP value as a percentage of the trailing window's peak.

    ``rrp_millions_history`` must already be in millions (this writer's own
    consistent unit, per B1) -- never a mix of legacy-millions and raw
    billions. ``None`` below the minimum history -- never a fabricated
    percentage.
    """
    finite = [v for v in rrp_millions_history if v is not None]
    if len(finite) < _MIN_HISTORY_FOR_PEAK:
        return None
    windowed = finite[-window:] if len(finite) >= window else finite
    peak = max(windowed)
    if peak <= 0:
        return None
    return (windowed[-1] / peak) * 100.0


def find_nearest_prior(
    history: Sequence[tuple[date, float]],
    current_date: date,
    target_days: int,
    tolerance_days: int = DELTA_TOLERANCE_DAYS,
) -> float | None:
    """Value of the prior observation closest to ``target_days`` before ``current_date``.

    ``None`` if nothing lands within ``tolerance_days`` of that target gap --
    never interpolated, never the nearest observation regardless of distance.
    """
    best: tuple[int, float] | None = None
    for obs_date, value in history:
        if obs_date >= current_date:
            continue
        gap = (current_date - obs_date).days
        diff = abs(gap - target_days)
        if diff <= tolerance_days and (best is None or diff < best[0]):
            best = (diff, value)
    return best[1] if best is not None else None


def coverage_fraction_for_window(n_obs: int, window: int = PEAK_WINDOW_WEEKS) -> float:
    if window <= 0:
        return 0.0
    return min(1.0, max(0.0, n_obs / window))


def classify_liquidity_regime(delta_4w_m: float | None) -> str:
    """``insufficient_history`` is a real classification, never a numeric-midpoint stand-in."""
    if delta_4w_m is None:
        return REGIME_INSUFFICIENT_HISTORY
    if delta_4w_m >= _REGIME_EXPANDING_THRESHOLD:
        return REGIME_EXPANDING
    if delta_4w_m <= _REGIME_CONTRACTING_THRESHOLD:
        return REGIME_CONTRACTING
    return REGIME_NEUTRAL


def _ensure_aware_utc(dt: datetime) -> datetime:
    """Treat a naive datetime as UTC (SQLite/some drivers hand back naive values)."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


# ---------------------------------------------------------------------------
# Provenance / result types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class InputProvenance:
    series_id: str
    obs_date: date
    value: float
    pull_timestamp: datetime | None
    source: str | None


@dataclass(frozen=True)
class RowResult:
    obs_date: date
    status: str  # "written" | "noop" | "skipped"
    reason: str | None = None
    net_liquidity_usd_m: float | None = None
    availability_basis: str | None = None
    release_at: datetime | None = None
    inputs: tuple[InputProvenance, ...] = ()


@dataclass(frozen=True)
class MaterializationResult:
    status: str  # SUCCESS | SUCCESS_NOOP | EMPTY | FAILED
    rows_written: int = 0
    rows_skipped: int = 0
    rows: tuple[RowResult, ...] = ()
    code_sha: str | None = None
    run_id: str | None = None
    message: str = ""


class _EmptyUpstream(Exception):
    pass


# ---------------------------------------------------------------------------
# DB wrappers
# ---------------------------------------------------------------------------

# B2: only this writer's own prior rows (provenance IS NOT NULL) feed the
# delta/peak history, and only if they are genuinely a Wednesday -- never a
# legacy forward-filled or fabricated row. reverse_repo_rrp is read as-is
# (B1: already millions on every row this writer produces).
_EXISTING_HISTORY_SQL = text(
    """
    SELECT obs_date, net_liquidity_usd_m, reverse_repo_rrp
    FROM fed_net_liquidity_daily
    WHERE obs_date < :before
      AND provenance IS NOT NULL
      AND EXTRACT(ISODOW FROM obs_date) = :iso_wednesday
    ORDER BY obs_date ASC
    """
)


def _existing_history(
    conn: Connection, before: date
) -> tuple[list[tuple[date, float]], list[tuple[date, float]]]:
    """Prior (obs_date, net_liquidity_usd_m) and (obs_date, rrp_millions) rows, ascending."""
    rows = conn.execute(_EXISTING_HISTORY_SQL, {"before": before, "iso_wednesday": _ISO_WEDNESDAY}).fetchall()
    net_liq = [(r[0], float(r[1])) for r in rows]
    rrp_m = [(r[0], float(r[2])) for r in rows]
    return net_liq, rrp_m


_EXISTING_PROVENANCE_SQL = text("SELECT provenance FROM fed_net_liquidity_daily WHERE obs_date = :d")


def _existing_provenance(conn: Connection, obs_date: date) -> tuple[bool, str | None]:
    """``(row_exists, provenance)`` for ``obs_date``. ``(False, None)`` when no row exists."""
    row = conn.execute(_EXISTING_PROVENANCE_SQL, {"d": obs_date}).fetchone()
    if row is None:
        return False, None
    return True, row[0]


# B3: the WHERE clause is a second, independent guard against ever touching
# a legacy (provenance IS NULL) row -- the caller also pre-checks via
# _existing_provenance and skips before reaching this statement, but the SQL
# itself refuses too, in case that pre-check is ever bypassed or raced.
_UPSERT_SQL = text(
    """
    INSERT INTO fed_net_liquidity_daily (
        obs_date, fed_assets_walcl, treasury_tga_wtregen, reverse_repo_rrp,
        net_liquidity_usd_m, rrp_as_pct_of_peak, delta_1w_m, delta_4w_m,
        liquidity_regime, release_at, available_at, availability_basis,
        provenance, source_ref, run_id, code_sha, updated_at, coverage_fraction
    ) VALUES (
        :obs_date, :walcl, :wtregen, :rrp_m, :net_liq_m, :rrp_pct_of_peak,
        :delta_1w_m, :delta_4w_m, :regime, :release_at, :available_at, :availability_basis,
        'measured', CAST(:source_ref AS JSONB), CAST(:run_id AS UUID), :code_sha, NOW(), :coverage_fraction
    )
    ON CONFLICT (obs_date) DO UPDATE SET
        fed_assets_walcl = EXCLUDED.fed_assets_walcl,
        treasury_tga_wtregen = EXCLUDED.treasury_tga_wtregen,
        reverse_repo_rrp = EXCLUDED.reverse_repo_rrp,
        net_liquidity_usd_m = EXCLUDED.net_liquidity_usd_m,
        rrp_as_pct_of_peak = EXCLUDED.rrp_as_pct_of_peak,
        delta_1w_m = EXCLUDED.delta_1w_m,
        delta_4w_m = EXCLUDED.delta_4w_m,
        liquidity_regime = EXCLUDED.liquidity_regime,
        release_at = EXCLUDED.release_at,
        available_at = EXCLUDED.available_at,
        availability_basis = EXCLUDED.availability_basis,
        provenance = EXCLUDED.provenance,
        source_ref = EXCLUDED.source_ref,
        run_id = EXCLUDED.run_id,
        code_sha = EXCLUDED.code_sha,
        updated_at = NOW(),
        coverage_fraction = EXCLUDED.coverage_fraction
    WHERE fed_net_liquidity_daily.provenance IS NOT NULL
      AND (
        fed_net_liquidity_daily.net_liquidity_usd_m IS DISTINCT FROM EXCLUDED.net_liquidity_usd_m
        OR fed_net_liquidity_daily.fed_assets_walcl IS DISTINCT FROM EXCLUDED.fed_assets_walcl
        OR fed_net_liquidity_daily.treasury_tga_wtregen IS DISTINCT FROM EXCLUDED.treasury_tga_wtregen
        OR fed_net_liquidity_daily.reverse_repo_rrp IS DISTINCT FROM EXCLUDED.reverse_repo_rrp
        OR fed_net_liquidity_daily.source_ref IS DISTINCT FROM EXCLUDED.source_ref
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
            "reasons": json.dumps(reasons),
            "input_watermarks": json.dumps(input_watermarks),
            "error": error,
            "code_sha": code_sha,
        },
    )


def _log_row(row: RowResult, *, code_sha: str, run_id: str) -> None:
    if row.status in ("written", "noop"):
        log.info(
            "godview.fed_liquidity {label} obs_date={obs_date} net_liquidity_usd_m={nl} "
            "availability_basis={basis} release_at={release_at} code_sha={sha} run_id={run_id} inputs={inputs}",
            label="WRITTEN" if row.status == "written" else "UNCHANGED",
            obs_date=row.obs_date,
            nl=row.net_liquidity_usd_m,
            basis=row.availability_basis,
            release_at=row.release_at,
            sha=code_sha,
            run_id=run_id,
            inputs=[
                {
                    "series_id": i.series_id,
                    "obs_date": i.obs_date.isoformat(),
                    "pull_timestamp": i.pull_timestamp.isoformat() if i.pull_timestamp else None,
                    "source": i.source,
                }
                for i in row.inputs
            ],
        )
    else:
        log.info(
            "godview.fed_liquidity SKIPPED obs_date={obs_date} reason={reason} code_sha={sha} run_id={run_id}",
            obs_date=row.obs_date,
            reason=row.reason,
            sha=code_sha,
            run_id=run_id,
        )


def materialize_fed_liquidity(
    engine: Engine,
    *,
    code_sha: str,
    as_of: date | None = None,
    as_of_ts: datetime | None = None,
    start: date | None = None,
) -> MaterializationResult:
    """Materialize Fed net-liquidity rows for every qualifying Wednesday.

    One DB transaction for the reads and the row upserts, plus the
    ``godview_runs`` ledger row (the FK from ``fed_net_liquidity_daily.run_id``
    is ``DEFERRABLE INITIALLY DEFERRED``, so the ledger row can commit after
    the rows that reference it, in the same transaction -- the #571 atomic
    pattern). An empty-upstream or failed run still writes its own ledger
    row, in a separate transaction, since no data-row transaction ran.

    Parameters
    ----------
    code_sha:
        Required (raises ``ValueError`` if empty) -- the release/commit this
        run executed under; also required by #674's `NOT NULL` and receipt
        check on every provenance-marked row.
    as_of / as_of_ts:
        Passed straight through to ``store.observations.read_window`` --
        ``as_of`` bounds ``obs_date``, ``as_of_ts`` bounds ``pull_timestamp``
        (true point-in-time: "what did we know as of this instant").
    start:
        Optional lower bound on ``obs_date`` for the component read.
    """
    if not code_sha:
        raise ValueError("materialize_fed_liquidity requires a non-empty code_sha")

    as_of = as_of or date.today()
    reference_ts = _ensure_aware_utc(as_of_ts) if as_of_ts is not None else datetime.now(timezone.utc)
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)

    try:
        with engine.begin() as conn:
            walcl_obs = obs_store.read_window(
                conn, WALCL_SERIES_ID, source=SOURCE_NAME, start=start, as_of=as_of, as_of_ts=as_of_ts
            )
            wtregen_obs = obs_store.read_window(
                conn, WTREGEN_SERIES_ID, source=SOURCE_NAME, start=start, as_of=as_of, as_of_ts=as_of_ts
            )
            rrp_obs = obs_store.read_window(
                conn, RRP_SERIES_ID, source=SOURCE_NAME, start=start, as_of=as_of, as_of_ts=as_of_ts
            )

            if not walcl_obs and not wtregen_obs and not rrp_obs:
                raise _EmptyUpstream()

            walcl_by_date = {o.obs_date: o for o in walcl_obs}
            wtregen_by_date = {o.obs_date: o for o in wtregen_obs}
            rrp_by_date = {o.obs_date: o for o in rrp_obs}

            candidate_dates = sorted(
                d
                for d in (set(walcl_by_date) | set(wtregen_by_date) | set(rrp_by_date))
                if d.weekday() == _WEDNESDAY
            )

            written: list[RowResult] = []
            noop: list[RowResult] = []
            skipped: list[RowResult] = []

            for obs_date in candidate_dates:
                walcl_o = walcl_by_date.get(obs_date)
                wtregen_o = wtregen_by_date.get(obs_date)
                rrp_o = rrp_by_date.get(obs_date)  # exact Wednesday only -- no nearest-day fallback

                if walcl_o is None:
                    skipped.append(RowResult(obs_date=obs_date, status="skipped", reason=SKIP_MISSING_WALCL))
                    continue
                if wtregen_o is None:
                    skipped.append(RowResult(obs_date=obs_date, status="skipped", reason=SKIP_MISSING_WTREGEN))
                    continue
                if rrp_o is None:
                    skipped.append(RowResult(obs_date=obs_date, status="skipped", reason=SKIP_MISSING_RRP))
                    continue

                # B3: never touch a legacy (provenance IS NULL) row -- this
                # date needs the plan's A2 archival before this pillar can
                # ever write it.
                row_exists, existing_provenance = _existing_provenance(conn, obs_date)
                if row_exists and existing_provenance is None:
                    skipped.append(
                        RowResult(obs_date=obs_date, status="skipped", reason=SKIP_BLOCKED_BY_LEGACY_ROW)
                    )
                    continue

                # candidate_dates is already filtered to obs_date.weekday()
                # == _WEDNESDAY above, so compute_release_at cannot return
                # None here -- its only None branch is a non-Wednesday obs_date.
                release_at, _release_note = compute_release_at(obs_date)
                assert release_at is not None

                # Publication-lag PIT gate: refuse a row we should not yet know
                # about even if raw_series happens to carry an earlier
                # pull_timestamp than the schedule allows (a leak, a clock
                # skew, or a revision landing early) -- independent of the
                # as_of_ts bound already applied inside read_window above.
                if reference_ts < release_at:
                    skipped.append(RowResult(obs_date=obs_date, status="skipped", reason=SKIP_BEFORE_RELEASE))
                    continue

                pull_timestamps = [
                    t for t in (walcl_o.pull_timestamp, wtregen_o.pull_timestamp, rrp_o.pull_timestamp) if t is not None
                ]
                available_at = min(pull_timestamps) if pull_timestamps else None
                if available_at is None:
                    # Defensive fail-closed: raw_series.pull_timestamp is NOT
                    # NULL in production, so this should not happen, but
                    # #674's receipt CHECK requires available_at whenever
                    # provenance is set -- never write a row that would
                    # violate it, and never fabricate a value to satisfy it.
                    skipped.append(
                        RowResult(obs_date=obs_date, status="skipped", reason=SKIP_MISSING_PULL_TIMESTAMP)
                    )
                    continue

                net_liq_m = compute_net_liquidity_millions(walcl_o.value, wtregen_o.value, rrp_o.value)
                rrp_m = rrp_billions_to_stored_millions(rrp_o.value)  # B1: store in millions, not raw billions

                prior_net_liq, prior_rrp_m = _existing_history(conn, obs_date)  # B2: provenance+Wednesday filtered
                rrp_history = [v for _, v in prior_rrp_m] + [rrp_m]
                rrp_pct_of_peak = compute_rrp_pct_of_peak(rrp_history)
                coverage = coverage_fraction_for_window(len(rrp_history))

                delta_1w_prior = find_nearest_prior(prior_net_liq, obs_date, DELTA_1W_TARGET_DAYS)
                delta_4w_prior = find_nearest_prior(prior_net_liq, obs_date, DELTA_4W_TARGET_DAYS)
                delta_1w_m = None if delta_1w_prior is None else net_liq_m - delta_1w_prior
                delta_4w_m = None if delta_4w_prior is None else net_liq_m - delta_4w_prior
                regime = classify_liquidity_regime(delta_4w_m)

                basis, _basis_note = classify_availability_basis(release_at.date(), available_at)

                source_ref = {
                    "inputs": [
                        {
                            "series_id": WALCL_SERIES_ID,
                            "obs_date": obs_date.isoformat(),
                            "pull_timestamp": walcl_o.pull_timestamp.isoformat() if walcl_o.pull_timestamp else None,
                            "source": walcl_o.source,
                        },
                        {
                            "series_id": WTREGEN_SERIES_ID,
                            "obs_date": obs_date.isoformat(),
                            "pull_timestamp": wtregen_o.pull_timestamp.isoformat() if wtregen_o.pull_timestamp else None,
                            "source": wtregen_o.source,
                        },
                        {
                            "series_id": RRP_SERIES_ID,
                            "obs_date": obs_date.isoformat(),
                            "pull_timestamp": rrp_o.pull_timestamp.isoformat() if rrp_o.pull_timestamp else None,
                            "source": rrp_o.source,
                        },
                    ]
                }

                upsert_result = conn.execute(
                    _UPSERT_SQL,
                    {
                        "obs_date": obs_date,
                        "walcl": walcl_o.value,
                        "wtregen": wtregen_o.value,
                        "rrp_m": rrp_m,
                        "net_liq_m": net_liq_m,
                        "rrp_pct_of_peak": rrp_pct_of_peak,
                        "delta_1w_m": delta_1w_m,
                        "delta_4w_m": delta_4w_m,
                        "regime": regime,
                        "release_at": release_at,
                        "available_at": available_at,
                        "availability_basis": basis,
                        "source_ref": json.dumps(source_ref),
                        "run_id": run_id,
                        "code_sha": code_sha,
                        "coverage_fraction": coverage,
                    },
                )

                inputs = (
                    InputProvenance(WALCL_SERIES_ID, obs_date, walcl_o.value, walcl_o.pull_timestamp, walcl_o.source),
                    InputProvenance(WTREGEN_SERIES_ID, obs_date, wtregen_o.value, wtregen_o.pull_timestamp, wtregen_o.source),
                    InputProvenance(RRP_SERIES_ID, obs_date, rrp_o.value, rrp_o.pull_timestamp, rrp_o.source),
                )
                # rowcount is 0 when the upsert's own "IS DISTINCT FROM" WHERE
                # clause found nothing to change (a re-run with unchanged input
                # vintages, or -- per the WHERE's provenance guard -- a legacy
                # row, though that path is already skipped above by the
                # pre-check and should never reach this statement).
                actually_changed = (upsert_result.rowcount or 0) > 0
                row = RowResult(
                    obs_date=obs_date,
                    status="written" if actually_changed else "noop",
                    net_liquidity_usd_m=net_liq_m,
                    availability_basis=basis,
                    release_at=release_at,
                    inputs=inputs,
                )
                if actually_changed:
                    written.append(row)
                else:
                    noop.append(row)

            for row in written:
                _log_row(row, code_sha=code_sha, run_id=run_id)
            for row in noop:
                _log_row(row, code_sha=code_sha, run_id=run_id)
            for row in skipped:
                _log_row(row, code_sha=code_sha, run_id=run_id)

            finished_at = datetime.now(timezone.utc)
            status = STATUS_SUCCESS if written else STATUS_SUCCESS_NOOP
            run_status = RUN_STATUS_COMPLETE if written else RUN_STATUS_NOOP
            reasons = _count_reasons(skipped)
            watermarks = _input_watermarks(walcl_obs, wtregen_obs, rrp_obs)
            _insert_run_ledger(
                conn,
                run_id=run_id,
                started_at=started_at,
                finished_at=finished_at,
                status=run_status,
                rows_written=len(written),
                rows_skipped=len(noop) + len(skipped),
                reasons=reasons,
                input_watermarks=watermarks,
                code_sha=code_sha,
            )

        log.info(
            "godview.fed_liquidity run complete: {n_written} written, {n_noop} unchanged, "
            "{n_skipped} skipped, run_id={run_id} code_sha={sha}",
            n_written=len(written),
            n_noop=len(noop),
            n_skipped=len(skipped),
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
        )

    except _EmptyUpstream:
        finished_at = datetime.now(timezone.utc)
        log.warning(
            "godview.fed_liquidity EMPTY: no FRED observations for WALCL/WTREGEN/RRPONTSYD, "
            "run_id={run_id} code_sha={sha}",
            run_id=run_id,
            sha=code_sha,
        )
        _try_write_ledger_only(
            engine,
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            status=RUN_STATUS_INPUTS_MISSING,
            code_sha=code_sha,
        )
        return MaterializationResult(
            status=STATUS_EMPTY,
            code_sha=code_sha,
            run_id=run_id,
            message="no raw_series history for WALCL/WTREGEN/RRPONTSYD",
        )
    except obs_store.MixedSourceError:
        raise
    except Exception as exc:  # noqa: BLE001
        finished_at = datetime.now(timezone.utc)
        log.error(
            "godview.fed_liquidity FAILED: {exc}, run_id={run_id} code_sha={sha}",
            exc=exc,
            run_id=run_id,
            sha=code_sha,
        )
        _try_write_ledger_only(
            engine,
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            status=RUN_STATUS_FAILED,
            code_sha=code_sha,
            error=str(exc),
        )
        return MaterializationResult(status=STATUS_FAILED, code_sha=code_sha, run_id=run_id, message=str(exc))


def _count_reasons(skipped: list[RowResult]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in skipped:
        if row.reason:
            counts[row.reason] = counts.get(row.reason, 0) + 1
    return counts


def _input_watermarks(walcl_obs, wtregen_obs, rrp_obs) -> dict[str, str | None]:
    def _latest(obs) -> str | None:
        dates = [o.obs_date for o in obs]
        return max(dates).isoformat() if dates else None

    return {
        WALCL_SERIES_ID: _latest(walcl_obs),
        WTREGEN_SERIES_ID: _latest(wtregen_obs),
        RRP_SERIES_ID: _latest(rrp_obs),
    }


def _try_write_ledger_only(
    engine: Engine,
    *,
    run_id: str,
    started_at: datetime,
    finished_at: datetime,
    status: str,
    code_sha: str,
    error: str | None = None,
) -> None:
    """Best-effort ledger write for a run that never opened (or rolled back)
    a data-row transaction (empty upstream, or an exception). Never raises:
    a ledger-write failure must not mask the original result."""
    try:
        with engine.begin() as conn:
            _insert_run_ledger(
                conn,
                run_id=run_id,
                started_at=started_at,
                finished_at=finished_at,
                status=status,
                rows_written=0,
                rows_skipped=0,
                reasons={},
                input_watermarks={},
                code_sha=code_sha,
                error=error,
            )
    except Exception as ledger_exc:  # noqa: BLE001
        log.warning("godview.fed_liquidity: could not write godview_runs ledger row: {e}", e=ledger_exc)
