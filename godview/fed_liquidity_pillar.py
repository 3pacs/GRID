"""godview.fed_liquidity_pillar — Fed net liquidity God View pillar (W6 Slice B).

    Net Liquidity = WALCL - WTREGEN - RRPONTSYD

Source: reads the raw FRED series ``WALCL``, ``WTREGEN``, ``RRPONTSYD``
directly from ``raw_series`` (as written by ``ingestion/fred.py`` -- read
only, not modified). Does NOT read
``ingestion/altdata/fed_liquidity.py``'s ``COMPUTED:fed_net_liquidity``
series -- that module's own materialized PIT-free number has no
per-component provenance/generation tracking, which this pillar needs and
that one doesn't carry. This pillar computes net liquidity itself, with
its own PIT/generation semantics, but for the UNIT CONVERSION specifically
imports ``normalize_to_millions`` / ``FRED_SERIES_UNIT_QUOTES`` /
``UNIT_SCALE_TO_MILLIONS`` from ``ingestion.altdata.fed_liquidity`` --
that module is the single source of truth for "what unit is series X
stored in, and what's the documented scale factor to millions USD" (fixed
2026-09-18, see its own module docstring for the cited FRED-page quotes:
WALCL/WTREGEN are millions USD, RRPONTSYD is billions USD). Before that
fix, ``ingestion/altdata/fed_liquidity.py`` combined RRPONTSYD (billions)
with WALCL/WTREGEN (millions) with no conversion at all -- a ~1000x scale
bug on the RRPONTSYD term; this pillar never had that bug (it always did
its own conversion), but importing the shared constants now means the two
paths use the exact same scale factor instead of two independently
maintained copies that could drift apart again.

Release schedule: WALCL and WTREGEN are both FRED "Weekly, (ending/as of)
Wednesday" series (confirmed via WebFetch 2026-09-18), published via the
Fed's H.4.1 statistical release --

    "The H.4.1 statistical release, 'Factors Affecting Reserve Balances of
    Depository Institutions and Condition Statement of Federal Reserve
    Banks,' is typically published on Thursday afternoon around 4:30 p.m."
    (https://www.federalreserve.gov/releases/h41/about.htm)

so ``obs_date`` (Wednesday) -> ``release_date`` = obs_date + 1 day
(Thursday) ONLY when obs_date really is a Wednesday; otherwise withheld
(quarantined from strict-PIT reads, same pattern as the CFTC pillar's
Tuesday rule -- see docs/reference/GODVIEW_PILLAR_CONTRACT.md section 8).
RRPONTSYD is a genuinely daily series; this pillar only ever needs the
exact Wednesday value, with NO fallback/interpolation if that exact day
is missing (a market holiday, a gap) -- per the operator's explicit "no
fallback constants" direction, a missing component makes the WHOLE row
unavailable rather than substituting a nearby day's value under the
Wednesday's own date.

Provenance: every raw component is ``measured``; ``net_liquidity_usd_m``
and ``rrp_as_pct_of_peak``/``delta_5d_m``/``delta_30d_m`` are ``derived``.
``forward_impulse_score`` (a column on the tracked table) is intentionally
left NULL in this slice -- no forward-return model has been built or
validated; NULL says "not implemented," never a fabricated number.

Everything above the "DB wrappers" marker is pure Python.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Sequence

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from godview.availability_basis import classify_availability_basis
from godview.generations import (
    STATUS_COMPLETE,
    STATUS_FAILED,
    latest_attempt as _latest_attempt,
    latest_complete_generation as _latest_complete_generation,
    new_generation_id,
    record_generation,
)
from ingestion.altdata.fed_liquidity import (
    UNIT_SCALE_TO_MILLIONS,
    normalize_to_millions,
)
from store.availability import measured_or_none

PILLAR_NAME = "fed_net_liquidity"

WALCL_SERIES_ID = "WALCL"
WTREGEN_SERIES_ID = "WTREGEN"
RRP_SERIES_ID = "RRPONTSYD"

#: Confirmed 2026-09-18 via FRED's own series pages (see module docstring).
#: The conversion itself (normalize_to_millions) is imported above from
#: ingestion.altdata.fed_liquidity -- the single source of truth -- rather
#: than duplicated here.
UNIT = "millions_usd"
RRP_BILLIONS_TO_MILLIONS = UNIT_SCALE_TO_MILLIONS["billions_usd"]

#: H.4.1 publication lag: Wednesday obs_date -> Thursday release (1 day).
FED_RELEASE_LAG_DAYS = 1
_WEDNESDAY = 2  # date.weekday(): Monday=0 ... Sunday=6
RELEASE_RULE_ID = "fed_h41_release_schedule_v1"
_H41_URL = "https://www.federalreserve.gov/releases/h41/about.htm"

#: Rolling window (in weekly Wednesday observations) for rrp_as_pct_of_peak,
#: same 3-year convention as the CFTC pillar's PERCENTILE_WINDOW_3Y.
PEAK_WINDOW_WEEKS = 156
_MIN_HISTORY = 4

#: Delta lookback targets and how far off a "nearest" prior obs may be and
#: still count -- our observations are weekly (7-day spaced), so "5 days"
#: and "30 days" can only ever be approximated by the nearest available
#: prior Wednesday; too far off and the delta is left NULL rather than
#: computed against a mismatched horizon.
DELTA_5D_TARGET_DAYS = 5
DELTA_30D_TARGET_DAYS = 30
DELTA_TOLERANCE_DAYS = 3

#: liquidity_regime thresholds on delta_30d_m (millions USD).
_REGIME_EXPANDING_THRESHOLD = 50_000.0  # +$50B
_REGIME_CONTRACTING_THRESHOLD = -50_000.0


def compute_release_date(obs_date: date) -> tuple[date | None, str]:
    """Apply the H.4.1 release rule to one obs_date. See module docstring."""
    if obs_date.weekday() == _WEDNESDAY:
        release_date = date.fromordinal(obs_date.toordinal() + FED_RELEASE_LAG_DAYS)
        source_ref = (
            f"{RELEASE_RULE_ID}: obs_date is Wednesday -> release_date = "
            f"obs_date + {FED_RELEASE_LAG_DAYS}d (Thursday ~16:30 ET, {_H41_URL})"
        )
        return release_date, source_ref

    source_ref = (
        f"{RELEASE_RULE_ID}: obs_date {obs_date.isoformat()} "
        f"({obs_date.strftime('%A')}) is not Wednesday; release_date withheld "
        "per quarantine rule"
    )
    return None, source_ref


def compute_net_liquidity_millions(
    walcl_raw: float, wtregen_raw: float, rrp_raw: float
) -> float | None:
    """Net Liquidity = WALCL - WTREGEN - RRPONTSYD, each normalised to millions USD.

    Each argument is the RAW value as stored in raw_series (WALCL/WTREGEN in
    their native millions USD, RRPONTSYD in its native billions USD) --
    normalisation happens here via ``normalize_to_millions``
    (``ingestion.altdata.fed_liquidity``'s single source of truth), not by
    the caller pre-scaling anything. Returns ``None`` -- never a fabricated
    number -- if any component's unit is unknown to that shared table.
    """
    w_m = normalize_to_millions(WALCL_SERIES_ID, walcl_raw)
    t_m = normalize_to_millions(WTREGEN_SERIES_ID, wtregen_raw)
    r_m = normalize_to_millions(RRP_SERIES_ID, rrp_raw)
    if w_m is None or t_m is None or r_m is None:
        return None
    return w_m - t_m - r_m


def compute_rrp_pct_of_peak(rrp_millions_history: Sequence[float], window: int = PEAK_WINDOW_WEEKS) -> float | None:
    """Current (last) RRP value as a percentage of the trailing window's peak."""
    finite = [v for v in rrp_millions_history if v is not None]
    if len(finite) < _MIN_HISTORY:
        return None
    windowed = finite[-window:] if len(finite) >= window else finite
    peak = max(windowed)
    if peak <= 0:
        return None
    return (windowed[-1] / peak) * 100.0


def find_nearest_prior(
    history: Sequence[tuple[date, float]], current_date: date, target_days: int, tolerance_days: int = DELTA_TOLERANCE_DAYS
) -> float | None:
    """Value of the prior observation closest to ``target_days`` before ``current_date``.

    ``None`` if no observation lands within ``tolerance_days`` of that
    target gap -- never interpolated, never the nearest observation
    regardless of how far off it actually is.
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


def classify_liquidity_regime(delta_30d_m: float | None) -> str:
    """Always returns a value (liquidity_regime column is NOT NULL)."""
    if delta_30d_m is None:
        return "insufficient_history"
    if delta_30d_m >= _REGIME_EXPANDING_THRESHOLD:
        return "expanding"
    if delta_30d_m <= _REGIME_CONTRACTING_THRESHOLD:
        return "contracting"
    return "neutral"


@dataclass(frozen=True)
class MaterializationResult:
    status: str  # "SUCCESS" | "SUCCESS_NOOP" | "EMPTY" | "FAILED"
    generation_id: str
    rows_written: int = 0
    message: str = ""


# ---------------------------------------------------------------------------
# DB wrappers
# ---------------------------------------------------------------------------


def _read_component_history(conn: Connection, series_id: str, as_of: date) -> dict[date, dict[str, Any]]:
    """PIT-style (LATEST_AS_OF) read of one FRED component from raw_series.

    Same shape as godview/cftc_pillar.py::_read_contract_history, applied to
    a single series_id instead of a contract's five metrics.
    """
    rows = conn.execute(
        text(
            """
            SELECT DISTINCT ON (obs_date)
                obs_date, value, pull_timestamp
            FROM raw_series
            WHERE series_id = :sid
              AND obs_date <= :as_of
              AND pull_status = 'SUCCESS'
            ORDER BY obs_date, pull_timestamp DESC
            """
        ),
        {"sid": series_id, "as_of": as_of},
    ).mappings().all()
    return {r["obs_date"]: {"value": measured_or_none(r["value"]), "pull_timestamp": r["pull_timestamp"]} for r in rows}


def _distinct_pull_count(conn: Connection, series_id: str, obs_date: date) -> int:
    n = conn.execute(
        text(
            "SELECT COUNT(DISTINCT pull_timestamp) FROM raw_series "
            "WHERE series_id = :sid AND obs_date = :od AND pull_status = 'SUCCESS'"
        ),
        {"sid": series_id, "od": obs_date},
    ).scalar()
    return int(n or 0)


def _existing_obs_dates(conn: Connection) -> set[date]:
    rows = conn.execute(text("SELECT obs_date FROM fed_net_liquidity_daily")).fetchall()
    return {r[0] for r in rows}


def _existing_net_liquidity_history(
    conn: Connection, before: date
) -> tuple[list[tuple[date, float]], list[tuple[date, float]]]:
    """Prior materialized (obs_date, net_liquidity_usd_m) AND (obs_date, rrp_millions)
    rows, ascending, for delta/peak math -- one query, two parallel series."""
    rows = conn.execute(
        text(
            "SELECT obs_date, net_liquidity_usd_m, reverse_repo_rrp * :scale AS rrp_m "
            "FROM fed_net_liquidity_daily WHERE obs_date < :before ORDER BY obs_date ASC"
        ),
        {"before": before, "scale": RRP_BILLIONS_TO_MILLIONS},
    ).fetchall()
    net_liq = [(r[0], float(r[1])) for r in rows]
    rrp_m = [(r[0], float(r[2])) for r in rows]
    return net_liq, rrp_m


def materialize_fed_liquidity_pillar(engine: Engine, *, as_of: date | None = None) -> MaterializationResult:
    """Materialize new Fed net liquidity rows as one atomic generation.

    Same transactional/idempotent/no-fallback shape as
    ``godview/cftc_pillar.py::materialize_cftc_pillar`` -- one DB
    transaction, INSERT-only, failed/empty upstream never advances the
    generation, unchanged upstream re-run is a no-op. See the module
    docstring for the pillar-specific rules (unit conversion, Wednesday
    cadence, no fallback for a missing component).
    """
    as_of = as_of or date.today()
    generation_id = new_generation_id()

    try:
        with engine.begin() as conn:
            walcl_hist = _read_component_history(conn, WALCL_SERIES_ID, as_of)
            wtregen_hist = _read_component_history(conn, WTREGEN_SERIES_ID, as_of)
            rrp_hist = _read_component_history(conn, RRP_SERIES_ID, as_of)

            if not walcl_hist and not wtregen_hist and not rrp_hist:
                raise _EmptyUpstream()

            existing = _existing_obs_dates(conn)

            # Only Wednesdays where WALCL and WTREGEN both have a value are
            # even candidate observation dates (their own weekly cadence).
            candidate_dates = sorted(
                d for d in (set(walcl_hist) & set(wtregen_hist)) if d.weekday() == _WEDNESDAY and d not in existing
            )

            rows_to_insert: list[dict[str, Any]] = []
            for obs_date in candidate_dates:
                walcl = walcl_hist[obs_date]["value"]
                wtregen = wtregen_hist[obs_date]["value"]
                rrp_entry = rrp_hist.get(obs_date)
                # NO fallback: the exact day's RRP or nothing.
                rrp = rrp_entry["value"] if rrp_entry is not None else None

                if walcl is None or wtregen is None or rrp is None:
                    continue  # missing component -> this obs_date stays unavailable, not fabricated

                net_liquidity_m = compute_net_liquidity_millions(walcl, wtregen, rrp)
                if net_liquidity_m is None:
                    continue  # unknown unit for a component -- no fallback, no row

                rrp_m = normalize_to_millions(RRP_SERIES_ID, rrp)
                if rrp_m is None:
                    continue  # same guard, belt-and-suspenders with the check above

                prior_net_liq, prior_rrp_m = _existing_net_liquidity_history(conn, obs_date)
                rrp_m_history = [v for _, v in prior_rrp_m] + [rrp_m]
                rrp_pct_of_peak = compute_rrp_pct_of_peak(rrp_m_history)
                coverage = coverage_fraction_for_window(len(rrp_m_history))

                delta_5d = find_nearest_prior(prior_net_liq, obs_date, DELTA_5D_TARGET_DAYS)
                delta_30d = find_nearest_prior(prior_net_liq, obs_date, DELTA_30D_TARGET_DAYS)
                delta_5d_m = None if delta_5d is None else net_liquidity_m - delta_5d
                delta_30d_m = None if delta_30d is None else net_liquidity_m - delta_30d
                regime = classify_liquidity_regime(delta_30d_m)

                release_date, source_ref = compute_release_date(obs_date)

                walcl_pulled_at = walcl_hist[obs_date]["pull_timestamp"]
                wtregen_pulled_at = wtregen_hist[obs_date]["pull_timestamp"]
                rrp_pulled_at = rrp_entry["pull_timestamp"]
                available_at = min(walcl_pulled_at, wtregen_pulled_at, rrp_pulled_at)

                distinct_pulls = max(
                    _distinct_pull_count(conn, WALCL_SERIES_ID, obs_date),
                    _distinct_pull_count(conn, WTREGEN_SERIES_ID, obs_date),
                    _distinct_pull_count(conn, RRP_SERIES_ID, obs_date),
                )
                basis, basis_note = classify_availability_basis(
                    release_date, available_at, distinct_pull_count=distinct_pulls
                )
                if basis_note:
                    source_ref = f"{source_ref}; {basis_note}"

                rows_to_insert.append(
                    {
                        "obs_date": obs_date,
                        "fed_assets_walcl": walcl,
                        "treasury_tga_wtregen": wtregen,
                        "reverse_repo_rrp": rrp,
                        "net_liquidity_usd_m": net_liquidity_m,
                        "rrp_as_pct_of_peak": rrp_pct_of_peak,
                        "delta_5d_m": delta_5d_m,
                        "delta_30d_m": delta_30d_m,
                        "liquidity_regime": regime,
                        "forward_impulse_score": None,
                        "release_date": release_date,
                        "available_at": available_at,
                        "walcl_pulled_at": walcl_pulled_at,
                        "wtregen_pulled_at": wtregen_pulled_at,
                        "rrp_pulled_at": rrp_pulled_at,
                        "provenance": "measured",
                        "availability_basis": basis,
                        "generation_id": generation_id,
                        "coverage_fraction": coverage,
                        "source_ref": source_ref,
                    }
                )

            for row in rows_to_insert:
                conn.execute(
                    text(
                        """
                        INSERT INTO fed_net_liquidity_daily (
                            obs_date, fed_assets_walcl, treasury_tga_wtregen, reverse_repo_rrp,
                            net_liquidity_usd_m, rrp_as_pct_of_peak, delta_5d_m, delta_30d_m,
                            liquidity_regime, forward_impulse_score, release_date, available_at,
                            walcl_pulled_at, wtregen_pulled_at, rrp_pulled_at, provenance,
                            availability_basis, generation_id, coverage_fraction, source_ref
                        ) VALUES (
                            :obs_date, :fed_assets_walcl, :treasury_tga_wtregen, :reverse_repo_rrp,
                            :net_liquidity_usd_m, :rrp_as_pct_of_peak, :delta_5d_m, :delta_30d_m,
                            :liquidity_regime, :forward_impulse_score, :release_date, :available_at,
                            :walcl_pulled_at, :wtregen_pulled_at, :rrp_pulled_at, :provenance,
                            :availability_basis, :generation_id, :coverage_fraction, :source_ref
                        )
                        ON CONFLICT (obs_date) DO NOTHING
                        """
                    ),
                    row,
                )

            record_generation(
                conn,
                pillar=PILLAR_NAME,
                generation_id=generation_id,
                status=STATUS_COMPLETE,
                row_count=len(rows_to_insert),
            )

        status = "SUCCESS" if rows_to_insert else "SUCCESS_NOOP"
        return MaterializationResult(
            status=status,
            generation_id=generation_id,
            rows_written=len(rows_to_insert),
            message=f"{len(rows_to_insert)} new row(s)",
        )

    except _EmptyUpstream:
        _record_failure(engine, generation_id, "empty_upstream")
        return MaterializationResult(status="EMPTY", generation_id=generation_id, message="no raw_series history for WALCL/WTREGEN/RRPONTSYD")
    except Exception as exc:  # noqa: BLE001
        _record_failure(engine, generation_id, str(exc))
        return MaterializationResult(status="FAILED", generation_id=generation_id, message=str(exc))


class _EmptyUpstream(Exception):
    pass


def _record_failure(engine: Engine, generation_id: str, reason: str) -> None:
    try:
        with engine.begin() as conn:
            record_generation(conn, pillar=PILLAR_NAME, generation_id=generation_id, status=STATUS_FAILED, failure_reason=reason)
    except Exception:  # noqa: BLE001
        pass


@dataclass(frozen=True)
class PillarReadResult:
    state: str  # "never_configured" | "materializer_failed" | "ok"
    rows: list[dict[str, Any]] = field(default_factory=list)
    generation_id: str | None = None
    generation_published_at: Any = None


def read_fed_liquidity_pillar(
    conn: Connection, as_of: date, *, include_inferred: bool = False
) -> PillarReadResult:
    """Strict-PIT read: the latest qualifying row as of ``as_of``.

    "Qualifying" = release_date IS NOT NULL AND release_date <= as_of, and
    (unless include_inferred) availability_basis = 'observed_acquisition'.
    Same contract as godview/cftc_pillar.py::read_cftc_pillar.
    """
    generation = _latest_complete_generation(conn, PILLAR_NAME)
    attempt = _latest_attempt(conn, PILLAR_NAME)

    if generation is None:
        if attempt is not None and attempt["status"] == STATUS_FAILED:
            return PillarReadResult(state="materializer_failed")
        return PillarReadResult(state="never_configured")

    basis_filter = "" if include_inferred else "AND availability_basis = 'observed_acquisition'"
    row = conn.execute(
        text(
            f"""
            SELECT obs_date, fed_assets_walcl, treasury_tga_wtregen, reverse_repo_rrp,
                   net_liquidity_usd_m, rrp_as_pct_of_peak, delta_5d_m, delta_30d_m,
                   liquidity_regime, forward_impulse_score, release_date, available_at,
                   walcl_pulled_at, wtregen_pulled_at, rrp_pulled_at, provenance,
                   availability_basis, generation_id, coverage_fraction, source_ref
            FROM fed_net_liquidity_daily
            WHERE release_date IS NOT NULL AND release_date <= :as_of
              {basis_filter}
            ORDER BY obs_date DESC
            LIMIT 1
            """
        ),
        {"as_of": as_of},
    ).mappings().fetchone()

    rows = [dict(row)] if row is not None else []
    return PillarReadResult(
        state="ok",
        rows=rows,
        generation_id=generation["generation_id"],
        generation_published_at=generation["published_at"],
    )
