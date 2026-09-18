"""godview.commodity_warehouse_pillar — commodity warehouse God View pillar (W6 Slice B).

Two independent fields, deliberately asymmetric because the underlying
data is asymmetric:

1. **LME cancelled-warrant ratio** -- reuses (read-only)
   ``ingestion/altdata/lme_warehouse.py``'s own ``raw_series`` writes:
   ``lme:stocks_total_mt:<metal>``, ``lme:stocks_cancelled_mt:<metal>``,
   ``lme:stocks_live_mt:<metal>``, ``lme:cancelled_ratio:<metal>`` (the
   puller already computes the ratio -- this module reads it, it does not
   recompute it independently, so the two can never silently disagree).
   Real data, real materializer, real table.

2. **Cushing, OK weekly crude stocks** -- grepped the whole ingestion tree
   (2026-09-18) for a real EIA Cushing series id (e.g. the standard
   ``WCSSTUS1``/``PET.WCSSTUS1.W``) and found none. The only superficially
   similar series pulled anywhere is ``WCESTUS1`` via
   ``ingestion/altdata/refinery_cracks.py``, and that is US refiner
   GASOLINE stocks (stored as ``refinery_cracks:gasoline_stocks``), not
   Cushing crude oil -- using it would silently substitute the wrong
   quantity under the Cushing label, exactly the kind of defect
   ``docs/reference/AVAILABILITY_CONTRACT.md`` calls out by name. Per the
   operator's explicit instruction, this field reports
   ``unavailable(reason="never_configured")`` unconditionally --
   ``CUSHING_SERIES_ID`` below is ``None`` on purpose, and nothing in this
   module ever substitutes a literal/placeholder value for it.

Release schedule: unlike the CFTC pillar (Tuesday->Friday, CFTC's own
published page) and the Fed liquidity pillar (Wednesday->Thursday, the
Fed's own H.4.1 page), no official LME warehouse-stocks publication
schedule was found to cite -- ``ingestion/altdata/lme_warehouse.py``'s own
docstring says as much ("the exact URL is subject to change and has not
been confirmed by a network capture"). Fabricating a same-day-publication
rule without a citation would be exactly the "assumed schedule" pattern
Slice A's operator direction warns against, just one hop earlier (assuming
a schedule exists at all, rather than assuming a backfill is on-schedule).
So: ``release_date`` is always ``NULL`` for this pillar's rows (schema
column kept for consistency with the other pillars and any future
citable-schedule pillar), and ``availability_basis`` is always
``'unknown'`` (``classify_availability_basis`` already returns exactly
that when there is no ``release_date`` to compare against -- no special
case needed here). The strict-PIT read below therefore bounds results by
``report_date <= as_of`` only -- the one honest temporal fact this pillar
actually has -- rather than pretending a schedule-based PIT gate exists
when it does not.

Everything above the "DB wrappers" marker is pure Python.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from godview.availability_basis import AVAILABILITY_BASIS_UNKNOWN
from godview.generations import (
    STATUS_COMPLETE,
    STATUS_FAILED,
    latest_attempt as _latest_attempt,
    latest_complete_generation as _latest_complete_generation,
    new_generation_id,
    record_generation,
)
from store.availability import measured_or_none

PILLAR_NAME = "commodity_warehouses"
EXCHANGE = "LME"
UNIT_METRIC_TONNES = "metric_tonnes"
UNIT_RATIO = "ratio_0_1"

LME_METALS: tuple[str, ...] = ("copper", "aluminum", "zinc", "nickel", "lead", "tin")

#: See module docstring section 2 -- deliberately None. Grepped 2026-09-18;
#: no real Cushing-specific series id (e.g. WCSSTUS1) exists in raw_series
#: anywhere in this codebase. Do not set this to a placeholder string.
CUSHING_SERIES_ID: str | None = None
CUSHING_UNAVAILABLE_REASON = (
    "never_configured: no real EIA Cushing, OK crude-stocks series id is pulled "
    "anywhere in this codebase (grepped 2026-09-18) -- WCESTUS1 "
    "(ingestion/altdata/refinery_cracks.py) is US refiner GASOLINE stocks, not "
    "Cushing crude, and using it would silently substitute the wrong quantity"
)

RELEASE_RULE_ID = "lme_no_cited_schedule_v1"
_NO_SCHEDULE_SOURCE_REF = (
    f"{RELEASE_RULE_ID}: no official LME warehouse-stocks publication schedule "
    "was found to cite (see ingestion/altdata/lme_warehouse.py's own docstring); "
    "release_date withheld rather than assumed"
)

#: Heuristic tightness threshold on cancelled_ratio, documented (not asserted
#: as calibrated) -- see lme_warehouse.py's own docstring: cancelled-warrant
#: spikes "lead copper, aluminum, zinc and nickel price moves by ~5-15 days."
PHYSICAL_TIGHTNESS_THRESHOLD = 0.30


def classify_physical_tightness(cancelled_ratio: float | None) -> bool:
    """True when cancelled_ratio meets the documented heuristic threshold. Never None."""
    if cancelled_ratio is None:
        return False
    return cancelled_ratio >= PHYSICAL_TIGHTNESS_THRESHOLD


def compute_net_change(current: float, prior: float | None) -> float | None:
    """Day-over-day change in total_inventory. None when there is no prior observation."""
    if prior is None:
        return None
    return current - prior


@dataclass(frozen=True)
class MaterializationResult:
    status: str  # "SUCCESS" | "SUCCESS_NOOP" | "EMPTY" | "FAILED"
    generation_id: str
    rows_written: int = 0
    message: str = ""


# ---------------------------------------------------------------------------
# DB wrappers
# ---------------------------------------------------------------------------


def _read_metal_history(conn: Connection, metal: str, as_of: date) -> dict[date, dict[str, Any]]:
    """PIT-style (LATEST_AS_OF) read of one metal's four LME series from raw_series."""
    series_ids = [
        f"lme:stocks_total_mt:{metal}",
        f"lme:stocks_cancelled_mt:{metal}",
        f"lme:stocks_live_mt:{metal}",
        f"lme:cancelled_ratio:{metal}",
    ]
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

    field_by_series = {
        f"lme:stocks_total_mt:{metal}": "total_mt",
        f"lme:stocks_cancelled_mt:{metal}": "cancelled_mt",
        f"lme:stocks_live_mt:{metal}": "live_mt",
        f"lme:cancelled_ratio:{metal}": "cancelled_ratio",
    }

    out: dict[date, dict[str, Any]] = {}
    for row in rows:
        key = field_by_series.get(row["series_id"])
        if key is None:
            continue
        obs_date = row["obs_date"]
        bucket = out.setdefault(obs_date, {"_pull_ts": {}})
        bucket[key] = measured_or_none(row["value"])
        bucket["_pull_ts"][key] = row["pull_timestamp"]
    return out


def _existing_report_dates(conn: Connection, metal: str) -> set[date]:
    rows = conn.execute(
        text(
            "SELECT report_date FROM commodity_warehouse_inventories "
            "WHERE exchange = :exch AND commodity = :metal"
        ),
        {"exch": EXCHANGE, "metal": metal},
    ).fetchall()
    return {r[0] for r in rows}


def _prior_total_inventory(conn: Connection, metal: str, before: date) -> float | None:
    row = conn.execute(
        text(
            "SELECT total_inventory FROM commodity_warehouse_inventories "
            "WHERE exchange = :exch AND commodity = :metal AND report_date < :before "
            "ORDER BY report_date DESC LIMIT 1"
        ),
        {"exch": EXCHANGE, "metal": metal, "before": before},
    ).fetchone()
    return float(row[0]) if row is not None else None


def materialize_commodity_warehouse_pillar(
    engine: Engine, *, as_of: date | None = None, metals: tuple[str, ...] = LME_METALS
) -> MaterializationResult:
    """Materialize new LME warehouse rows as one atomic generation.

    Same transactional/idempotent shape as the CFTC and Fed liquidity
    pillars. Cushing is never written here -- there is nothing real to
    write (see module docstring); it is handled entirely at the API layer
    as a permanent ``unavailable(never_configured)`` field.
    """
    as_of = as_of or date.today()
    generation_id = new_generation_id()

    try:
        with engine.begin() as conn:
            any_history = False
            rows_to_insert: list[dict[str, Any]] = []

            for metal in metals:
                history = _read_metal_history(conn, metal, as_of)
                if history:
                    any_history = True

                existing = _existing_report_dates(conn, metal)

                for report_date, values in sorted(history.items()):
                    if report_date in existing:
                        continue
                    total = values.get("total_mt")
                    if total is None:
                        continue  # no fallback -- an incomplete day stays unmaterialized

                    cancelled = values.get("cancelled_mt")
                    live = values.get("live_mt")
                    ratio = values.get("cancelled_ratio")

                    prior_total = _prior_total_inventory(conn, metal, report_date)
                    net_change = compute_net_change(total, prior_total)
                    tightness = classify_physical_tightness(ratio)

                    pull_ts = values.get("_pull_ts", {})
                    available_at = min(pull_ts.values()) if pull_ts else None

                    rows_to_insert.append(
                        {
                            "report_date": report_date,
                            "exchange": EXCHANGE,
                            "commodity": metal,
                            "location_hub": None,
                            "total_inventory": total,
                            "unit": UNIT_METRIC_TONNES,
                            "on_warrant": live,
                            "canceled_warrants": cancelled,
                            "canceled_ratio": ratio,
                            "operational_floor": None,
                            "floor_buffer_pct": None,
                            "net_change_daily": net_change,
                            "physical_tightness_flag": tightness,
                            "release_date": None,  # see module docstring: no cited schedule
                            "available_at": available_at,
                            "provenance": "measured",
                            "availability_basis": AVAILABILITY_BASIS_UNKNOWN,
                            "generation_id": generation_id,
                            "coverage_fraction": None,
                            "source_ref": _NO_SCHEDULE_SOURCE_REF,
                        }
                    )

            if not any_history:
                raise _EmptyUpstream()

            for row in rows_to_insert:
                conn.execute(
                    text(
                        """
                        INSERT INTO commodity_warehouse_inventories (
                            report_date, exchange, commodity, location_hub,
                            total_inventory, unit, on_warrant, canceled_warrants,
                            canceled_ratio, operational_floor, floor_buffer_pct,
                            net_change_daily, physical_tightness_flag,
                            release_date, available_at, provenance,
                            availability_basis, generation_id, coverage_fraction,
                            source_ref
                        ) VALUES (
                            :report_date, :exchange, :commodity, :location_hub,
                            :total_inventory, :unit, :on_warrant, :canceled_warrants,
                            :canceled_ratio, :operational_floor, :floor_buffer_pct,
                            :net_change_daily, :physical_tightness_flag,
                            :release_date, :available_at, :provenance,
                            :availability_basis, :generation_id, :coverage_fraction,
                            :source_ref
                        )
                        ON CONFLICT (report_date, exchange, commodity, location_hub) DO NOTHING
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
            status=status, generation_id=generation_id, rows_written=len(rows_to_insert),
            message=f"{len(rows_to_insert)} new row(s)",
        )
    except _EmptyUpstream:
        _record_failure(engine, generation_id, "empty_upstream")
        return MaterializationResult(status="EMPTY", generation_id=generation_id, message="no raw_series lme:* history")
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
    metals_with_data: int = 0
    metals_expected: int = field(default_factory=lambda: len(LME_METALS))
    generation_id: str | None = None
    generation_published_at: Any = None


def read_commodity_warehouse_pillar(
    conn: Connection, as_of: date, *, metals: tuple[str, ...] = LME_METALS
) -> PillarReadResult:
    """Latest row per metal with ``report_date <= as_of``.

    No schedule-based PIT gate -- see module docstring for why. Never
    raises for a missing table; the caller (the API router) is responsible
    for the ``to_regclass`` probe.
    """
    generation = _latest_complete_generation(conn, PILLAR_NAME)
    attempt = _latest_attempt(conn, PILLAR_NAME)

    if generation is None:
        if attempt is not None and attempt["status"] == STATUS_FAILED:
            return PillarReadResult(state="materializer_failed", metals_expected=len(metals))
        return PillarReadResult(state="never_configured", metals_expected=len(metals))

    rows = conn.execute(
        text(
            """
            SELECT DISTINCT ON (commodity)
                report_date, exchange, commodity, location_hub, total_inventory,
                unit, on_warrant, canceled_warrants, canceled_ratio,
                operational_floor, floor_buffer_pct, net_change_daily,
                physical_tightness_flag, release_date, available_at, provenance,
                availability_basis, generation_id, coverage_fraction, source_ref
            FROM commodity_warehouse_inventories
            WHERE exchange = :exch AND commodity = ANY(:metals) AND report_date <= :as_of
            ORDER BY commodity, report_date DESC
            """
        ),
        {"exch": EXCHANGE, "metals": list(metals), "as_of": as_of},
    ).mappings().all()

    rows_out = [dict(r) for r in rows]
    return PillarReadResult(
        state="ok",
        rows=rows_out,
        metals_with_data=len(rows_out),
        metals_expected=len(metals),
        generation_id=generation["generation_id"],
        generation_published_at=generation["published_at"],
    )
