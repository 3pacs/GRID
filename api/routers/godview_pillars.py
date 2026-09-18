"""God View pillar endpoints.

Built: CFTC positioning, Fed net liquidity, commodity warehouses (LME leg
only -- Cushing is permanently unavailable(never_configured), see
godview/commodity_warehouse_pillar.py). Every other pillar name renders the
honest "not built yet" state, with the specific reason it is blocked --
see docs/reference/GODVIEW_PILLAR_CONTRACT.md's status table.

Never raises 500 for a missing table: every table this router reads is
probed with ``to_regclass`` first, mirroring the ``_table_exists`` pattern
already used in api/routers/flows.py and friends.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine
from godview.availability_basis import (
    AVAILABILITY_BASIS_INFERRED,
    AVAILABILITY_BASIS_UNKNOWN,
    INFERRED_BASIS_NOTE,
    UNKNOWN_BASIS_NOTE,
)
from godview.cftc_pillar import (
    CFTC_PILLAR_CONTRACTS,
    PILLAR_NAME,
    STALE_AFTER_DAYS,
    read_cftc_pillar,
)
from godview.commodity_warehouse_pillar import (
    CUSHING_UNAVAILABLE_REASON,
    LME_METALS,
    PILLAR_NAME as COMMODITY_PILLAR_NAME,
    read_commodity_warehouse_pillar,
)
from godview.fed_liquidity_pillar import (
    PILLAR_NAME as FED_PILLAR_NAME,
    UNIT as FED_UNIT,
    read_fed_liquidity_pillar,
)
from store.availability import unavailable
from store.availability_fields import (
    STALE_MATERIALIZER_FAILED,
    STALE_NEVER_CONFIGURED,
    STALE_PARTIAL_HISTORY,
    STALE_STALE,
    derived_field,
    measured_field,
    unavailable_field,
)

router = APIRouter(prefix="/api/v1/godview", tags=["godview"])

#: Pillars this slice knows the *name* of but has not built, with WHY each
#: is blocked (operator direction, 2026-09-18) -- kept explicit (rather than
#: a catch-all) so a typo in the URL still reads as "not built" honestly,
#: not a silent 404.
_KNOWN_UNBUILT_PILLARS = {
    "finra_short_volume": "adapter exists but is unscheduled/unverified live",
    "sec_regsho_ftd": "adapter exists but is unscheduled/unverified live",
    "buyback_blackouts": "no measured source",
    "dealer_gex": "engine correctness unproven",
}

_RAW_FIELD_UNIT = {
    "total_open_interest": "contracts",
    "commercial_long": "contracts",
    "commercial_short": "contracts",
    "commercial_net": "contracts",
    "noncommercial_long": "contracts",
    "noncommercial_short": "contracts",
    "noncommercial_net": "contracts",
    "spec_net_pct_oi": "pct",
}
_DERIVED_FIELD_UNIT = {
    "z_score_1y": "zscore",
    "z_score_3y": "zscore",
    "percentile_3y": "pct",
    "crowding_regime": None,
}


def _table_exists(conn: Any, table_name: str) -> bool:
    try:
        row = conn.execute(text("SELECT to_regclass(:n)").bindparams(n=table_name)).fetchone()
        return bool(row and row[0])
    except Exception:  # noqa: BLE001 -- missing table must degrade, never 500
        return False


def _availability_basis_note(basis: str | None) -> str | None:
    """Human-readable caveat for a non-observed basis. See contract doc section 10."""
    if basis == AVAILABILITY_BASIS_INFERRED:
        return INFERRED_BASIS_NOTE
    if basis == AVAILABILITY_BASIS_UNKNOWN:
        return UNKNOWN_BASIS_NOTE
    return None


def _row_to_field_records(row: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """One FieldRecord per positioning value, per contract doc section 4's two-layer split.

    Every field's dict also carries ``availability_basis`` and
    ``availability_basis_note`` as SIBLING keys alongside the vendored
    FieldRecord's own ``to_dict()`` output (contract doc section 10) --
    deliberately not folded into FieldRecord itself, which is vendored
    verbatim from feat/availability-provenance-contract and must not diverge
    further from that upstream shape before it merges.
    """
    out: dict[str, dict[str, Any]] = {}
    common = {
        "obs_date": row["report_date"],
        "published_at": row["release_date"],
        "available_at": row["available_at"],
        "revision": row["generation_id"],
        "source_catalog": "raw_series:cftc",
        "series_id": row["contract_code"],
    }
    basis = row.get("availability_basis")
    basis_note = _availability_basis_note(basis)

    for name, unit in _RAW_FIELD_UNIT.items():
        record = measured_field(row[name], unit=unit, **common).to_dict()
        record["availability_basis"] = basis
        record["availability_basis_note"] = basis_note
        out[name] = record

    for name, unit in _DERIVED_FIELD_UNIT.items():
        value = row[name]
        if value is None:
            record = unavailable_field(
                STALE_PARTIAL_HISTORY,
                unit=unit,
                calculation_version="cftc_pillar_v1",
                coverage_fraction=row["coverage_fraction"],
                **common,
            ).to_dict()
        else:
            record = derived_field(
                value,
                unit=unit,
                calculation_version="cftc_pillar_v1",
                coverage_fraction=row["coverage_fraction"],
                **common,
            ).to_dict()
        record["availability_basis"] = basis
        record["availability_basis_note"] = basis_note
        out[name] = record
    return out


@router.get("/pillars/cftc")
def get_cftc_pillar(
    as_of: date | None = Query(default=None),
    include_inferred: bool = Query(
        default=False,
        description=(
            "Admit availability_basis='inferred_schedule'/'unknown' rows "
            "(revised/backfilled records). Strict PIT default is False -- "
            "observed_acquisition rows only. See contract doc section 10."
        ),
    ),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Strict-PIT read of the CFTC positioning pillar as of ``as_of`` (default today)."""
    as_of = as_of or date.today()
    engine = get_db_engine()

    try:
        with engine.connect() as conn:
            if not _table_exists(conn, "cftc_positioning_daily") or not _table_exists(
                conn, "godview_generations"
            ):
                return unavailable(
                    "cftc_positioning_daily or godview_generations does not exist yet",
                    source=PILLAR_NAME,
                    pillar=PILLAR_NAME,
                    coverage=None,
                )

            result = read_cftc_pillar(
                conn, as_of, contracts=CFTC_PILLAR_CONTRACTS, include_inferred=include_inferred
            )
    except Exception as exc:  # noqa: BLE001 -- never 500 on a data-layer surprise
        return unavailable(
            f"godview read failed: {exc}",
            source=PILLAR_NAME,
            pillar=PILLAR_NAME,
            coverage=None,
        )

    if result.state == "never_configured":
        return unavailable(STALE_NEVER_CONFIGURED, source=PILLAR_NAME, pillar=PILLAR_NAME, coverage=None)
    if result.state == "materializer_failed":
        return unavailable(STALE_MATERIALIZER_FAILED, source=PILLAR_NAME, pillar=PILLAR_NAME, coverage=None)

    coverage = (
        result.contracts_with_data / result.contracts_expected
        if result.contracts_expected
        else None
    )

    stale_reason = None
    if result.newest_release_date is not None:
        age_days = (as_of - result.newest_release_date).days
        if age_days > STALE_AFTER_DAYS:
            stale_reason = STALE_STALE

    fields_by_contract: dict[str, dict[str, dict[str, Any]]] = {}
    for row in result.rows:
        fields_by_contract[row["contract_code"]] = _row_to_field_records(row)

    return {
        "available": True,
        "status": "ok" if coverage == 1.0 and stale_reason is None else "partial",
        "pillar": PILLAR_NAME,
        "as_of": as_of.isoformat(),
        "include_inferred": include_inferred,
        "coverage": coverage,
        "contracts_with_data": result.contracts_with_data,
        "contracts_expected": result.contracts_expected,
        "stale_reason": stale_reason,
        "generation_id": result.generation_id,
        "generation_published_at": (
            result.generation_published_at.isoformat() if result.generation_published_at else None
        ),
        "contracts": {
            key: meta["contract_code"] for key, meta in CFTC_PILLAR_CONTRACTS.items()
        },
        "fields": fields_by_contract,
    }


def _fed_field_records(row: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Per-component + derived FieldRecords for one fed_net_liquidity_daily row."""
    basis = row.get("availability_basis")
    basis_note = _availability_basis_note(basis)
    out: dict[str, dict[str, Any]] = {}

    component_specs = (
        ("fed_assets_walcl", "walcl_pulled_at", "WALCL"),
        ("treasury_tga_wtregen", "wtregen_pulled_at", "WTREGEN"),
        ("reverse_repo_rrp", "rrp_pulled_at", "RRPONTSYD"),
    )
    for field_name, pulled_at_col, series_id in component_specs:
        component_unit = "billions_usd" if series_id == "RRPONTSYD" else FED_UNIT
        record = measured_field(
            row[field_name],
            unit=component_unit,
            obs_date=row["obs_date"],
            published_at=row["release_date"],
            available_at=row[pulled_at_col],
            revision=row["generation_id"],
            source_catalog="raw_series:fred",
            series_id=series_id,
        ).to_dict()
        record["availability_basis"] = basis
        record["availability_basis_note"] = basis_note
        out[field_name] = record

    derived_specs = {
        "net_liquidity_usd_m": FED_UNIT,
        "rrp_as_pct_of_peak": "pct",
        "delta_5d_m": FED_UNIT,
        "delta_30d_m": FED_UNIT,
        "liquidity_regime": None,
    }
    common = {
        "obs_date": row["obs_date"],
        "published_at": row["release_date"],
        "available_at": row["available_at"],
        "revision": row["generation_id"],
        "source_catalog": "raw_series:fred",
        "series_id": "COMPUTED:fed_net_liquidity",
    }
    for name, unit in derived_specs.items():
        value = row[name]
        if value is None:
            record = unavailable_field(
                STALE_PARTIAL_HISTORY, unit=unit, calculation_version="fed_liquidity_pillar_v1",
                coverage_fraction=row["coverage_fraction"], **common,
            ).to_dict()
        else:
            record = derived_field(
                value, unit=unit, calculation_version="fed_liquidity_pillar_v1",
                coverage_fraction=row["coverage_fraction"], **common,
            ).to_dict()
        record["availability_basis"] = basis
        record["availability_basis_note"] = basis_note
        out[name] = record
    return out


@router.get("/pillars/fed_net_liquidity")
def get_fed_liquidity_pillar(
    as_of: date | None = Query(default=None),
    include_inferred: bool = Query(default=False),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Strict-PIT read of the Fed net liquidity pillar as of ``as_of`` (default today)."""
    as_of = as_of or date.today()
    engine = get_db_engine()

    try:
        with engine.connect() as conn:
            if not _table_exists(conn, "fed_net_liquidity_daily") or not _table_exists(conn, "godview_generations"):
                return unavailable(
                    "fed_net_liquidity_daily or godview_generations does not exist yet",
                    source=FED_PILLAR_NAME, pillar=FED_PILLAR_NAME, coverage=None,
                )
            result = read_fed_liquidity_pillar(conn, as_of, include_inferred=include_inferred)
    except Exception as exc:  # noqa: BLE001
        return unavailable(f"godview read failed: {exc}", source=FED_PILLAR_NAME, pillar=FED_PILLAR_NAME, coverage=None)

    if result.state == "never_configured":
        return unavailable(STALE_NEVER_CONFIGURED, source=FED_PILLAR_NAME, pillar=FED_PILLAR_NAME, coverage=None)
    if result.state == "materializer_failed":
        return unavailable(STALE_MATERIALIZER_FAILED, source=FED_PILLAR_NAME, pillar=FED_PILLAR_NAME, coverage=None)

    if not result.rows:
        return {
            "available": True,
            "status": "partial",
            "pillar": FED_PILLAR_NAME,
            "as_of": as_of.isoformat(),
            "include_inferred": include_inferred,
            "coverage": 0.0,
            "stale_reason": None,
            "generation_id": result.generation_id,
            "generation_published_at": result.generation_published_at.isoformat() if result.generation_published_at else None,
            "fields": {},
        }

    row = result.rows[0]
    age_days = (as_of - row["release_date"]).days if row["release_date"] else None
    stale_reason = STALE_STALE if age_days is not None and age_days > 10 else None

    return {
        "available": True,
        "status": "ok" if stale_reason is None else "partial",
        "pillar": FED_PILLAR_NAME,
        "as_of": as_of.isoformat(),
        "include_inferred": include_inferred,
        "coverage": 1.0,
        "stale_reason": stale_reason,
        "generation_id": result.generation_id,
        "generation_published_at": result.generation_published_at.isoformat() if result.generation_published_at else None,
        "fields": _fed_field_records(row),
    }


_COMMODITY_BASIS_NOTE = (
    "no official LME warehouse-stocks publication schedule is cited; "
    "availability_basis is 'unknown' by design (not a data-quality issue) -- "
    "see godview/commodity_warehouse_pillar.py"
)


def _commodity_field_records(row: dict[str, Any]) -> dict[str, dict[str, Any]]:
    # Deliberately NOT _availability_basis_note(): that helper's UNKNOWN_BASIS_NOTE
    # text ("acquisition observed before the scheduled release") describes the
    # CFTC/Fed schedule-comparison case, which doesn't apply here -- this
    # pillar has no schedule to compare against at all, by design.
    basis = row.get("availability_basis")
    basis_note = _COMMODITY_BASIS_NOTE if basis == AVAILABILITY_BASIS_UNKNOWN else None
    common = {
        "obs_date": row["report_date"],
        "published_at": row["release_date"],
        "available_at": row["available_at"],
        "revision": row["generation_id"],
        "source_catalog": "raw_series:lme",
        "series_id": row["commodity"],
    }
    specs = {
        "total_inventory": "metric_tonnes",
        "on_warrant": "metric_tonnes",
        "canceled_warrants": "metric_tonnes",
        "canceled_ratio": "ratio_0_1",
    }
    out: dict[str, dict[str, Any]] = {}
    for name, unit in specs.items():
        record = measured_field(row[name], unit=unit, **common).to_dict()
        record["availability_basis"] = basis
        record["availability_basis_note"] = basis_note
        out[name] = record

    tightness_record = derived_field(
        row["physical_tightness_flag"], unit=None, calculation_version="commodity_warehouse_pillar_v1",
        coverage_fraction=row["coverage_fraction"], **common,
    ).to_dict()
    tightness_record["availability_basis"] = basis
    tightness_record["availability_basis_note"] = basis_note
    out["physical_tightness_flag"] = tightness_record

    net_change = row["net_change_daily"]
    if net_change is None:
        change_record = unavailable_field(
            STALE_PARTIAL_HISTORY, unit="metric_tonnes", calculation_version="commodity_warehouse_pillar_v1", **common,
        ).to_dict()
    else:
        change_record = derived_field(
            net_change, unit="metric_tonnes", calculation_version="commodity_warehouse_pillar_v1", **common,
        ).to_dict()
    change_record["availability_basis"] = basis
    change_record["availability_basis_note"] = basis_note
    out["net_change_daily"] = change_record
    return out


@router.get("/pillars/commodity_warehouses")
def get_commodity_warehouse_pillar(
    as_of: date | None = Query(default=None),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """LME cancelled-warrant ratio per metal as of ``as_of``. Cushing is permanently
    unavailable(never_configured) -- see godview/commodity_warehouse_pillar.py."""
    as_of = as_of or date.today()
    engine = get_db_engine()

    try:
        with engine.connect() as conn:
            if not _table_exists(conn, "commodity_warehouse_inventories") or not _table_exists(conn, "godview_generations"):
                return unavailable(
                    "commodity_warehouse_inventories or godview_generations does not exist yet",
                    source=COMMODITY_PILLAR_NAME, pillar=COMMODITY_PILLAR_NAME, coverage=None,
                )
            result = read_commodity_warehouse_pillar(conn, as_of, metals=LME_METALS)
    except Exception as exc:  # noqa: BLE001
        return unavailable(f"godview read failed: {exc}", source=COMMODITY_PILLAR_NAME, pillar=COMMODITY_PILLAR_NAME, coverage=None)

    if result.state == "never_configured":
        lme_payload = unavailable(STALE_NEVER_CONFIGURED, source=COMMODITY_PILLAR_NAME, pillar=COMMODITY_PILLAR_NAME, coverage=None)
    elif result.state == "materializer_failed":
        lme_payload = unavailable(STALE_MATERIALIZER_FAILED, source=COMMODITY_PILLAR_NAME, pillar=COMMODITY_PILLAR_NAME, coverage=None)
    else:
        coverage = result.metals_with_data / result.metals_expected if result.metals_expected else None
        fields_by_metal = {row["commodity"]: _commodity_field_records(row) for row in result.rows}
        lme_payload = {
            "available": True,
            "status": "ok" if coverage == 1.0 else "partial",
            "pillar": COMMODITY_PILLAR_NAME,
            "as_of": as_of.isoformat(),
            "coverage": coverage,
            "metals_with_data": result.metals_with_data,
            "metals_expected": result.metals_expected,
            "metals": list(LME_METALS),
            "generation_id": result.generation_id,
            "generation_published_at": result.generation_published_at.isoformat() if result.generation_published_at else None,
            "fields": fields_by_metal,
        }

    return {
        "lme": lme_payload,
        "cushing_crude_stocks": unavailable(
            CUSHING_UNAVAILABLE_REASON, source="eia", pillar=COMMODITY_PILLAR_NAME, coverage=None
        ),
    }


@router.get("/pillars/{pillar_name}")
def get_pillar_not_built(
    pillar_name: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Every other pillar: honest 'not built yet' rather than 404 or a fabricated value,
    with the SPECIFIC reason it is blocked (operator direction, 2026-09-18).

    Registered after the concrete pillar routes so it only catches
    everything else -- FastAPI matches routes in registration order.
    """
    reason = _KNOWN_UNBUILT_PILLARS.get(pillar_name)
    if reason is None:
        return unavailable(
            f"unknown godview pillar: {pillar_name}",
            source=pillar_name,
            pillar=pillar_name,
            coverage=None,
        )
    return unavailable(
        f"not built yet — {reason}",
        source=pillar_name,
        pillar=pillar_name,
        coverage=None,
    )
