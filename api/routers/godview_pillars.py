"""God View pillar endpoints (W6, first slice: CFTC positioning only).

``GET /api/v1/godview/pillars/cftc?as_of=YYYY-MM-DD`` returns the strict-PIT
state of the CFTC positioning pillar as of ``as_of`` (default: today). Every
other pillar name renders the honest "not built yet" state rather than a
404 or a fabricated number -- see docs/reference/GODVIEW_PILLAR_CONTRACT.md.

Never raises 500 for a missing table: ``cftc_positioning_daily`` and
``godview_generations`` are probed with ``to_regclass`` first, mirroring the
``_table_exists`` pattern already used in api/routers/flows.py and friends.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine
from godview.cftc_pillar import (
    AVAILABILITY_BASIS_INFERRED,
    AVAILABILITY_BASIS_UNKNOWN,
    CFTC_PILLAR_CONTRACTS,
    INFERRED_BASIS_NOTE,
    PILLAR_NAME,
    STALE_AFTER_DAYS,
    UNKNOWN_BASIS_NOTE,
    read_cftc_pillar,
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

#: Pillars this slice knows the *name* of but has not built. Kept explicit
#: (rather than a catch-all) so a typo in the URL still reads as "not built"
#: and not as a silent 404.
_KNOWN_UNBUILT_PILLARS = (
    "finra_short_volume",
    "sec_regsho_ftd",
    "commodity_warehouses",
    "fed_net_liquidity",
    "buyback_blackouts",
    "dealer_gex",
)

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


@router.get("/pillars/{pillar_name}")
def get_pillar_not_built(
    pillar_name: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Every other pillar: honest 'not built yet' rather than 404 or a fabricated value.

    Registered after the concrete /pillars/cftc route so it only catches
    everything else -- FastAPI matches routes in registration order.
    """
    if pillar_name not in _KNOWN_UNBUILT_PILLARS:
        return unavailable(
            f"unknown godview pillar: {pillar_name}",
            source=pillar_name,
            pillar=pillar_name,
            coverage=None,
        )
    return unavailable(
        "not built yet — no data",
        source=pillar_name,
        pillar=pillar_name,
        coverage=None,
    )
