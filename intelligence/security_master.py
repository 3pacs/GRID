"""GD1 — point-in-time security master (issuers/companies only).

Slice GD1 of ``GRID-GRANULAR-DISCOVERY-PLAN-20260927.md`` §4, built from the
schema proposed in ``GRID-GD0-SECURITY-MASTER-AUDIT-20260927.md`` §5. GD0
found at least six independent identifier spaces for "the same company"
(bare ticker, issuer CIK, a *separate* 13F-filer-CIK space with three
mutually contradictory hardcoded maps, CUSIP, five distinct ``actors.id``
shapes, and ICIJ's own numbering) and confirmed the sector map is an
undated snapshot: 170 of 1,268 tickers sit in more than one top-level
sector with no primary-sector rule anywhere in the code, and 4 of the 102
Technology tickers (``CFLT``, ``CYBR``, ``JNPR``, ``PSTG``) have no live SEC
CIK at all.

Scope, deliberately narrow (GD0 §6 owner decisions):
    * This table crosswalks **issuers/companies** (ticker, CIK, CUSIP,
      sector) only. It does NOT touch:
        - the 13F **filer**-CIK space (``ingestion/edgar.py``,
          ``institutional_flows.py``, ``sec_13f_live.py`` disagree with each
          other on the same filer CIK — GD0 §1.3, §6 item 4). That is a
          separate identifier space (fund, not issuer) and stays a GD8
          follow-up.
        - ICIJ/offshore-leaks entities (95.6% of ``actors`` rows, no
          ticker/CIK) — GD0 §6 item 5, explicitly deferred.
        - the 320 non-company ``category='corporation'`` junk rows in
          ``actors`` (news-headline fragments, foreign ad-hoc ids,
          miscategorized people) — GD0 §6 item 6. This module creates new
          tables and never touches ``actors``.
    * Three schema decisions GD0 flagged as owner-pending are made
      *representable, not decided* (nothing here is irreversible):
        1. Primary-sector tie-break (170 multi-sector tickers): the seed
           script *proposes* a primary sector from ``sector_map``'s
           subsector x actor weight product (GD0 §3's cheapest defensible
           default) and always sets ``conflict_flag=true`` on every
           multi-sector membership row, so the proposal is visibly a
           proposal — an owner can flip ``is_primary`` per row without a
           migration. ``tie_break_method`` records which rule produced it.
        2. Delisting criteria: ``is_active`` defaults ``true`` and this
           slice's seed script never flips it — SEC absence alone (the only
           signal available for CFLT/CYBR/JNPR/PSTG) is a *candidate*
           signal per GD0 §6 item 2, not proof. ``delisted_basis`` records
           which kind of evidence would justify the flip when an owner
           decides the corroboration bar.
        3. Canonical taxonomy: ``security_sector_membership.taxonomy`` lets
           ``sector_map_v1``, ``fundamental_divergence_v1`` and
           ``company_profiles_yahoo`` sector labels for the same entity
           coexist as separate rows instead of forcing a name-string merge
           (GD0 §6 item 3 warns ``Consumer Discretionary``/``Consumer
           Cyclical`` already collide by string equality alone).

Tables (migration ``migrations/versions/security_master_20260927.py`` is the
contract; ``ensure_tables`` below mirrors the same DDL idempotently, the
convention documented in ``migrations/0060_sponsor_ticker_map.sql`` and used
by ``intelligence/actor_identity.py::ensure_merged_into_column``):

    security_master              -- one row per entity: cik, name, sic,
                                     is_active/delisted_*, provenance.
    security_identifiers         -- one row per (entity, id_scheme, id_value,
                                     valid_from): ticker, cik, cusip, and the
                                     ``corp_<TICKER>``/legacy ``actors.id``
                                     shapes from GD0 §1.1-1.2, each dated and
                                     conflict-flaggable independently.
    security_sector_membership   -- one row per (entity, taxonomy, sector,
                                     valid_from): dated sector membership,
                                     primary flag, tie-break provenance.

Public API:
    ``entity_id_for_cik(cik) -> str``
    ``entity_id_for_ticker(ticker) -> str``
    ``ensure_tables(engine)``
    ``resolve_entity(engine, id_scheme, id_value, as_of=None) -> str | None``
    ``resolve_primary_sector(engine, entity_id, as_of=None, taxonomy=...) -> str | None``
    ``compute_sector_weights(sector_map, ticker) -> dict[str, float]``
    ``propose_primary_sector(weights) -> tuple[str | None, bool]``
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Optional

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# ── Identifier schemes this table is authoritative for ────────────────────
#
# 'actor_corp' / 'actor_corporation_cik' mirror the two dominant shapes GD0
# §1.1 measured in actors.id (corp_<TICKER>: 6,109 rows; corporation_<slug>
# _cik_<NNN>: 1,119 rows) so a future fold of actor_connections.actor_b onto
# this spine (GD0 §1.2: 38% of insider_trade edges use the bare-ticker shape,
# the rest corp_<TICKER>) can join through security_identifiers directly
# instead of re-deriving the prefix logic.
ID_SCHEME_TICKER = "ticker"
ID_SCHEME_CIK = "cik"
ID_SCHEME_CUSIP = "cusip"
ID_SCHEME_ACTOR_CORP = "actor_corp"
ID_SCHEME_ACTOR_CORPORATION_CIK = "actor_corporation_cik"

KNOWN_ID_SCHEMES = frozenset({
    ID_SCHEME_TICKER, ID_SCHEME_CIK, ID_SCHEME_CUSIP,
    ID_SCHEME_ACTOR_CORP, ID_SCHEME_ACTOR_CORPORATION_CIK,
})

DEFAULT_TAXONOMY = "sector_map_v1"

# ── DDL — mirrors migrations/versions/security_master_20260927.py ─────────

_CREATE_SECURITY_MASTER_SQL = """
CREATE TABLE IF NOT EXISTS security_master (
    entity_id        TEXT PRIMARY KEY,
    cik              INTEGER,
    name             TEXT NOT NULL,
    security_type    TEXT NOT NULL DEFAULT 'equity',
    is_active        BOOLEAN NOT NULL DEFAULT TRUE,
    delisted_at      DATE,
    delisted_reason  TEXT,
    delisted_basis   TEXT,
    sic              INTEGER,
    source           TEXT NOT NULL,
    provenance       JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

_CREATE_SECURITY_MASTER_INDEXES_SQL = (
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_security_master_cik "
    "ON security_master (cik) WHERE cik IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_security_master_active "
    "ON security_master (is_active)",
)

_CREATE_SECURITY_IDENTIFIERS_SQL = """
CREATE TABLE IF NOT EXISTS security_identifiers (
    id               BIGSERIAL PRIMARY KEY,
    entity_id        TEXT NOT NULL REFERENCES security_master(entity_id) ON DELETE CASCADE,
    id_scheme        TEXT NOT NULL,
    id_value         TEXT NOT NULL,
    valid_from       DATE NOT NULL,
    valid_to         DATE,
    is_primary       BOOLEAN NOT NULL DEFAULT TRUE,
    source           TEXT NOT NULL,
    conflict_flag    BOOLEAN NOT NULL DEFAULT FALSE,
    conflict_detail  JSONB,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (entity_id, id_scheme, id_value, valid_from)
)
"""

_CREATE_SECURITY_IDENTIFIERS_INDEXES_SQL = (
    "CREATE INDEX IF NOT EXISTS idx_security_identifiers_lookup "
    "ON security_identifiers (id_scheme, id_value, valid_from DESC)",
    "CREATE INDEX IF NOT EXISTS idx_security_identifiers_entity "
    "ON security_identifiers (entity_id, id_scheme)",
    "CREATE INDEX IF NOT EXISTS idx_security_identifiers_conflict "
    "ON security_identifiers (id_scheme, id_value) WHERE conflict_flag",
)

_CREATE_SECURITY_SECTOR_MEMBERSHIP_SQL = """
CREATE TABLE IF NOT EXISTS security_sector_membership (
    id                BIGSERIAL PRIMARY KEY,
    entity_id         TEXT NOT NULL REFERENCES security_master(entity_id) ON DELETE CASCADE,
    taxonomy          TEXT NOT NULL DEFAULT 'sector_map_v1',
    sector            TEXT NOT NULL,
    subsector         TEXT,
    is_primary        BOOLEAN NOT NULL DEFAULT TRUE,
    tie_break_method  TEXT,
    weight            NUMERIC,
    source            TEXT NOT NULL,
    conflict_flag     BOOLEAN NOT NULL DEFAULT FALSE,
    conflict_detail   JSONB,
    valid_from        DATE NOT NULL,
    valid_to          DATE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (entity_id, taxonomy, sector, valid_from)
)
"""

_CREATE_SECURITY_SECTOR_MEMBERSHIP_INDEXES_SQL = (
    "CREATE INDEX IF NOT EXISTS idx_sector_membership_lookup "
    "ON security_sector_membership (taxonomy, sector, valid_from DESC)",
    "CREATE INDEX IF NOT EXISTS idx_sector_membership_entity "
    "ON security_sector_membership (entity_id, taxonomy, valid_from DESC)",
    "CREATE INDEX IF NOT EXISTS idx_sector_membership_primary "
    "ON security_sector_membership (entity_id, taxonomy) WHERE is_primary",
)


def ensure_tables(engine: Engine) -> None:
    """Create the GD1 security-master tables and indexes if they don't exist.

    Convenience mirror of the migration (the migration is the contract — see
    module docstring). Never drops or alters an existing column; safe to call
    from any read path.
    """
    with engine.begin() as conn:
        conn.execute(text(_CREATE_SECURITY_MASTER_SQL))
        for idx_sql in _CREATE_SECURITY_MASTER_INDEXES_SQL:
            conn.execute(text(idx_sql))
        conn.execute(text(_CREATE_SECURITY_IDENTIFIERS_SQL))
        for idx_sql in _CREATE_SECURITY_IDENTIFIERS_INDEXES_SQL:
            conn.execute(text(idx_sql))
        conn.execute(text(_CREATE_SECURITY_SECTOR_MEMBERSHIP_SQL))
        for idx_sql in _CREATE_SECURITY_SECTOR_MEMBERSHIP_INDEXES_SQL:
            conn.execute(text(idx_sql))
    log.info("security_master tables ensured")


# ── Entity id construction ─────────────────────────────────────────────────


def entity_id_for_cik(cik: int | str) -> str:
    """``1364742`` -> ``"sm_0001364742"``. Raises ValueError on non-numeric input."""
    digits = str(cik).strip()
    if not digits.isdigit():
        raise ValueError(f"entity_id_for_cik: not a CIK: {cik!r}")
    return f"sm_{int(digits):010d}"


def entity_id_for_ticker(ticker: str) -> str:
    """``"aapl"`` -> ``"sm_tkr_AAPL"``. Fallback key when no CIK is known yet."""
    return f"sm_tkr_{ticker.strip().upper()}"


# ── Resolver ────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ResolvedEntity:
    entity_id: str
    is_primary: bool
    conflict_flag: bool


def resolve_entity(
    engine: Engine,
    id_scheme: str,
    id_value: str,
    as_of: Optional[date] = None,
) -> Optional[str]:
    """Return the ``entity_id`` a channel materializer should use for
    ``(id_scheme, id_value)`` as of ``as_of`` (default: today).

    This is the PIT-correct join point every channel materializer
    (``insider_trades``, ``congressional_trades``, ``institutional_holdings``,
    ``actor_connections`` writers — GD1 gap G1) should use instead of writing
    its own ticker/CIK string directly. When more than one entity claims the
    same ``(id_scheme, id_value)`` as of ``as_of`` — a genuine identifier
    conflict, not a bug in this function — the primary row wins, and ties
    break on the most recently opened ``valid_from``. Returns ``None`` when
    nothing matches; never guesses.
    """
    as_of = as_of or date.today()
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT entity_id FROM security_identifiers "
                "WHERE id_scheme = :scheme AND id_value = :value "
                "AND valid_from <= :as_of "
                "AND (valid_to IS NULL OR valid_to >= :as_of) "
                "ORDER BY is_primary DESC, valid_from DESC "
                "LIMIT 1"
            ),
            {"scheme": id_scheme, "value": id_value, "as_of": as_of},
        )
        found = result.fetchone()
        return found[0] if found else None


def resolve_primary_sector(
    engine: Engine,
    entity_id: str,
    as_of: Optional[date] = None,
    taxonomy: str = DEFAULT_TAXONOMY,
) -> Optional[str]:
    """Primary sector for ``entity_id`` under ``taxonomy`` as of ``as_of``."""
    as_of = as_of or date.today()
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT sector FROM security_sector_membership "
                "WHERE entity_id = :entity_id AND taxonomy = :taxonomy "
                "AND is_primary AND valid_from <= :as_of "
                "AND (valid_to IS NULL OR valid_to >= :as_of) "
                "ORDER BY valid_from DESC LIMIT 1"
            ),
            {"entity_id": entity_id, "taxonomy": taxonomy, "as_of": as_of},
        )
        found = result.fetchone()
        return found[0] if found else None


# ── Primary-sector tie-break (owner decision #1 — proposal only) ──────────


def compute_sector_weights(sector_map: dict[str, Any], ticker: str) -> dict[str, float]:
    """``sector -> sum(subsector_weight * actor_weight)`` for ``ticker``.

    Pure function over the already-loaded ``analysis.sector_map.SECTOR_MAP``
    document, so it is unit-testable without touching the YAML file or a DB.
    Only ``type: company`` actor entries count — the sector map also carries
    people, funds and sovereigns under the same ticker-ish key space (e.g.
    ETF proxies like ``BITO``, ``TLT``), which are not securities this table
    represents.
    """
    ticker_u = ticker.strip().upper()
    weights: dict[str, float] = {}
    for sector, sector_data in (sector_map or {}).items():
        if not isinstance(sector_data, dict):
            continue
        for subsector_data in (sector_data.get("subsectors") or {}).values():
            sub_weight = float(subsector_data.get("weight") or 0.0)
            for actor in subsector_data.get("actors") or []:
                if actor.get("type") != "company":
                    continue
                if (actor.get("ticker") or "").strip().upper() != ticker_u:
                    continue
                actor_weight = float(actor.get("weight") or 0.0)
                weights[sector] = weights.get(sector, 0.0) + sub_weight * actor_weight
    return weights


def propose_primary_sector(weights: dict[str, float]) -> tuple[Optional[str], bool]:
    """``(proposed_primary_sector, is_multi_sector)`` from :func:`compute_sector_weights`.

    ``is_multi_sector`` is true whenever the ticker appears under more than
    one top-level sector at all — regardless of whether the weights broke the
    tie cleanly — because GD0 found no existing primary-sector rule anywhere
    in the codebase (§3): every multi-sector case is a proposal pending owner
    sign-off, not a settled fact, even when this function picks a clear
    winner. A true tie (equal max weight) breaks alphabetically so the
    function is deterministic; the tie itself is visible in the returned
    weights dict, which the caller stores in ``conflict_detail``.
    """
    if not weights:
        return None, False
    if len(weights) == 1:
        return next(iter(weights)), False
    max_weight = max(weights.values())
    tied = sorted(s for s, w in weights.items() if w == max_weight)
    return tied[0], True
