"""security_master — GD1 point-in-time issuer/company crosswalk.

Revision ID: security_master_20260927
Revises: raw_series_quarantined_20260926

GD1 from ``GRID-GRANULAR-DISCOVERY-PLAN-20260927.md`` §4 (schema drafted in
``GRID-GD0-SECURITY-MASTER-AUDIT-20260927.md`` §5, this migration reshapes it
into three normalized, dated tables — see ``intelligence/security_master.py``
for the full design rationale and the owner decisions each column leaves
open rather than resolving).

CREATE TABLE + indexes only. Never touches an existing table, so it carries
no lock risk against the nightly ``pg_dump`` window and needs no ACCESS
EXCLUSIVE anywhere.

Tables:
    security_master              -- one row per entity (cik, name, sic,
                                     is_active/delisted_*, provenance).
    security_identifiers         -- one row per (entity, id_scheme, id_value,
                                     valid_from): ticker, cik, cusip, and the
                                     corp_<TICKER> / corporation_<slug>_cik_
                                     <NNN> actor-id shapes GD0 §1.1 measured,
                                     each independently dated and
                                     conflict-flaggable.
    security_sector_membership   -- one row per (entity, taxonomy, sector,
                                     valid_from): dated sector membership
                                     with a primary flag and tie-break
                                     provenance, so the 170 multi-sector
                                     tickers GD0 found (§3) are representable
                                     without forcing a single silent winner.

Scope: issuers/companies only. Deliberately does NOT touch:
  * the 13F **filer**-CIK space (three mutually contradictory hardcoded maps
    per GD0 §1.3) — a separate identifier space, GD8 follow-up;
  * ICIJ/offshore-leaks ``actors`` rows (GD0 §6 item 5) or the 320
    non-company ``category='corporation'`` junk rows (GD0 §6 item 6) — this
    migration creates new tables and never ALTERs ``actors``;
  * GD2's ``people_events.security_id`` (built in a parallel PR,
    ``feat/people-events-table-20260927``, nullable with no FK today) — an
    FK from ``people_events.security_id`` onto ``security_master.entity_id``
    is a follow-up once both land, not part of this migration.

Downgrade note (grid-svr specific): the real ``griddb`` has the Apache AGE
extension installed (``CREATE EXTENSION age`` -> the ``ag_catalog`` schema).
``age`` sits in ``shared_preload_libraries`` at the *cluster* level, so on
grid-svr's Postgres cluster a plain ``DROP TABLE`` fails with
``schema "ag_catalog" does not exist`` on any database on that same cluster
that has not itself run ``CREATE EXTENSION age`` — confirmed empirically
2026-09-27 against a disposable scratch database
(``gd1_security_master_scratch_20260927080426``, dropped after the test):
the same ``DROP TABLE`` failed before ``CREATE EXTENSION IF NOT EXISTS age;``
and succeeded after. This migration's ``downgrade()`` runs unmodified
against ``griddb`` (which already has the extension). A scratch/test
database created fresh on a different Postgres cluster that never loaded
``age`` in ``shared_preload_libraries`` is unaffected by this at all; the
failure mode only appears on a cluster where ``age`` is preloaded server-wide
but the specific database lacks ``CREATE EXTENSION age``. If a future test DB
on *grid-svr's* cluster hits this, run ``CREATE EXTENSION IF NOT EXISTS age;``
in that database first, or test against a database that already has it.
"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "security_master_20260927"
down_revision: Union[str, Sequence[str], None] = "raw_series_quarantined_20260926"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. security_master — entity spine.
    op.execute("""
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
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_security_master_cik
        ON security_master (cik) WHERE cik IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_security_master_active
        ON security_master (is_active);
    """)

    # 2. security_identifiers — dated ticker/CIK/CUSIP/legacy-actor-id crosswalk.
    op.execute("""
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
    );
    CREATE INDEX IF NOT EXISTS idx_security_identifiers_lookup
        ON security_identifiers (id_scheme, id_value, valid_from DESC);
    CREATE INDEX IF NOT EXISTS idx_security_identifiers_entity
        ON security_identifiers (entity_id, id_scheme);
    CREATE INDEX IF NOT EXISTS idx_security_identifiers_conflict
        ON security_identifiers (id_scheme, id_value) WHERE conflict_flag;
    """)

    # 3. security_sector_membership — dated, multi-taxonomy sector membership.
    op.execute("""
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
    );
    CREATE INDEX IF NOT EXISTS idx_sector_membership_lookup
        ON security_sector_membership (taxonomy, sector, valid_from DESC);
    CREATE INDEX IF NOT EXISTS idx_sector_membership_entity
        ON security_sector_membership (entity_id, taxonomy, valid_from DESC);
    CREATE INDEX IF NOT EXISTS idx_sector_membership_primary
        ON security_sector_membership (entity_id, taxonomy) WHERE is_primary;
    """)

    # ====== GRANT FOOTER (REQUIRED — DO NOT SKIP) ======
    # Migrations run as `postgres`; the API and ingestors connect as `grid`.
    # Without these grants the `grid` user gets `permission denied for table`.
    op.execute("""
    GRANT ALL ON security_master TO grid;
    GRANT ALL ON security_identifiers TO grid;
    GRANT ALL ON security_sector_membership TO grid;
    GRANT USAGE, SELECT ON SEQUENCE security_identifiers_id_seq TO grid;
    GRANT USAGE, SELECT ON SEQUENCE security_sector_membership_id_seq TO grid;
    """)


def downgrade() -> None:
    # Reverse dependency order: children before the entity_id they reference.
    op.execute("DROP TABLE IF EXISTS security_sector_membership;")
    op.execute("DROP TABLE IF EXISTS security_identifiers;")
    op.execute("DROP TABLE IF EXISTS security_master;")
