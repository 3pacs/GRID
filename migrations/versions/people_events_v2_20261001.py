"""people_events v2: TEXT security_id FK, append-only versioning, revision log, run log.

Revision ID: people_events_v2_20261001
Revises: options_append_only_20260930

OWNER APPROVAL REQUIRED BEFORE MERGE/APPLY. Design:
``wha/outputs/GRID-PEOPLE-EVENTS-PIPELINE-DESIGN-20261001.md`` sections 4-5.

What it changes, all on ``people_events`` (GD2) and two new tables
-----------------------------------------------------------------------
1. ``security_id`` BIGINT -> TEXT with a foreign key onto
   ``security_master(entity_id)`` (GD1 ids are TEXT: ``sm_0000320193``).
   The migration refuses to run if any row already has a non-NULL
   ``security_id`` (nothing could have written one: there was no BIGINT key
   to point at), so the type change never discards a value.
2. Append-only versioning: ``superseded_at``/``superseded_by``,
   ``retracted_at``/``retraction_reason``. ``UNIQUE (channel, dedup_key)``
   becomes a partial unique index over *current* rows only, so a corrected
   act can get a new version row while the old one stays. A PIT reader at
   ``as_of`` sees ``known_at <= as_of`` and not yet superseded/retracted
   at ``as_of`` -- exactly one version per act.
3. New columns the pipeline fills: ``loose_key`` (act group), ``confidence``,
   ``content_hash``, ``materializer_version``, ``run_id``, ``n_source_rows``.
4. ``channel`` vocabulary gains ``fara`` (DOJ FARA, a sector-proxy channel).
5. ``people_event_revisions``: every UPDATE of a ``people_events`` row is
   logged by trigger (old/new known_at, refs, supersede/retract), so a
   past research run is reproducible from (rows, revisions) as of its time.
6. ``people_events_runs``: one row per materializer run (mode, watermarks,
   counts, status) -- the honest-success record: a run that wrote nothing
   says so.
7. Guards (triggers): ``people_events`` rows cannot be DELETEd or
   TRUNCATEd; an UPDATE may only move ``known_at`` earlier, grow
   ``source_refs``, set ``superseded_*``/``retracted_*``/``security_id``/
   ``echo_of``/``entity_cik`` once, or enrich a name-keyed actor to a
   stable id; descriptive content is immutable. A version floor trigger
   refuses any row (INSERT or known_at UPDATE) that would be known before an
   earlier version of the same act stopped being visible, so the database
   itself guarantees one visible version per act at every ``as_of``. The
   revision log cannot be updated or deleted at all.
9. Preconditions: the table must be empty (the superseded GD2 materializer
   was never activated; any pre-v2 row would carry an incompatible key and
   known_at convention) and hold no BIGINT security_id.
8. Grants to ``grid`` -- including on ``people_events`` itself, which the
   GD2 migration created without a grant footer.

Locking: ``people_events`` is a new, small (expected empty) table that no
service reads yet; the ALTERs take a brief ACCESS EXCLUSIVE lock on it only,
bounded by ``lock_timeout``. Run outside the 03:30-10:30Z backup window.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "people_events_v2_20261001"
down_revision: Union[str, Sequence[str], None] = "options_append_only_20260930"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

CHANNELS = ("form4", "congress", "thirteen_f", "gov_contract", "gov_contract_qq_aggregate", "lobbying", "news", "fara")

_IMMUTABLE_COLUMNS = (
    "channel", "dedup_key", "event_time", "actor_type", "co_actor_ids",
    "entity_ticker", "direction", "transaction_code", "size_usd", "source",
    "source_record_id", "loose_key", "content_hash", "materializer_version", "run_id",
    "ingested_at", "provenance",
)
# actor_id/actor_id_basis/entity_cik may change only as an identity enrichment
# (a name-keyed actor now known by a stable id; an issuer CIK now known);
# echo_of and security_id may be set once. Everything else is immutable.
_STRONG_ACTOR_BASES = ("owner_cik", "bioguide", "filer_cik", "registrant_id")


def upgrade() -> None:
    bind = op.get_bind()
    bind.execute(sa.text("SET LOCAL lock_timeout = '5s'"))
    n = bind.execute(sa.text("SELECT count(*) FROM people_events WHERE security_id IS NOT NULL")).scalar()
    if n:
        raise RuntimeError(
            f"people_events has {n} non-NULL security_id values; refusing to change the column type "
            "(they cannot be GD1 entity ids). Resolve them first."
        )
    rows = bind.execute(sa.text("SELECT count(*) FROM people_events")).scalar()
    if rows:
        # Rows written by the superseded GD2 materializer use another key format
        # and a created_at-based known_at that is not a valid bound for
        # QuiverQuant rows; they must not sit next to v2 rows unmarked.
        raise RuntimeError(
            f"people_events holds {rows} pre-v2 rows; retract or remove them (owner decision) before upgrading"
        )

    channel_list = ", ".join(f"'{c}'" for c in CHANNELS)
    op.execute(f"""
    ALTER TABLE people_events ALTER COLUMN security_id TYPE TEXT USING NULL;
    ALTER TABLE people_events ADD CONSTRAINT people_events_security_id_fkey
        FOREIGN KEY (security_id) REFERENCES security_master (entity_id) NOT VALID;
    ALTER TABLE people_events VALIDATE CONSTRAINT people_events_security_id_fkey;

    ALTER TABLE people_events DROP CONSTRAINT IF EXISTS people_events_channel_check;
    ALTER TABLE people_events ADD CONSTRAINT people_events_channel_check
        CHECK (channel IN ({channel_list}));

    ALTER TABLE people_events
        ADD COLUMN IF NOT EXISTS loose_key            TEXT,
        ADD COLUMN IF NOT EXISTS confidence           TEXT
            CHECK (confidence IS NULL OR confidence IN ('high', 'medium', 'low')),
        ADD COLUMN IF NOT EXISTS content_hash         TEXT,
        ADD COLUMN IF NOT EXISTS materializer_version TEXT,
        ADD COLUMN IF NOT EXISTS run_id               TEXT,
        ADD COLUMN IF NOT EXISTS n_source_rows        INTEGER NOT NULL DEFAULT 1 CHECK (n_source_rows >= 1),
        ADD COLUMN IF NOT EXISTS superseded_by        BIGINT REFERENCES people_events (id)
            DEFERRABLE INITIALLY DEFERRED,
        ADD COLUMN IF NOT EXISTS superseded_at        TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS retracted_at         TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS retraction_reason    TEXT;

    ALTER TABLE people_events DROP CONSTRAINT IF EXISTS people_events_channel_dedup_key_key;
    CREATE UNIQUE INDEX IF NOT EXISTS people_events_current_key
        ON people_events (channel, dedup_key) WHERE superseded_at IS NULL AND retracted_at IS NULL;
    CREATE INDEX IF NOT EXISTS idx_people_events_key_all_versions
        ON people_events (channel, dedup_key);
    DROP INDEX IF EXISTS idx_people_events_security_id;
    CREATE INDEX IF NOT EXISTS idx_people_events_security_known_at
        ON people_events (security_id, known_at DESC) WHERE security_id IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_people_events_loose_key
        ON people_events (channel, loose_key) WHERE loose_key IS NOT NULL;

    CREATE TABLE IF NOT EXISTS people_event_revisions (
        id                 BIGSERIAL PRIMARY KEY,
        event_id           BIGINT NOT NULL REFERENCES people_events (id),
        op                 TEXT NOT NULL CHECK (op IN (
                               'tighten_known_at', 'add_sources', 'supersede', 'retract',
                               'resolve_security', 'enrich_identity', 'set_echo', 'other')),
        old_known_at       TIMESTAMPTZ,
        new_known_at       TIMESTAMPTZ,
        old_known_at_basis TEXT,
        new_known_at_basis TEXT,
        old_n_sources      INTEGER,
        new_n_sources      INTEGER,
        old_source_refs    JSONB,
        detail             JSONB NOT NULL DEFAULT '{{}}'::jsonb,
        recorded_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
    );
    CREATE INDEX IF NOT EXISTS idx_people_event_revisions_event
        ON people_event_revisions (event_id, recorded_at);

    CREATE TABLE IF NOT EXISTS people_events_runs (
        run_id               TEXT PRIMARY KEY,
        mode                 TEXT NOT NULL CHECK (mode IN ('backfill', 'incremental', 'rebuild')),
        materializer_version TEXT NOT NULL,
        started_at           TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        finished_at          TIMESTAMPTZ,
        status               TEXT NOT NULL DEFAULT 'RUNNING'
            CHECK (status IN ('RUNNING', 'SUCCESS', 'NO_NEW_ROWS', 'FAILED')),
        inputs               JSONB NOT NULL DEFAULT '{{}}'::jsonb,
        watermarks           JSONB NOT NULL DEFAULT '{{}}'::jsonb,
        counts               JSONB NOT NULL DEFAULT '{{}}'::jsonb,
        error                TEXT,
        CHECK (status <> 'SUCCESS' OR (counts ? 'written' AND (counts->>'written')::bigint > 0))
    );
    """)

    immutable_checks = " OR ".join(f"NEW.{c} IS DISTINCT FROM OLD.{c}" for c in _IMMUTABLE_COLUMNS)
    strong = ", ".join(f"'{b}'" for b in _STRONG_ACTOR_BASES)
    op.execute(f"""
    CREATE OR REPLACE FUNCTION people_events_guard() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
        IF TG_OP = 'DELETE' THEN
            RAISE EXCEPTION 'people_events is append-only: DELETE refused (retract instead)';
        END IF;
        IF {immutable_checks} THEN
            RAISE EXCEPTION 'people_events: descriptive columns are immutable (supersede instead)';
        END IF;
        IF NEW.known_at > OLD.known_at THEN
            RAISE EXCEPTION 'people_events: known_at may only move earlier (% -> %)', OLD.known_at, NEW.known_at;
        END IF;
        IF NEW.known_at = OLD.known_at AND NEW.known_at_basis IS DISTINCT FROM OLD.known_at_basis THEN
            RAISE EXCEPTION 'people_events: known_at_basis changes only with an earlier known_at';
        END IF;
        IF OLD.superseded_by IS NOT NULL AND NEW.superseded_by IS DISTINCT FROM OLD.superseded_by THEN
            RAISE EXCEPTION 'people_events: superseded_by is set once';
        END IF;
        IF OLD.superseded_at IS NOT NULL AND NEW.superseded_at IS DISTINCT FROM OLD.superseded_at THEN
            RAISE EXCEPTION 'people_events: superseded_at is set once';
        END IF;
        IF OLD.retracted_at IS NOT NULL AND NEW.retracted_at IS DISTINCT FROM OLD.retracted_at THEN
            RAISE EXCEPTION 'people_events: retracted_at is set once';
        END IF;
        IF OLD.security_id IS NOT NULL AND NEW.security_id IS DISTINCT FROM OLD.security_id THEN
            RAISE EXCEPTION 'people_events: security_id is set once (supersede to change it)';
        END IF;
        IF OLD.echo_of IS NOT NULL AND NEW.echo_of IS DISTINCT FROM OLD.echo_of THEN
            RAISE EXCEPTION 'people_events: echo_of is set once';
        END IF;
        IF (NEW.actor_id IS DISTINCT FROM OLD.actor_id OR NEW.actor_id_basis IS DISTINCT FROM OLD.actor_id_basis)
           AND NOT (OLD.actor_id_basis = 'normalized_name' AND NEW.actor_id_basis IN ({strong})) THEN
            RAISE EXCEPTION 'people_events: actor identity may only be enriched from a name to a stable id';
        END IF;
        IF OLD.entity_cik IS NOT NULL AND NEW.entity_cik IS DISTINCT FROM OLD.entity_cik THEN
            RAISE EXCEPTION 'people_events: entity_cik is set once';
        END IF;
        IF NOT (NEW.source_refs @> OLD.source_refs) THEN
            RAISE EXCEPTION 'people_events: source_refs may only grow';
        END IF;
        RETURN NEW;
    END $$;

    CREATE OR REPLACE FUNCTION people_events_log_revision() RETURNS trigger LANGUAGE plpgsql AS $$
    DECLARE
        v_op TEXT := 'other';
    BEGIN
        IF NEW IS NOT DISTINCT FROM OLD THEN
            RETURN NULL;  -- an idempotent re-upsert changed nothing: nothing to log
        END IF;
        IF NEW.superseded_at IS NOT NULL AND OLD.superseded_at IS NULL THEN
            v_op := 'supersede';
        ELSIF NEW.retracted_at IS NOT NULL AND OLD.retracted_at IS NULL THEN
            v_op := 'retract';
        ELSIF NEW.known_at < OLD.known_at THEN
            v_op := 'tighten_known_at';
        ELSIF NEW.source_refs IS DISTINCT FROM OLD.source_refs THEN
            v_op := 'add_sources';
        ELSIF NEW.actor_id IS DISTINCT FROM OLD.actor_id OR NEW.entity_cik IS DISTINCT FROM OLD.entity_cik THEN
            v_op := 'enrich_identity';
        ELSIF NEW.security_id IS DISTINCT FROM OLD.security_id THEN
            v_op := 'resolve_security';
        ELSIF NEW.echo_of IS DISTINCT FROM OLD.echo_of THEN
            v_op := 'set_echo';
        END IF;
        INSERT INTO people_event_revisions (event_id, op, old_known_at, new_known_at, old_known_at_basis,
            new_known_at_basis, old_n_sources, new_n_sources, old_source_refs, detail)
        VALUES (OLD.id, v_op, OLD.known_at, NEW.known_at, OLD.known_at_basis, NEW.known_at_basis,
            OLD.n_sources, NEW.n_sources, OLD.source_refs,
            jsonb_build_object('superseded_by', NEW.superseded_by, 'retraction_reason', NEW.retraction_reason,
                               'security_id', NEW.security_id, 'old_actor_id', OLD.actor_id,
                               'new_actor_id', NEW.actor_id, 'echo_of', NEW.echo_of));
        RETURN NULL;
    END $$;

    -- One visible version per act, enforced by the database: no row of a key
    -- may become known before an earlier version of that key stopped being
    -- visible (superseded or retracted). Covers INSERT (a re-appearing act,
    -- a store upsert of a retracted key) and an UPDATE that moves known_at.
    CREATE OR REPLACE FUNCTION people_events_version_floor() RETURNS trigger LANGUAGE plpgsql AS $$
    DECLARE
        v_floor TIMESTAMPTZ;
    BEGIN
        SELECT max(GREATEST(superseded_at, retracted_at)) INTO v_floor
        FROM people_events
        WHERE channel = NEW.channel AND dedup_key = NEW.dedup_key AND id <> NEW.id
          AND (superseded_at IS NOT NULL OR retracted_at IS NOT NULL);
        IF v_floor IS NOT NULL AND NEW.known_at < v_floor THEN
            RAISE EXCEPTION 'people_events: known_at % precedes the end (%) of an earlier version of this act',
                NEW.known_at, v_floor;
        END IF;
        RETURN NEW;
    END $$;

    CREATE OR REPLACE FUNCTION people_events_refuse() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
        RAISE EXCEPTION '% is append-only: % refused', TG_TABLE_NAME, TG_OP;
    END $$;

    DROP TRIGGER IF EXISTS people_events_guard_trg ON people_events;
    CREATE TRIGGER people_events_guard_trg BEFORE UPDATE OR DELETE ON people_events
        FOR EACH ROW EXECUTE FUNCTION people_events_guard();
    DROP TRIGGER IF EXISTS people_events_truncate_trg ON people_events;
    CREATE TRIGGER people_events_truncate_trg BEFORE TRUNCATE ON people_events
        FOR EACH STATEMENT EXECUTE FUNCTION people_events_refuse();
    DROP TRIGGER IF EXISTS people_events_version_floor_trg ON people_events;
    CREATE TRIGGER people_events_version_floor_trg BEFORE INSERT OR UPDATE OF known_at ON people_events
        FOR EACH ROW EXECUTE FUNCTION people_events_version_floor();
    DROP TRIGGER IF EXISTS people_events_revision_trg ON people_events;
    CREATE TRIGGER people_events_revision_trg AFTER UPDATE ON people_events
        FOR EACH ROW EXECUTE FUNCTION people_events_log_revision();
    DROP TRIGGER IF EXISTS people_event_revisions_guard_trg ON people_event_revisions;
    CREATE TRIGGER people_event_revisions_guard_trg BEFORE UPDATE OR DELETE ON people_event_revisions
        FOR EACH ROW EXECUTE FUNCTION people_events_refuse();
    DROP TRIGGER IF EXISTS people_event_revisions_truncate_trg ON people_event_revisions;
    CREATE TRIGGER people_event_revisions_truncate_trg BEFORE TRUNCATE ON people_event_revisions
        FOR EACH STATEMENT EXECUTE FUNCTION people_events_refuse();
    """)

    # ====== GRANT FOOTER (REQUIRED) ======
    # Migrations run as `postgres`; the materializer and readers connect as `grid`.
    # grid gets no DELETE/TRUNCATE: append-only is also a privilege, not only a trigger.
    op.execute("""
    DO $$
    BEGIN
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grid') THEN
            GRANT SELECT, INSERT, UPDATE ON people_events TO grid;
            GRANT USAGE, SELECT ON SEQUENCE people_events_id_seq TO grid;
            GRANT SELECT, INSERT ON people_event_revisions TO grid;
            GRANT USAGE, SELECT ON SEQUENCE people_event_revisions_id_seq TO grid;
            GRANT SELECT, INSERT, UPDATE ON people_events_runs TO grid;
        END IF;
    END $$;
    """)


def downgrade() -> None:
    bind = op.get_bind()
    versions = bind.execute(sa.text(
        "SELECT count(*) FROM people_events WHERE superseded_at IS NOT NULL OR retracted_at IS NOT NULL"
    )).scalar()
    if versions:
        raise RuntimeError(
            f"people_events holds {versions} superseded/retracted rows; downgrading would collapse "
            "versions under a plain UNIQUE (channel, dedup_key). Refusing."
        )
    keyed = bind.execute(sa.text("SELECT count(*) FROM people_events WHERE security_id IS NOT NULL")).scalar()
    if keyed:
        raise RuntimeError(f"people_events has {keyed} security_id values; refusing to drop them")
    revisions = bind.execute(sa.text("SELECT count(*) FROM people_event_revisions")).scalar()
    if revisions:
        raise RuntimeError(f"people_event_revisions holds {revisions} rows of history; refusing to drop it")
    fara = bind.execute(sa.text("SELECT count(*) FROM people_events WHERE channel = 'fara'")).scalar()
    if fara:
        raise RuntimeError(f"people_events has {fara} fara rows; refusing to narrow the channel CHECK")
    op.execute("""
    DROP TRIGGER IF EXISTS people_event_revisions_truncate_trg ON people_event_revisions;
    DROP TRIGGER IF EXISTS people_event_revisions_guard_trg ON people_event_revisions;
    DROP TRIGGER IF EXISTS people_events_revision_trg ON people_events;
    DROP TRIGGER IF EXISTS people_events_version_floor_trg ON people_events;
    DROP TRIGGER IF EXISTS people_events_truncate_trg ON people_events;
    DROP TRIGGER IF EXISTS people_events_guard_trg ON people_events;
    DROP FUNCTION IF EXISTS people_events_refuse();
    DROP FUNCTION IF EXISTS people_events_log_revision();
    DROP FUNCTION IF EXISTS people_events_version_floor();
    DROP FUNCTION IF EXISTS people_events_guard();
    DROP TABLE IF EXISTS people_events_runs;
    DROP TABLE IF EXISTS people_event_revisions;
    DROP INDEX IF EXISTS idx_people_events_loose_key;
    DROP INDEX IF EXISTS idx_people_events_security_known_at;
    DROP INDEX IF EXISTS idx_people_events_key_all_versions;
    DROP INDEX IF EXISTS people_events_current_key;
    ALTER TABLE people_events ADD CONSTRAINT people_events_channel_dedup_key_key UNIQUE (channel, dedup_key);
    ALTER TABLE people_events
        DROP COLUMN IF EXISTS retraction_reason, DROP COLUMN IF EXISTS retracted_at,
        DROP COLUMN IF EXISTS superseded_at, DROP COLUMN IF EXISTS superseded_by,
        DROP COLUMN IF EXISTS n_source_rows, DROP COLUMN IF EXISTS run_id,
        DROP COLUMN IF EXISTS materializer_version, DROP COLUMN IF EXISTS content_hash,
        DROP COLUMN IF EXISTS confidence, DROP COLUMN IF EXISTS loose_key;
    ALTER TABLE people_events DROP CONSTRAINT IF EXISTS people_events_channel_check;
    ALTER TABLE people_events ADD CONSTRAINT people_events_channel_check CHECK (channel IN (
        'form4', 'congress', 'thirteen_f', 'gov_contract', 'gov_contract_qq_aggregate', 'lobbying', 'news'));
    ALTER TABLE people_events DROP CONSTRAINT IF EXISTS people_events_security_id_fkey;
    ALTER TABLE people_events ALTER COLUMN security_id TYPE BIGINT USING NULL;
    CREATE INDEX IF NOT EXISTS idx_people_events_security_id
        ON people_events (security_id) WHERE security_id IS NOT NULL;
    """)
