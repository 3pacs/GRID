"""Create people_events — canonical, point-in-time, de-duplicated people-linked acts.

Revision ID: people_events_20260927
Revises: causal_links_provenance_20260927

GD2 of the 2026-09-27 granular-discovery plan
(wha/outputs/GRID-GRANULAR-DISCOVERY-PLAN-20260927.md, section 2.1 and gap G3).

Why this table exists
----------------------
Every people-linked event GRID has today (Form 4, congressional trades, 13F,
gov contracts, lobbying, news) is stored either as a raw `signal_sources` row
or as a materialized copy of one (`insider_trades`, `congressional_trades`,
`signal_data`, `actor_connections` edges, ...). None of those copies carries a
`known_at` -- the earliest time the public could see the act -- and the same
real-world act is frequently double-counted across QuiverQuant and EDGAR
native feeds, or across a filing and its news echo. A density or flywheel
construct built directly on any of those tables would be counting ingestion
artifacts, not activity.

`people_events` is the target of a de-duplicating materializer (not part of
this migration -- see intelligence/people_events_materializer.py) that reads
those existing tables read-only and writes one row per real-world act, merging
duplicate sources into `source_refs`/`n_sources` per the dedup keys in plan
section 2.1.

This migration only creates the table, its indexes and its CHECK constraints.
It performs no writes and touches no existing table.

known_at honesty (the whole point of this table)
-------------------------------------------------
`known_at` is NOT NULL: a row with no defensible public-known timestamp must
not exist here at all (the materializer skips it and logs why, rather than
guessing). `known_at_basis` is a separate, closed-vocabulary NOT NULL column
naming *how* known_at was derived (`filing`, `qq_last_modified`,
`statutory_bound`, `first_seen`, `publish`) -- per the plan's per-channel
known_at rule table -- so a downstream reader can immediately tell a filing
timestamp from a "first time our own ingestion saw it" fallback, and exclude
the weaker bases when a scan requires real disclosure timing. Neither column
can be faked into looking like the other: there is no default and no CHECK
that would let a NULL or fabricated value pass as `filing`.

Dedup and echo links
---------------------
`dedup_key` is the per-channel deterministic string described in plan section
2.1 (e.g. for Form 4: issuer id + reporting-owner id + transaction_date +
transaction_code + rounded shares). `UNIQUE (channel, dedup_key)` is the
enforcement: two sources describing the same act collide on insert and the
materializer merges them (source_refs, n_sources) instead of creating a
second row. `echo_of` self-references a row that is the *same information
event* seen again through a different channel (plan section 2.1's
cross-channel sameness rule, e.g. a news story about an already-known Form 4)
-- an echo never counts toward density on its own.

security_master (GD1) is not created here
-------------------------------------------
GD1 (the security_master table + resolver) is a separate slice, done after the
GD0 audit lands. `security_id` is added here as a plain nullable BIGINT with
no foreign key, so GD1 can backfill it and add the FK later without touching
this migration. Until then, `entity_ticker`/`entity_cik` carry the issuer
identity exactly as filed by the source (ungoverned, per gap G1).

Safety on production
---------------------
CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS only. No ALTER, no
lock, on any existing table. This is a brand-new, empty table, so there is no
production data to migrate and no long-running statement.
"""

from alembic import op

revision = "people_events_20260927"
down_revision = "causal_links_provenance_20260927"
branch_labels = None
depends_on = None

# NOTE (coordinator): re-parented 2026-09-27 onto causal_links_provenance_20260927,
# the single head on origin/main at merge time (train order: ... #690 causal-links,
# #693 people_events). The coordinator handles this re-parenting; this PR does not
# merge itself.


def upgrade() -> None:
    op.execute("""
    CREATE TABLE IF NOT EXISTS people_events (
        id                  BIGSERIAL PRIMARY KEY,

        -- Canonical event identity (plan section 2.1).
        channel             TEXT NOT NULL
            CHECK (channel IN (
                'form4', 'congress', 'thirteen_f', 'gov_contract',
                'gov_contract_qq_aggregate', 'lobbying', 'news'
            )),
        dedup_key           TEXT NOT NULL,

        -- The economic act itself, and when the public could first see it.
        event_time          TIMESTAMPTZ NOT NULL,
        known_at            TIMESTAMPTZ NOT NULL,
        known_at_basis      TEXT NOT NULL
            CHECK (known_at_basis IN (
                'filing', 'qq_last_modified', 'statutory_bound',
                'first_seen', 'publish'
            )),

        -- Actor(s). No canonical actor-key registry exists yet (gap G2) --
        -- actor_id carries whichever key the channel's known_at rule implies
        -- (owner_cik / bioguide / filer_cik / agency_code / registrant_id),
        -- or a normalized name when no stable id is available, and
        -- actor_id_basis records which.
        actor_id            TEXT NOT NULL,
        actor_id_basis       TEXT NOT NULL
            CHECK (actor_id_basis IN (
                'owner_cik', 'bioguide', 'filer_cik', 'agency_code',
                'registrant_id', 'normalized_name'
            )),
        actor_type          TEXT NOT NULL,
        co_actor_ids        TEXT[] NOT NULL DEFAULT '{}',

        -- Entity/issuer, as filed by the source (ungoverned; see G1/GD1).
        entity_ticker       TEXT,
        entity_cik          TEXT,
        -- Future FK to GD1's security_master(id). Nullable, no FK yet: GD1
        -- has not been built. See the migration docstring.
        security_id         BIGINT,

        direction           TEXT
            CHECK (direction IS NULL OR direction IN (
                'buy', 'sell', 'award', 'positive', 'negative', 'neutral'
            )),
        -- Raw source transaction code, stored verbatim (e.g. SEC Form 4
        -- TransactionCode: P/S/A/M/X/C/F/G/...) so a future feature can read
        -- the exact code a channel supplied without having to reconstruct it
        -- from the closed-vocabulary, lossy `direction` column above (which
        -- deliberately maps several distinct codes, e.g. M/X/C/F/G, to the
        -- same NULL). Nullable and unconstrained: not every channel has an
        -- equivalent code, and this table does not hard-code the SEC code
        -- vocabulary into a CHECK constraint the way it does for `direction`.
        transaction_code    TEXT,
        size_usd            DOUBLE PRECISION
            CHECK (size_usd IS NULL OR size_usd >= 0),

        -- Provenance: which source(s) produced this row, and with what
        -- record id. A merged row (see dedup above) keeps every contributing
        -- source in source_refs; `source`/`source_record_id` name the first
        -- (primary) one.
        source              TEXT NOT NULL,
        source_record_id    TEXT,
        source_refs         JSONB NOT NULL DEFAULT '[]'::jsonb,
        n_sources           INTEGER NOT NULL DEFAULT 1 CHECK (n_sources >= 1),

        -- Cross-channel sameness (plan section 2.1): this row is the same
        -- information event as an earlier one, seen through another channel.
        -- An echo never counts toward density on its own.
        echo_of             BIGINT REFERENCES people_events (id),

        -- Free-form materializer provenance (module/version, run id, the
        -- raw field names it read) -- deliberately not a foreign key to any
        -- job-run table, since none exists yet.
        provenance          JSONB NOT NULL DEFAULT '{}'::jsonb,
        ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),

        UNIQUE (channel, dedup_key)
    );

    CREATE INDEX IF NOT EXISTS idx_people_events_entity_known_at
        ON people_events (entity_ticker, known_at DESC);
    CREATE INDEX IF NOT EXISTS idx_people_events_actor_known_at
        ON people_events (actor_id, known_at DESC);
    CREATE INDEX IF NOT EXISTS idx_people_events_channel_known_at
        ON people_events (channel, known_at DESC);
    CREATE INDEX IF NOT EXISTS idx_people_events_echo_of
        ON people_events (echo_of) WHERE echo_of IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_people_events_security_id
        ON people_events (security_id) WHERE security_id IS NOT NULL;
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS people_events;")
