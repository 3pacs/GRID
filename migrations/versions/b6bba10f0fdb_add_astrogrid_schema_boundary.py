"""add astrogrid schema boundary

Revision ID: b6bba10f0fdb
Revises: 7e4dfecce247
Create Date: 2026-03-28 16:35:00.000000
"""

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "b6bba10f0fdb"
down_revision: Union[str, Sequence[str], None] = "7e4dfecce247"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


DDL = """
CREATE SCHEMA IF NOT EXISTS astrogrid;

CREATE TABLE IF NOT EXISTS astrogrid.grid_input_allowlist (
    id          BIGSERIAL PRIMARY KEY,
    input_kind  TEXT NOT NULL CHECK (input_kind IN ('feature', 'table', 'view', 'briefing', 'series')),
    object_name TEXT NOT NULL,
    purpose     TEXT NOT NULL,
    notes       TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deprecated_at TIMESTAMPTZ,
    UNIQUE (input_kind, object_name)
);

CREATE INDEX IF NOT EXISTS idx_astrogrid_allowlist_kind
    ON astrogrid.grid_input_allowlist (input_kind);
CREATE INDEX IF NOT EXISTS idx_astrogrid_allowlist_active
    ON astrogrid.grid_input_allowlist (input_kind, object_name) WHERE deprecated_at IS NULL;

CREATE TABLE IF NOT EXISTS astrogrid.celestial_object_registry (
    id              BIGSERIAL PRIMARY KEY,
    object_key      TEXT NOT NULL UNIQUE,
    display_name    TEXT NOT NULL,
    object_class    TEXT NOT NULL CHECK (object_class IN (
                        'luminary', 'planet', 'node', 'asteroid',
                        'fixed_star', 'satellite', 'surface', 'region',
                        'mission', 'field')),
    source          TEXT NOT NULL,
    precision_label TEXT NOT NULL CHECK (precision_label IN (
                        'authoritative', 'derived', 'approximate', 'mixed')),
    visual_priority INTEGER NOT NULL DEFAULT 0,
    track_mode      TEXT NOT NULL CHECK (track_mode IN ('reliable', 'experimental', 'derived')),
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deprecated_at   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_astrogrid_object_registry_class
    ON astrogrid.celestial_object_registry (object_class);
CREATE INDEX IF NOT EXISTS idx_astrogrid_object_registry_enabled
    ON astrogrid.celestial_object_registry (enabled);

CREATE TABLE IF NOT EXISTS astrogrid.lens_set (
    id               BIGSERIAL PRIMARY KEY,
    lens_set_key     TEXT NOT NULL,
    version          INTEGER NOT NULL CHECK (version > 0),
    name             TEXT NOT NULL,
    mode             TEXT NOT NULL CHECK (mode IN ('solo', 'chorus', 'intersection', 'shadow')),
    allowed_lenses   TEXT[] NOT NULL DEFAULT '{}'::text[],
    forbidden_lenses TEXT[] NOT NULL DEFAULT '{}'::text[],
    weighting        JSONB NOT NULL DEFAULT '{}'::jsonb,
    doctrine         TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (lens_set_key, version)
);

CREATE INDEX IF NOT EXISTS idx_astrogrid_lens_set_key
    ON astrogrid.lens_set (lens_set_key, version DESC);

CREATE TABLE IF NOT EXISTS astrogrid.sky_snapshot (
    id                   BIGSERIAL PRIMARY KEY,
    snapshot_date        DATE NOT NULL,
    snapshot_ts          TIMESTAMPTZ NOT NULL,
    location_key         TEXT NOT NULL DEFAULT 'geocentric',
    source_mode          TEXT NOT NULL CHECK (source_mode IN ('grid', 'local', 'archive', 'hybrid')),
    precision_label      TEXT NOT NULL CHECK (precision_label IN (
                             'authoritative', 'derived', 'approximate', 'mixed')),
    source_trace         JSONB NOT NULL DEFAULT '{}'::jsonb,
    bodies_payload       JSONB NOT NULL DEFAULT '[]'::jsonb,
    aspects_payload      JSONB NOT NULL DEFAULT '[]'::jsonb,
    cycles_payload       JSONB NOT NULL DEFAULT '{}'::jsonb,
    events_payload       JSONB NOT NULL DEFAULT '[]'::jsonb,
    signals_payload      JSONB NOT NULL DEFAULT '{}'::jsonb,
    grid_overlay_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (snapshot_ts, location_key, source_mode)
);

CREATE INDEX IF NOT EXISTS idx_astrogrid_snapshot_date
    ON astrogrid.sky_snapshot (snapshot_date DESC);
CREATE INDEX IF NOT EXISTS idx_astrogrid_snapshot_source
    ON astrogrid.sky_snapshot (source_mode, snapshot_date DESC);

CREATE TABLE IF NOT EXISTS astrogrid.engine_run (
    id                    BIGSERIAL PRIMARY KEY,
    sky_snapshot_id       BIGINT NOT NULL REFERENCES astrogrid.sky_snapshot(id),
    lens_set_id           BIGINT REFERENCES astrogrid.lens_set(id),
    engine_key            TEXT NOT NULL,
    engine_family         TEXT NOT NULL,
    provider_mode         TEXT NOT NULL CHECK (provider_mode IN ('deterministic', 'llm', 'hybrid')),
    model_name            TEXT,
    direction_label       TEXT,
    confidence            DOUBLE PRECISION CHECK (confidence BETWEEN 0 AND 1),
    horizon_label         TEXT,
    reading               TEXT NOT NULL,
    omen                  TEXT,
    prediction            TEXT,
    claim_payload         JSONB NOT NULL DEFAULT '[]'::jsonb,
    rationale_payload     JSONB NOT NULL DEFAULT '[]'::jsonb,
    contradiction_payload JSONB NOT NULL DEFAULT '[]'::jsonb,
    feature_trace         JSONB NOT NULL DEFAULT '{}'::jsonb,
    citation_payload      JSONB NOT NULL DEFAULT '[]'::jsonb,
    raw_output            JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_astrogrid_engine_run_snapshot
    ON astrogrid.engine_run (sky_snapshot_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_astrogrid_engine_run_key
    ON astrogrid.engine_run (engine_key, created_at DESC);

CREATE TABLE IF NOT EXISTS astrogrid.seer_run (
    id                     BIGSERIAL PRIMARY KEY,
    sky_snapshot_id        BIGINT NOT NULL REFERENCES astrogrid.sky_snapshot(id),
    lens_set_id            BIGINT REFERENCES astrogrid.lens_set(id),
    merge_mode             TEXT NOT NULL CHECK (merge_mode IN ('solo', 'chorus', 'intersection', 'shadow')),
    supporting_lenses      TEXT[] NOT NULL DEFAULT '{}'::text[],
    source_engine_runs     JSONB NOT NULL DEFAULT '[]'::jsonb,
    convergence_map        JSONB NOT NULL DEFAULT '{}'::jsonb,
    contradiction_map      JSONB NOT NULL DEFAULT '{}'::jsonb,
    world_overlay_payload  JSONB NOT NULL DEFAULT '{}'::jsonb,
    reading                TEXT NOT NULL,
    prediction             TEXT NOT NULL,
    confidence             DOUBLE PRECISION CHECK (confidence BETWEEN 0 AND 1),
    confidence_band        TEXT CHECK (confidence_band IN ('low', 'medium', 'high', 'extreme')),
    key_factors            JSONB NOT NULL DEFAULT '[]'::jsonb,
    conflict_payload       JSONB NOT NULL DEFAULT '[]'::jsonb,
    action_bias            TEXT,
    window_label           TEXT,
    raw_output             JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_astrogrid_seer_run_snapshot
    ON astrogrid.seer_run (sky_snapshot_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_astrogrid_seer_run_conf_band
    ON astrogrid.seer_run (confidence_band, created_at DESC);

CREATE TABLE IF NOT EXISTS astrogrid.persona_run (
    id               BIGSERIAL PRIMARY KEY,
    seer_run_id      BIGINT REFERENCES astrogrid.seer_run(id),
    lens_set_id      BIGINT REFERENCES astrogrid.lens_set(id),
    persona_key      TEXT NOT NULL,
    provider_mode    TEXT NOT NULL CHECK (provider_mode IN ('deterministic', 'llm', 'hybrid')),
    model_name       TEXT,
    question         TEXT NOT NULL,
    declared_lens    TEXT,
    allowed_lenses   TEXT[] NOT NULL DEFAULT '{}'::text[],
    excluded_lenses  TEXT[] NOT NULL DEFAULT '{}'::text[],
    answer_text      TEXT NOT NULL,
    answer_payload   JSONB NOT NULL DEFAULT '{}'::jsonb,
    citation_payload JSONB NOT NULL DEFAULT '[]'::jsonb,
    raw_output       JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_astrogrid_persona_run_seer
    ON astrogrid.persona_run (seer_run_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_astrogrid_persona_run_key
    ON astrogrid.persona_run (persona_key, created_at DESC);

CREATE TABLE IF NOT EXISTS astrogrid.hypothesis_log (
    id                 BIGSERIAL PRIMARY KEY,
    sky_snapshot_id    BIGINT NOT NULL REFERENCES astrogrid.sky_snapshot(id),
    seer_run_id        BIGINT REFERENCES astrogrid.seer_run(id),
    hypothesis_key     TEXT NOT NULL,
    title              TEXT NOT NULL,
    statement          TEXT NOT NULL,
    action_label       TEXT NOT NULL,
    window_start       TIMESTAMPTZ,
    window_end         TIMESTAMPTZ,
    confidence         DOUBLE PRECISION CHECK (confidence BETWEEN 0 AND 1),
    supporting_lenses  TEXT[] NOT NULL DEFAULT '{}'::text[],
    supporting_runs    JSONB NOT NULL DEFAULT '[]'::jsonb,
    feature_payload    JSONB NOT NULL DEFAULT '{}'::jsonb,
    event_payload      JSONB NOT NULL DEFAULT '[]'::jsonb,
    outcome_state      TEXT NOT NULL DEFAULT 'open' CHECK (outcome_state IN (
                           'open', 'confirmed', 'mixed', 'failed', 'expired')),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_astrogrid_hypothesis_snapshot
    ON astrogrid.hypothesis_log (sky_snapshot_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_astrogrid_hypothesis_state
    ON astrogrid.hypothesis_log (outcome_state, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_astrogrid_hypothesis_key
    ON astrogrid.hypothesis_log (hypothesis_key, created_at DESC);

CREATE TABLE IF NOT EXISTS astrogrid.world_state (
    id                   BIGSERIAL PRIMARY KEY,
    sky_snapshot_id      BIGINT NOT NULL REFERENCES astrogrid.sky_snapshot(id),
    seer_run_id          BIGINT REFERENCES astrogrid.seer_run(id),
    atlas_payload        JSONB NOT NULL DEFAULT '{}'::jsonb,
    observatory_payload  JSONB NOT NULL DEFAULT '{}'::jsonb,
    coordinate_payload   JSONB NOT NULL DEFAULT '{}'::jsonb,
    capital_flow_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_astrogrid_world_state_snapshot
    ON astrogrid.world_state (sky_snapshot_id, created_at DESC);

CREATE TABLE IF NOT EXISTS astrogrid.outcome_log (
    id               BIGSERIAL PRIMARY KEY,
    target_kind      TEXT NOT NULL CHECK (target_kind IN (
                         'engine_run', 'seer_run', 'persona_run', 'hypothesis')),
    target_id        BIGINT NOT NULL,
    horizon_label    TEXT,
    observed_at      TIMESTAMPTZ NOT NULL,
    outcome_state    TEXT NOT NULL CHECK (outcome_state IN (
                         'open', 'confirmed', 'mixed', 'failed', 'expired')),
    observed_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    score            DOUBLE PRECISION,
    notes            TEXT,
    provenance       JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_astrogrid_outcome_target
    ON astrogrid.outcome_log (target_kind, target_id, observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_astrogrid_outcome_state
    ON astrogrid.outcome_log (outcome_state, observed_at DESC);

CREATE TABLE IF NOT EXISTS astrogrid.session_log (
    id              BIGSERIAL PRIMARY KEY,
    session_key     TEXT NOT NULL,
    event_type      TEXT NOT NULL CHECK (event_type IN (
                        'open', 'view', 'config', 'sync', 'question', 'close')),
    page_key        TEXT,
    lens_set_id     BIGINT REFERENCES astrogrid.lens_set(id),
    sky_snapshot_id BIGINT REFERENCES astrogrid.sky_snapshot(id),
    seer_run_id     BIGINT REFERENCES astrogrid.seer_run(id),
    persona_run_id  BIGINT REFERENCES astrogrid.persona_run(id),
    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_astrogrid_session_key
    ON astrogrid.session_log (session_key, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_astrogrid_session_event
    ON astrogrid.session_log (event_type, created_at DESC);

CREATE OR REPLACE FUNCTION astrogrid.prevent_log_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION '% is append-only: % is not permitted on id=%',
        TG_TABLE_NAME, TG_OP, COALESCE(OLD.id, NEW.id);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_astrogrid_lens_set_no_mutation ON astrogrid.lens_set;
CREATE TRIGGER trg_astrogrid_lens_set_no_mutation
    BEFORE UPDATE OR DELETE ON astrogrid.lens_set
    FOR EACH ROW EXECUTE FUNCTION astrogrid.prevent_log_mutation();

DROP TRIGGER IF EXISTS trg_astrogrid_sky_snapshot_no_mutation ON astrogrid.sky_snapshot;
CREATE TRIGGER trg_astrogrid_sky_snapshot_no_mutation
    BEFORE UPDATE OR DELETE ON astrogrid.sky_snapshot
    FOR EACH ROW EXECUTE FUNCTION astrogrid.prevent_log_mutation();

DROP TRIGGER IF EXISTS trg_astrogrid_engine_run_no_mutation ON astrogrid.engine_run;
CREATE TRIGGER trg_astrogrid_engine_run_no_mutation
    BEFORE UPDATE OR DELETE ON astrogrid.engine_run
    FOR EACH ROW EXECUTE FUNCTION astrogrid.prevent_log_mutation();

DROP TRIGGER IF EXISTS trg_astrogrid_seer_run_no_mutation ON astrogrid.seer_run;
CREATE TRIGGER trg_astrogrid_seer_run_no_mutation
    BEFORE UPDATE OR DELETE ON astrogrid.seer_run
    FOR EACH ROW EXECUTE FUNCTION astrogrid.prevent_log_mutation();

DROP TRIGGER IF EXISTS trg_astrogrid_persona_run_no_mutation ON astrogrid.persona_run;
CREATE TRIGGER trg_astrogrid_persona_run_no_mutation
    BEFORE UPDATE OR DELETE ON astrogrid.persona_run
    FOR EACH ROW EXECUTE FUNCTION astrogrid.prevent_log_mutation();

DROP TRIGGER IF EXISTS trg_astrogrid_hypothesis_log_no_mutation ON astrogrid.hypothesis_log;
CREATE TRIGGER trg_astrogrid_hypothesis_log_no_mutation
    BEFORE UPDATE OR DELETE ON astrogrid.hypothesis_log
    FOR EACH ROW EXECUTE FUNCTION astrogrid.prevent_log_mutation();

DROP TRIGGER IF EXISTS trg_astrogrid_world_state_no_mutation ON astrogrid.world_state;
CREATE TRIGGER trg_astrogrid_world_state_no_mutation
    BEFORE UPDATE OR DELETE ON astrogrid.world_state
    FOR EACH ROW EXECUTE FUNCTION astrogrid.prevent_log_mutation();

DROP TRIGGER IF EXISTS trg_astrogrid_outcome_log_no_mutation ON astrogrid.outcome_log;
CREATE TRIGGER trg_astrogrid_outcome_log_no_mutation
    BEFORE UPDATE OR DELETE ON astrogrid.outcome_log
    FOR EACH ROW EXECUTE FUNCTION astrogrid.prevent_log_mutation();

DROP TRIGGER IF EXISTS trg_astrogrid_session_log_no_mutation ON astrogrid.session_log;
CREATE TRIGGER trg_astrogrid_session_log_no_mutation
    BEFORE UPDATE OR DELETE ON astrogrid.session_log
    FOR EACH ROW EXECUTE FUNCTION astrogrid.prevent_log_mutation();

INSERT INTO astrogrid.grid_input_allowlist (input_kind, object_name, purpose, notes)
VALUES
    ('feature', 'geomagnetic_kp_index', 'Solar weather overlay for AstroGrid timing state.', 'Read-only input from public.resolved_series.'),
    ('feature', 'geomagnetic_ap_index', 'Secondary geomagnetic overlay.', 'Read-only input from public.resolved_series.'),
    ('feature', 'sunspot_number', 'Solar-cycle context for AstroGrid signal state.', 'Read-only input from public.resolved_series.'),
    ('feature', 'solar_flux_10_7cm', 'Solar radio flux overlay.', 'Read-only input from public.resolved_series.'),
    ('feature', 'solar_wind_speed', 'Solar wind overlay.', 'Read-only input from public.resolved_series.'),
    ('feature', 'solar_storm_probability', 'Storm-risk overlay.', 'Read-only input from public.resolved_series.'),
    ('feature', 'solar_cycle_phase', 'Solar-cycle phase overlay.', 'Read-only input from public.resolved_series.'),
    ('feature', 'nakshatra_index', 'Cross-check for historical nakshatra studies.', 'Read-only input from public.resolved_series.'),
    ('feature', 'spy_full', 'Market overlay for retrospective scoring and correlation only.', 'AstroGrid may read this; it must not write derived outputs back into GRID market tables.'),
    ('table', 'regime_history', 'Read-only regime overlay when available.', 'Optional upstream table outside astrogrid schema.'),
    ('table', 'briefings', 'Legacy celestial briefing cache if present.', 'Optional upstream table outside astrogrid schema.'),
    ('table', 'celestial_briefings', 'Preferred celestial briefing cache if present.', 'Optional upstream table outside astrogrid schema.')
ON CONFLICT (input_kind, object_name) DO NOTHING;

INSERT INTO astrogrid.celestial_object_registry (
    object_key, display_name, object_class, source, precision_label, visual_priority, track_mode, metadata
)
VALUES
    ('sun', 'Sun', 'luminary', 'analysis.ephemeris', 'derived', 100, 'reliable', '{"glyph":"Su"}'::jsonb),
    ('moon', 'Moon', 'luminary', 'analysis.ephemeris', 'derived', 95, 'reliable', '{"glyph":"Mo"}'::jsonb),
    ('mercury', 'Mercury', 'planet', 'analysis.ephemeris', 'derived', 90, 'reliable', '{"glyph":"Me"}'::jsonb),
    ('venus', 'Venus', 'planet', 'analysis.ephemeris', 'derived', 88, 'reliable', '{"glyph":"Ve"}'::jsonb),
    ('mars', 'Mars', 'planet', 'analysis.ephemeris', 'derived', 86, 'reliable', '{"glyph":"Ma"}'::jsonb),
    ('jupiter', 'Jupiter', 'planet', 'analysis.ephemeris', 'derived', 84, 'reliable', '{"glyph":"Ju"}'::jsonb),
    ('saturn', 'Saturn', 'planet', 'analysis.ephemeris', 'derived', 82, 'reliable', '{"glyph":"Sa"}'::jsonb),
    ('uranus', 'Uranus', 'planet', 'analysis.ephemeris', 'derived', 76, 'reliable', '{"glyph":"Ur"}'::jsonb),
    ('neptune', 'Neptune', 'planet', 'analysis.ephemeris', 'derived', 74, 'reliable', '{"glyph":"Ne"}'::jsonb),
    ('pluto', 'Pluto', 'planet', 'analysis.ephemeris', 'derived', 72, 'reliable', '{"glyph":"Pl"}'::jsonb),
    ('rahu', 'Rahu', 'node', 'analysis.ephemeris', 'derived', 68, 'reliable', '{"glyph":"Ra"}'::jsonb),
    ('ketu', 'Ketu', 'node', 'analysis.ephemeris', 'derived', 66, 'reliable', '{"glyph":"Ke"}'::jsonb)
ON CONFLICT (object_key) DO NOTHING;
"""


def upgrade() -> None:
    """Create the AstroGrid persistence boundary inside a dedicated schema."""
    op.execute(DDL)


def downgrade() -> None:
    """Drop the dedicated AstroGrid schema."""
    op.execute("DROP SCHEMA IF EXISTS astrogrid CASCADE")
