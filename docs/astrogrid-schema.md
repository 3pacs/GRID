# AstroGrid Schema

## Position
- same Postgres server
- separate schema: `astrogrid`
- shared GRID tables stay upstream-only

## Read Boundary
AstroGrid may read:
- `public.raw_series`
- `public.resolved_series`
- `public.feature_registry`
- `public.source_catalog`
- `public.regime_history` when present
- `public.briefings` or `public.celestial_briefings` when present

AstroGrid does not write into those relations.

Allowed shared inputs are declared in:
- `astrogrid.grid_input_allowlist`

## Write Boundary
AstroGrid writes only to:
- `astrogrid.celestial_object_registry`
- `astrogrid.lens_set`
- `astrogrid.sky_snapshot`
- `astrogrid.engine_run`
- `astrogrid.seer_run`
- `astrogrid.persona_run`
- `astrogrid.hypothesis_log`
- `astrogrid.world_state`
- `astrogrid.outcome_log`
- `astrogrid.session_log`

## Laws
- compute sky first
- write derived state only to `astrogrid.*`
- version lens sets; do not mutate past lens rows
- append-only for snapshots, runs, hypotheses, world states, outcomes, sessions
- every Seer row cites source engine runs
- every persona row cites Seer or engine basis
- every market overlay must come from the allowlist

## Why
- past AstroGrid inference stays stable
- GRID market logic stays separate
- provenance stays inspectable
- later scoring is possible without contaminating shared tables

## Migration
Fresh install:
1. apply [`/Users/anikdang/dev/GRID-astrogrid/schema.sql`](/Users/anikdang/dev/GRID-astrogrid/schema.sql)
2. run `alembic stamp head`

Existing install:
1. run `alembic upgrade head`

## Next
- move AstroGrid run logging from browser storage into `astrogrid.*`
- switch snapshot caching to `astrogrid.sky_snapshot`
- bind live overlay queries to `astrogrid.grid_input_allowlist`
- score Seer and hypothesis rows into `astrogrid.outcome_log`
