# AstroGrid Schema

## Position
- same Postgres server
- separate schema: `astrogrid`
- shared GRID tables stay upstream-only

## Read Boundary
[[AstroGrid]] may read:
- `public.raw_series`
- `public.resolved_series`
- `public.feature_registry`
- `public.source_catalog`
- `public.regime_history` when present
- `public.briefings` or `public.celestial_briefings` when present

[[AstroGrid]] does not write into those relations.

Allowed shared inputs are declared in:
- `astrogrid.grid_input_allowlist`

## Write Boundary
[[AstroGrid]] writes only to:
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
- past [[AstroGrid]] inference stays stable
- GRID market logic stays separate
- provenance stays inspectable
- later scoring is possible without contaminating shared tables

## Row cohorts and known gaps (issue #500)
`astrogrid.sky_snapshot` rows do not all mean the same thing. Know which cohort a row is in before treating it as a point-in-time record. Counts and dates were measured read-only on 2026-11-05.

| Cohort | `snapshot_date` | `created_at` | What it is |
|---|---|---|---|
| Back-computed | 2024-01-01 → 2026-03-30 (181 rows) | 2026-03-28 → 03-31 | Built after the fact with the code and data of 03-28 → 03-31. **Not** point-in-time. |
| On-demand | April 2026 (6 irregular rows: 04-12, 04-19, …, 04-28) | same day | Written only when someone opened `GET /astrogrid/snapshot`. |
| Gap | 2026-04-29 → 2026-09-13 | none | No scheduled writer existed, so nothing ran once page views stopped. |
| Gap | 2026-09-14 | none | `NameError` from #493 until the #497 fix deployed. |
| Scheduled | 2026-09-15 → now | same UTC day | Hermes step 7c2 (`oracle/astrogrid_cycle.py`), hourly and idempotent: one row per day. |
| Gap | 2026-10-12, 2026-10-14 → 10-19 | none | grid-hermes was down; reported in vault notes, not log-verified. `seer_run` has the same holes. |

**Decision (2026-11-05): forward-only.**
- No gap is backfilled; each stays recorded as known-missing.
- A backfill would not be point-in-time: `build_snapshot` reads `regime_history` with no knowledge-time filter, so a rebuilt past day would see today's revised regimes.
- A consumer that needs point-in-time rows filters on `created_at <= as_of`, which excludes the back-computed cohort automatically.

**Freshness.** `astrogrid.sky_snapshot_missing` (`alerts/health_alerter.py`) fires when today's UTC row is still absent after 02:00Z. `scripts/hermes_health.py::check_sky_snapshot_freshness` produces the check.

**Run tables**
- **`persona_run`:** on-demand until 2026-05-01, written only by `POST /astrogrid/interpret`. The scheduled cycle has written it since 2026-09-15 (`save_interpretation` always writes the persona row).
- **`seer_run`:** empty before the scheduled cycle, because an interpret request without a `snapshot` skips the Seer row. The cycle has written it since 2026-09-15; its holes match the sky_snapshot gaps.
- **`engine_run` has no producer.** Rows come only from `engine_outputs` in an interpret request. The scheduled cycle sends none, and no client in this repo does either. So the table is empty by construction (this is read from the code; the table's row count was not re-checked), and every Seer row's `source_engine_runs` is `[]`. That breaks the "every Seer row cites source engine runs" law above until the lens engines run on the server.

## Migration
Fresh install:
1. apply [`/Users/anikdang/dev/GRID-astrogrid/schema.sql`](/Users/anikdang/dev/GRID-astrogrid/schema.sql)
2. run `alembic stamp 7e4dfecce247` (the baseline — not `head`, which would
   skip every later revision)
3. run `alembic upgrade head`

Existing install:
1. run `alembic upgrade head`

## Next
- move [[AstroGrid]] run logging from browser storage into `astrogrid.*`
- switch snapshot caching to `astrogrid.sky_snapshot`
- bind live overlay queries to `astrogrid.grid_input_allowlist`
- score Seer and hypothesis rows into `astrogrid.outcome_log`
