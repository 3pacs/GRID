"""People-events pipeline (GD2 materializer + GD3 Form 4 backfill planner).

Design: ``wha/outputs/GRID-PEOPLE-EVENTS-PIPELINE-DESIGN-20261001.md``.
Plan: ``GRID-GRANULAR-DISCOVERY-PLAN-20260927.md`` sections 2.1 and 4.

Layers, each pure and separately tested:

``rules``     known_at rules, name/ticker normalization, dedup keys, direction,
              confidence. No I/O.
``adapters``  one function per source: source rows -> canonical *candidate*
              rows (a pandas frame with ``CANDIDATE_COLUMNS``) plus skip
              counts. No I/O.
``security``  point-in-time resolution of (issuer CIK, ticker, date) onto
              ``security_master.entity_id`` from an in-memory identifier frame.
``merge``     candidates -> one canonical event per (channel, dedup_key), with
              ``n_sources``/``source_refs``, near-duplicate flags and echo links.
``plan``      canonical events vs. the rows already stored -> an append-only
              write plan (insert / add_sources / tighten_known_at / supersede /
              unchanged). Idempotent by construction.
``readonly``  the only module that touches a database: read-only, bounded,
              refuses the 03:30-10:30Z backup window and ``raw_series``.
``dryrun``    the dry-run report the CLI (``scripts/people_events_dry_run.py``)
              prints. It never writes.

Nothing in this package writes to any database. The writer that applies a
write plan depends on the ``people_events_v2`` migration and ships with it.
"""

PIPELINE_VERSION = "people-events-pipeline-v1-20261001"
