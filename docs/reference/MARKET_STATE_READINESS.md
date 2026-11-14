# Market-state target-session outcomes (#1191)

The process exit is not a completeness/provenance certificate. The writer now emits per-target outcomes and stores additive writer provenance on newly written records. Existing records are never relabelled as this run's work.

- `written`: actual INSERT succeeded. Input dates/statuses, analysis gaps, pending signals, exact target dealer readiness, run UUID/start/cutoff and named source-file hashes are retained in JSON.
- `already_present`: existing row preserved before compute or FRED refresh. Its original computed_at/rule are reported, but no provenance or readiness is invented. This intentionally avoids refreshing FRED just to skip an old row; it is not a replacement standalone vintage-ingestion schedule.
- `already_present_race`: another insert won. The losing candidate's provenance is not attributed to the stored row.
- `held_missing_target_dealer`: only with optional `--require-target-dealer`; no market-state INSERT when gamma is absent or from another session. Exit3 is actionable, not a success-health result. The required gate is daily/insert-only and rejects --replace and backfill ranges.
- Dry runs never refresh providers or write data; their returned provenance explicitly says dry_run and whether the dealer gate would be required. Process exit0 means the dry run completed, not that a subsequent strict write would pass.

Targets are frozen once using UTC, not host-local date. Explicit --as-of pins a recovery target; dates at/after today and nontrading dates are refused. A timer's Persistent catch-up still cannot infer which missed sessions were intended: use explicit recovery dates and preserve their real execution time. No overwriting or backdating.

No new timer, service, provider or schema is added here. Existing schedule candidate must still be finalized by its author and applied by Anik. The strict gate is opt-in; existing normal runs preserve missing inputs as unknown while reporting degraded readiness. It is not automatically activated by this source change.

All outcomes say forward_qualified=false. Why: exact publication/capture/registration lineage for every source and independent evidence of scheduled execution are not established by these writer summaries. Observation dates/statuses and code hashes improve auditability but do not fill that gap. An existing backfill cannot become forward evidence simply because a timer later skipped it. Five qualified actual scheduled sessions remains the milestone, not five exit0s. Stale news/calendar, baseline shortages and unknown release/earnings surprises remain visible; no PIT/publication lag rule was relaxed.

Checks use synthetic in-memory SQLite and mocked computation; original market-state tests separately exercise real PIT calculations against synthetic source tables. No live write or provider request is part of the kit check. PostgreSQL live integration and revised scheduling remain operator/author follow-through, not claimed complete.
