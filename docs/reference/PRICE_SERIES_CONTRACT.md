# Provisional signal-evaluation price contract (sig-eval-3-dryrun)

This is an isolated, manual, read-only evaluator. It has no schema migration,
persistence path, imported production call site, provider call, or scheduler
hook. Its result is an exploratory cohort, not an accepted performance metric.

The accessor can select `raw_series` rows with an exact
`YF:{instrument}:close` identifier, `source_catalog.name = yfinance`,
`pull_status = SUCCESS`, observation date no later than the requested day,
and pull timestamp no later than that day's end in America/New_York. The
series convention and explicit `auto_adjust=False` current ingestion call
come from `ingestion/yfinance_pull.py`; the fields and status come from
`schema.sql`. Historical rows do not carry the ingestion code version or
an adjustment flag. Therefore a verified deployment/ingestion cutover must
be supplied before the accessor reads any price. This branch has no such
evidence; the manual CLI supplies no cutover and refuses ambiguous history.
Unit tests inject a hypothetical cutover solely to exercise the real query.
Only a row passing that guard can be recorded as `raw_close`.
It does not infer basis from `feature_registry` or `resolved_series`: the
entity map can send both `close` and `adj_close` into the same feature, while
resolved rows omit raw `series_id`.

The evaluator requires an exact-date entry and exact-date exit bar: a latest
bar before the entry target predates signal availability, and a bar before
the exit target cannot stand in for the nominal horizon. On weekends or
holidays this conservatively yields an ineligible result; it does not infer
the next tradable session. It assumes a calendar-day horizon and an after-16:00
New York timestamp becoming eligible on the following **calendar** date.
These are versioned dry-run assumptions, not approved exchange-calendar or
execution policy. The signal entry date is at least its `signal_date` and
the market date of its timezone-aware `known_at`, or, if absent, `created_at`.
`created_at` is an ingestion-time proxy; it does not prove publication or
public availability. Congressional transaction dates are refused unless an
explicit publication timestamp is supplied. The current CLI has no such
column and will therefore classify those rows as `known_at_unverified`.

The CLI accepts explicit source and date bounds and a maximum of 1000 rows.
It checks the live column type through `information_schema`: native DATE is
compared against DATE bounds; TIMESTAMPTZ is compared against UTC instants
derived from New York midnight boundaries. It then normalizes native Python
values and rejects naive timestamps. This avoids claiming a blanket SQL
`::date` cast works for both schemas. PostgreSQL behavior has **not** been
tested against a real database in this branch.

An immature horizon remains `UNRESOLVED` without fetching an exit. A stale
entry or stale/same exit is `INELIGIBLE`, never `NO_MOVE`. Dead-band and cost
parameters must be finite and within declared bounds. The input record has
no origin tag and all outcomes remain `unknown` because no row-level origin
proof is read.
A later provider
pull cannot retroactively establish a bar was known on an earlier day.
Historical bars first pulled after the cutoff will be absent by design;
without a verified raw-basis cutover, the cohort cannot have priced outcomes.
Real exchange sessions, disclosures,
corporate actions, splits, timezone/session alignment, and source lineage
still require validation before any production interpretation.
