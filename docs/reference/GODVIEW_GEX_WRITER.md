# God View G5 SPY persistence contract

Prepared offline from main `9377cd0eacf4bd9c26013dbcddcd37e8e725903a` for
V5a of `GRID-GODVIEW-DELIVERY-PLAN-20261006.md`. The tracked main tree had G2
schema, the GEX read model and the computation engine, but no G5 writer.
Open PR inventory and current task summaries did not establish a separate G5
implementation. The granular benchmark and RTD bridge are separate work.

`godview.dealer_gex.materialize_dealer_gex` wraps the existing
`DealerGammaEngine.compute_gex_profile("SPY", session, capture_batch_id=...)`.
It does not replace its capture or verified prior-close contract. There are
no provider calls, capture triggers, migrations, installed units or timers.

## Inputs and clocks

- One ET session per call. Default is the cutoff's ET date. A non-session is
  `non_session`; a different session is `inputs_stale`. An explicit historical
  cutoff replays its own session; it cannot overwrite a newer daily batch.
- Choose the highest registered capture ordinal for SPY and the exact session
  whose completion and registration are both known by the cutoff. Use
  `options_capture_batches` and replay `options_snapshots_all` by exact ID.
  A newer incomplete or mixed capture refuses the row; it never falls back to
  an older capture to manufacture a success. Source schema/read errors fail.
- Registered row count must equal stored count. The unchanged engine validates
  the batch identity, ordinal, start/completion, source quote dates, session and
  receipt, before computation. The writer also checks its returned identity
  against the selected header and rejects acquisition after the cutoff.
- Spot remains the engine's verified prior unadjusted SPY close, known by
  completion, 1–4 calendar days old, with receipt identity. Other tickers have
  no G5 writer. Neither capture-header spot nor paper-log prices are substitutes.
- `release_at` is capture completion. `available_at` is the latest of completion,
  registration, snapshot-row acquisition and spot-receipt creation. This is a
  conservative refinement of the original completion-only proposal: late
  registration/acquisition must not make a row visible before that input existed.
  Preserve quote, capture, acquisition, registration and spot clocks separately
  in `source_ref`; generation is represented by the run/updated-at metadata.

## Output, coverage and publication

Natural key is `(obs_date, ticker)`, with `ticker = SPY`. Store only G2's
engine-native values: aggregate, normalized GEX, flip/crossing count, regime,
walls, dealer delta and chain/spot provenance. Always `provenance = modeled`,
with explicit long-call/short-put assumptions and Black–Scholes basis. Native
aggregate is gamma × OI × 100 × spot, not the legacy dollars-per-1%-move column.
No conversion is inferred. Legacy GEX values, max pain, ATM IV and ratio remain
NULL. A zero engine output remains zero; no crossing remains NULL.

`coverage_fraction` is usable model rows divided by registered rows, after the
engine's expiry/positive-OI/positive-IV filters. `source_ref.coverage` retains
both counts and states this basis. This is neither provider-universe coverage
nor evidence of observed dealer inventory. Complete storage and model coverage
are separate properties. The existing GEX reader exposes this fraction; no
frontend or API availability policy is changed by this slice.

All source reads and the two business writes use one connection in one
repeatable-read transaction. `lock_timeout = 5s`, `statement_timeout = 60s`.
Maximum writes are one pillar row and one terminal `godview_runs` row, below
the plan's 50-row transaction limit. G2's deferred run FK is exercised at
commit. No permanent `running` entry is created.

Publication guards are enforced in the upsert itself:

- A legacy provenance-NULL row is never updated or deleted. It produces
  `partial_blocked_by_legacy`; clearing it remains the controller's A2 gate.
- Only a strictly higher capture ordinal with nondecreasing completion and
  availability may replace the daily row. An older replay is a no-op.
- Repeating the same batch preserves its original row/run/code identity.
  A changed modeled output for the same batch is refused with
  `immutable_batch_model_conflict`; this writer does not silently retcon a model.
- Refusals create a terminal run with reasons and no numeric row. A previous
  valid daily row stays unchanged; the reader's existing stale rules apply.
- A failure within the transaction body cannot commit either write. After its
  rollback, a new bounded transaction attempts a sanitized failed run (exception
  class only). A failure before that failed-run transaction reaches commit is
  reported with `ledger_recorded = false`.
- A COMMIT exception after the terminal insert can mean a lost reply to a durable
  commit. It triggers exactly one bounded read-only lookup by the original run ID,
  never a duplicate failed-run insert or another publication attempt. A visible
  terminal run returns its actual status/counts/code identity with `reconciled =
  true`. A receipt remains valid even if exiting that read-only transaction fails.
- Missing receipt or unavailable reconciliation returns `OUTCOME_UNKNOWN`,
  `outcome = unknown`, the original run ID, and NULL written/skipped/ledger and
  persisted fields. Absence at a single read does not prove an in-flight commit
  finished. `unknown` is an ephemeral caller state; it is never stored as a new
  ledger status. The same rule protects an uncertain failed-run COMMIT.
- Recovery is a subsequent separately authorized invocation, never automatic.
  A replay after an already durable publication is a no-op and retains its row,
  run and code identity; after actual rollback it can publish the row once.

## Verification and adoption

Synthetic offline tests:

```powershell
$env:DB_PASSWORD = 'testpass'
$env:GRID_JWT_SECRET = 'test-secret-offline'
$env:TESTING = 'true'
python -m pytest tests/godview/test_dealer_gex_writer.py tests/test_run_godview_writers.py tests/test_godview_no_incident_imports.py tests/test_dealer_gamma.py tests/test_dealer_gamma_dated_sources.py tests/test_dealer_gamma_spot_unavailable.py -q --basetemp=../pytest-g5-offline
```

Actual PostgreSQL proof (only a local disposable database with `test` in its
name, explicitly provided as `GRID_TEST_DB_URL`):

```text
python -m pytest tests/godview/test_dealer_gex_writer_pg.py -q -rA
```

This proof applies the real G2 and append-only source migrations in its own
throwaway schema and uses the real engine/SQL/read-model mapping. Only the
shared receipt verifier's return value is stubbed with synthetic receipts;
the engine's spot checks still run. Existing SPY receipt contracts separately
prove raw/resolved lineage. The dedicated CI step fails on any PG skip.

The runner accepts `--pillar gex`, `--session-date` and `--as-of-ts`. Its
existing `--dry-run` rolls back pillar and run writes. GEX rejects `--start`
ranges. Its code-SHA dirty check includes the engine, Greeks, receipt store and
calendar. This document is not authority to execute against production.

Controller `01a1067b-3df5-738c-86c7-5f20c98200dc` retains the DFb-first serial
release queue. Independent source review, actual PostgreSQL proof, exact-head
CI, controller adoption and release remain gates. Scheduling/first run and
V5b source → API → UI verification require separate authorization and a natural
eligible capture. Existing #788/GEM/live-input gates remain. No model accuracy,
scientific efficacy, current freshness or successful production delivery is
claimed. Frozen evaluation files and their ledgers are unchanged.
