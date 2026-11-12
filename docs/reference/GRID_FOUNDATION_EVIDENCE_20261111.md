# GRID foundation evidence candidate, 2026-11-11

Source-only review work for #1129 and #1130 under #1128. Base:
`1aeb5b94710151e03feb7409a46b56995f3160ee`. Not a release, a completed
coverage audit, a backtest, a new registered trial or a claim of edge.

## Historical event admission is held

The existing Sleuth construction v2 remains byte-for-byte unchanged in its
pre-existing executable functions/classes. Its registered forward study,
lead IDs, baselines, scorer, journals and outcomes are not changed. No new
candidate function is called by a runtime producer.

The legacy raw reader filters `signal_sources.created_at`, which cannot prove
when an overwritten payload became known. Reusing canonical `people_events`
is necessary but does not by itself solve the receipt clock:

- `known_at`/basis can be tightened later by a newly found source.
- Actor/security identity, source counts and echo labels can be enriched later.
- `ingested_at` is PostgreSQL `NOW()`, the transaction-start timestamp.
- Revision `recorded_at` is `clock_timestamp()` inside the writing transaction,
  not a post-commit visibility receipt.
- Successful run `finished_at` follows insertion chunk commits, but the
  revision log has no durable run linkage proving retraction visibility.

Sources: canonical adapters/plan/writer under
`intelligence/people_events_pipeline/`, store readers in `store/people_events.py`,
and `people_events_20260927` / `people_events_v2_20261001` migrations. No migration,
writer or store query is changed in this candidate.

`collect_people_event_evidence` validates an explicit ticker/aware decision
then returns no evidence with `commit_visibility_unproven`, before any I/O.
It must not fall back to mutable raw data or relabel transaction timestamps as
first-seen. This is a tested refusal gate, not a repaired historical panel.

The pure `qualify_people_event_versions` prototype is exercised only by synthetic
fixtures. It requires an explicit post-commit receipt ID/hash/clock contract,
reuses canonical `plan.visible_at`, retains event version IDs/content hashes,
and uses exact half-open seven-day receipt windows. It validates structure,
not authenticity. No existing producer is claimed to satisfy this contract.

It excludes unsupported/aggregate channels and unqualified directions, handles
correction/retraction equality, and checks same-ID integrity before visibility.
An identifiable malformed-clock act is quarantined; more than one visible
version of an act is withheld. Identical repeated versions remain one record.
Mutable identity/echo/source data are not features, eligibility tests or receipt
fields. A record count is not an independent source/actor count. Cross-channel
independence, source custody, provider correctness and historical completeness
remain unproven.

### Required next acceptance, before historical use

1. Design and review a durable post-commit capture receipt over exact payload
   versions and decisions, including corrections, retractions and failure/retry
   behavior. Reuse canonical version storage, do not introduce a parallel ledger.
2. Prove the clock against a disposable PostgreSQL instance with long transactions,
   correction/retraction races, rolled-back writes and restart/retry cases. SQLite
   or mocks cannot prove PostgreSQL visibility semantics.
3. Measure retained original-source custody and source/date/universe coverage
   without outcomes. Do not turn missing history into zero signal.
4. Freeze a separately named prospective construction and evaluation contract
   before activation. Do not amend v2 retroactively or reuse spent holdouts.

## Forced-flow rule-count candidate

Five raw diagnostic checks are retained, but the score groups them into at most
two inputs: dealer positioning and an admitted scheduled catalyst. Gamma flip,
short-gamma regime and put-wall proximity share one group. The compound
short-gamma/catalyst check adds no vote. The count is neither an independence
claim nor a calibrated probability.

Calendar distance reuses `ingestion.market_calendar` standard US-equity sessions,
including ordinary holidays. Aware decision instants use the America/New_York
calendar date, with midnight/DST regression coverage. Extraordinary closures and foreign/product calendars
are not established by that helper. OPEX conventions and quarter-end JHEQX proxies
are explicitly estimates. The static 2026 FOMC list has no verified revision receipt.
Quarterly OPEX no longer invents an autocall observation date.

Schedule admission requires a source, event-specific identity, timezone-aware
revision-known-at no later than decision, supported kind, nonnegative matching
US-session distance and the source-backed status. It checks supplied evidence
metadata; it does not authenticate an external source or establish actual flow.
There is no new built-in admitted schedule source.

### Compatibility and release hold

The legacy `waterfall_risk_score` field changes from an overlapping 0–5 raw-check
sum to a versioned 0–2 grouped count. Full briefing JSON carries the method,
maximum, groups and limits. Historical stored rows are not rewritten. Consumers
must compare only like score methods.

Built-in estimates cannot supply the calendar group. Therefore the built-in path
cannot reach the two-group alert threshold until a schedule source is independently
qualified and integrated. This intentionally suppresses unsupported warnings;
it is a material behavior change requiring explicit release review. The
alert renderer now shows groups separately from raw diagnostics, uses the new
0–2 scale only for its explicit method, and preserves legacy rendering. Missing
explicit SPY spot now yields UNKNOWN instead of a fabricated wall midpoint;
existing explicit row/summary spot sources remain supported.
No production acceptance, alerts, persistence, services or deployment were run.

## Verification scope

Synthetic regression and independent source review cover the refusal and receipt
prototype, existing Sleuth v2 behavior, canonical people-event logic, calendar
boundaries, grouped checks, method-aware alerts, missing spot and alert gating. Tests run with cleared
environment, no root conftest/plugin autoload, blocked sockets/non-memory database
connections, and only in-memory SQLite where existing fixtures require it.

No provider data, paid work, live DB, historical outcome, sealed holdout or
production scoring was consumed. Full CI, disposable PostgreSQL acceptance,
post-commit receipt producer, calendar source qualification and release acceptance
remain outstanding. #1129 stays open.

Final local combined check: 362 tests passed across the six named synthetic
suites, with 11 pre-existing pandas FutureWarnings. Scoped Ruff E/W/F excluding
E501 and the blocking E9/F63/F7/F82 gate passed; whitespace checks passed.
AST comparison confirms every pre-existing Sleuth function/class is unchanged.
Default 88-character E501 style is not a claimed gate on these legacy files.
This is not a full repository/CI or production/PostgreSQL acceptance pass.

## 2026-11-12 reconciliation against main

The original Nov 11 candidate and 362-test receipt above remain historical
artifacts. The reconciliation base is main `adfa50a9578000d8d90931b2bf34fa77764b93fb`,
which includes #1138. Its `compute_latest_completed_gex_profile("SPY")` call
replaces the former market-summary reader; do not restore summary fallbacks.
The selected capture's chain date stays attached to the snapshot. The previous
spot rule is now applied to that one profile: missing, boolean, nonpositive,
nonfinite or unparseable values give UNKNOWN, with no fabricated wall midpoint.
The original summary-source compatibility statement above applies only to the
Nov 11 candidate, not this reconciled reader. This is still a prior completed
model, not proof of fresh executable quotes or observed dealer positions.

Only forced-flow integration and its synthetic tests required source adaptation.
Sleuth, canonical writer/store, fixed studies, scoring and outcomes are unchanged.
#1139's independent-stream work is a planning dependency, not implemented here.
The post-commit receipt producer and warning-suppression release hold remain.

Reconciliation verification: 380 synthetic tests passed across the prior six
suites plus `test_dealer_gamma_default_day.py`, with the same 11 pre-existing
pandas FutureWarnings. Scoped E/W/F lint excluding E501 and whitespace passed.
The adapter's invalid-spot regression first failed six cases on the unrepaired
#1138 reader, then passed after the finite-positive guard. Existing Sleuth
functions/classes are AST-identical to the new base; the entire Sleuth candidate,
its test file and the alert renderer are byte-identical to `dc7d53a9`.
No full-suite, PostgreSQL concurrency, CI or production acceptance is claimed.

Independent reconciliation review also found and corrected a stale integration
fixture in `test_dealer_gamma_spot_unavailable.py`: the old fixture returned
UNKNOWN from an unsupported registry query, with zero chain/spot-reader calls.
The adapted test explicitly checks selection of the completed batch, loading
that exact chain/cutoff and one missing-spot receipt lookup. A synthetic
fabricated-strike witness changes the result to SHORT_GAMMA, showing the UNKNOWN
assertion is sensitive to the original failure. No production data was used.
