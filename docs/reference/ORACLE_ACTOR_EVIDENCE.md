# Oracle actor evidence selection

This is a containment and provenance contract, not a validated trading edge.
It changes the actor injection path only. No source rows are repaired, rewritten,
backfilled or deleted, and no generators, providers or schedules are enabled.

## Reader and identity

`get_actor_evidence_for_ticker` receives Oracle's timezone-aware decision clock.
It reads the ticker's last 30 calendar days, bounds the stored observation DATE
by that decision date, and bounds `created_at` by the decision instant. The
latter is ingestion availability, not a market event/publication timestamp.
Null timing fields are retained as excluded candidates, never made fresh.

Known aggregate source types, placeholder names, feed identifiers (including
keyed `qq_*` names) and missing source channels cannot become actor votes.
Direction is an exact, case-insensitive allowlist: BULL/bullish/buy/long/call/up
and BEAR/bearish/sell/short/put/down. Neutral and unknown values are not bearish.
No directional meaning is inferred from arbitrary payload prose.

## Correlated observation rule

The deduplication unit is normalized actor name plus `signal_type`, the source
channel. Raw `source_id` is lineage, not the grouping key: it can encode a
transaction direction or event-specific identifier. Whitespace/case/name
punctuation normalization follows the existing actor ID convention. This is
not entity resolution and does not establish statistical independence between
remaining actor/channel groups or between these and other Oracle signals.

Within the bounded candidate window:

1. Select the latest stored observation DATE for each actor/channel.
2. If any row on that date is unknown/neutral, or directions conflict, withhold
   the entire group's vote. Do not resurrect an older directional observation.
   Arrival time cannot resolve the ordering of date-only actions.
3. Otherwise emit one directional vote. Preserve all same-day supporting row
   identities and counts of all older, superseded candidates. A stable representative row
   is only a serialization choice, not an event-order claim.
4. Only then apply the existing influence threshold and five-direct-vote cap.
   Zero-trust actors cannot contribute a direction-only coherence vote.
   Heuristic z magnitude, trust/influence weight and type-level credibility
   boost are unchanged; used weight boosts have their own audit receipts.

A query reads at most 1,001 candidates. The 1,001st is an overflow sentinel for
an allowed 1,000-row window. On overflow, no actor boost or vote is used. The
receipt declares incomplete coverage, a lower bound on candidate count and
a twelve-record explicitly sampled audit. It never selects from a possibly
truncated conflict. Candidate-window completeness and sampled-detail completeness
are separate fields, and disposition counts cover every candidate actually read.
Excluded/superseded sample strings are limited to 160 characters with named
`truncated_fields`; used evidence references are never clipped.

A latest-day group with more than twenty supporting rows is withheld rather than
storing incomplete used lineage. Before any weight mutation or injection, Oracle
plans the complete used groups and enforces a 16 KiB JSON cap covering both the
receipt and duplicated per-Signal provenance. Overflow leaves all input weights
and signals unchanged and emits a small explicit unavailable receipt. These are
coverage limits, not claims that the excluded evidence has no information.

## Time and persisted receipts

`Signal.freshness_hours` is null for actor evidence because signal_data supplies
only an observation DATE. Provenance separately carries that date, its declared
precision, `created_at` availability, the UTC calendar-date age and an
observation-date age lower bound measured from the end of that UTC calendar day.
This lower bound is not an exact event age or guaranteed provider publication
age. It allows the stale-data guard to recognize old observation dates without
representing same-day data as exactly zero hours old. Unknown freshness is
reported explicitly; this patch does not introduce an unknown-age penalty.

The signal's provenance includes row/source identities, selected canonical and
original direction, current actor trust/influence, grouping basis and candidate
dispositions. The in-memory `flow_context.actor_evidence` retains all used groups and complete
support references (each inherits its group's observation DATE and canonical
direction, and carries its own row/source ID, ingestion time and raw direction),
exact counts/dispositions for the candidate window, sampled
withheld/superseded detail, direct-vote identities and source-weight changes.
The writer persists this receipt under `signals.actor_evidence` in the enriched
signals JSON, not in stored `flow_context`. The existing upsert replaces that
entire signals JSON atomically. Each Signal's `used_group_row_id` therefore
resolves within the same stored value, and the used groups survive Oracle's
ten-signal display cap. There is no duplicate actor receipt in persisted flow
context. Reports accept both in-memory and persisted forms and label date-only
age and ingestion separately and disclose incomplete actor coverage.

## Limits

- `signal_data` and the actors table are mutable and have no historical-vintage
  replay contract. Filtering `created_at` is necessary but not sufficient for
  historical replay; current actor attributes are explicitly labelled as such.
- Existing prediction upserts can mix fields from separate evaluations. This
  patch keeps new actor receipts atomic with their signals, but does not fix
  the mixed confidence/target policy or reconstruct historical predictions.
- Other Oracle inputs, including options snapshots and convergence injection,
  retain their existing freshness behavior. This is not an all-source freshness
  repair, model recalibration, provider coverage guarantee or financial-edge
  validation.
- Unknown aliases, duplicate people across channels and semantically correlated
  sources need a separate entity/provenance model. The name/channel rule makes
  no stronger independence claim.

## Validation

Synthetic tests in `test_actor_signal_bridge.py` and
`test_oracle_actor_evidence.py` cover canonical/unknown directions, aggregate
feeds and keyed identifiers, repeated observations, same-day conflicts, genuine
distinct actors, missing/future/old dates, availability bounds, overflow,
serialized lineage, date-only reporting and actual Oracle score/coherence
changes. The source SELECT also runs against a synthetic in-memory SQL table;
PostgreSQL-specific actor lookup is mocked. Live production data is not needed.
