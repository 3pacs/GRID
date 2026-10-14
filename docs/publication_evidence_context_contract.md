# Publication evidence context contract

This boundary changes future presentation and rejection only. It does not authenticate providers, repair data freshness, change scoring/queries/selection, or revise historical publications. Numeric grounding remains a literal-context check and does not establish units, age, actor identity or causality.

## Clocks and feature identity

Snapshot `timestamp` is the assembly clock; result `timestamp` is the generation clock and database `created_at` is a separate persistence clock. None fills a missing observation or availability time. Displays use original `obs_date` (when supplied) or `date`; missing values stay `unknown`. Source is the original `source_id` or `source`, otherwise `unknown`. Each field retains its category-qualified identity, so `equities:SPY` and `equity_volume:SPY` cannot alias.

Selected feature presentation looks up the original snapshot entry by the selected name. The shared `select_prompt_features` algorithm, its exact legacy raw-value `z` ranking inputs, limits, threshold and order remain unchanged. That ranking proxy is explicitly not a measured z-score. Raw values are labelled `raw value`; absent source/unit/availability/freshness remains unknown. A newer assembly clock does not imply newer data. The exception fallback renders the original first 20 features with the same provenance rules.

A supplied normalization is displayed separately as `source-reported z` only with `kind=z_score`, a finite nonboolean numeric value, and nonempty string `source_id`, `receipt_id`, and `date`. A bare feature `z` or raw value grants no normalization label. These references are source declarations; the publisher does not recompute or independently verify them. Normalization has its own as-of/source clock and does not change ranking.

## Publication outcome

`publication_status` is retained both on the result and in its snapshot before persistence:

- `content_kind=narrative`, `reason=null`: a model response passed the claim guard.
- `content_kind=data_summary`, `reason=model_unavailable`: this invocation's model callback returned `None`. Text says model response unavailable, not guard withheld.
- `content_kind=data_summary`, `reason=claim_guard_withheld`: a returned candidate failed the claim guard and was replaced in full. Text says narrative withheld, without declaring model unavailability.

`generation_outcome` retains `model_response` versus `model_unavailable` independently of rejection. `source_evidence_status=unknown` does not claim verified availability or freshness. A direct fallback call defaults to `source_evidence_unknown`, which cannot diagnose a model outage. Model exceptions propagate; they are not converted to a summary or a different reason. Rejected deterministic summaries still raise before file or database writes. A summary is not promoted to a model narrative. No retry is added.

## Actor statements

The existing before-write/TTS claim gate also rejects bounded affirmative institutional/retail positioning, ownership, distribution, accumulation and actor-driven trend assertions. Prices, insider/congressional ratios, model confidence, proxy edges and unstructured narrative do not grant actor permission. Unicode compatibility formatting, zero-width characters, emphasis and adjacent clauses cannot make known claims acceptable.

Bounded whole-clause caveats such as `Institutional distribution is unmeasured.` and `Prices alone cannot establish institutional distribution.` are nonclaims. Explicit hypotheses use a narrow grammar: `Hypothesis: institutional distribution may be occurring.` or `Hypothesis: retail buying could explain the rally.` A modal or unknown adjective does not license an affirmative trailing claim. Actor context is conservatively retained across conjunctions, punctuation and pronouns through the message, including elided `is occurring`/`is driving` and `they are buying` continuations. Whole bounded nonclaims and exact source attribution are evaluated independently and cannot license the next clause. This can reject unrelated protected-predicate prose following an actor mention; it does not perform general pronoun resolution. This lexer is conservative and incomplete; unfamiliar wording may be rejected or missed. It is not a general natural-language verifier.

A narrow direct observation can be attributed with `SOURCE source-reported ACTOR holdings DIRECTION for INSTRUMENT over the reported interval.` An optional structured `actor_evidence` entry must match source, actor, instrument and holdings direction, have a declared `ownership_filing`, `position_report`, or `broker_classified_transactions` source type, a receipt reference, and an aware increasing interval. Narrative/proxy/unknown sources, source substitutions and scope/direction swaps reject. These are declared provenance checks, not independent filing validation or a freshness/PIT certification. Such a holdings observation never permits the stronger distribution/retail-causal story. Current source queries do not produce new actor evidence; this change adds no collector or provider operation. Audio shares the conservative rejection gate and receives no new provenance grant.

## Preserved boundaries and limits

All scorer, selector and SQL source remains unchanged. The missing maximum-age/release cutoff/raw-row lineage, same-date tie semantics, scorer instrument-label mismatch, repeated values and unknown upstream provider/cache/writer cause remain separate owner/controller work. This patch supplies no deployed-writer verification or runtime closure. Original Oct12 publications/capture bytes, parked PCR candidate and negative exporter receipts remain immutable. Independent exact-source review, integration and release remain controller-owned; no commit, push, deployment or activation is performed by this source task.

## V2 verification exception

The existing fallback regression now expects `Model response unavailable` and explicitly asserts model-unavailable reason/outcome. Its grounding presence, nonempty numeric check and zero-ungrounded assertions remain unchanged. This is the sole intentional original-test function AST delta; V2 does not claim all original tests are unchanged. V1 and both historical baseline fixture versions/receipts remain immutable. The actual final V1 baseline has 22 failure entries (including subtests) and 5 errors over 29 items; the earlier forced-formatter-exception baseline has 21 entries/6 errors. Error and failure-entry counts are not distinct failing-test counts.

## Invocation consumption, v1 (source only)

`generate_briefing(..., *, attempt_id=None, receipt_callback=None)` adds an opt-in,
in-memory `PublicationReceipt` (`grid.publication_call_guard.v1`). With no callback,
the existing client kwargs, text/None/error behavior, status, snapshot and writer
payloads/order remain unchanged. With a callback, the existing injected client is
passed to accepted `llm.router.chat_with_receipt`; no selection, retry, extra health
check, provider or scoring operation is added. A caller identity is allocated before
that branch. Invalid supplied identities remain explicitly invalid/unscorable.

The consumer revalidates the accepted immutable `LLMCallReceipt`: schema, caller
identity, operation/stage, exact full usable candidate digest and character/UTF-8
lengths. Missing, multiple, foreign, malformed or mismatched receipts cannot supply
another call's ID. Router-reported unsupported/missing provenance remains explicit;
`caller_intent` is never promoted to a formed transport request. A returned ID has
only the existing logger namespace. Local record/export states stay unknown or
not attempted; publication does not upgrade them. `call_guard_correlation_eligible`
requires supported, full formed-request evidence, an actual logger ID, a usable
candidate and an evaluated candidate guard. It is an identity join capability,
not a source-truth, correctness, score or approval verdict. `scoring_state` remains
`not_scored`; source evidence and publication durability remain unknown.

Candidate SHA256 covers exact UTF-8 text before replacement or numeric annotation.
Character/byte lengths differ for Unicode; no normalization or prefix join occurs.
The candidate guard uses role `CANDIDATE`. With None there is no candidate guard
(`CANDIDATE_NOT_EVALUATED`, null verdict), and only the existing summary guard runs.
`fallback_guards` is an immutable ordered tuple of `DATA_SUMMARY` guard receipts;
a passing summary never overwrites a rejected candidate decision. If the first
unavailable-response summary fails and the existing second summary is tried, both
summary decisions are retained. No extra guards are run. Empty legacy strings
retain legacy behavior but are explicitly no usable candidate for correlation.
The legacy `model_unavailable` label is not a new endpoint-outage diagnosis. A
bound client's `no_candidate_cause` is retained; otherwise cause is unknown.

Each guard receipt contains its exact input digest, verdict/reasons and scope.
The claim-guard contract version is `grid.publication_claim_guard.v2`; deployment
implementation SHA is unavailable to this in-memory boundary and explicitly
unknown. The numeric scope remains `check_numbers_grounded; annotation only` and
its implementation hash is also unknown. Frozen source artifacts separately pin
both implementations for source review. `grounding_input_digest` records the
post-replacement, pre-annotation text; `emitted_digest` covers the final annotated
text. Hash capture failures/null candidates are explicit and do not change text.
Receipts contain no raw prompt, request, candidate, secret or payload body.

Only existing writer return values are captured. `save=False` is `not_requested`.
The existing file writer has no acknowledgement API, so a normal return is unknown.
Only a strictly positive integer DB return (excluding bool) is `row_acknowledged`
with that row reference. None, zero, negative, bool and noninteger returns remain
unknown. `emitted_narrative_acknowledged` or `emitted_data_summary_acknowledged`
means this invocation observed that existing row acknowledgement, with the actual
emitted digest/kind; it implies neither readback nor durable publication. The
content outcomes `no_usable_candidate`, `candidate_withheld`, and
`narrative_guard_passed` remain separate from writer outcomes. Numeric annotation
is reported by `ungrounded_count` and is not upgraded to numeric verification.

The callback fires after the existing writers, and optional return metadata is
attached only afterward, outside their payloads and outside the snapshot. Writer
exceptions retain their original identity/order and suppress later writers; a
best-effort failure receipt records `writer_failed`. A rejected final summary
retains the existing ValueError and emits `guard_withheld`, with no emitted digest
or writer call. Callback exceptions are secondary, never trigger a retry or alter
text. Callback values and nested guard/digest/call values are frozen; all state is
local to an invocation, including re-entrant interleaving on one engine. This adds
no persistence/schema/export/backfill/raw retention or publication authority.

Validation uses exact accepted client/router/DTO excerpts with synthetic transport,
logger, SDK, guard and writer dependencies in one bounded serial process. Unicode,
clipped-prefix collisions, foreign/multiple receipts, fallback failures, writer
failures and adversarial narratives are covered. Re-entrant interleaving verifies
local state without threads; real concurrent/native module/SDK/backend/runtime
integration and full CI are unrun. Baseline deficiency is demonstrated against the
immutable projected base in the same serial process; historical response bodies
and IDs are not reconstructed.

## Narrow irregular options-intent verb correction

The generic options-intent predicate now recognizes `bought` alongside its existing
buy/purchase/sell/write stems, in both verb-before-options and immediately
options-before-verb branches. This covers the evidenced actor-free sentence
`Narrative says has_signed_order_flow=true. Traders bought puts.` without treating
those narrative flags as structured authority. The existing phrase window, word
boundaries, normalization, disclosure handling, flow/actor allowances, numeric
logic and public declarations stay unchanged. Non-options purchases and longer
words such as calligraphy, callback and putative remain paired positive controls.
Hypotheses and disclaimers do not license asserted options intent. This is a
bounded lexical correction, not a general fact-verification or grammar guarantee.

The historical 23/24 failure and assertion remain immutable. A separate byte-exact
baseline reproduction precedes this derivative. Final source validation retains
all 24 consumer items and adds eight focused regression items, the six prepared
receipt mutants and one predicate-omission recurrence mutant, within the amended
serial process budget. Pass/fail counts are reported in the frozen test receipt;
source-only verification grants no runtime, publication or release authority.

## Blank-output acknowledgement correction (V3)

Acknowledgement outcome selection now checks the actual post-grounding emitted
text and its existing `publication_status.content_kind`, separately from whether
the original model candidate was usable. A strictly positive DB row return remains
`database_ack_state=row_acknowledged` with its original row reference. If the
emitted text is empty or whitespace, the publication outcome is explicitly
`no_usable_emitted_content`; that row acknowledgement does not establish a usable
narrative or invent a fallback summary. The legacy blank text, narrative status,
snapshot, writers and writer order/count remain unchanged, and no fallback or
model retry is added.

An actual nonblank emitted data summary can still have
`emitted_data_summary_acknowledged`, including a genuine fallback after a None
model response (`content_outcome=no_usable_candidate`) or a withheld candidate.
A nonblank emitted narrative retains `emitted_narrative_acknowledged`. With no
positive row acknowledgement, the existing `not_requested`,
`acknowledgement_unknown`, and failure states stay separate. Correlation,
logger identity, source truth and durability states are not upgraded by any
publication outcome.

The V2 reviewer negatives for empty, space and newline-tab strings with row79
and zero fallback calls are retained and reproduced in a separate byte-identical
baseline before repair. The original functional test bodies and bought predicate
remain unchanged; eight focused acknowledgement controls and an exact V2 outcome
recurrence mutant exercise this correction. The reviewer’s separate 24 timestamp
differential failures remain frozen and uncredited. This author check does not
reuse that comparator or the consumed reviewer process allowance, and changes no
production timestamps. A separately authorized recheck by the same retained
reviewer is still required; source tests grant no runtime or publication authority.

## Isolated composition with parked PR824

Existing PR824 context-fact, normalization, disclosure and annotation helpers are composed with V3. The original candidate claim decision and exact immutable call/guard digest binding run before context omission. Original source-claim rejection still replaces the whole candidate; context annotation cannot launder bought/actor/flow claims into a passing candidate. Fallback claim decisions likewise bind the original summary before context annotation. Grounding input and emitted digests then describe the actual context-annotated and number-annotated text respectively. Existing PR824 context decisions are retained in result/snapshot before writers; they do not grant source truth or durability.

The V3 evidence formatter/raw-value versus z separation, selector inputs, blank acknowledgement and actual emitted-kind distinctions remain. PR824 adds its existing optional include_small_integers=False extraction argument for VIX eligibility; default numeric-grounding behavior is unchanged. Observation cutoff/date checks do not establish intraday availability or freshness. Original PR824 context tests remain byte-identical; integration controls are additive. Writer/SQL/scoring/collector and six tracing paths remain protected. This projection is source composition only, with no parked branch update or publication authority.

## Exact-current-head PR844 preservation

This integration starts from3d1f1565affd778a7046b14ce7f37e14b529faa6/tree883a0de1dbd0d86769f96630dc1617a0f95fc605. Existing PR844 proxy labels/notes, score-only component rendering, flow-direction removal, guarded vocabulary and three regression bodies/helpers are preserved. Full rejected_candidate retention remains an existing internal snapshot field, assigned after the summary is built and bound to the actual original pre-context claim input. No prompt/response export or additional raw retention is introduced by receipt metadata.

All current tracing production files, including four router timeout additions, remain unchanged. Accepted tracing source is a read-only synthetic dependency projection only; this six-path change does not integrate its module/API into current main. An opt-in publication callback with unavailable tracing schema/router returns explicit unavailable/unknown provenance without inventing a logger ID, changing content, or retrying. Native/fullCI/provider/DB/SDK/export/durability and real publication are unrun. Exactly one new final child is allowed; frozen earlier results and failures are not replayed as current runtime proof.
