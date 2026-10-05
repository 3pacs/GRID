# Publication evidence boundaries

This repair applies to future publications. It does not edit archived briefings,
thesis snapshots, scoring outcomes, journals, calibration, provider data or
released evaluations.

## Liquidity units

`WALCL` and `WTREGEN` are millions USD; `RRPONTSYD` is billions USD and is
scaled by the existing factor of 1,000 before subtraction. The result and its
30-day change remain millions USD. `format_liquidity_usd` requires explicit
input units and uses an explicit display unit. Missing/unknown units, missing
values, booleans, strings and nonfinite values render unavailable. Zero remains
zero. For example, 24,262 million USD renders `$24,262M` or `$24.262B`.

Thesis strings previously labeled the million-unit result as billions. Current
and change strings now use the shared format boundary. Existing comparisons
against 50/100 also operate on millions: their published threshold description
now says $50M/$100M. Arithmetic, confidence, directions, scores and comparison
bands remain unchanged. This is no validation or recalibration of those bands.

## Inferred relationships versus source-reported transfers

The current money-flow engine estimates every edge from endpoint pool values,
changes and structural weights. Confirmed endpoint data does not turn an edge
into measured money movement. Audio collection explicitly labels correlation
and structural estimates. Their modeled dollar magnitudes remain in the
structured result, but are omitted from the writer's transfer-amount context.
Layer changes are labeled stock changes. Missing changes are distinguished from
zero. Unstructured flow narrative is excluded from that context.

Unknown sources cannot support transfer claims. A future source-reported flow
requires a recognized transactional source type, explicit USD units, finite
nonnegative magnitude, direction, timezone-aware ordered interval, source and
receipt identifiers, and conservation/dedup receipt identifiers with declared
passed checks. Correlation channels always stay proxies even if tagged transactional. These declarations
are a provenance contract, not independent provider truth or receipt-hash
verification. No current inferred producer is promoted into this category.
Capture receipt verification remains the capture-adapter owner's responsibility.

## Publication gate and impact

`check_publication_claims` extends the existing numeric-grounding boundary with a
bounded, conservative lexical check. It normalizes Unicode compatibility forms,
invisible formatting characters and common Markdown before detecting protected
options-intent, realized-volatility and money-transfer language. PCR alone does
not establish buyer/seller initiation, opening/closing, protection buying, hedge
intent or signed dealer gamma. VIX does not establish realized volatility.
Narrative text and broad boolean permission flags cannot supply that evidence.
Source-reported transfer statements need the source identifier, direction and
matching dollar amount in a narrow attribution clause. Multi-edge directions
are bound separately, and semicolons/conjunctions cannot grant permission to an
additional clause. Displayed-unit precision determines allowed rounding, rather
than a broad percentage tolerance. Fixed unmeasured/unavailable disclaimers are
removed before inspection; a disclaimer cannot exempt a neighboring claim.

Hourly/daily/weekly text candidates that fail are entirely replaced by the
existing deterministic snapshot summary before saving/persistence; a guard
receipt remains in the result and stored snapshot. Audio candidates from every
existing provider are checked outside the provider fallback block before any
public caller receives them. A failed candidate raises ValueError before
synthesis, metadata saving or publishing. Rejection does not trigger another
model request. Provider routing, opt-in gates and credentials are unchanged.

The deterministic fallback omits unstructured regime recommendation prose and
labels reported model confidence with calibration unverified. The original
recommendation remains in the audit snapshot. A rejected candidate's replacement
summary is inspected again: remaining protected claims fail before publication
file/DB writes, without another model request. Real-module integration tests
cover hourly/daily/weekly persistence and both unavailable-model and rejected
candidate paths, including a recommendation that previously reintroduced the
withheld hedging claim.

This gate is not a general natural-language verifier. It can conservatively
reject accurate negated explanations or unsupported claims expressed with
other wording can escape detection. It does not establish causal predictive
value, trend, calibrated confidence, action thresholds, PIT lineage or source
truth. Existing numeric grounding still annotates unsupported figures; its
annotation policy is unchanged. Schema-backed, claim-specific verification is a
separate improvement. Current source-reported-flow support does not validate
intervals mentioned in free prose; use the structured receipt interval.

## Verification

`python tests/test_publication_semantics.py` executes exact production functions
under stdlib AST isolation with fake data. It covers units, signs, zero, missing
and malformed input, source provenance, correlation overrides, narrative
injection, Unicode obfuscation, source/amount/direction mismatch, real text
publication fallback and audio rejection across all provider labels. The
standalone runner blocks network calls. Normal suite collection requires the
repository's pandas/loguru and other dependencies; an AST-isolated receipt is
not a full application/environment test or live-source validation.

Controller integration on verified mainc56823af additionally collected and ran126 targeted tests with the repository dependency environment available, including normal module-level market/audio persistence boundaries and the retained numeric/AST regressions. TCP/live SQL I/O was fenced; no attempts recorded.13 released entries and granular GEX bytes are unchanged. Scoring assignments and statement conditions remain AST-identical. This targeted receipt is not the complete application/target-runtime CI suite, which remains a release prerequisite.
