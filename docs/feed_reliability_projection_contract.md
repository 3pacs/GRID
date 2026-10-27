# Synthetic SPY/QQQ feed quality contract

Base commit: `e8d68da7ad4c71b8c308cb7e5f685ee0758e8ce5`; tree:
`10aec2c8183de68b512f1a868f6920ed6ac9604c`. This dormant, standard-library
module evaluates caller-supplied typed facts for `TEST:SPY` and `TEST:QQQ`.
It performs no collection, raw parsing, price math, calendar lookup, journal
write, scoring, strategy, execution or application wiring.

## Public API

```python
from analysis.feed_reliability_projection import evaluate_feed_quality

result = evaluate_feed_quality(
    observations, policy, now=evaluation_clock,
    decision_at=original_decision_clock, sessions=session_facts,
)
```

`observations` and `sessions` are immutable tuples of `Observation` and
`SessionFacts`. `policy` is an `EvaluationPolicy`. Both injected clocks are
mandatory aware `datetime` objects; decision cannot follow evaluation.
Datetime instances use standard-library `timezone` or `ZoneInfo` tzinfo.
Comparisons and ages normalize to UTC, including ambiguous DST folds. There
is no string/epoch/nanosecond parser, ambient clock, environment lookup or
production freshness default. Numeric policy values are explicit research
inputs, never new GRID operational thresholds.

All input/result dataclasses are frozen. Provenance maps are copied into
read-only mapping proxies; changing a caller dictionary cannot alter a frozen
policy. IDs are bounded ASCII tokens (letters, digits, `:._-/`); hash references
are nullable, lowercase 64-character SHA256 strings. Constructors reject
malformed types with fixed reason-code `ValueError`s, without interpolating
inputs or exception causes. Missing nullable facts remain `None`; malformed
facts are distinct from unknown facts. Boolean numeric substitution,
NaN/infinity, negative durations and naive datetimes reject. No raw payload
field is accepted, transformed, retained or logged.

| Type | Fields and role |
| --- | --- |
| `SourceIdentity` | Instrument, source, tool, field, adapter version. Instrument is synthetic and mandatory; the remaining identities may be unknown. |
| `FactReference` | Evidence ID, content hash, available and recorded clocks for an ancillary fact. Every nullable item remains independently unknown. A reference does not authenticate its contents. |
| `Observation` | Identity, raw-evidence ID/hash, nullable event, receipt, as-of, available and recorded clocks; `Presence`; scoped `DelayEvidence`; interval, `Finality`, revision ID and original reference; coverage fraction, continuity; lifecycle/options facts; explicit fallback flag and fallback provenance. It contains no price or raw payload. |
| `DelayEvidence` | Numeric seconds or unknown, exact `SourceIdentity` scope, and its own fact reference. Scope includes instrument and adapter version as well as source/tool/field. |
| `LifecycleFacts` | Nullable connected status, observation generation, current generation and provenance known at decision. A reconnect must explicitly supply matching generations; elapsed time cannot repair them. |
| `OptionsFacts` | Separate OI vintage timestamp, expiry/settlement verification, Greeks verification, and provenance. No exchange timestamp or OI vintage is inferred from retrieval. |
| `SessionFacts` | Synthetic instrument, supplied state, and provenance. Arbitrary bounded states can describe DST, holiday or early close; only explicit policy allows them. No date or finality inference follows. |
| `CapabilityPolicy` | Name, expected source identity, exact evidence ID and expected content hash, `Mode`, explicit required `Check` tuple, nullable maximum event/receipt age, future skew, delay, OI age, required interval, minimum coverage, and allowed session states. |
| `EvaluationPolicy` | Nullable version, provenance map, nonempty tuple of distinct instrument/name capabilities, optional frozen publication provenance map. |
| `EvaluationResult` | Policy version, normalized evaluation and original decision clocks, and fixed `TEST:SPY`/`TEST:QQQ` result rows. |
| `InstrumentResult` | All supplied descriptive contexts in original order and all configured capability results for this instrument. An empty capability tuple means none was configured; it casts no vote. |
| `DescriptiveContext` | Original immutable observation, visibility and knowledge state, scoped delay label, independent event/receipt ages at both evaluation and original decision. Retained future or missing-knowledge facts stay inspectable but are not visible as available context. |
| `CapabilityResult` | Name/mode, selected evidence ID/hash, aggregate state, eligibility boolean and complete ordered `CheckResult` tuple. |
| `CheckResult` | Check enum, `pass`/`fail`/`unknown`/`not_required`, and a bounded reason code. |

## Checks, selection and clocks

The caller selects one evidence ID per capability and pins its expected hash
and source identity. The evaluator does not choose the last arrival, newest
event or highest revision. Missing selection is unknown. More than one record
under a selected ID is an explicit ambiguity failure, including identical
retries; adapters must provide distinct observation/receipt identities when
retaining retries. All supplied observations, hashes and revision references
remain in context in input order. A retry with a newer receipt and the same
old event stays economically old. This evaluator has no evidence registry or
cross-call history and cannot detect a caller who rewrites both facts and pins.

Every capability requires policy configuration, presence, source identity,
evidence ID/hash, original decision cutoff, clock order and explicit fallback
status/provenance. Code and config SHA256 references plus a nonempty policy
version are mandatory. A null or empty required-check tuple is unconfigured,
not a vacuously passing policy. Missing source identity/pins/facts is unknown;
supplied mismatches, unavailable observations or contradictions fail.

| Additional check | Explicit rule |
| --- | --- |
| Event/receipt age | Age uses its own clock, never a substitute. Exact elapsed duration equals maximum passes; one microsecond beyond fails across the admitted datetime range. Future age within supplied skew can pass the age check, but never relaxes the original decision cutoff. Missing maximum or skew is unknown. |
| Delay | Requires scoped numeric declaration, exact identity and fact provenance known by decision. Equality passes the maximum; unknown stays null. Age is not reduced by delay. |
| Finality | Only explicit `FINAL` passes. `PROVISIONAL` and `INCOMPLETE` fail, `UNKNOWN` is unknown. Interval completion, elapsed time, session close or revision labels cannot prove finality. |
| Interval/coverage/continuity | Exact required interval, minimum supplied coverage and explicitly true continuity respectively. Each missing dependent policy or fact is unknown. |
| Lifecycle | Explicitly connected, both supplied generations known and equal, and fact provenance known by decision. Disconnection or retained generation fails. |
| Session | Supplied state must be in a nonempty allowed-state tuple, with reference known by decision. It never changes event age or provider finality. |
| OI vintage | Explicit vintage must be known by decision and cannot follow its fact availability; supplied age/skew policy applies independently. |
| Expiry/settlement and Greeks | Require their separately supplied true verification facts and provenance. Missing values are unknown. |
| Publication pins | Both provenance maps must contain the four literal `code_sha256`, `config_sha256`, `calendar_sha256`, `outcome_definition_sha256` keys with non-null valid hashes and must match. Matching incomplete maps never pass. This is a quality dependency, not a journal/publication verifier. |
| 0DTE hold | Always fails when required, and is mandatory for `ZERO_DTE` mode. |

`RESEARCH` ages use the frozen original decision. This can describe historical
quality using disconnected retained evidence when lifecycle is not required;
its result makes no claim of current feed readiness. `CURRENT` ages use the
injected evaluation clock and additionally requires event age, receipt age,
delay and lifecycle, even when omitted from the caller check list. Re-evaluation
with later injected now can make current quality stale. `ZERO_DTE` stays held.

Receipt, availability and recorded clocks must exist and satisfy
`available <= receipt <= recorded`; all known event/as-of/knowledge clocks must
be no later than the original decision. Evaluation cannot admit a later revision
retroactively. When both event and as-of exist, event cannot follow as-of.
Source event/as-of ahead of availability requires explicit skew tolerance;
unknown tolerance produces unknown and exceeded tolerance fails. Non-event
knowledge ordering is never relaxed. Ancillary delay/lifecycle/options/session/
fallback references independently enforce available/recorded order and cutoff.

Eligibility comparisons preserve signed integer microseconds from timedelta
components. Integer policy seconds are exact; finite float policy seconds are
compared as their exact stored binary rational values. This introduces no
epsilon, duration/range clamp, new threshold or policy-to-timedelta conversion.
For example, an exactly representable `0.5` second threshold equals 500000
microseconds. A float spelling a nonrepresentable decimal can lie just above or
below that decimal; its stored value determines the comparison. Arbitrarily
large admitted numeric policy durations do not cause timedelta overflow.

Descriptive age fields remain float seconds for API compatibility. Their display
precision is approximate: at very large spans they can round away microseconds.
These reporting fields never determine event/receipt/OI-vintage age or source
clock-skew eligibility. Original decision cutoffs remain aware datetime
comparisons. No public API, fact types or source ownership changes in this repair.

Non-required checks are reported `not_required` and do not veto the capability.
Any required failure makes aggregate state `fail`; otherwise any required
unknown makes it `unknown`; only all required passes make it eligible. This
permits underlying price context when optional OI/expiry/Greeks are missing.
Instrument absence produces unknown for its configured capabilities and cannot
become a neutral or successful global vote. There is no combined vote.

Descriptive visibility requires supplied presence and known clocks no later
than evaluation; it does not assert economic freshness, identity validity,
clock consistency, finality, live connectivity or decision eligibility. All
those facts and capability checks remain available for inspection. Positive
declared delay is labeled `declared_delayed`; zero is `declared_zero_delay`,
which still does not mean live. Missing delay, wrong scope or unknown proof
receives an explicit unknown/unverified label.

## Source facts and retained evidence

TradingView screeners report 900-second delay in the supplied planning evidence;
the synthetic tests retain that exact screener/tool/field declaration. The module
has no provider defaults. TradingView OHLCV delay and finality remain unknown
without explicit evidence. ToS feed health and GEX exchange event timestamp/OI
vintage remain unverified. Successful retrieval supplies receipt evidence only.

The accepted ETF V2 fixture/test/doc and review are read-only semantic references:
patch SHA256 `bc6361fdd851f5e244146f5ef2ec9546fce7067620061d6e449a50a2bfe5a945`,
review seal `9452d54e080c7265329cd41152f59194482f75c0f3fb30154290c1e8caa59af0`,
fixture `97fb18b2cd3a4e966811adb0e8a9b8cd665faf423c66bb9088bd42b373491476`.
This module reuses their PIT/provenance/type/unknown semantics without importing
their test-local verifier or depending on unmerged accepted paths. Existing GRID
SQL/day freshness, cadence/calendar and accepted GEX clock code stay untouched.

## Reproduction and limits

From a repository root or this isolated three-addition export:

```sh
/home/anik/.local/share/uv/python/cpython-3.11.15-linux-x86_64-gnu/bin/python3.11 -I -B tests/test_feed_reliability_projection.py
```

Tests load only this module by file path and use literal synthetic expected
states, boundaries, aware offset/DST controls, missing provenance, malformed
maps, numeric/Boolean substitutions, input immutability and deterministic clocks.
Author receipts separately pin the actual interpreter, source/test hashes,
commands, method/control counts, outcomes and failures. The external fenced
runner rejects network/process operations, file writes and protected runtime/
database imports during module import and tests. Synthetic audit self-controls
prove the fence is active. Static AST controls check the import surface and
absence of ambient clock calls. These are bounded Python/static checks, not
arbitrary native-code containment.

Four actual source mutants must produce assertion failures with zero test
errors: event-to-receipt substitution, ignoring post-cutoff revision availability,
global optional-OI veto and disconnected-clock promotion. Mutation failures are
expected negative controls, not independent acceptance. Source freeze is content
hash sealing, without permission changes. No source-side writes or logging occur.

The V2 F01 repair retains all prior controls and adds exact/equal/one-microsecond
under/over boundaries for large spans, negative future skew and source skew,
fractional/zero/huge admitted policies, timezone equivalence and DST. Its author
receipts reproduce the original four false PASS cases before repair and replay
the unchanged reviewer controls after repair. Actual precision-recurrence mutants
must also fail assertions. The sealed V1 REVISE evidence remains unchanged.

Quality PASS does not authenticate opaque hashes or caller status, establish
actual feed health, prove payload completeness or market accuracy, admit journal
publication or trades, or lift any producer/reviewer/runtime hold. Full repository
CI, live providers, DB, runtime, trading and fresh independent source acceptance
are separate UNRUN gates. The held price-feature lane must await independent
acceptance and the exact frozen API/source pin.
