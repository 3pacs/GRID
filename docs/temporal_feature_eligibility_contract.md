# Pure temporal feature eligibility contract

This dormant module classifies caller-supplied immutable rows. It performs no
fetches, SQL, scoring, inference, clock reads or publication. `current` describes
temporal evidence only; it grants no trading, causal, numerical, authenticity or
live-readiness claim. `unknown` never becomes zero, a neutral score or a guessed
as-of timestamp. No leakage verdict is inferred from a future event.

The controller explicitly reserved these three new paths against commit
`e8d68da7ad4c71b8c308cb7e5f685ee0758e8ce5`, tree
`10aec2c8183de68b512f1a868f6920ed6ac9604c`. Existing `store/pit.py` remains the
canonical database PIT reader. Its date-level interface and the DB/default-SLA
freshness guard are not this supplied-row intraday API. Static inventory/source
search found those overlaps; the exact parent exception permits this offline
slice. Dispatcher, pre-create script, deployment and live smoke were not run.
Any database integration requires separate ownership and canonical PITStore.
Existing publication author paths, SQL, fixtures, receipts and original evidence
remain protected. The native original matrix was unavailable: the tests implement
the **34 controller-authored requirements**, without claiming to reproduce it.

## Inputs and public API

All DTOs are frozen dataclasses; enums are `Status`, `SourceKind` and `Missing`.
`FeatureKey(feature_id, source_id, observation_id)` identifies one observation
whose supplied revisions can be selected. Keys and row/source version identifiers
must be nonempty strings. Caller declares `SourceKind.OBSERVATION`,
`PRECOMPUTED_EPHEMERIS` or `DERIVED_SCORE`; arbitrary strings are unknown.

`Candidate` contains `row_id`, `key`, `source_version`, `kind`, `observation`,
`release`, `available_at`, `vintage`, `retracted_at`, `payload_json`,
`provenance_json`, optional `computed_at` and optional `lineage`.
Payload/provenance are opaque immutable original **bytes**; no parsing,
normalization, reserialization or hashing substitutes for provenance validation.
The module preserves their exact bytes, JSON types, order and row object identity.
The caller must supply valid original JSON if it wants JSON interpretation. DTO
fields are validated during evaluation; mutable payloads produce unknown. Objects
outside the Candidate API or mutable selection containers raise TypeError.

`TemporalPolicy` requires a nonempty `version`, matching `feature_id` and
`source_id`, explicit nonnegative `timedelta max_age`, declared `clock_roles`
`("observation", "release", "available_at", "vintage")`,
`retraction_semantics="knowledge_time"`, a literal Boolean
`permit_precomputed_ephemeris`, and `future_horizon` (None or a nonnegative exact
timedelta). Missing/unversioned/mismatched/invalid policy yields unknown. Boolean,
float, NaN, infinite, numeric seconds and strings are not durations. Explicit
zero is valid. No universal maximum, family SLA or wall-clock default exists.

`classify(row, *, cutoff, policy)` returns a `Classification` with the original
row and cutoff, status, reason, supplied policy version and component diagnostics.
Observation, release, arrival and vintage remain distinct timezone-aware datetime
roles. Date-only, naive and missing clocks are unknown. UTC elapsed time, including
DST folds, governs exact timedelta comparisons. No float rounding or date-level
end-of-day substitution decides boundaries. Release/arrival/vintage must each be
at or before cutoff. Equality passes; one microsecond after fails knowledge.
Clock ordering among the roles is not guessed.

`retracted_at=None` explicitly declares no known retraction.
`Missing.RETRACTION_KNOWLEDGE` declares absent knowledge, yielding unknown. Aware
retraction knowledge at or before cutoff excludes the row. Later retraction
knowledge is retained but does not rewrite the historical result. This is knowledge
time, not event time or the current retraction state of a production table.

Ordinary future observations are unknown. Otherwise, a knowledge-valid row is
current when age is at most max_age, stale when it exceeds that bound. A hash or
source version string does not prove actual availability or authenticate a source.

Precomputed ephemeris additionally requires explicit permission, a horizon, a
nonempty provenance byte record and computation known at cutoff. Its future event
within the inclusive horizon is `known_future`, never observed current and never
automatically leakage. Missing permission/provenance/horizon or a one-microsecond
horizon excess is unknown. Past ephemeris events follow ordinary freshness after
these requirements pass. Computational freshness is not silently substituted for
observation age; a caller needing it requires a separately declared policy.

## Derived score support

`Lineage(model_id, model_version, declared_component_ids, components)` declares
the complete direct support set. `Component(component_id, row, policy)` supplies
each leaf row and its applicable policy. IDs must be unique and cover exactly the
declared set. Model/source versions and nonempty score provenance, aware known
computation, and direct component identity are required. Every leaf must classify
current under its own policy/cutoff. Its release/arrival/vintage and, for a
precomputed ephemeris leaf, required computed_at must also be known by the score
computation instant. Equality passes; one microsecond afterward is unknown even
when that leaf is independently current at the later decision cutoff. This
consumer support bound adds no guessed ordering among component clocks and no
computation-clock requirement for ordinary observations. Stale, missing, future-known, retracted or
unknown support produces unknown score diagnostics. A score's own stale age is
still stale after support passes. Nested scores are explicitly unknown in this
bounded direct-leaf API; no implicit recursive graph or second policy engine is
introduced. The module verifies declarations, not model validity, causal lineage,
training correctness or score arithmetic.

## Selection and preservation

`SelectionPolicy(version, mode)` must explicitly specify a versioned
`mode="LATEST_AS_OF"`. `select_latest_as_of(rows, *, key, cutoff, policy,
selection_policy)` accepts an immutable tuple for exactly one FeatureKey. It
classifies **every** row before choosing the maximum vintage among current rows.
A later unreleased, unavailable, future-vintage, stale or retracted revision cannot
hide an earlier current one. Feature/observation/source identity never shifts.
Known observation instants must agree across revisions of that identity;
conflicting instants yield unknown with all original rows retained. Separate
future events require separate observation keys and selection calls.
Missing selection policy, foreign keys, duplicate row IDs (even identical copies)
or multiple current rows tied at the latest vintage yield unknown; no arbitrary
tie breaker or Python bool/number equality resolves conflicts.

`SelectionResult` contains key/status/reason, current candidate or None,
`known_future` candidates in a separate deterministic lane, complete diagnostics,
original history tuple, cutoff and selection-policy version. Future candidates
are never used as current fallback. Diagnostics sort by row ID; the future lane
sorts by UTC event/vintage/row ID. Input permutations do not alter selected identity
or canonical diagnostics for valid unique IDs; history always retains original
input order. Duplicate-ID diagnostics retain all entries, without claiming an
order for indistinguishable invalid identities. Losing/retracted rows survive.

## Verification and limits

The standalone stdlib unittest file contains literal synthetic C01-C34 controls,
microsecond/offset/fold/overnight and long-duration boundaries, complete/missing
score support, permutation/fallback/conflict and type-sensitive byte preservation.
Author verification runs existing actual CPython 3.11 in UTC/New_York/Tokyo under
an inspected effect fence. Actual-source corruptions must cause functional
assertion failures, not collection/import errors. Raw development failures and
negative evidence remain in the separate delivery receipts. Exact counts and
passed/failed/unrun checks are reported there, not inferred from this contract.

Full repository CI, integration, DB/provider/live tests, publication, scoring,
runtime activation, merge/push/deploy and trading are unrun and outside this
slice. Author PASS is not independent acceptance; a fresh independent reviewer
follows the frozen exact-base patch.
