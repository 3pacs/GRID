# Synthetic numeric bar and session contract

Current admitted continuation: this artifact now adds the three exact OR/VWAP
functions described in "Opening-range and VWAP continuation" below. The original
structural-stage scope text is retained as historical context for the unchanged
13 accepted definitions; its price-math hold is superseded only for these three
explicitly assigned functions. All runtime, provider, scoring, trading and
release holds remain active.

This stage adds immutable values and structural checks for `TEST:SPY` and
`TEST:QQQ` at base `e8d68da7ad4c71b8c308cb7e5f685ee0758e8ce5`, tree
`10aec2c8183de68b512f1a868f6920ed6ac9604c`. All four price paths are new at
that tree. The dormant module has no production parser, collector, calendar
lookup, persistence, signal, admission engine or feature calculation.

The accepted quality dependency remains byte identical and outside this patch:
API receipt SHA256 `0dde6cf1146b3660cc6da4da1efb5f499227d0a225a86301dfe954b2879efeeb`,
source SHA256 `9c723762766cdb92a66107169a3036090f7409a29b4e8b6401f33869022f6da1`.
The candidate imports its public `Observation`, `FactReference` and `State`.
Source identities, event/receipt/as-of/availability/recorded clocks, revisions,
finality, generation, delay and lifecycle stay exclusively in the exact supplied
`Observation`. It is retained by object identity. Ancillary references also stay
the supplied accepted `FactReference` objects. No quality enums or policies are
copied or extended.

## Exports and construction

All fields below are explicit constructor arguments, with no defaults. All
dataclasses are frozen; collections are immutable tuples. These are typed
runtime inputs. JSON decimal strings are converted only by synthetic tests.
Malformed types and contradictory values raise fixed reason-code `ValueError`s;
unknown nullable facts remain `None`. No floats, Boolean numeric substitution,
subclass coercion, environment settings or global Decimal changes are used.

| Export | Exact fields / values |
| --- | --- |
| `Adjustment` | `RAW`, `SPLIT`, `TOTAL_RETURN`, `UNKNOWN` |
| `NotionalBasis` | `TRADE`, `OHLCV_PROXY`, `UNKNOWN` |
| `TimestampConvention` | `START`, `END`, `UNKNOWN` |
| `SessionConvention` | `REGULAR`, `EXTENDED`, `AUCTION`, `UNKNOWN` |
| `BarInterval` | `start: datetime`, `end: datetime` |
| `PriceBasis` | `price_currency: str \| None`, `volume_unit: str \| None`, `notional_unit: str \| None`, `adjustment: Adjustment`, `basis_id: str \| None`, `basis_version: str \| None`, `adjustment_reference: FactReference \| None`, `notional_basis: NotionalBasis` |
| `SessionWindow` | `instrument: str`, `exchange: str \| None`, `session_id: str \| None`, `timezone_id: str \| None`, `open_at: datetime`, `close_at: datetime`, `calendar_id: str \| None`, `calendar_version: str \| None`, `calendar_reference: FactReference \| None`, `convention: SessionConvention`, `segments: tuple[BarInterval, ...] \| None` |
| `NumericBar` | `observation: Observation`, `interval_start: datetime`, `interval_end: datetime`, `timestamp_convention: TimestampConvention`, `session: SessionWindow`, `basis: PriceBasis`, `open: Decimal \| None`, `high: Decimal \| None`, `low: Decimal \| None`, `close: Decimal \| None`, `volume: int \| None`, `trade_notional: Decimal \| None`, `notional_reference: FactReference \| None` |
| `ExpectedBarGrid` | `interval_start: datetime`, `interval_end: datetime`, `session: SessionWindow`, `basis: PriceBasis`, `intervals: tuple[BarInterval, ...] \| None`, `reference: FactReference \| None` |
| `StructuralResult` | `state: State`, `reasons: tuple[str, ...]` |

The three exported functions have these exact signatures:

```python
compare_price_basis(left: PriceBasis, right: PriceBasis) -> StructuralResult
validate_numeric_bar(bar: NumericBar) -> StructuralResult
validate_bar_grid(bars: tuple[NumericBar, ...], grid: ExpectedBarGrid) -> StructuralResult
```

There is no `eligible` or `current` field. Constructor success establishes
well-formed typed facts; it does not establish structural completeness.

## Numeric and basis semantics

Prices are strictly finite positive `Decimal` values. Known low/high bounds
must contain each known open/close, including partial observations. Volume is
strictly nonnegative `int`, with Boolean excluded. Missing prices, volume and
notional remain distinct from reported zero. Reported zero volume is valid
structure and proves no usable denominator.

Notional is a strictly finite `Decimal` retaining its full sign, exponent and
digits. This contract imposes no economic notional-sign or price-times-volume
rule: separately supplied evidence is opaque, and a later reviewed consumer
must validate economic compatibility. No notional is inferred from OHLC,
OHLCV, HLC3 or price times volume. `NotionalBasis` explicitly distinguishes
caller-declared trade evidence from a supplied OHLCV approximation. A proxy
label performs no calculation and proves no true trade notional.

Currency, units, adjustment kind, basis identifier/version and notional basis
are explicit. They are never inferred from an instrument or provider. Unknown
fields prevent a compatible PASS even when both sides are unknown. Known
differences fail with `incompatible_<field>`. Comparison includes every field of
the adjustment reference, including both original clock representations and
UTC instants. Missing reference components are unknown; differing complete
references fail. Hashes are provenance labels, not authentication.

## Session and interval semantics

All intervals are positive half-open `[start, end)` aware datetime intervals,
using standard-library `timezone` or `ZoneInfo`. Ordering normalizes to UTC and
does not use floating durations. Original timezone/offset/fold, clocks and text
remain unchanged. Distinct same-zone DST folds are distinct instants. Datetimes
whose UTC conversion overflows are malformed and reject without clamping.

The timezone label is retained caller provenance; it does not validate upstream
localization. Calendar IDs, versions and their complete reference, exchange,
session ID, timezone label, convention and allowed segments are required for
known session structure. No holiday, early close, overnight session, regular
09:30 open or auction inclusion is inferred. The caller supplies the exact
calendar convention and permitted segments. Segments must be ordered, positive,
nonoverlapping and inside the outer window. Gaps denote breaks. `None` denotes
unknown segments; `()` explicitly declares no permitted trading.

For `START`/`END`, a known observation event instant must equal the respective
supplied interval boundary. A differing instant fails. An unknown event or
timestamp convention remains unknown. Receipt and other clocks are neither
substitutes nor rewritten. Exact session close is allowed as the final interval
end, without proving provider finality.

The direct bar must also bind its positive UTC interval duration to the same
`Observation.interval_seconds`. `None` remains unknown and reports
`unknown_observation_duration`. A known mismatch reports
`observation_duration_mismatch` and fails both numeric and whole grid/reference
structure. A shorter observation cannot silently describe an aggregated bar;
aggregation would require a separately reviewed mapping/provenance contract.
No duration or other observation metadata is overwritten or inferred.

Duration comparison uses exact timedelta days/seconds/microseconds to form an
integer microsecond count. Integer declared seconds are exact. Float seconds
are compared by their exact stored binary rational numerator/denominator,
using integer cross multiplication. There is no floating `total_seconds`,
epsilon, rounding, clamp or float conversion of the elapsed interval. For
example, 60 and 60.0 exactly match 60 seconds; 0.5 exactly matches 500000
microseconds. The float spelling `0.000001` does not exactly equal one
microsecond, so geometrically complete one-microsecond bars with that declaration
FAIL this direct binding. The unchanged accepted int/float metadata API cannot
exactly represent every datetime microsecond duration; this limitation is
explicit rather than repaired through a guessed decimal interpretation.

## Structural results

`validate_numeric_bar` describes a bar's instrument, segment membership,
timestamp convention, declared session/basis and numeric/notional presence.
Any missing required numeric or declared context produces UNKNOWN; known
contradictions produce FAIL. A complete declared bar can produce PASS with
provisional, delayed, disconnected or future-known quality metadata. No temporal
rules are evaluated here.

`validate_bar_grid` separately describes **coverage of the supplied expected
intervals and declared reference compatibility**. Numeric/notional presence and
provider quality do not determine its coverage result. The analysis window must
fit the outer session, with start/end in supplied permitted segments when
trading segments exist. The window may span an explicitly supplied break.
Expected intervals must exactly tile the permitted portions of that window,
without crossing breaks, overlap, duplicate intervals or ordering repairs.
Observed intervals must match the supplied expected intervals exactly.
The whole result also requires each attached observation duration to match its
own bar geometry. A duration UNKNOWN/FAIL remains UNKNOWN/FAIL even when the
expected geometry tiles correctly. Unrelated missing prices/notional and
quality eligibility remain independent of grid coverage and binding.

A missing bar fails `missing_expected_interval`; an extra bar fails
`unexpected_observed_interval`. Duplicate, overlapping and reversed input
intervals report `observed_*` or `expected_*` reasons. Missing expected tiles
fail `expected_grid_gap`; leftovers fail `expected_grid_not_segment_tiling`.
Duplicate evidence IDs and per-source revision IDs fail independently.
Different revisions/content hashes/source identities for the same interval
also report `conflicting_interval_revision`. Every original bar and correction
remains in caller order. No sorting, deduplication, latest revision selection,
forward fill, merging or repair occurs.

Unknown grid, grid origin, session convention, basis, timestamp/event or
observation identifiers keep completeness unknown. Explicit empty expected
coverage with no trading segments and no bars can PASS; an empty grid over
permitted trading fails. A caller-supplied sparse or malformed expected grid is
never presumed complete by an implicit minute frequency. Matching declared
metadata has no external calendar or market-truth authority.

Results retain all unique reasons in deterministic first-seen order, with
failures first, followed by unknowns. Any failure takes precedence; otherwise
any unknown yields UNKNOWN; otherwise PASS with `structural_match`.

## Point-in-time and future consumers

Calendar, adjustment, grid and notional references retain independent known-at
clocks. These structural checks inspect reference **presence**, and comparison
inspects the complete declared identity; they do not adjudicate cutoff, clock
order or authenticity. `evaluate_feed_quality` does not automatically inspect
these arbitrary nested numeric references. A later consumer needs an explicit
reviewed binding for each required ancillary reference. Until that exists,
economic and temporal usability of those references remains unqualified.

Tests call the exact accepted public quality evaluator with injected policy,
evaluation and decision clocks to demonstrate quality FAIL/UNKNOWN alongside
numeric structure PASS. There is no ambient clock, elapsed-time finality or
late-correction promotion. Original observations and losing revisions stay
intact; structural validation does not retroactively select usable evidence.

Opening ranges, exact/proxy VWAP, RVOL, multi-ETF breadth, setup signals and live
integration remain held for a separately reserved stage after independent
contract acceptance. The retained original P01 highs/lows and P02 volumes and
supplied notionals are fixture facts only; there are no calculated outputs.

## Verification and limits

Run the new tests without repository conftest or runtime entrypoints:

```sh
/home/anik/.local/share/uv/python/cpython-3.11.15-linux-x86_64-gnu/bin/python3.11 -I -B tests/test_etf_price_features.py
```

The author delivery also supplies the approved external audit fence, synthetic
fence self-controls, complete commands/results and actual-source corruption
controls. Tests use independent exact expected states/values, complete
type-sensitive recursive preservation, positive/negative boundaries, both DST
folds, extreme finite Decimals and datetime ranges, and strict public dependency
imports. Candidate calls are fenced against network, processes, writes,
environment mutations and protected production imports. Static AST checks
constrain the import surface, ambient clock/parsing/coercion calls and feature
arithmetic. Multiplication is permitted only for the five explicitly reviewed
integer duration-unit/cross-product expressions in `_observation_duration`;
price multiplication, division and power remain excluded. These are bounded
Python checks, not native-code containment.

F01 repair evidence retains the original sealed eight false PASS assertions,
then replays the unchanged two reviewer F01 methods against the replacement.
New checks cover valid integer/float equality, missing and contradictory
declarations, exact versus inexact binary fractions, one-microsecond boundaries,
full-range integer spans, DST folds and offset equivalence, unchanged metadata
and separate geometry/numeric/quality states. V1 source/tests/review and all
negative evidence remain preserved. The public exports and signatures are
unchanged, as is the accepted quality dependency.

The exact-base four-addition patch excludes the three accepted quality files
and the unchanged base `analysis/__init__.py`. Author receipts seal source,
fixture, tests, API projection, dependency bytes, patch replay and all development
or negative evidence. Source PASS is separate from fresh independent review.
Full repository CI, DB, live providers, runtime integration, trading, publication,
merge and deployment are UNRUN/held by this delivery.

## Opening-range and VWAP continuation

Reservation SHA256:
`40ee4059c480fb0384956a2d0e04a3069c2688903565fa8a7f49112b8eeee817`.
Direct approval reconciliation SHA256:
`ddb5e8e2c0d3240435d7199bac4f133c93b6dea2b5cd0564e8f14dee562a3d60`.
Accepted numeric API receipt:
`e66187c258f136a58e0b7c11079ccd08fc01adbca94e5c7cce0f7458dcd3d234`.
The original 13 public definitions, every existing constructor, private helper
and F01 validator definition retain identical ASTs. Original source artifacts
and fixture keys/rows remain unchanged in their sealed destinations. New named
`math_cases` are additive. Existing Alpha101/calendar/quality/scorer modules are
not changed or imported.

Five new exports are appended to the existing exports:

```python
class PriceFeatureMethod(str, Enum):
    OPENING_RANGE = "opening_range"
    TRADE_NOTIONAL_VOLUME_VWAP = "trade_notional_volume_vwap"
    OHLCV_HLC3_VOLUME_PROXY = "ohlcv_hlc3_volume_proxy"

opening_range(
    bars: tuple[NumericBar, ...], grid: ExpectedBarGrid, *, minutes: int,
    cutoff: datetime, quality_policy: EvaluationPolicy, evaluation_at: datetime,
    decision_at: datetime, session_facts: tuple[SessionFacts, ...],
) -> PriceFeatureResult

trade_vwap(
    bars: tuple[NumericBar, ...], grid: ExpectedBarGrid, *, cutoff: datetime,
    quality_policy: EvaluationPolicy, evaluation_at: datetime, decision_at: datetime,
    session_facts: tuple[SessionFacts, ...],
) -> PriceFeatureResult

ohlcv_hlc3_volume_proxy(
    bars: tuple[NumericBar, ...], grid: ExpectedBarGrid, *, cutoff: datetime,
    quality_policy: EvaluationPolicy, evaluation_at: datetime, decision_at: datetime,
    session_facts: tuple[SessionFacts, ...],
) -> PriceFeatureResult
```

All arguments are supplied explicitly. `minutes` must be strict integer 5, 15
or 30, excluding Boolean. There is no symbol/session/calendar/threshold default.
Opening range starts at the supplied session open and requires exactly the
selected number of elapsed UTC minutes. Supplied grid/session segments control
breaks, auction inclusion, DST, overnight and early-close behavior. The range
uses known high/low only; missing open/close, volume, trade notional or its
unneeded reference do not prevent the range.

`PriceFeatureResult` is frozen and contains these explicit fields, in order:
`method`, `inputs`, `grid`, `cutoff`, `quality_policy`, `evaluation_at`,
`decision_at`, `session_facts`, `opening_minutes`, `numeric`, `coverage`, `basis`,
`temporal`, `high`, `low`, `vwap`. The first fields retain the exact original
input tuples/objects and caller clocks/policy. `opening_minutes` is the supplied
OR selection, otherwise None. The four checks are accepted `StructuralResult`s
using accepted `State` and stable reasons. `high`/`low` are original Decimal
values for a qualified range, otherwise None. `vwap` is exact `Fraction` for a
qualified declared trade ratio or explicitly labeled proxy, otherwise None.
There is no current/eligible/signal/side/order field. Constructing a typed result
is not authentication or authorization.

A feature value is returned only when **all four checks PASS**. Separate checks
retain descriptive numeric validity when quality is FAIL/UNKNOWN; no failed
check is silently promoted to a qualified mathematical output. Delayed final
research evidence may pass an explicit research policy with its declared delay
allowed. The same evidence under a current policy may fail, without changing
its numeric state. No trade or live-readiness claim follows from any PASS.

| Check | Admitted rule |
| --- | --- |
| Numeric | Range needs high/low. Trade VWAP needs supplied volume/notional. Proxy needs high/low/close/volume. Missing fields are UNKNOWN. Negative economic notional, zero volume with nonzero notional and a known all-zero denominator FAIL. Reported zero remains distinct from missing. |
| Coverage | Exact accepted `validate_bar_grid`; no changes to F01 duration, expected-grid, duplicate, ordering, overlap, conflicting-revision or session semantics. OR additionally checks supplied session-open anchor and exact UTC window duration. |
| Basis | Exact accepted `compare_price_basis` for every selected basis and grid basis. Trade requires declared TRADE; proxy requires OHLCV_PROXY. Both require explicit shares and equal price-currency/notional-currency tokens. Unknown stays UNKNOWN and incompatible fields FAIL. No conversion or adjustment guess. |
| Temporal | Explicit original decision equals cutoff; window end cannot follow cutoff; selected observation quality comes from unchanged public `evaluate_feed_quality`; each used bar requires a matching evidence-selected policy capability and an explicitly required FINALITY check. Arbitrary unselected capabilities cast no vote. Ancillary references have the independent cutoff binding below. |

The caller supplies already selected immutable revisions. There is no history
registry, sorting, deduplication, latest-vintage selection, repair, forward fill
or aggregation mapping. All originals, losing/conflicting and late selected
inputs remain inspectable in order. An unavailable-at-cutoff correction cannot
produce a qualified earlier feature. A conflicting selection fails the unchanged
structural validator, rather than selecting a winner.

The public quality evaluator receives the exact selected Observations, exact
caller policy, aware evaluation/decision clocks and session facts. Its existing
mandatory evidence/identity/policy/clock-order/cutoff checks are not copied or
reimplemented. Selected capability identity/hash mismatches remain FAIL; missing
configuration remains UNKNOWN. Its required FINALITY check provides the explicit
finality result; omitted finality is UNKNOWN in this closed-window composition.
Calendar time or reaching window close never establishes finality.

This continuation explicitly binds calendar, expected-grid and adjustment
FactReferences, plus notional references used by trade VWAP. Every needed
evidence ID/hash/available/recorded clock must be present. Both clocks must be
no later than cutoff, and availability cannot follow recorded time. Equality at
cutoff passes; one microsecond beyond fails. Each original clock/hash survives
unchanged. No event/receipt clock substitutes for independent ancillary
availability. Proxy does not use or qualify an unrelated trade-notional
reference. Opaque references are not authenticated economic facts, and actual
provider-to-typed-input mapping remains outside this stage.

Trade VWAP is the exact sum of supplied TRADE notionals divided by the exact
integer sum of supplied shares. It never invents notional from OHLC, OHLCV,
HLC3 or price times volume. Positive-volume reported zero notional is retained
as a declared zero; its economic truth is not authenticated here. The proxy is
separate: sum((high + low + close)/3 * volume) / sum(volume), using exact rational
conversion of each finite stored Decimal. The proxy is always
`OHLCV_HLC3_VOLUME_PROXY`, and a trade/proxy basis mixture refuses. It is not
represented as true trade VWAP.

Decimal values convert directly to exact rational stored values via `Fraction`.
All averaging, notional accumulation and division then use rational arithmetic.
A nonterminating result remains a Fraction, such as 1/3, with no implicit Decimal
rounding. OR extrema preserve full Decimal digits/exponent. No ambient Decimal
context/precision/traps/rounding change occurs. Arithmetic resource use scales
with supplied digits/exponents and integer sizes; this slice introduces no
resource clamp or hidden bound. Any future bound requires reviewed caller policy.

New synthetic tests retain all accepted F01/value/immutability controls and add
independent literal rational/state oracles, declared 104/98 range and 23/2 VWAP,
nonterminating ratios, exact large quantities under Decimal traps, every source
and ancillary cutoff boundary, finality omission/failure, independent numeric
needs, method/unit/basis refusal, zero/unknown/economic sign cases, caller-selected
late/conflicting revisions, UTC/DST/break/auction/early-close cases, and old AST
regressions. Actual-source mathematical corruption controls must fail assertions
with zero runtime/collection errors. The approved offline fence applies during
candidate import and tests. New outputs are source/synthetic proof only; a fresh
independent mathematical reviewer is required after sealing. Full repository CI,
DB, providers, live runtime, scoring, orders/trading and all publication/release
actions remain UNRUN/held.
