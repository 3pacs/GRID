# Synthetic market feature display contract

`analysis.market_feature_display.project_price_feature(result)` accepts exactly
`analysis.etf_price_features.PriceFeatureResult` and returns a fresh dict with
JSON-safe data. It reads supplied diagnostics and values; it does not calculate
OR/VWAP, choose revisions, query a feed, or evaluate another quality or temporal
policy. The accepted DTO restricts instruments to `TEST:SPY` and `TEST:QQQ`.

Schema `market-feature-display-v2` has these fixed top-level keys:

| Key | Meaning |
| --- | --- |
| `schema` | Literal schema identifier above |
| `method` | Exact upstream method enum value |
| `method_label` | Opening range, Trade-notional / volume VWAP, or HLC3-volume proxy (not trade VWAP) |
| `opening_minutes` | Supplied opening-window declaration, including null |
| `values` | `high`, `low`, `vwap`, with method and diagnostic masks below |
| `diagnostics` | Separate `numeric`, `coverage`, `basis`, `temporal` state/reasons objects |
| `clocks` | Separate supplied `decision_at`, `evaluation_at`, `cutoff` records |
| `provenance` | Declared reference, input, session, grid and quality-policy metadata |

Values appear only if **all four supplied diagnostics are exactly PASS**.
FAIL, UNKNOWN and NOT_REQUIRED all withhold every value, even when a directly
constructed or replaced DTO still contains populated values. Opening range
emits only `high`/`low`. Both VWAP methods emit only `vwap`. Irrelevant populated
fields never cross the method mask. Each missing or withheld value is JSON null.
A present zero Fraction emits `{"numerator":"0","denominator":"1"}`.

Decimals use `str(Decimal)`: trailing zeros and exponent representation survive
without float conversion or decimal-context rounding. VWAP uses a dict of exact
base-10 integer **strings**: `numerator` and `denominator`, taken from the
normalized Fraction. Local exact Decimal-to-fixed-digit rendering supports every
admitted integer size without Python int-string digit limits or context rounding;
no interpreter limit is changed. For example 1/3 emits `{"numerator":"1","denominator":"3"}`,
never an approximate decimal. The projection does not reject or reinterpret a
signed Fraction supplied with PASS diagnostics; upstream qualification owns the
meaning of the declared result.

Every clock emits `iso`, `utc`, `zone`, `fold`. `iso` is its original ISO string,
`utc` its aware UTC ISO instant, `zone` the ZoneInfo key or fixed-zone string, and
`fold` the original DST-fold integer. Equivalent instants with different original
representations retain those representations. Later evaluation or reference
clocks remain explicit metadata; they are not labeled current or re-adjudicated.
A replaced DTO with PASS and contradictory clocks therefore remains a *declared*
display, not proof that the clocks satisfy the upstream policy.

Diagnostics preserve each state's enum string and every reason in original
order, including duplicates. There is no overall state, readiness vote,
trade/scoring flag or inferred current eligibility.

Provenance has `semantics: "declared_not_authenticated"`, complete `grid`,
`quality_policy` and `session_facts`, and ordered `input_metadata`. A metadata row
contains exactly `observation`, `interval_start`, `interval_end`,
`timestamp_convention`, `session`, `basis`, `notional_reference`. Observation
includes all declared identity fields, revision IDs and `revises_evidence_id`,
clocks, presence/finality, evidence IDs/hash, lifecycle/options/fallback and
coverage metadata. It retains both revisions if the caller supplied them; it
never sorts, deduplicates or selects observations. References retain their
available/recorded clocks and opaque SHA256 strings. Policy maps use sorted keys;
all original list/tuple order is otherwise retained. V2 encodes **every
integer-valued provenance fact and policy parameter as an exact decimal string**,
including small integers and zero. This format never depends on the host's digit
limit or the integer's magnitude. For example interval_seconds300 becomes
`"300"`, delay seconds0 becomes `"0"`, and coverage_fraction1 becomes `"1"`.
Float-valued facts remain JSON numbers; bool remains bool and missing stays null,
so integer0, float0.0, false and null remain distinct. Observation integer fields
are interval_seconds, coverage_fraction and delay.seconds; policy integer fields
include max_event_age_seconds, max_receipt_age_seconds, max_future_skew_seconds,
max_delay_seconds, required_interval_seconds, min_coverage_fraction and
max_oi_age_seconds. Clock `fold` (0/1) and top-level `opening_minutes`
(5/15/30/null) remain bounded JSON integer tags. The schema version change makes
this ordinary-int encoding amendment explicit; no DTO or underlying policy
representation is mutated. **No input OHLC, volume or
trade-notional payload is exposed**, including when the feature is withheld.
The hashes and identities are declarations, not authenticated evidence.

Returned dicts/lists are freshly allocated on every call and share no mutable
containers with the input or previous outputs. Caller edits do not alter the
source DTO, provenance or later projections. No input clock, type, history or
value is changed. Output serializes with `json.dumps(..., allow_nan=False)` and
round-trips to the same JSON types for valid accepted inputs.

Tests use ordinary imports in an isolated namespace export. They neither replace
`sys.modules` entries nor empty package paths at collection. Repository execution:

```sh
python3.11 -B -m unittest discover -s tests -p test_market_feature_display.py
```

The source-artifact verification also runs the same tests under an inspected
stdlib-only audit fence and checks functional source mutations. Dependency copies
are byte-identical accepted overlays excluded from the three-addition patch.
No API/UI registration, provider/network binding, journal, persistence, live
collection, production runtime, trading, scoring or publication is part of this
contract.
