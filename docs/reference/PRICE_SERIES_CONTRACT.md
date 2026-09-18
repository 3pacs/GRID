# Price Series Contract (workstream W3d)

> Load when: touching `evaluation/prices.py`, `evaluation/signal_outcomes.py`'s
> `make_default_price_accessor`, or `scripts/evaluate_signals.py`.

## What this replaces

Before this change, `evaluation/signal_outcomes.py::make_default_price_accessor`
resolved an instrument to a `feature_registry` row by **guessing** three
candidate names (`instrument`, `f"{instrument}_CLOSE"`, `f"{instrument}_close"`)
and taking whichever one a bare `SELECT ... WHERE name = ANY(:names) LIMIT 1`
happened to return first. That query has no `ORDER BY`, so if more than one
guess matched a registered feature, which one "won" was undefined — never
audited, never surfaced, never even logged.

`evaluation/prices.py` replaces that with an explicit, tested contract:
`PriceSeriesContract` (the mapping) and `PITPriceAccessor` (the PIT-safe
read). Both are pure additions — nothing in `store/pit.py`, `schema.sql`,
`ingestion/yfinance_pull.py`, `intelligence/trust_scorer.py`, or
`api/routers/watchlist_overview.py` is modified.

## The rule this contract reuses (never a fresh guess)

The contract's candidate-name step is not new logic — it calls the exact
function the GRID app itself uses to look up a ticker's close:

* **`api/routers/watchlist_helpers.py:393-426`** — `_resolve_feature_names(ticker)`.
  Checks the entity map (`normalization/entity_map.py`'s `SEED_MAPPINGS` /
  `NEW_MAPPINGS_V2`, keyed on `f"YF:{ticker}:close"` /
  `f"YF:{ticker}:adj_close"`) first, then falls back to naming-convention
  guesses (`{ticker}_close`, `{ticker}_full`, bare `{ticker}`).

* **`api/routers/watchlist_overview.py:418-429`** — `get_ticker_quote()`'s
  own SQL: `... JOIN feature_registry fr ON fr.id = rs.feature_id WHERE
  fr.name = ANY(:names) ...`. This is the app's production, user-facing
  ticker-close lookup (`ticker_pulse` widget) — the literal ground truth
  for "how the app itself looks up a ticker's close."

`PriceSeriesContract.resolve()` calls `_resolve_feature_names()` for the
candidate list, then confirms against `feature_registry` with the SAME
`name = ANY(:names)` condition. The difference from the app's own query is
deliberate: the app's version is `LIMIT 1` with no tie-break (non-deterministic
under ambiguity); this contract never does that — see failure modes below.

`source_catalog` confirmation mirrors `ingestion/base.py:321`
(`_resolve_source_id`'s `SELECT id FROM source_catalog WHERE
LOWER(name) = LOWER(:name)`), for the one source this contract's bar
convention assumes: **`yfinance`** (`ingestion/yfinance_pull.py:103`,
`SOURCE_NAME = "yfinance"`).

## Scope: what "price series" means here

This contract resolves **daily close bars sourced from yfinance** only —
the convention documented in `ingestion/yfinance_pull.py`:

* Series id: `f"YF:{ticker}:{field_key}"` (`ingestion/yfinance_pull.py:203`),
  `field_key="close"` from `_FIELD_MAP` (`ingestion/yfinance_pull.py:66-73`).
* Bar kind: always `"close"` (`evaluation.prices.BAR_KIND`).
* Multi-source conflict resolution for a feature happens upstream, in
  `normalization/resolver.py`, before a row ever lands in `resolved_series`
  — this contract reads the already-resolved row via `store/pit.py`, it
  does not re-resolve sources itself.

## The resolver: `PriceSeriesContract`

```python
contract = PriceSeriesContract(engine)
descriptor = contract.resolve("AAPL")
# SeriesDescriptor(instrument="AAPL", feature_id=..., feature_name="aapl_full",
#                  series_id="YF:AAPL:close", source_catalog_id=..., 
#                  source_catalog_name="yfinance", bar_kind="close", unit="price")
```

`resolve()` is cached per instance (an instrument resolves once, not once
per signal). `candidate_name_fn` is injectable — production code leaves it
as the default (the real `_resolve_feature_names`); tests inject a fixed
list so resolver tests are pure Python with a fake engine, independent of
that function's own guessing internals.

### Explicit failure modes — never a silent guess

| Condition | Result |
|---|---|
| No `feature_registry` row matches any candidate name | `UnsupportedInstrumentError(instrument, reason)` |
| More than one `feature_registry` row matches | `AmbiguousInstrumentError` (subclass of the above) — lists every `(feature_id, feature_name)` match, **never picks one** |
| `source_catalog` has no `yfinance` row | `UnsupportedInstrumentError` — the bar convention itself cannot be confirmed |
| Candidate-name rule returns an empty list | `UnsupportedInstrumentError` |

## The accessor: `PITPriceAccessor`

```python
accessor = PITPriceAccessor(engine, contract)
point = accessor("AAPL", date(2026, 1, 5))  # -> PricePoint | None
```

* Reads through `store/pit.py::PITStore.get_pit([feature_id], as_of_date=as_of,
  vintage_policy="LATEST_AS_OF")` — the same PIT engine every other
  inference path in GRID uses. Never reads a row with `obs_date` or
  `release_date` beyond `as_of`.
* Returns `None` when no bar exists yet at or before `as_of` (distinct from
  "instrument unsupported" — `evaluate_signal` treats these differently:
  `None` before the horizon elapses is `UNRESOLVED`; `None` after is
  `INELIGIBLE(missing_exit_price)`).
* Raises `evaluation.signal_outcomes.UnsupportedInstrumentError` (not this
  module's own exception type) when the contract can't resolve the
  instrument, so `PITPriceAccessor` instances are usable directly as
  `evaluate_signal`'s `price_accessor` argument — no adapter required.

### `PricePoint` — full provenance, not just a number

```python
@dataclass(frozen=True)
class PricePoint:
    obs_date: date
    value: float
    release_date: date
    vintage_date: date
    source_ref: str        # e.g. "yfinance:YF:AAPL:close"
    basis: str = "close"
    sanity_ok: bool = True
    sanity_reason: Optional[str] = None
    # .price / .bar_date are read-only aliases for .value / .obs_date —
    # this is a drop-in for anything that duck-types on the three
    # attributes evaluate_signal() itself reads (.price, .bar_date, .basis).
```

### Price-sanity flag — flagged, not dropped

A price outside `sanity_bounds` (default `(0.0, 1_000_000.0)` — the SAME
default `evaluate_signal()` itself uses) is **returned, not raised and not
silently dropped**, with `sanity_ok=False` and
`sanity_reason="price_outside_sanity_bounds"`. `evaluate_signal()` applies
its own bounds check against `entry.price`/`exit.price` (duck-typed via the
`.price` alias) and marks the signal `INELIGIBLE(price_outside_sanity_bounds)`
from that real, flagged value — never a crash, never an unflagged bad print.

## The known gap: `unit`

**No `unit`/currency column exists anywhere in `feature_registry` or
`source_catalog` today** (confirmed against `schema.sql`). `SeriesDescriptor.unit`
is a hard-coded constant (`DEFAULT_UNIT = "price"`), not a database read.
This is a documented assumption, not a hidden one: every ticker this
contract resolves is assumed to be a single-currency price level (mostly
USD, per `ingestion/yfinance_pull.py::YF_TICKER_LIST`'s composition). A
non-USD-denominated instrument would silently carry the wrong implied unit
— tracked as a follow-up, not solved here.

## Wiring: `make_default_price_accessor`

`evaluation/signal_outcomes.py::make_default_price_accessor(pit_store)` now
builds `PriceSeriesContract(pit_store.engine)` +
`PITPriceAccessor(pit_store.engine, contract)` internally and returns that
as the default accessor. The injectable-accessor contract for
`evaluate_signal()` is unchanged — tests, and any future real caller that
wants a different price source, still pass their own `PriceAccessor`
callable.

## The real caller: `scripts/evaluate_signals.py`

`evaluation/signal_outcomes.py` and `evaluation/prices.py` had no caller
before this change. `scripts/evaluate_signals.py` is that caller:

* Selects `signal_sources` rows by **explicit filters only**
  (`--source-type`, `--date-from`, `--date-to`, `--limit`) — never "all
  rows."
* **Dry run by default**: evaluates via `PITPriceAccessor` and prints the
  cohort summary (`evaluation.signal_outcomes.summarize_outcomes`) as JSON.
  Writes nothing.
* `--persist` requires `--i-understand-this-writes-evaluations` as well —
  either flag alone refuses. A persist run is INSERT-only into
  `signal_evaluations` (`persist_outcomes`, idempotent via
  `ON CONFLICT ... DO NOTHING`).
* Never writes to `signal_sources.trust_score`/`.outcome` or any other
  `signal_sources` column — it only ever `SELECT`s that table. Never
  imports `intelligence/trust_scorer.py`.
* Refuses to run against a database whose name doesn't start with
  `griddb_` unless `--allow-any-db` is passed.
* `--origin-tag` is a required-by-convention passthrough recorded on every
  outcome row it evaluates.

**This script is not scheduled** (not in `ingestion/scheduler.py`, not a
systemd unit, not a Hermes job) **and has never been run against
production.** It exists so the versioned evaluator has a real,
explicitly-invoked entry point instead of remaining tested-but-uncalled
code — replacing the old `intelligence/trust_scorer.py` meter is a
separate decision for whoever owns that cutover, not something this
script does by existing.
