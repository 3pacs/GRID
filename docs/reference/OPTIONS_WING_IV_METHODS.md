# Options wing IV method contract

This is a descriptive data correction. It does not establish an investment edge,
executable quotes, calibrated scanner thresholds or research-panel eligibility.
No historical values are backfilled, relabeled, retracted or deleted.

## Fixed-moneyness v1

`store/options_iv.py` implements `fixed_moneyness_90_110_band3_mean_v1`:

| Field | Definition |
| --- | --- |
| `iv_m90_put_v1` | Equal-weight mean of positive finite put IVs with strike/spot in [0.87, 0.93] |
| `iv_m110_call_v1` | Equal-weight mean of positive finite call IVs with strike/spot in [1.07, 1.13] |
| Units | Annualized decimal IV; a spread is in decimal IV units, not a ratio |
| Expiry | One expiry per calculation, never a pooled term structure |
| Missing side | NULL; no ATM substitution, nearest-strike extrapolation or invented zero |
| Sparse side | One contract is allowed and explicitly counted; no liquidity claim |

These are fixed-moneyness **proxies**, not 25-delta IV. The band is three
percentage points of spot around each target, not three percent of the target.
Time to expiry and volatility change a delta-targeted strike; fixed spot bands
do not track that change.

Each `wing_iv_metadata` records method, units, expiry, snapshot date, calendar DTE,
actual provider, spot and its basis, available spot observation time, capture
batch identity, and each side's contributing strikes/count. The primary writer
uses its nearest selected expiry at least two days away (or its existing
short-expiry fallback) and the spot from the same capture's first response.
An empty selected expiry has missing wings; another tenor is never substituted.
Batch identity enables replay from the immutable options store. These clocks
and identities are lineage, not proof of executable quote freshness.

The term-structure API takes the latest snapshot day from the view, then that
day's highest-ordinal registered batch and its actual spot. It does not search
earlier days or earlier same-day batches for a more usable capture. A backfilled
latest batch, missing registration or missing observed spot makes it abstain.
It reads contracts by immutable batch ID, once per expiry.
Missing observed spot/registration produces an explicit unavailable response;
the highest-OI strike is never used as a spot price. A later capture during the
read cannot mix its contracts with the selected batch.

When a client supplies no provider identity, the stored `source` is NULL.
The API withholds the proxy and percentile even if the band contains contracts;
the sample counts distinguish this unknown-lineage case from an empty band.

## Storage and compatibility

Migration `wing_iv_proxy_20261116` adds three nullable daily-signal columns and
nullable `resolved_series.options_iv_metadata`. There is no default, data update
or backfill. The primary upsert omits the old `iv_25d_put`/`iv_25d_call` columns,
including on same-day conflict: their original values remain intact. The
Surfacer expectation writer cannot establish this capture contract and clears
the new fields/metadata when replacing a daily signal, preventing stale wings
from being combined with a different spot/expiry. Its old wing values remain.

New primary resolved features use the new v1 names, descriptive registry text,
`NAN` missing-data policy and `model_eligible=false`. Each inserted resolved value
stores its own metadata atomically. The existing first-insert-per-day conflict
policy remains: a later daily-signal update does not overwrite the earlier
resolved value **or its lineage**. Joining these stores by date alone can yield
different values; a date is not a capture identity. New registry eligibility starts on publication
day. No feature is admitted to research by this patch.

The bulk script uses the same helper but writes only new raw series names,
including `iv_m90_m110_spread_v1` for put-minus-call. Its `raw_payload` retains the
method/expiry/provider lineage; unavailable capture ID and spot clock stay NULL.
It no longer appends its different wing-median formula to old `iv_25d_*` or
`iv_skew` raw series. These new raw IDs deliberately have **no EntityMap mapping**:
the generic resolver cannot carry their per-value metadata into the new primary
resolved features. A future lineage-aware resolver change needs separate review.
Other legacy bulk metrics and formulas are outside this correction.
Their existing raw IDs are preserved, including BRK-B's legacy `b_pcr`,
`b_iv_atm`, etc.; only the new wing/spread suffixes use the new naming contract.

Watchlist and term-structure responses keep deprecated `iv_25d_*` keys as NULL.
Watchlist retains old numbers under `legacy_wing_iv` with `method=unknown_legacy`.
Consumers must read the new fields and metadata; mapping old numeric fields into
the new names is forbidden. The derivatives UI uses the new proxy fields and
labels. Related-feature and anomaly descriptions label old names as legacy with
unknown method. Existing stored registry descriptions and original research
remain unchanged. Old external clients will see missing wings until updated.
`scripts/run_analytics_snapshots.py` retains only the legacy wing suffixes in
`SECTOR_FEATURE_SUFFIXES`: it does not automatically add the new model-ineligible
features. Sector sets therefore lose current wing members as legacy series stop
updating; admitting replacement model inputs needs a separate reviewed change.

## Historical percentiles

`VolSurfaceEngine.historical_percentile` reads only the new daily wings for
put-minus-call spread. The current row must have a recognized method, units,
spot basis, positive spot and internally consistent expiry/snapshot/DTE. History
must have the same method, actual provider and **exact calendar DTE**, with each
row's metadata snapshot date matching its signal date. This is a same-tenor
descriptive comparison, not a constant-maturity interpolation or a 25-delta
distribution. Unknown legacy, incompatible sources/tenors, nonfinite values and
missing sides are excluded. The current row never counts as its own history.

At least two compatible prior rows are needed. The response provides method,
current lineage, used/excluded history counts and a reason when unavailable.
Two samples do not imply a statistically useful estimate. Sample composition,
liquidity and intraday capture time may still differ; none is qualification for
an empirical backtest or a calibrated trading threshold.

The older `iv_skew` signal is separately an OTM-put/ATM-put IV **ratio**. It is
not the fixed-moneyness spread. `OptionsScanner` loads the old wing columns for
compatibility but does not score them; its direct skew score reads that ratio.
Regression coverage verifies legacy-wing changes do not change scanner scores.

## Separate surface approximation

`compute_skew` has a separate legacy model calculation: invert unadjusted
zero-dividend Black-Scholes spot delta +/-0.25 using **ATM IV**, then interpolate
the observed smile at those modeled strikes. This does not solve for a
smile-consistent 25-delta contract. It now says
`bs_atm_iv_delta_strike_proxy_v1`, exposes the assumptions and target strikes,
returns numeric `*_delta_strike_proxy_iv` and NULL deprecated `*_25d_iv` keys.
There is no fixed-wing fallback or clamping outside observed strike support.
Provider/batch identity is explicitly unknown on this older chain reader; its
date-bounded spot reader does not establish intraday spot availability.

The scanner's existing surface thresholds remain unchanged; less-supported
chains can now abstain instead of fabricating wing support. Its receipt carries
the model method/expiry/DTE, and the digest labels this a modeled skew proxy.
It is never pooled into the fixed-moneyness historical percentile. No scoring
weights or historical scanner outputs were rewritten.

## Verification and operator release boundary

Synthetic regressions in `tests/test_wing_iv_semantics.py` exercise the real
producer, resolved writer, API calculation/JSON response, both Watchlist readers,
bulk writer, percentile and surface/scanner paths. They block outbound sockets.
The optional `tests/test_wing_iv_migration_pg.py` executes the migration twice
and the actual producer upsert against a randomly scoped, disposable loopback
PostgreSQL schema, proving old values survive. It only accepts an explicit
`GRID_WING_IV_TEST_DSN` for a database named `grid_wing_iv_test_*`.

`tests/test_wing_iv_http.py` exercises the registered FastAPI HTTP route,
synchronous dependencies/endpoint, auth rejection and JSON response through
an in-process ASGI transport, with outbound socket connections blocked. Its
successful-auth case uses an explicit synthetic dependency override; it does
not exercise a real user store, app startup, proxy or deployed service. AsyncIO
requires its local self-pipe IPC: a sandbox that denies even that `socket.send`
will stall the worker-thread completion, independently of this endpoint. Run
that test in an environment permitting local IPC; no HTTP server is required.

Anik applies releases. After review, the additive migration must precede code
that selects/writes the new columns; deploy API and UI together. A pre-migration
API query fails unavailable instead of substituting legacy wings. Rollback may
leave the nullable columns intact; downgrade refuses provenance deletion.
Old producer code would resume misleading legacy writes, so code rollback needs
an explicit operator decision. This source change performs no migration,
provider request, production query, historical repair, holdout access, merge,
deployment, schedule/service change or notification delivery.
