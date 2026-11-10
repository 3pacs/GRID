# Private native-volume core

Offline additive implementation; no producer, timer, service, production DB or
live GEX change. `native_volume_view` consumes existing immutable MinuteBar
revisions; `retained_volume_view` binds the existing read-only retention loader.
Source, symbol and price basis remain isolated. The existing selector resolves
latest revisions available at as-of; the existing rollup validates native bars.

`XNYSCalendar.prior_sessions` returns exactly the preceding ten exchange
sessions, including early closes and excluding holidays/weekends. No search for
ten nonempty days. Any missing expected prior session withholds RVOL. All prior
session bars must be complete; if an early-close session lacks today's elapsed
minute, RVOL is withheld, never rescaled or silently substituted.

Output `grid.native-volume.v1`: `cumulative_shares`, `bar_vwap_estimate`, `rvol`,
`baseline_median_shares`, explicit baseline errors, as-of, completed-window end,
receipt and event ages, provenance/revisions. Ready means descriptive metrics
are present, NOT signal validity. `live`, `signal_confirmation`,
`exact_trade_vwap`, `cross_source_reconciled` remain false. HLC3 bar estimate is
not true trade VWAP. Current zero volume preserves zero RVOL where baseline is
positive but status is degraded because no weighted price exists.

Five-second close settlement allowance; exact current session coverage plus
receipt/event age limits (120 seconds default). Missing minutes are not zero;
forming bars excluded by parser. Historical retrieval availability is never
backdated. An API/GEX consumer must additionally compare requested as-of against
actual wall clock; this function supports historical research as-of values and
does not claim they are currently live.

Tests: calendar holiday/DST/early-close windows, missing sessions/bars, mixed
source, ambiguity, late receipt, old events, zero/fractional shares, arithmetic.

## Private cache/API composition (offline implementation, not deployed)

`native_volume_cache.fetch_and_retain` takes an explicitly configured existing
Schwab client with retries disabled, an initialized dedicated local SQLite store,
and an owner-only receipt directory. It preserves exact native response/request
metadata before appending immutable revisions. Receipt replay is idempotent.
Paths must be provisioned under trusted owner-controlled parent directories:
final path checks are not a defense against an attacker controlling ancestors.
No timer, service, credential installation or production store is created here.

`api.native_volume.build_router(loader=sqlite_loader(store), calendar=XNYSCalendar())`
is an unregistered admin-only read API. It never fetches or refreshes provider
credentials in response to a dashboard request. GEX's separate pure
`grid_native_volume.project` accepts private ready/fresh GRID responses, labels
HLC3 as an estimate, and rejects stale/historical/source/provenance mismatches.
It is not wired to GEX's server or UI yet; no new live metric is claimed.

Release prerequisites: review calendar/package deployment compatibility, designate
one cache owner and invocation cadence, initialize private store/receipt custody,
mount admin router, provide authenticated private GEX transport, test end-to-end
freshness and failure handling, and obtain the separate runtime release approval.
Do not enable the rollover gate or modify feed selection as part of this work.
Current restored GEX and live cumulative-volume assets must remain untouched.
