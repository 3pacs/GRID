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
