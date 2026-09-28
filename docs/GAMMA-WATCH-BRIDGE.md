# Gamma Watch in GRID

The authenticated Options → Gamma Watch tab reads the co-located collector through
GRID's API. No iframe, cross-origin browser token transfer, new subscription,
database migration, scheduler activation or trading-score promotion is involved.

## Endpoints

- `GET /api/v1/gamma-watch/state`: complete collector state, including quote,
  delayed provider GEX, partial/frozen model, pressure, sector/contract snapshots,
  structural RTD, official operations and hypothetical rebalance demand.
- `GET /api/v1/gamma-watch/contracts?symbol=SPY`: contract listing only. The
  scenario-computation route is not proxied because it records a new scenario.
- `GET /api/v1/gamma-watch/journal?since=0`: recent events/interactions, with the
  collector's existing 300-event limit. Use archive for complete recorded history.
- `GET /api/v1/gamma-watch/archive?stream=artifacts&after=0&limit=100`:
  read-only pagination of the existing SQLite receipt journal. `stream` is one
  of `artifacts`, `events`, `samples`; next request uses `next_after`. Stop when
  records is empty. Cursors belong to one stream/database and must not be shared
  between streams. Page rows <=500 and encoded source rows <=4MiB.

The collector remains at fixed loopback port 8769. Its journal is the source of
truth already stored on the GRID host. `GAMMA_WATCH_JOURNAL` can override the
default journal path for installations; no endpoint accepts a path or URL.
SQLite is opened `mode=ro`, `query_only=ON`, never created by a GET.
The API does not copy/relabel data into GRID's canonical observations, signals,
price history or dealer models. Research consumers must opt into this source.

## Fidelity and availability

`status=available` describes bridge transport, not live market entitlement.
Original source times, collection times, errors, missing values, zeroes,
coverage and assumptions are retained inside `data`. Receipt journals are
prospective only; their record dates do not establish earlier availability.
They do not contain every raw option tick/contract subscribed by the broker;
only the snapshots and events the standalone app actually recorded.

RTD LAST callbacks are not exchange timestamps, execution tape or signed flow.
Current structural session checks are conservative cash-session clock checks,
not an exchange holiday calendar. Frozen chains and delayed providers remain
separate. Rebalancing estimates are hypothetical SPY/AGG allocation resets per
initial $1bn, not observed orders or aggregate institutional AUM. Monthly and
quarterly scenarios are not additive.

Unavailable upstream, stale served clock, malformed/oversized JSON or missing
journal returns 503 with no fabricated/cached replacement or host-path details.
Redirects and environment HTTP proxies are disabled for loopback requests.
Frontend bypasses its normal 60s API cache, polls serially at five seconds and
ages receipts independently of request completion.

## Validation and remaining research

Tests cover source-preserving transport, stale/error handling, query allowlists,
read-only/missing journal handling, pagination/size limits and UI stale/failure
transitions. These are software checks, not profitable directional backtests.
Before use for trading research, verify regular-session inputs and prospectively
evaluate preregistered rules with executable prices and costs.
