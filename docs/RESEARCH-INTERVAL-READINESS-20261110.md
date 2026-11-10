# GRID interval readiness: 2026-11-10

## Status: offline core tested, not installed or connected

User goal: GRID is the common API/data base for research and GEX. Agent research
must have standard intervals available without silently inventing unavailable
resolution, hiding missing data, or using future revisions.

Audited source baseline: `18e49822ec220fa636ce20bb3a73b56c5f42e030`.
No production DB, credentials, feed, service, timer, or shared GEX Python changed.

## What exists versus what is missing

- GRID `ingestion/realtime/candle_builder.py` accepts a configurable seconds
  interval but the listener constructs the default 5-minute builder only.
- Yahoo requests 1-minute data, but `_fetch_prices` returns only each symbol's
  last close, volume and bar time. It does not retain all downloaded minutes or
  their high/low/open. Aggregated sampled closes are not genuine exchange OHLC.
- Restart persistence `ON CONFLICT DO NOTHING` retains the first truncated row;
  source/event identity is missing. This is already documented in
  `TODO-REALTIME-CANDLE-CORRECTNESS.md`. Do not use a MAX-volume upsert shortcut.
- Schwab has a `price_history` method. Actual entitlements, retention and live
  delivery are not verified here; no provider call was made.
- Precision's legacy research cache has 2m,5m,60m,1d SPY but no retained1m/ticks.
  Six one-minute fixtures have OHLCV but lack immutable arrival/revision lineage.
  Fixture geometry can test software, not historical live probability claims.
- Separate GEX `bars_1m` and sampled `bars_1m_live` are not a unified GRID API.
  Receipt-clock sampled bars cannot be relabeled native/all-trades one-minute.

## Implemented isolated core

`ingestion/realtime/research_intervals.py` provides 1,2,3,5,10,15,30,60,120,240
minute and trading-session daily rollups from native one-minute input. Caller
supplies a verified, versioned exchange session including early close/DST.
Buckets anchor at session open; final short buckets expose true end/duration.
Closed windows only. Each result has source, price basis, source revisions,
observed/expected count, missing minutes and availability time. Gapped OHLCV is
null, never filled; missing volume is not zero. Multiple providers, duplicate
minute revisions, receipt-sampled input and naive timestamps refuse. As-of
filtering uses availability, not only market timestamp. No VWAP is fabricated.

18 focused unit tests and Ruff pass. Not integrated with ingestion/API; no
live coverage or full-suite pass claimed. Independent review found a missingness timestamp backdating defect; corrected with explicit assessed_at, last_source_available_at and conservative incomplete-window available_at, plus regression test.

## Remaining acceptance gates, in order

1. Read-only coverage inventory: per symbol/provider native resolution,
   earliest/latest event and receipt times, missing-minute percent, retention,
   revision history and correction policy. SPY first, then user watchlist.
2. Retain immutable native1m revisions and full OHLCV, source session/adjustment
   metadata. Backfill can be labeled historical-as-retrieved, never historical
   knowledge-at-entry. Record source/provider-time and received-at separately.
3. Point-in-time adapter selects latest revision available at requested as-of,
   isolates providers, and passes truthful native/sampled classifications.
4. GRID read API exposes bars AND coverage, interval/session/price basis,
   partial-versus-complete state, watermark, stale reason and provenance.
   GEX consumes same endpoint; do not create a divergent dashboard bar engine.
5. Weekly/monthly rollups use complete daily session manifests and verified
   exchange calendar; absent session must not be silently omitted. Native daily
   history stays distinct from daily computed from incomplete intraday capture.
6. Seconds/ticks remain explicitly unavailable unless a suitable native feed
   is authorized and retention tested. Two/five-minute bars cannot create3m.
7. Test true complete/missing/delayed/revised bars, overnight boundaries, DST,
   early closes, interruptions/replay and duplicate trade identity. Only then
   deploy through controller after morning GEX bootstrap is verified.
8. Probability replay uses separate research stream: target-before-invalidation
   after each completed3m checkpoint. Do not change frozen GEX evaluations.

No new subscription or claim of complete live interval readiness is implied.

## Follow-up: adapter and read API implemented locally

`ingestion/realtime/native_minutes.py` now parses retained Schwab price-history
responses only with explicit minute/1 request metadata and matching symbol. It
preserves every closed candle's OHLCV, actual response receipt, payload hash and
revision identity. Forming candles are counted/excluded. Adjustment basis is
`provider_unspecified`, not assumed unadjusted. Historical downloads become
available at retrieval time, never at their historical candle timestamps.

The point-in-time selector chooses the latest eligible receipt for each event
minute, refusing conflicting revisions at that latest time. Selection is
permutation-invariant. A fixed-path, size-bounded read-only JSONL loader accepts
`grid.native-minute.v1` rows with all MinuteBar provenance fields explicit.
This is a consumer contract, not a new writer/timer. A schema label alone is not
proof of source quality; the supplying producer and receipt chain still require
verification. No existing private files were exported or read for these tests.

`api/research_intervals.py` supplies an **unregistered** private admin router
factory. Endpoint: GET `/api/v1/research/intervals` with symbol, source,
session_key, interval, timezone-aware as_of and price_basis. The factory requires
an explicitly configured retained-data loader and verified session lookup; both
absent by default. No provider fallback, network call, credentials, writer,
production app import or public projection. Responses include provenance,
coverage and `live:false`; historical completeness is not live-feed health.
The future production mount must retain existing GRID `require_role('admin')`.
No `api.main` registration was added.

44 focused tests pass, including synthetic retained-file→private-API coverage,
401/403 guards, fail-closed unconfigured source, future/naive cutoffs,
provenance failure, gaps, private caching, and sanitized errors. Test auth is
injected; the existing GRID JWT implementation is not re-tested here. Tests use
existing local FastAPI dependencies without installing/changing anything.
Independent review found an input-order ambiguity bug in PIT selection; fixed
with a permutation regression. Full backend/integration CI not run.

Remaining: producer/source validation and actual coverage, durable retention,
trusted exchange-calendar adapter, production API mounting and GEX consumer,
weekly/monthly manifests, controller review and safe deployment after bootstrap.

## Follow-up: durable local retention and exchange calendar

`minute_retention.py` implements an explicitly initialized, dedicated offline
SQLite store. Atomic append batches preserve immutable minute revisions and
content SHA-256, exact replay is idempotent, revision mutation refuses, concurrent
writers serialize, and UPDATE/DELETE guards prevent routine accidental edits.
Readers use SQLite `mode=ro`, bounded queries and content-hash verification.
These are integrity/accident controls, not tamper-proof cryptographic attestation
against someone with filesystem/DB ownership. Unrelated existing DB files refuse.
Only temporary synthetic stores were created during tests; no live store exists.

`research_calendar.py` supplies an optional, pinned `exchange_calendars` XNYS
RTH adapter with bounded date coverage and dependency provenance. Verified normal
390-minute session, 210-minute early close, DST UTC offset change, holidays,
weekends and the January 9 2025 ad-hoc closure. It is packaged calendar data,
not a live exchange-announcement monitor; new closures/halts require updated
reviewed calendar evidence. Extended/futures/crypto sessions are unsupported.

The initial unconstrained pandas3.1.1 dependency produced wrong calendar bounds.
Tests caught it. The isolated venv now uses pandas2.3.3 and the adapter refuses
other pandas/calendar versions. Exact tested optional pins are saved in
`requirements-research-calendar.txt`; main production requirements unchanged.
Known upstream NumPy timedelta deprecation warnings remain, not test failures.
No shared environment changed. Missing optional calendar dependencies fail closed.

Verification:55 core/adapter/API/retention tests pass in the existing local test
venv;7 calendar tests pass in the isolated Python3.14 calendar venv, including a
synthetic early-close calendar→SQLite retention→hourly rollup integration test.
Ruff and diff check pass. Independent bounded review confirmed revision receipt
identity and previous PIT corrections; no additional demonstrated defect found.
A new provider receipt of identical candle content has a new receipt-qualified
revision identity, while replaying the same retained envelope remains idempotent.

Remaining to serve real data: verify provider request/receipt custody and actual
coverage, authorize/configure the retained producer, mount the private read API,
wire GEX, review deployment after bootstrap, and add weekly/monthly manifests.
No field named source/quality alone proves truth; producer identity must be
established. No production completeness or live probability claim is made.
