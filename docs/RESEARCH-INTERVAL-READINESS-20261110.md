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
