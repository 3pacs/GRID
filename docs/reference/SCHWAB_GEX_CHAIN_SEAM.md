# Schwab → GEX option-chain seam (design note, no GEX changes)

**Status:** documentation only, 2026-10-30. Nothing in `collectors/gamma_watch/`,
`api/routers/gamma_watch.py`, gex-watch on grid-svr or the ANIK collector is changed by this
note. GEX changes stay with the GEX owner (Claude session `local_d0cd874c`, gex lane; Codex
custodians for the RTD collector) and every existing GEX gate stays in force. Nothing
Schwab-sourced may reach `gex.stepdad.finance` without Anik's redistribution sign-off.

Pins: GRID reader `ingestion/schwab_market.py` at commit `29f636a2` (PR #931); GEX consumer
contract read from `collectors/gamma_watch/broker.py` on `origin/main` `d22747ef`
(the version-control import of the deployed gex-watch source; the deployed file on grid-svr
is pinned separately in `~/dev/gex/STATE.md`).

## 1. What the GEX consumer reads today (the contract to preserve)

`broker.py:get_snapshot()` pulls one JSON file (`snapshot.json`, schema `spy-rtd-v1`) over
SSH from ANIK and rejects it unless:

| gate | rule in `broker.py` |
|---|---|
| identity | `schema == "spy-rtd-v1"` and `host.upper() == "ANIK"` |
| collector lag | `-30 s <= now - collected_at <= 20 s`, `heartbeat > 0` |
| SPY quote | `LAST`, `BID`, `ASK` present, `0 < bid <= ask`, `ask - bid <= 2`, `LAST.received_at` within 20 s |
| per contract | `OPEN_INT`, `BID`, `ASK` numeric, `oi >= 0`, `b >= 0`, `a > 0`, `a >= b` |
| IV | `IMPL_VOL` numeric (`%` strings divided by 100), `0.02 <= iv <= 3`, `received_at` within `[-30 s, +120 s]` of now; ITM contracts may borrow IV from the matching OTM side |
| gamma | `GAMMA` optional, kept only when `>= 0` (`provider_gamma`) |

Record shape consumed (`records[]`): `{symbol, field, value, received_at, callback_seen}` where
`symbol` is the thinkorswim form `.SPY260928C735` and `field` is one of
`LAST BID ASK IMPL_VOL GAMMA OPEN_INT` (the 2026-10-26 roll adds `DELTA VOLUME` and SPY
`VOLUME LAST_SIZE BID_SIZE ASK_SIZE HIGH LOW OPEN VOL_INDEX PUT_CALL_RATIO`). Snapshot
envelope: `{schema, host, source, collected_at, heartbeat, update_count, topic_count,
exchange_timestamp_available, entitlement, records}`. The contract universe comes from
`contracts.json` (`{symbol, expiry, opt_type, strike}`), rolled weekly by the GEX owner.

Output labels the feed `source: "thinkorswim RTD on ANIK"` and
`entitlement: "real-time per user confirmation; exchange timestamps unavailable"`; the GRID
source catalogue (`config/gamma_watch_sources.json`) lists it as
`gamma_watch_rtd_model_gex_v1` / `gamma_watch_rtd_quote_v1` with
`latency: callback_receipt_only_exchange_time_unknown`.

## 2. What the Schwab reader can supply (documented shape, not live-verified)

`SchwabMarketClient.option_chain("SPY", **params)` returns the provider's `/chains` body
unchanged. Per Schwab's published Trader API reference (not verified against a live
response in this slice): `putExpDateMap` / `callExpDateMap` keyed `"YYYY-MM-DD:<dte>"` →
strike string → list of contract maps with `putCall`, `symbol` (OCC form
`SPY   260928C00735000`), `strikePrice`, `expirationDate`, `bid`, `ask`, `last`,
`bidSize`, `askSize`, `totalVolume`, `openInterest`, `volatility` (percent), `delta`,
`gamma`, `quoteTimeInLong`, `tradeTimeInLong` (epoch ms), plus `underlyingPrice`,
`isDelayed`, `status` at the top level. `SchwabMarketClient.quotes("SPY")` gives the
underlying `lastPrice/bidPrice/askPrice/quoteTime` (see `ingestion/schwab_quote_path.py`
for the private, freshness-classified form).

## 3. The smallest seam

One pure function, living in GRID (not in the GEX tree), with no I/O:

```
schwab_chain_to_rtd_records(chain_body, contracts, *, received_at) -> dict
```

* **Input:** the raw `/chains` body, the GEX `contracts.json` universe (so the universe stays
  the GEX owner's decision), and the receipt clock.
* **Output:** a snapshot in the `spy-rtd-v1` *record* shape so `broker.py` needs no parser
  change, but with a distinct identity so no gate is bypassed by accident:
  `schema: "spy-chain-schwab-v1"`, `host: "<grid-svr>"`, `source: "schwab-marketdata-v1"`,
  `exchange_timestamp_available: true`, `entitlement: "<per Schwab app, readonly>"`,
  `heartbeat`, `update_count`, `collected_at = received_at`, and `records[]` mapped
  per contract:

| RTD field | Schwab field | conversion |
|---|---|---|
| `BID` / `ASK` | `bid` / `ask` | as returned |
| `LAST` (SPY) | quotes `lastPrice` | as returned |
| `IMPL_VOL` | `volatility` | percent → fraction (÷100); `NaN`/`-999` sentinel → absent |
| `OPEN_INT` | `openInterest` | as returned (daily, no as-of date supplied by the provider) |
| `GAMMA` | `gamma` | as returned; sign/scale to be confirmed against RTD by the owner |
| `DELTA` / `VOLUME` | `delta` / `totalVolume` | as returned |
| `symbol` | `symbol` (OCC) | `SPY   260928C00735000` → `.SPY260928C735` (strike `/1000`, trailing zeros dropped) |
| `received_at` | `quoteTimeInLong` | epoch ms → ISO-8601 UTC; this is an **exchange** time, unlike RTD receipt time |
| `callback_seen` | — | `true` when the contract was present in the body |

Contracts in the universe but absent from the body are emitted with `callback_seen: false`
and no value, so `broker.py` counts them as `missing_contracts` exactly as it does today.

## 4. Decisions that belong to the GEX owner (not made here)

1. **Identity gate.** `broker.py` rejects `host != "ANIK"` and `schema != "spy-rtd-v1"`. Accepting
   the Schwab snapshot is a one-line owner decision per gate; this note does not propose
   relaxing either.
2. **Timestamp semantics.** `received_at` would become exchange time; the 20 s / 120 s age
   gates were tuned for collector receipt time. The owner decides whether to keep them, and
   whether to set `exchange_timestamp_available: true` in the public projection.
3. **Transport.** Today: SSH pull from ANIK. A Schwab leg would run on grid-svr and write the
   snapshot locally, so the SSH leg and the ANIK task are untouched until the owner retires them.
4. **Universe roll.** Unchanged: `contracts.json` is still the owner's weekly roll; the seam reads
   it, never rewrites it.
5. **Redistribution gate.** The public projection (`public_surface.py`) must not expose
   Schwab-sourced numbers until Anik signs off. Until then a Schwab snapshot may only feed the
   gated/loopback surfaces, and the catalogue entry must say so
   (`admission: "private_only"` or equivalent, owner's wording).
6. **Entitlement text and source catalogue.** New `config/gamma_watch_sources.json` entries
   (`gamma_watch_schwab_chain_v1`, `gamma_watch_schwab_quote_v1`) with
   `latency: "exchange_timestamp_per_provider"`, written by the owner when the leg exists.

## 5. What this slice does and does not include

* Included: this note and `ingestion/schwab_quote_path.py` (SPY underlying quote with
  provenance/freshness, fail-closed classification, declared-audience label, no persistence)
  plus its tests. The `audience` argument is a caller declaration, not access control; a real
  authorized private endpoint is a separate integration gate that neither this note nor the
  module provides.
* Not included, by design: the `schwab_chain_to_rtd_records` implementation, any GEX-tree edit,
  any scheduler/Hermes step, any `raw_series` write, any live call, any public projection.
* Still unverified: the live `/chains` field names and units above, Schwab's `isDelayed`
  semantics for SPY options, gamma sign/scale relative to RTD `GAMMA`, and the refresh-token
  workflow on grid-svr (the login helper lives on precision5520 only).
