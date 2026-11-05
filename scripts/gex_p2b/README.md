# P2-B matched real-input packets (Cboe delayed SPY chain)

Scripts and tests only. Binding protocol: `docs/GAMMA-WATCH-P2-RECONCILIATION.md`.
This is protocol class 1, exact-input arithmetic, on a real chain. Feeding Cboe data to
both local engines and an independent recompute does not validate any vendor model,
observed dealer positioning or a directional edge. No canonical-engine choice follows
from a result here; that is GEX-P2C and an owner decision.

```text
python -m pytest tests/test_gex_p2b.py -q --noconftest
python -m scripts.gex_p2b.harness --cboe <raw.json> --receipt <receipt.json> --output <new-dir>
```

## Inputs

The harness never fetches. An operator saves Cboe's public, free delayed chain
(`https://cdn.cboe.com/api/global/delayed_quotes/options/SPY.json`, which redirects to
`cdn-api.cboe.com`) as exact bytes, plus a receipt:

```json
{"schema": "gex-p2b-receipt-v1", "url": "...", "request_started_at": "...Z",
 "receipt_completed_at": "...Z", "clock": "untrusted_workstation | grid_svr_pull_receipt",
 "raw_sha256": "<sha256 of the raw bytes>"}
```

The receipt must bind the bytes. A workstation clock is recorded as untrusted, so timing
validation is `INDETERMINATE`. Even a grid-svr receipt proves only when the bytes were
held, not exchange freshness or OI age. Do not commit real market snapshots. Raw
licensing is unverified; keep raw bytes and run output outside Git.

## Normalization (once; every engine receives the same rows)

- Standard `SPY` OSI roots only. Adjusted or other roots are excluded as
  `NONSTANDARD_SYMBOL`. The multiplier (100) and deliverable are inferred from the
  standard root, because Cboe does not state them. This is recorded on every row.
- Expiry is 16:00 America/New_York on the OSI date, DST-aware. Early-close candidates
  (day after Thanksgiving, Dec 24 and Jul 3 Mon-Thu) are flagged, not modeled.
- The valuation instant is Cboe's naive underlying `last_trade_time`, assumed to be
  America/New_York. That assumption is recorded on the packet.
- Exclusion ledger with OI mass: `EXPIRED`, `OI_INVALID` (unknown mass, never 0),
  `ZERO_OI_NO_EXPOSURE`, `IV_MISSING_OR_NONPOSITIVE`, `IV_OUT_OF_RANGE_PERCENT_SUSPECT`.
- A duplicate identity, a malformed time, a receipt that precedes valuation, a non-SPY
  document or an unbound receipt rejects the packet (`INPUT_REJECTED`). The run still
  emits a complete manifest and report.

## What is compared (protocol order)

1. **Native behavior first.** GRID `DealerGammaEngine.compute_gex_profile` runs with only
   its two DB loaders replaced by packet adapters (r=.05, q=0, integer DTE, prior close).
   Gamma Watch `broker.py:curves` runs from its own source with three disclosed
   substitutions: the wall clock is frozen at valuation, `CONTRACTS` and the feed are built
   from packet rows, and the RTD coverage gates are set to 1.0 (every packet row has direct
   IV). Its gates, rounding, IV pairing, 20:00Z expiry and r/q are untouched; `build_feed`'s
   missing-IV recovery is measured as an attribution factor instead. Native results are
   `NOT_COMPARABLE` across engines; nonfinite native fields are recorded by path, and each
   engine's native universe omissions are ledgered with OI.
2. **Per contract at the packet spot**, against the P2-A 80/110-digit Decimal reference:
   GRID's shared primitive and the P2-A Gamma Watch kernel. The protocol tolerance is
   primary; the P2-A propagated bound is reported alongside. Expiry subtotal, strike
   subtotal and total are checked at $0.01 + 1e-8 x gross.
3. **Engine-level curve** on the common grid (Gamma Watch's 735..790 by 0.25, recentred if
   spot is outside its gate). The engines are GRID's `_gex_at_spots_vectorized` (unit x S x .01)
   and Gamma Watch's actual `iv/d1/gamma/net` statements (x 1e9, unrounded; r/q
   parameterized as in P2-A). The curve reference is an independent float64 + `fsum`
   recompute, certified per contract against the Decimal reference (max relative error at
   most 1e-10, else `INDETERMINATE`). Roots are bracketed on the grid and bisected to $0.001;
   root sets match only with equal counts and separation of at most $0.01.
   Sign-indeterminate brackets give `INDETERMINATE`.
4. **Walls**: max positive call exposure, most negative put exposure and max absolute net,
   with ties within tolerance kept as sets. GRID per-strike is compared to the reference.
   Gamma Watch computes no walls (`NOT_SUPPORTED`).
5. **One-factor attribution** from the common baseline: r, q, integer DTE, the fixed 20:00Z
   expiry, prior-close spot, paired-OTM IV and Gamma Watch's strike window. The combined
   native sets report an interaction residual, never apportioned. 0DTE and OI vintage are
   reported as unsupported or not applicable.
6. **Vendors** (classes 2/3, never pooled): class 2 eligible packets are 0, so
   `NOT_SUPPORTED`. ZeroGEX, Cboe's own per-contract greeks (a diagnostic) and RTD GAMMA are
   `NOT_COMPARABLE`. Paid vendors stay off (owner ask).

Collector source drift (pinned AST fingerprints, Python-version stable) makes the affected
engine `NOT_SUPPORTED`, and the class-1 status can then not be a full PASS. Statuses:
`INPUT_REJECTED` only when `normalize()` rejects the packet; an engine fault or nonfinite
engine output on valid input is that engine's `FAIL_NUMERICAL` (other engines keep their
results); a harness fault on valid input is `INDETERMINATE` with the error recorded. GRID
primitive rows below its `T_MIN` clip are `NOT_SUPPORTED`. Timing is `INDETERMINATE` even with
a grid-svr receipt (server clock health is not evidenced), and Cboe's document timestamp is
cross-checked against the receipt with any zone conflict recorded.

## Output (create-only)

`input/cboe_raw.json`, `input/receipt.json`, `packet.json`, `exclusions.json`,
`results.json`, `REPORT.md`, `manifest.json` (hashes of every file, code hashes, environment,
tolerances, class counts). Everything is serialized before the directory is created. An
existing directory is refused untouched. Replay is byte-deterministic.

No HTTP, SSH, broker, SQLite or PostgreSQL access. No collector import, timer, deployment or
write. The frozen GEX-levels v1 log and its pinned code are not read, imported or written.
