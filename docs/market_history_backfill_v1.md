# Market-history backfill v1 (SPY, BTC, ETH daily closes)

Tool: `scripts/market_history_backfill.py`.
Tests: `tests/test_market_history_backfill.py` (no DB) and `tests/test_market_history_backfill_pg.py` (disposable PostgreSQL).
The tool is operator-run only and is not wired into the scheduler, Hermes or deploy.

## Why

The 2026-09-24 read-only reconciliation (receipts R1, R1b and R1c in the lane's evidence folder) found three gaps:

| Series | Gap | Cause |
|---|---|---|
| `binance.{BTC,ETH}USDT.close` (live puller) | zero rows | 79 zero-row `SUCCESS` runs, 2026-05-27..09-22 |
| `BINANCE:{BTC,ETH}USDT:close` (legacy `scripts/bulk_historical_pull.py`) | nothing after 2024-12-31 (range is 2021-04-01..2024-12-31) | Binance spot archive files switched to microsecond timestamps on 2025-01-01; that script swallows the resulting `ValueError` |
| `YF:SPY:close` (source `yfinance`) | 92 sessions, 2026-05-07..2026-09-18, with no capture pulled after the observation's UTC day ended | only same-day captures exist, so there is no provably final unadjusted close |

Canonical `btc_full`/`eth_full` were never on the Binance basis. They are `yfinance` through 2026-05, then `TWELVEDATA` on weekdays only through 2026-07-10.

## Design guarantees

- **Isolation.** Rows go only to `raw_series`, under `backfill_v1:<shadowed id>`, and under two dedicated `source_catalog` rows: `binance_history_backfill_v1` and `yfinance_history_backfill_v1`, both `active=FALSE` and `pit_available=FALSE`.
  - No entity mapping, `^YF:` pattern, resolver branch or receipt path consumes the namespace.
  - Payloads carry no `close_contract` / `price_contract_version` marker.
  - Nothing is ever written under `binance`/`yfinance`. `api/routers/system.py` derives a source's freshness from its newest `raw_series.pull_timestamp`, so writing there would paint the forward Binance puller green.
- **Known-at.** `pull_timestamp` is the insert transaction time, which is at or after the provider acquisition time recorded in the payload. Rows are never dated back to the observation date.
- **Forward boundary.** Observation dates ≥ `FORWARD_OWNED_FROM` (2026-09-23) belong to the #639 forward cadence and are refused, as is anything newer than run date − 2 days.
- **Finalized bars.**
  - Binance bars must be a completed UTC day (`binance_close_contract.completed_kline_payload`) with consistent OHLC.
  - SPY bars must fall on an NYSE session (`ingestion.market_calendar` plus the 2025-01-09 closure) and be acquired after the observation's UTC day ended.
  - Basis is raw: Binance spot last-trade close in USDT; Yahoo `Close` with `auto_adjust=False`.
- **Coverage.** A date is covered when an existing post-day-end capture in the shadowed identity agrees with the provider. Several writers share `YF:SPY:close`/`yfinance`, including `scripts/fill_missing_features.py` with `auto_adjust=True`. If every post-day-end capture of a date disagrees, the plan blocks unless the operator passes `--fill-basis-conflicts` or `--allow-validation-mismatch`. Both flags are bound into the digest.
- **Plan / apply.**
  - `plan` is read-only against the database. It emits a digest that binds series, sources, dates, values and flags.
  - `apply` requires that digest, a `--max-rows` cap and a plan younger than 72 h, and holds a session advisory lock (single writer).
  - `apply` fetches each monthly chunk again and stops on drift. It commits one short transaction per chunk, so a rerun resumes and is idempotent.
  - A differing stored value stops the run. `--record-revisions` appends a new row instead; it never updates or deletes.
- **Verify.** `verify` is read-only. It checks every planned date is present once with the planned value, that payloads and sources are correct, and that no row is dated before acquisition or before its observation day ended.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | OK |
| 2 | Guard refusal |
| 3 | Lock held |
| 4 | Plan drift |
| 5 | Revision |
| 6 | Incomplete (rerun to resume) |
| 7 | Plan blocked |

## Local proof

Point `GRID_TEST_DB_URL` at a local, disposable `*test*` database, then run:

```bash
python -m pytest tests/test_market_history_backfill.py tests/test_market_history_backfill_pg.py -q
```

## Promotion is a separate decision

Canonical features never read the namespace. Promoting it into `btc_full`/`eth_full`/`spy_full` would first need a known-at guard in consumers that read `resolved_series` without vintage filters. An example is `store/astrogrid.py::_lookup_symbol_price` / `_load_price_path`; otherwise backfilled history would look known at its observation date.
