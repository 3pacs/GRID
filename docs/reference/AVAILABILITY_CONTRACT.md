# Availability and provenance contract

**Status:** adopted 2026-09-17 for all new and remediated analytical outputs. Enforced by
`store/availability.py` helpers, `store/observations.py` (status-aware `raw_series` reads),
`tests/test_raw_series_read_guard.py`, and the per-module source-guard tests introduced by
the remediation PRs (#534, #535, …). Background: `docs/audits/fake-data-2026-09-17/`.

## The rule

A number in an API response is one of exactly three things, and the payload says which:

| kind | how it is marked | example |
|---|---|---|
| **observation** | `available: true`, `provenance.source`, `as_of` = the data's own date | a close from `raw_series` with its `obs_date` |
| **model / assumption** | `provenance.estimated: true` + `basis` | Black-Scholes price with `basis: "sigma=0.25 r=0.05"`; a 50/30 split with `basis: "assumed_split_50_30"` |
| **unavailable** | `available: false`, `status: "unavailable"`, `reason`, every measured field `null`, `as_of: null` | `"no VIX close series in resolved_series"` |

There is no fourth kind. In particular these are **defects**, not defaults:

- `x if x else 0.5` / `or 50` / `"NEUTRAL"` / `"moderate"` when the column is NULL — a midpoint is
  byte-identical to a measured midpoint. Use `measured_or_none()` and serialise `null`.
- `as_of = now()` / `generated_at = new Date()` on an empty, stale or synthesised payload.
- `"confidence": "confirmed"`, `"credibility": "hard_data"`, `"status": "healthy"`, `"LIVE"` written as
  literals per code path instead of derived from the row's provenance.
- returning `value: 0` for a missing observation (a FAILED pull marker, an unmatched ticker, an
  empty list) — `0` is an observation of nothing, not the absence of an observation.
- silently substituting a related quantity under the original label (latest chain for a historical
  `snap_date`, SPY for an unmapped target, 31-day high as `high_52w`, adjusted close as raw).
- a `confidence` / `probability` / `expected_edge_pct` / `backtest` number that is a tuned constant.
  Rename it (`heuristic_rank`, `prior_weight`) and ship its inputs, or return `null`.

## What consumers must do with `unavailable`

- **Serve it** (HTTP 200 with `available: false`, or 503 for a whole-endpoint failure). Never
  convert to a neutral score, never sort it as 0/0.5 (`mean_of_available()` shows the pattern).
- **Do not cache or persist it as an observation** (`cacheable()`): no TTL-cache pin, no snapshot-table
  row, no `sector_health_snapshots` / `analytical_snapshots` write.
- **Render it honestly:** `--`, `UNAVAILABLE`, `no data`, `confidence unknown`. Never `neutral`,
  `stable`, `0%`, `$0`, `50`.

## Reading source tables

- `raw_series` is a pull log: filter `pull_status = 'SUCCESS'`, collapse vintages
  (latest `pull_timestamp` per `obs_date`), bound by `as_of` — i.e. read through
  `store/observations.py`. The guard test blocks new unfiltered reads.
- `resolved_series` has vintages too: `ORDER BY obs_date DESC LIMIT 1` is not enough; use
  `store/pit.py` (`DISTINCT ON`) or order by `vintage_date` explicitly.
- Series ids are what the puller wrote (`T10Y2Y`, not `FRED:T10Y2Y`). A wrong id reads as
  "no data" and masks the real value — verify the id exists before shipping a reader.
- Cross-series arithmetic (spreads, ratios, net liquidity) aligns on `obs_date` and forward-fills
  only with an explicit label; it never index-pairs two series with different calendars.

## Checklist for a remediation PR

1. Every fabricated fallback replaced by `unavailable(reason, **fields)` or `null` fields.
2. Every model/assumption number carries `basis` (+ `estimated: true`).
3. `as_of` comes from the data; `freshness()` used for staleness, `stale: null` when unknown.
4. Caches and snapshot writers skip `is_unavailable(payload)`.
5. Downstream consumers (grep `pwa/src`, `api/`, `intelligence/`, `alerts/`, `scripts/`) updated so a
   `null` cannot become a number; sorts put `null` last.
6. Tests: missing data, failed-zero rows, stale inputs, conflicting vintages, partial coverage, unit
   mismatch, and a source guard that greps for the retired literal.
7. UI: vitest renders the unavailable payload; for important views, a browser check against a mock API.
