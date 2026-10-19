# Flows API provenance envelope

**Provenance describes materialization state, not data truth.** `provenance.<table>.unavailable = false`
means the table has rows and its newest observation date is within the table's threshold. It does **not**
say the values are correct, complete, or free of vendor errors. `unavailable = true` means "do not present this
table's contents as current" and says why.

## Why

`api/routers/flows.py` reads `insider_trades`, `congressional_trades`, `dark_pool_weekly`, `etf_flows` and
`junction_point_readings` directly. They are filled only by `ingestion/flow_materializer.py::sync_all`; when that
sync stops (grid-hermes was off from 2026-10-13) the API kept serving the last rows with no signal that they were
old. The envelope makes that visible without changing any existing response field.

## Where it appears

A new top-level key `provenance` is added to the JSON of these endpoints (all under `/api/v1/flows`). Existing
fields are unchanged; cached payloads are copied, never mutated.

| Endpoint | Tables in `provenance` |
|---|---|
| `GET /sectors` | `options_daily_signals` |
| `GET /sectors/{name}/detail` | `insider_trades`, `congressional_trades`, `dark_pool_weekly`, `etf_flows`, `options_daily_signals` |
| `GET /sankey` | `capital_flow_snapshots`, `options_daily_signals` |
| `GET /money-map` | `signal_sources`, `options_daily_signals` |
| `GET /company/{ticker}` | `signal_sources` |
| `GET /junction-points` | `junction_point_readings` |

The lists name the tables each endpoint **actually reads** (verified against the handlers and
`analysis/money_flow.py`). Only `/sectors/{name}/detail` and `/junction-points` read the five materialized
tables directly; `/money-map` and `/company/{ticker}` read the signal tables `flow_materializer` derives them
from, and `/sectors` and `/sankey` read price/option/snapshot tables. Do not read the absence of a table from an
endpoint's envelope as "that table is fine".

## Shape

```json
"provenance": {
  "etf_flows": {
    "table": "etf_flows",
    "row_count": 18420,
    "max_obs_date": "2026-10-16",
    "materialized_at": "2026-10-16T21:05:11.402+00:00",
    "source_last_ingested": null,
    "age_days": 3,
    "stale_after_days": 3,
    "unavailable": false,
    "reason": null
  },
  "generated_at": "2026-10-19T06:40:02.118+00:00"
}
```

| Field | Meaning |
|---|---|
| `row_count` | `count(*)` of the table; `null` for tables not counted on a request path (`signal_sources`, `options_daily_signals`) |
| `max_obs_date` | newest value (never later than today) of the table's observation-date column (`trade_date`, `disclosure_date`, `report_date`, `flow_date`, `obs_date`, `signal_date`, `snapshot_date`) |
| `materialized_at` | `max(created_at)` (`generated_at` for `capital_flow_snapshots`). The materializer upserts, so rows it only updates do not move this; treat it as a **lower bound** on the last sync |
| `source_last_ingested` | newest ingest time of the feeding rows where cheaply derivable with an indexed bound: `signal_sources.created_at` for `source_type` insider / congressional / darkpool (last 30 days), `raw_series.pull_timestamp` for the FRED junction series (`series_id` and `obs_date` bounded, `SUCCESS` only). `null` when not derivable, or when that lookup failed. It never changes `unavailable` |
| `age_days` | UTC calendar days between today and `max_obs_date` (never negative; see "Future-dated dates") |
| `future_dated_rows` | `congressional_trades` only: number of rows whose `disclosure_date` is after today (pending statutory disclosures); `null` for every other table |
| `stale_after_days` | the table's threshold |
| `unavailable`, `reason` | see below |
| `generated_at` | when the envelope was computed (probes are cached for 30 s) |

## Thresholds and reasons

- **Daily tables: 3 calendar days** (`insider_trades`, `congressional_trades`, `etf_flows`,
  `junction_point_readings`, `signal_sources`, `options_daily_signals`, `capital_flow_snapshots`).
- **Weekly tables: 35 calendar days** (`dark_pool_weekly`). `report_date` is the Monday of the FINRA reporting
  week and FINRA publishes about two weeks after the week ends (`ingestion/altdata/dark_pool.py`), so a healthy
  table's newest `report_date` is about 21 days old at publication and up to about 28 just before the next
  weekly release; 35 leaves a week of slack. A shorter threshold (the first draft used 10) would flag a healthy
  pipeline as stale permanently. If the feed ever moves to the longer Tier-2 lag (about 4 weeks), raise it.
- Age is measured on `max_obs_date`. Event tables with natural lulls (insider trades, disclosures) can read as
  stale over a long holiday weekend; that errs on the side of "do not call it current".

`unavailable = true` with `reason`:

| reason | when |
|---|---|
| `empty` | no rows, or no non-null observation date |
| `missing_table` | the table does not exist |
| `stale` | `age_days` > `stale_after_days` (`row_count`, `max_obs_date` and the rest are still reported) |
| `only_future_dated` | `congressional_trades` has rows, but every one is dated after today (pending statutory disclosures), so nothing says how recently the table was synced |
| `future_dated` | `max_obs_date` is after today on any other table (or from clock skew). `max_obs_date` is reported clamped to today with `age_days` 0, and the table is not called current |
| `timeout` | a probe exceeded the 5 s `statement_timeout` |
| `error:<ExceptionName>` | any other failure (the endpoint itself still answers) |
| `unknown_table` | a table name outside the helper's fixed list |

Sample, table older than its threshold:

```json
"junction_point_readings": {
  "table": "junction_point_readings",
  "row_count": 4210,
  "max_obs_date": "2026-10-13",
  "materialized_at": "2026-10-13T21:30:07.911+00:00",
  "source_last_ingested": "2026-10-14T05:12:44.100+00:00",
  "age_days": 6,
  "stale_after_days": 3,
  "unavailable": true,
  "reason": "stale"
}
```

Sample, probe timed out:

```json
"etf_flows": {"table": "etf_flows", "row_count": null, "max_obs_date": null, "materialized_at": null,
              "source_last_ingested": null, "age_days": null, "stale_after_days": 3,
              "unavailable": true, "reason": "timeout"}
```

## Future-dated dates (`congressional_trades`)

`congressional_trades.disclosure_date` can lie in the future by design. When the source gives no disclosure
date, `ingestion/altdata/congressional.py::resolve_disclosure_date` returns `transaction_date + 45 days` with
basis `statutory_bound` (the STOCK Act upper bound; PIT-safe because never earlier than the real filing). The
table has no basis column, so the envelope cannot tell those rows apart by basis. A `max(disclosure_date)` would
therefore report a date weeks ahead of today and overstate freshness (a 2026-10-16 receipt showed 2026-11-26).

- `max_obs_date` and `age_days` are computed over rows dated **on or before today** only, so pending rows never
  make the table look current; a table whose only rows are pending reads `only_future_dated`.
- `future_dated_rows` counts the pending rows so a UI can say "N pending statutory disclosures". It is a lower
  bound: a statutory-bound row whose estimated date has already passed is indistinguishable from a reported one.
- `materialized_at` (`max(created_at)`) still shows when rows were last written, pending or not.
- For every table, `max_obs_date` never exceeds today; a later value is flagged `future_dated`, not reported fresh.

## Guarantees and limits

- Read-only, fixed literal SQL (`store/materialization_provenance.py::TABLE_SPECS`), values bound; each probe in
  its own transaction under `SET LOCAL statement_timeout = 5000`. `raw_series` is only read with `series_id` and
  `obs_date` bounds.
- The envelope never raises into an endpoint and never alters, removes or reorders existing fields.
- It does not make the endpoint's numbers right: a table can be "available" and still wrong, and an
  `unavailable` table's rows are still served in the existing fields. Consumers decide how to present that.
- Adding a table: add a `TableSpec` (literal SQL returning `count, max_obs_date, materialized_at`), a cadence, and
  list it in the endpoint's `_PROV_*` tuple in `api/routers/flows.py`.
