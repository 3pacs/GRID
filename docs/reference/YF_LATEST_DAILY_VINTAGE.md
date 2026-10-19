# `public.yf_latest_daily_vintage` — latest Yahoo daily vintage per day

Migration: `migrations/versions/yf_latest_daily_vintage_20261019.py`.
Refresher: `scripts/refresh_yf_latest_vintage.py`.
Timer (template, not installed): `deploy/systemd/grid-yf-latest-vintage.{service,timer}.template`.

## What it is

A materialized view with **one row per `(series_id, obs_date)`** for every
`ingestion.yfinance_pull.YF_TICKER_LIST` ticker x the six fields (`open`, `high`,
`low`, `close`, `volume`, `adj_close`), `obs_date >= 2026-01-01`: the latest
`SUCCESS` vintage (greatest `pull_timestamp`, then `id`) of `source_id = 2`.

| column | meaning |
|---|---|
| `series_id`, `ticker`, `field` | `YF:<ticker>:<field>` and its parts |
| `obs_date` | observation date |
| `raw_series_id`, `pull_timestamp`, `value` | the winning `raw_series` row |
| `capture_state` | `raw_payload->>'capture_state'` (`INCOMPLETE` / `UNKNOWN` / `POLICY_ELIGIBLE`); `NULL` on legacy unmarked rows (pre-PR #853) |
| `capture_policy` | `raw_payload->>'capture_policy'` |
| `policy_marked` | `raw_payload ? 'capture_policy'` |
| `repair_run_tag` | `raw_payload->>'repair_run_tag'` (set by the 2026-10 repair phases) |

Unique index `uq_yf_latest_daily_vintage_series_obs (series_id, obs_date)`
makes `REFRESH ... CONCURRENTLY` possible.

The ticker list is a literal `VALUES` list generated from `YF_TICKER_LIST` **when
the migration runs**. A ticker added later is not in the view until it is rebuilt
(new migration).

## What it is not

**A convenience cache, never a point-in-time source.** It keeps only the
*latest* vintage, so it cannot say what was known at an earlier time. Any
`as_of` read (inference, backtests, the decision journal, anything under the PIT
rules in `CLAUDE.md`) must still read `raw_series` through
`store/observations.py` (`read_latest`, `read_window`, `as_of=` / `as_of_ts=`).
It is also only as fresh as its last refresh (`SELECT max(pull_timestamp)` tells
you how stale). Do not join it into API or analytics paths that return numbers
to users without the availability contract (`docs/reference/AVAILABILITY_CONTRACT.md`).

## Cost and refresh

`raw_series` has ~1.93e9 rows (511 GB). The view query is a per-series index
probe (`VALUES` tickers x fields `CROSS JOIN LATERAL` into `raw_series`, using
`idx_raw_series_dedup`), run with `SET LOCAL enable_hashjoin / mergejoin /
seqscan / bitmapscan = off`. It still takes **~100 s per field for 70 tickers,
~10 minutes in total**.

* The migration creates the view `WITH NO DATA` (instant). Reading it raises
  "materialized view has not been populated" until the first refresh.
* First population is run by the operator, by hand:
  `python3 scripts/refresh_yf_latest_vintage.py` (plain `REFRESH`, since
  `CONCURRENTLY` is refused on an unpopulated view).
* Afterwards: daily **06:10Z** via the timer template (after the 05:15Z yfinance
  morning slot and the 05:30Z market diary), `REFRESH MATERIALIZED VIEW
  CONCURRENTLY`: readers are never blocked. `statement_timeout` is 1200 s, one
  transaction, the script logs row count and duration and exits 1 on failure.
  `--dry-run` prints the SQL without connecting.
* The 06:10Z slot overlaps the nightly core `pg_dump` window (03:33–10:05Z
  observed 2026-09-28): the refresh is read-only and runs at `Nice=10` / idle IO.

## Audit queries it replaces

The 2026-10-19 yfinance close audit (`evidence/q7.sql`) answered these with a
~100 s-per-field LATERAL probe each. On the view they are ordinary queries.
Bucket by pull time of day (UTC): `tod < 13:30` pre-open, `< 20:00` intraday,
`< 20:15` near-close, otherwise post.

**L1 — what readers see: bucket of the latest `close` vintage per ticker/day**

```sql
SELECT ticker,
       CASE WHEN policy_marked THEN 'marked_policy'
            WHEN obs_date < (pull_timestamp AT TIME ZONE 'UTC')::date THEN 'later_day'
            WHEN obs_date > (pull_timestamp AT TIME ZONE 'UTC')::date THEN 'future'
            WHEN (pull_timestamp AT TIME ZONE 'UTC')::time < '13:30' THEN 'same_day_pre'
            WHEN (pull_timestamp AT TIME ZONE 'UTC')::time < '20:00' THEN 'same_day_intraday'
            WHEN (pull_timestamp AT TIME ZONE 'UTC')::time < '20:15' THEN 'same_day_nearclose'
            ELSE 'same_day_post' END AS bucket,
       count(*) AS n, min(obs_date), max(obs_date)
FROM public.yf_latest_daily_vintage
WHERE field = 'close' AND obs_date >= '2026-04-05'
GROUP BY 1, 2 ORDER BY 1, 2;
```

**L2 — per `obs_date`: how many unmarked US-listed tickers have an intraday/near-close latest vintage**

```sql
SELECT obs_date, count(*) AS tickers_with_rows,
       count(*) FILTER (WHERE obs_date = pd AND tod < '20:00')                  AS latest_is_intraday,
       count(*) FILTER (WHERE obs_date = pd AND tod >= '20:00' AND tod < '20:15') AS latest_is_nearclose,
       count(*) FILTER (WHERE obs_date < pd)                                     AS latest_later_day
FROM (SELECT *, (pull_timestamp AT TIME ZONE 'UTC')::date AS pd,
                (pull_timestamp AT TIME ZONE 'UTC')::time AS tod
      FROM public.yf_latest_daily_vintage
      WHERE field = 'close' AND NOT policy_marked
        AND ticker NOT LIKE '%-USD' AND ticker NOT LIKE '%=X' AND ticker NOT LIKE '%=F'
        AND ticker NOT IN ('^VIX','^VIX9D','^VIX3M','^VIX6M','DX-Y.NYB')) l
GROUP BY 1 ORDER BY 1;
```

**L3 — per field: the same mechanism for all six fields of a ticker**

```sql
SELECT ticker, field, count(*) AS dates,
       count(*) FILTER (WHERE obs_date = pd AND tod < '20:00') AS latest_intraday,
       count(*) FILTER (WHERE obs_date < pd)                   AS latest_later_day
FROM (SELECT *, (pull_timestamp AT TIME ZONE 'UTC')::date AS pd,
                (pull_timestamp AT TIME ZONE 'UTC')::time AS tod
      FROM public.yf_latest_daily_vintage WHERE ticker IN ('^GSPC','XLK')) l
GROUP BY 1, 2 ORDER BY 1, 2;
```

(The in-view `ORDER BY ... id DESC` tiebreak and the `source_id = 2` / `SUCCESS`
filters match the audit's definition of "latest vintage".)
