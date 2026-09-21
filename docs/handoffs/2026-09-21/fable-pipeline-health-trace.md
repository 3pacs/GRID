# Pipeline-health latency trace — 2026-09-21

Bounded dev-only trace, no production access. Base commit `92b8c743` (`origin/main`).
Branch `fable/pipeline-health-trace-20260921`.

## 1. Call-by-call trace: `pipeline_health()` in `api/routers/system.py:556-814`

All work happens inside one `with engine.connect() as conn:` / `try` block (568-793).
Each statement below runs sequentially on the same connection; none is async, none has
its own timeout (before this change) — they all share the engine-wide
`statement_timeout=120000` set at connect-string level in `db.py:271-283`.

1. **`source_rows`** (576-609) — the request-path query. `SELECT sc.name,
   COALESCE(latest.last_pull, sc.last_pull_at), COALESCE(recent.recent_rows,0),
   COALESCE(series.series_count,0) FROM source_catalog sc` with three
   `LEFT JOIN LATERAL` subqueries per `source_catalog` row (48 rows —
   `_SOURCE_TYPE_MAP` has 48 entries):
   - `latest`: `SELECT rs.pull_timestamp FROM raw_series rs WHERE rs.source_id = sc.id
     ORDER BY rs.pull_timestamp DESC LIMIT 1`
   - `recent`: `SELECT COUNT(*) FROM raw_series rs WHERE rs.source_id = sc.id AND
     rs.pull_timestamp >= NOW() - INTERVAL '48 hours'`
   - `series`: `SELECT COUNT(DISTINCT sampled.series_id) FROM (SELECT rs.series_id FROM
     raw_series rs WHERE rs.source_id = sc.id ORDER BY rs.pull_timestamp DESC LIMIT
     :series_limit) sampled`, `series_limit = _SERIES_COUNT_SAMPLE_LIMIT = 50_000` (line 52).
   No guard, no per-statement timeout. **This is the query that logged the
   `QueryCanceled` at line 787** (the `log.warning` call sits in the `except` that
   wraps this whole block; the `[SQL: SELECT sc.name, ... FROM source_catalog sc LEFT
   JOIN L…` in the journal is this exact statement, truncated).
2. Per-row Python loop (611-698): classifies `status`/`freshness` from `age_hours` vs.
   `_SOURCE_SCHEDULE` thresholds, builds a `FieldRecord` (`measured_field`/
   `unavailable_field`, from `store/availability_fields.py`), computes `next_scheduled`.
   Pure Python, no DB calls, negligible cost.
3. **`cov_rows`** (701-713) — coverage-by-family: `feature_registry` LEFT JOIN LATERAL
   `EXISTS(SELECT 1 FROM resolved_series WHERE feature_id = fr.id LIMIT 1)`, one
   `EXISTS…LIMIT 1` probe per `feature_registry` row. Bounded by construction (LIMIT 1
   per probe); not implicated in the timeout.
4. **`err_rows`** (727-742) — `SELECT ... FROM server_log WHERE level IN ('ERROR',
   'CRITICAL') ORDER BY created_at DESC LIMIT 25`, wrapped in its own
   `try/except Exception as exc: log.debug(...)` — failure here is swallowed and does
   not fail the request. Not implicated.
5. **Resolver status** (745-784), own `try/except` (swallowed on failure, same as #4):
   - pending: `WITH recent_raw AS (SELECT series_id FROM raw_series WHERE
     pull_status='SUCCESS' ORDER BY id DESC LIMIT :pending_limit) SELECT COUNT(DISTINCT
     rr.series_id) FROM recent_raw rr LEFT JOIN feature_registry fr ON fr.name =
     rr.series_id WHERE fr.id IS NULL`, `pending_limit =
     _RESOLVER_PENDING_SAMPLE_LIMIT = 100_000` (line 53) — ordered by `id DESC` (PK),
     so this is a fast reverse index scan bounded to 100k rows, unlike #1's per-source
     fan-out.
   - `last_run`: `SELECT MAX(vintage_date) FROM resolved_series WHERE vintage_date <=
     CURRENT_DATE` (clamped to avoid forward-dated FRED vintages, see comment 764-769).
   - `last_resolved`: `COUNT(*) FROM resolved_series WHERE vintage_date >=
     CURRENT_DATE - INTERVAL '1 day' AND vintage_date <= CURRENT_DATE`.
6. Outer `except Exception as exc` (786-793): `log.warning("Pipeline health query
   failed: {e}", ...)` — **this is line 787** — then
   `query_failed_reason = _classify_query_failure(exc)` (410-425): "timeout" or
   "connection"/"could not connect" in the message → `STALE_FETCH_FAILED`; "does not
   exist"/"column"/"relation"/"no such table" → `STALE_CONSUMER_QUERY_MISMATCH`;
   else → `STALE_UNKNOWN`. A `psycopg2.errors.QueryCanceled: ... statement timeout`
   message contains "timeout" → classified `fetch_failed`, matching the observed
   `stale_reason='fetch_failed'`.
7. Summary/response build (795-814) always runs (outside the `try`), using whatever
   partial `sources`/`coverage`/`recent_errors` existed at the point of failure — for a
   failure in step 1, that's the empty initializers from 567-570, i.e. 0 sources.

**Conclusion:** step 1 (`source_rows`) is the sole request-path query with no bound of
its own; it is the one that was cancelled. Steps 3-5 are either LIMIT-bounded per-probe
or individually swallow their own exceptions.

## 2. Why the cancelled statement scans `raw_series`

Indexes on `raw_series` (`schema.sql:49-66`): `uq_raw_series_composite (series_id,
source_id, obs_date, pull_timestamp)`, `idx_raw_series_series_obs (series_id, obs_date
DESC)`, **`idx_raw_series_source_pull (source_id, pull_timestamp DESC)`**,
`idx_raw_series_pull_status (pull_status)`, `idx_raw_series_obs_date (obs_date DESC)`,
`idx_raw_series_series_id (series_id)`, `idx_raw_series_pull_timestamp (pull_timestamp
DESC)`.

`idx_raw_series_source_pull` exactly matches the `latest` and `recent` LATERAL
predicates (`WHERE source_id = sc.id [AND pull_timestamp >= ...] ORDER BY
pull_timestamp DESC`), and both queries only need columns already in the index
(`pull_timestamp` for the value, `source_id`/`pull_timestamp` for the predicate/count)
— so those two can be answered as **index-only** scans and are not the bottleneck.

`series` is different: it needs `series_id`, which is **not** in
`idx_raw_series_source_pull`. The planner can still use that index to walk the 50,000
(`_SERIES_COUNT_SAMPLE_LIMIT`) most-recent rows per source in `pull_timestamp DESC`
order, but for every one of those rows it must do a heap fetch to read `series_id`
(a non-covering index scan). With 48 sources × up to 50,000 rows = up to 2.4M heap
fetches against a table `pg_stat` reports at ~1.94B live rows — the coordinator's
number implies roughly ~510GB+ on disk going by the `idx_raw_series_pull_timestamp`
comment at `schema.sql:63-64` ("1.93B rows / 510 GB as of 2026-09-11"). At that size the
table cannot be fully cached, so a large fraction of those heap fetches are random
physical I/O rather than buffer hits — that is what the planner "cannot bound": the
*count* of rows read is bounded (50,000/source), but the *cost per row* is not, because
no index covers `series_id` alongside `(source_id, pull_timestamp)`. A composite index
`(source_id, pull_timestamp DESC) INCLUDE (series_id)` (or adding `series_id` to
`idx_raw_series_source_pull` directly) would make this an index-only scan too and
remove the heap-fetch fan-out; that is not present in `schema.sql` or any migration in
this tree.

## 3. History

- **Origin of the current 3-LATERAL-join shape (`git log -S`):** commit `676438d8`
  ("Add Dad ticker dashboard and warm visualization cache", 2026-06-17), which
  *replaced* a simpler, even-more-expensive form:
  `SELECT sc.name, MAX(rs.pull_timestamp), COUNT(*) FILTER (...), COUNT(DISTINCT
  rs.series_key) FROM source_catalog sc LEFT JOIN raw_series rs ON rs.source_id =
  sc.id GROUP BY sc.name` — an unbounded `LEFT JOIN` + `GROUP BY` over the entire
  table with no sampling limit at all. `676438d8` introduced
  `_SERIES_COUNT_SAMPLE_LIMIT`/`_RESOLVER_PENDING_SAMPLE_LIMIT` and the LATERAL/sample
  structure specifically to bound the scan. So today's query is already a prior
  performance fix; it degraded again as `raw_series` grew past the point where a
  50,000-row-per-source *sample* (48 × 50,000 heap fetches) is itself expensive.
- **Changed by #586: NO** (for the SQL). `git show 28b536df -- api/routers/system.py`
  (28b536df = "feat(contract): adopt FieldRecord in system freshness/pipeline_health",
  merged as part of #586 = `92b8c743`, alongside `721992fd` = "feat(store): availability
  and provenance contract for analytical outputs") shows the diff touches only: the
  `FieldRecord`/`measured_field`/`unavailable_field` construction per source (added
  after the existing per-row loop, `last_pull_utc` extraction), the new
  `_classify_query_failure()` helper, and the outer `except` block gaining
  `query_failed_reason = _classify_query_failure(exc)` plus
  `availability=...`/`stale_reason=...` on the returned response. **The `source_rows`
  SQL text itself (lines 576-609) is byte-for-byte unchanged by this diff** — `676438d8`
  is still its most recent content change. What #586 *did* change is externally
  visible behavior: before it, a query failure here fell through silently to a `200`
  with `sources: []` (indistinguishable from "zero sources configured"); after it, the
  same failure now surfaces as `availability: "unavailable"`,
  `stale_reason: "fetch_failed"` — which is exactly the shape the coordinator observed
  in production. So #586 is why the *failure is now visible and classified*, not why
  the query got slow.
- **`source_catalog.last_pull_at` / #582:** `ingestion/scheduler.py:1087` defines
  `run_daily_pulls()`, the function wired to the scheduled pull path. A comment at
  `ingestion/scheduler.py:1283-1288` (added by #582, PR "fable/hermes-repair-bound",
  merge commit `266b9235`) states that `run_daily_pulls` never used to update
  `source_catalog.last_pull_at`, leaving it stuck stale; the fix adds `UPDATE
  source_catalog SET last_pull_at = NOW() ...` at `ingestion/scheduler.py:1307`. The
  `source_rows` query's `COALESCE(latest.last_pull, sc.last_pull_at)` (line 579) uses
  `sc.last_pull_at` as a **fallback** when the per-source `raw_series` LATERAL finds
  nothing — it is not "compensating" for #582's bug in the sense of masking it; rather,
  once grid-scheduler is restarted with #582's fix, `sc.last_pull_at` becomes a
  materially more trustworthy, *O(1)*-to-read column, which matters directly for the
  fix proposal below (§4, option a). The scheduler restart is out of scope here per the
  coordinator's note ("scheduler not yet restarted").

## 4. Fix proposal

Three options, as scoped by the coordinator:

- **(a) Use `source_catalog.last_pull_at` / a cheap per-source watermark instead of the
  LATERAL aggregates.** Real fix: drop the `latest`/`series` LATERAL subqueries
  entirely; have the scheduler (already touching `source_catalog` per #582) also
  maintain `rows_last_pull`/`series_count_estimate` columns on `source_catalog` at
  pull time, so `pipeline_health()` becomes a single `SELECT * FROM source_catalog`
  with no `raw_series` access on the request path at all. This is the correct
  long-term shape (health becomes O(48) instead of O(sources × sample_size)), but it
  is a schema change + scheduler change + backfill, well over the ~60-line/self-contained
  bar for this session.
- **(b) `SET LOCAL statement_timeout` scoped to this query, degrading via the existing
  `fetch_failed` path.** Cheap, and this pattern is already established elsewhere in
  this codebase (`scripts/hermes_health.py:705`, `intelligence/source_quality_ablation.py:592`,
  `scripts/enrich_connections.py:48`, `gem_hunter/gem_hunter.py:598,1024` all do
  `conn.execute(text("SET LOCAL statement_timeout = '...'"))` as the first statement on
  a connection). It does not make the query fast — it makes a slow/degraded
  `raw_series` fail in ~5s instead of ~120s, which the handler already turns into a
  clean `availability: "unavailable", stale_reason: "fetch_failed"` (verified: the
  `"timeout"` substring in a `QueryCanceled` message is classified `fetch_failed` by
  `_classify_query_failure`, line 410-425).
- **(c) Cached/background-computed value with explicit age in `field_record`.** Would
  need a periodic job (Hermes cycle or a small scheduled task) to compute and store the
  same payload, plus an `ingested_at`/age field on the response so operators can see how
  stale the *health report itself* is. Reasonable middle ground between (a) and (b), but
  is new infrastructure (a job + a cache table/row), not a same-session change.

**Recommendation: ship (b) now, plan (a) as the real fix.** (b) is what was actually
committed on this branch (see below) — it is a genuine, if partial, improvement: it
converts a ~120s hang (and the observed *worse-than-120s* client experience — first
probe timed out client-side at 60s while the query kept running server-side, second
probe then hit the already-in-flight 120s cancellation) into a fast, correctly-labeled
degradation. It does **not** make pipeline-health actually report fresh per-source data
under load — that requires (a). (b) also does nothing for the missing covering index
noted in §2; that index would help LATERAL-scan cost but not the fundamentally
still-unbounded-row-count-in-a-1.94B-row-table growth trend, so (a)'s "stop touching
raw_series on the request path" direction is still recommended for a follow-up PR.

**Explicitly unresolved / out of scope:** this does not address the `quote:AAPL`,
`quote:GLD`, or news-widget failures the coordinator flagged separately — those are
unrelated code paths and were not investigated here.

## 5. Change committed on this branch

Since (b) is self-contained and under 60 lines, it was committed here per the
coordinator's allowance:

- `api/routers/system.py`: added `conn.execute(text("SET LOCAL statement_timeout =
  '5s'"))` as the first statement inside `pipeline_health()`'s `with engine.connect()
  as conn:` block, before `source_rows` (~11 lines incl. comment).
- `tests/test_pipeline_health_contract.py`: two new tests —
  `test_source_rows_query_is_bounded_by_a_short_local_timeout` (asserts the `SET LOCAL`
  statement is the first one executed on the connection and carries `5s`) and
  `test_statement_timeout_cancellation_still_degrades_to_fetch_failed` (asserts a
  `QueryCanceled`-shaped message still classifies as `fetch_failed`, not a 500).

`DB_PASSWORD=x PYTHONUTF8=1 python -m pytest tests/test_pipeline_health_contract.py
tests/test_freshness_stale_sources.py -q` → **13 passed** (11 pre-existing + 2 new).

Only `coverage`/`recent_errors`/resolver-status sub-queries (§1 steps 3-5) remain
outside this timeout bound; they were left alone because they already have their own
`LIMIT`s or their own swallowed `try/except` and were not implicated in the incident.
