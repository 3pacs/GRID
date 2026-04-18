# Session Handoff - 2026-04-18

**Current local repo:** `/Users/anikdang/dev/GRID`  
**Current branch:** `contracts-phase-1`  
**Current head:** `f500f9edcfc8970006b0829d523dc483339e2030`  
**GitHub state at handoff:** `HEAD`, `origin/contracts-phase-1`, and `origin/main` all point at `f500f9ed`.  
**Scope:** Surfacer hardening, paid-source ingestion, options expectation fill, Oracle gap diagnostics, raw-series duplicate cleanup, and frontend status visibility.  
**Result:** Surfacer is stricter and more usable. It blocks adverse histories, shows backend work instead of looking dead, queues missing evidence with dedupe, and has live paid-source/options/backfill jobs still running on `grid-svr`.

---

## Start Here

Use the local repo:

```bash
cd /Users/anikdang/dev/GRID
git status --short
git fetch origin main contracts-phase-1
git rev-parse HEAD origin/contracts-phase-1 origin/main
```

Production runs on `grid@grid-svr`. Runtime files must be deployed to both server trees with:

```bash
python3 scripts/deploy.py --snapshot path/to/file.py
```

For API changes, prefer:

```bash
python3 scripts/deploy.py --snapshot --restart --smoke api/routers/surfacer.py
```

Then verify service state:

```bash
ssh grid@grid-svr 'systemctl is-active grid-api grid-hermes grid-scheduler grid-spider.timer grid-backlinker grid-realtime grid-llamacpp-oracle'
```

At handoff, all seven services returned `active`.

---

## What Shipped

### Surfacer conviction and data requirements

- `f53f0279` - block adverse surfacer track records.
- `41c2891f` - zero blocked surfacer conviction scores.
- `ecbdd853` - speed up surfacer options fill.
- `a9796e5e` - preserve surfacer requirement state.
- `4216d644` - fix surfacer requirement summary count.
- `e55a3714` - harden surfacer gap workers.
- `850f8ee7` - improve oracle gap diagnostics.

Behavioral state:

- Hard-blocked candidates return conviction score `0.0`.
- Coarse/fallback calibration can remain visible but does not inflate conviction.
- Requirement materialization no longer truncates `surfacer_data_requirements`.
- Requirement rows are upserted/preserved; terminal and processing states survive materializer runs.
- Missing-data queue metadata now reports unique requests, raw request objects, queued rows, skipped rows, and request types.
- Backlog dedupe uses a partial unique index for active `surfacer_data_backfill` tasks.
- `scripts/drain_surfacer_oracle_gap_worker.py` exists for one-ticker-at-a-time Oracle gap attempts, with timeout diagnostics written back into requirement payloads.

### Surfacer frontend

- `85cb028b` - show surfacer backend work status.
- `f500f9ed` - avoid lazy surfacer import path.

Frontend state:

- [Surfacer.jsx](/Users/anikdang/dev/GRID/pwa/src/views/Surfacer.jsx) now shows a loud backend-work notice above the KPIs while loading or while missing-evidence metadata is nonzero.
- The notice shows unique gaps, raw requests, newly queued rows, already-active/skipped rows, request types, and last sync.
- [app.jsx](/Users/anikdang/dev/GRID/pwa/src/app.jsx) excludes `Surfacer.jsx` from the lazy `import.meta.glob`, so Surfacer stays statically bundled and the old duplicate static/dynamic import warning is gone.
- Local `npm run build` passed.
- Remote builds passed in both server trees:
  - `/data/grid_v4/astrogrid_dedup/pwa`
  - `/home/grid/grid_v4/grid_repo/pwa`
- Production `index.html` now references `/assets/index-BQDQJpv3.js`.
- `https://grid.stepdad.finance/assets/index-BQDQJpv3.js` returned `200`.
- `service-worker.js` is stamped with cache `grid-1776503503064`.

### Paid source and duplicate hardening

- `1ed27075` - harden paid source surfacer pipeline.
- `d5224816` - dedupe Tiingo raw writes.
- `cda97a89` - harden raw series duplicate guards.
- `cfb3bd43` - finish raw series timestamp dedupe cleanup.
- `637550e3` - add raw series logical dedupe operator.

Paid lanes wired:

- Tiingo price/fundamentals/news.
- TwelveData stats/splits/dividends.
- QuiverQuant insider.
- HuggingFace public financial-news subsets.

Important duplicate work:

- Major raw-series writers now use logical success guards so reruns do not create duplicate observations just because `pull_timestamp` changes.
- `scripts/raw_series_logical_dedupe.py` is dry-run-first and source-scoped. It can later clean duplicates and add a concurrent logical unique index.
- Do not run broad `raw_series` aggregate scans while ingestion is active. Use source-specific probes and job logs.

---

## Live Jobs At Handoff

Checked on `grid-svr` around 2026-04-18 09:13 UTC:

```text
1368478 S   02:10:45 python3 scripts/drain_surfacer_backfill.py --batch-size 1 --sleep 30
1408791 Sl  01:07:29 python3 scripts/pull_surfacer_paid_sources.py --limit 1000 --news-days 30 --news-limit 25 --sleep 0.2 --skip-quiver --skip-hf
1427207 Sl     42:57 python3 scripts/pull_surfacer_options.py --limit 300 --priority-max 1 --sleep 0.4 --ticker-timeout 90 --defer-minutes 60 --reset-stale-minutes 1 --max-expirations 1
```

### Paid-source top-1000

Log:

```bash
ssh grid@grid-svr 'tail -n 80 /tmp/surfacer_paid_sources_1000.log'
```

Latest observed:

- Tiingo was at `903/1000` around 2026-04-18 09:13 UTC.
- Recent insert examples: `GME`, `TTMI`, `HAS`, `FNB` each inserted 72 rows.
- This job skips Quiver and HF because those lanes were already run separately.

### Options expectation fill

Log:

```bash
ssh grid@grid-svr 'tail -n 80 /tmp/surfacer_options_pull_fast.log'
```

Latest observed:

- Worker is still running.
- It is producing `done` rows and clean `deferred` rows for yfinance ticker timeouts.
- Recent completed rows include `AAL`, `PCG`, `SCHF`, `LHX`, `VTEB`, and `MDLZ`.
- Recent deferred rows include `DIA`, `SMH`, `PL`, and `ROP`.
- This process started before the latest deployed transaction/stale-reset improvements, but it is producing useful rows. Let it finish unless it starts looping or writing errors.

### Surfacer backfill drainer

Log:

```bash
ssh grid@grid-svr 'tail -n 80 /tmp/surfacer_backfill.log'
```

Latest observed:

- Running cleanly.
- Processed count reached `86` by 2026-04-18 09:13 UTC.

---

## Known Data State

Active priority-1 missing requirements after the fixed materializer:

- `ticker_direction_calibration`: 959 pending.
- `options_expectation`: 644 pending, 288 processing, 1 no_data.
- Total active desired rows stabilized at `1,892`.
- Full active missing CSV on server:
  - `/tmp/surfacer_missing_requirements_20260418_0842.csv`
- Obsidian report:
  - `/Users/anikdang/grid_obsidian/Reports/Surfacer-Missing-Requirements-2026-04-18.md`

Oracle prediction coverage:

- `oracle_predictions` has many historical rows, but only a limited set of scoreable tickers for Surfacer calibration.
- `scoreable_pending` was observed at `0`.
- The gap is coverage, not unscored backlog.
- One-ticker Oracle runs are still slow because `oracle/engine.py::run_cycle` pays scoring/evolution/trace and serial per-model/ticker work before writes become visible.

---

## Tests And Build Verification

Python verification already run successfully:

```bash
python3 -m py_compile api/routers/surfacer.py oracle/engine.py oracle/run_cycle.py scripts/backfill_surfacer_calibration.py scripts/pull_surfacer_options.py scripts/drain_surfacer_oracle_gap_worker.py tests/test_pull_surfacer_options.py tests/test_surfacer_oracle_gap_worker.py
python3 -m pytest -q tests/test_surfacer_api.py tests/test_pull_surfacer_options.py tests/test_surfacer_oracle_gap_worker.py
```

Result:

```text
24 passed
```

PWA verification:

```bash
cd /Users/anikdang/dev/GRID/pwa
npm run build
ssh grid@grid-svr 'cd /data/grid_v4/astrogrid_dedup/pwa && npm run build'
ssh grid@grid-svr 'cd /home/grid/grid_v4/grid_repo/pwa && npm run build'
curl -I https://grid.stepdad.finance/assets/index-BQDQJpv3.js
curl -s https://grid.stepdad.finance/service-worker.js | rg 'grid-1776503503064|CACHE_NAME'
```

Results:

- Local and remote builds passed.
- No Surfacer duplicate import warning remains.
- Published asset returned `200`.
- Service worker cache name matched `grid-1776503503064`.

---

## Next Useful Work

1. Let live paid-source and options jobs finish, then summarize final counts.
2. Run the Surfacer materializer again after those jobs finish:

```bash
ssh grid@grid-svr 'cd /data/grid_v4/astrogrid_dedup && python3 scripts/backfill_surfacer_calibration.py --limit 1000 --queue-requirements 0'
```

3. Recheck active requirements by type/status and confirm options gap count dropped.
4. Rerun `/api/v1/surfacer/candidates` through an authenticated browser session and verify the backend-work notice is visible when gaps remain.
5. Continue Oracle gap work, but do not use broad full-cycle batches for calibration. Use one-ticker/small-batch workers and add per-ticker commits/logging before trying larger jobs.
6. Fix remaining `TRUNCATE` usage in `scripts/backfill_surfacer_calibration.py` for the other materialized Surfacer tables. `surfacer_data_requirements` is fixed, but universe/ticker/signal/options materialized tables still deserve staged swaps or upserts.
7. Configure an authenticated HuggingFace token on `grid-svr` if available. Public HF works, but auth should improve reliability/rate limits.
8. Eventually run source-scoped raw-series logical dedupe and create the `(series_id, source_id, obs_date)` unique index concurrently.

---

## Do Not Waste Time On

- Do not chase the old Surfacer dynamic import error first. Latest bundle is `index-BQDQJpv3.js`, and Surfacer is statically bundled.
- Do not treat public `/api/v1/surfacer/candidates` returning `Invalid or expired token` as a bug. Auth is required.
- Do not broaden the Oracle run to hundreds of names until per-ticker storage/progress exists.
- Do not run expensive whole-table `raw_series` aggregates during active ingestion.
- Do not mark transient yfinance/options timeouts as terminal `no_data`; use `deferred`.

---

## Current Subagent State

Prior helper agents were closed after their reports were incorporated:

- Darwin: Oracle bottleneck exploration.
- Kant: one-ticker Oracle gap worker implementation.
- Boyle: Surfacer/backfill edge-case review.

No subagent output is pending at this handoff.

