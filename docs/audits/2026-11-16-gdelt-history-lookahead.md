# GDELT event features: historical rows to correct (proposal, nothing written)

Status: **Anik chose option B (2026-11-16).** The script is `scripts/rebuild_gdelt_history.py` (`plan` is read-only; `apply` snapshots, copies, deletes and rebuilds in one transaction). Nothing is written until `apply` runs on Anik's word. Measured since: GDELT 1.0's daily file equals the day's 96 GDELT 2.0 files on 2016-03-15 (239,103 ids) and 2025-06-10 (140,233) as well as 2026-11-15; GDELT published nothing from 2025-06-14 17:45 to 2025-07-01, so those days stay empty. The measurements were read-only against griddb on grid-svr, 2026-11-16 (PGOPTIONS `default_transaction_read_only=on`).

Context: slice 2 of moving GDELT onto its bulk files. Slice 1 (3pacs/GRID#1232) loads 2026-03-20 onward, dated by publication. This document covers the rows already in `resolved_series`.

## What is wrong

Thirteen daily features carry **23,270 rows**, every one dated `release_date = vintage_date = obs_date`:

- the 12 from `scripts/parse_gdelt.py`: `gdelt_total_events`, `gdelt_conflict_count`, `gdelt_cooperation_count`, `gdelt_avg_goldstein`, `gdelt_avg_tone`, `gdelt_num_mentions`, `gdelt_events_{us,cn,ru,ir,tw,ua}`;
- `gdelt_article_count` from `scripts/migrate_and_load.py`.

No GDELT row has a retraction.

**1. Lookahead.** GDELT publishes the daily file for day D at about 07:00 UTC on D+1. HEAD requests on nine files from 2014 to 2026 all return Last-Modified D+1 07:00 (the 2013-04-01 file is a 2014 re-upload). So none of these values was knowable on D.

The rows were also backfilled long after the fact:
- the `GDELT_BULK` features were registered 2026-07-12;
- `gdelt_conflict_count` and `gdelt_article_count` were registered 2026-03-21/22.

**2. The values themselves are wrong in most groups.**

| group | rows | what is wrong |
|---|---:|---|
| G1 `gdelt_events_*`, `GDELT_BULK`, all dates | 11,268 | All 0. `parse_gdelt.py` compared CAMEO actor codes ("USA") with "US". The real count for 2026-03-18 was USA 37,083, IRN 12,566. |
| G2 six non-country features, `GDELT_BULK`, 2014-01-03 .. 2023-12-31 | 6,414 | Not daily totals. No files before 2024 were loaded, so these days hold only late reports (events with an earlier SQLDATE) found in the 2024+ files: 24 to 1,193 events a day. |
| G3 six non-country features, `GDELT_BULK`, 78 days in 2024-01-01 .. 2026-03-19 | 468 | Also only late reports, 365 to 3,285 events. `overnight_bulk.sh` downloaded days 1–28 of each month only (62 days), and 2025-06-14..06-28 plus 07-01 never downloaded (16 days). |
| G4 `gdelt_conflict_count`, `GDELT` source, 2024-01-01 .. 2026-03-19 | 731 | All 0: the GKG theme match in `migrate_and_load.py` never hit. These rows took the slot ahead of `GDELT_BULK` for those days, so **the feature has no correct value anywhere in 2024 through 2026-03-19** (731 zeros plus the 78 G3 days). |
| G5 five non-country features (not conflict), `GDELT_BULK`, the other 731 days of 2024-01-01 .. 2026-03-19 | 3,655 | Roughly right but grouped by SQLDATE. That adds late reports arriving up to a year later and leaves out about 2% of the day's own events (2025-06-10: 138,051 vs 140,233 in the file). Not knowable at D+1 either. |
| G6 `gdelt_article_count`, `GDELT` source, 2024-01-01 .. 2026-03-19 | 731 | A real count (lines in GDELT 1.0's daily GKG file), dated D instead of D+1. |
| G7 after 2026-03-19 | 3 | `gdelt_conflict_count` 2026-04-04 = 76,725 and `gdelt_article_count` 2026-03-21 = 76,725 (the same number in two features); `gdelt_avg_tone` 2026-04-07 = 0 (`GDELT_NEWS`). |

Total 23,270. Only G5 and G6 (4,386 rows) have values worth keeping, and both are misdated.

## Who used them

- **Model.** `model_registry` `regime_xgboost_20260325-061629` (CANDIDATE, 2026-03-25) trained on `gdelt_article_count`, `gdelt_avg_tone` and `gdelt_conflict_count`. The last was constant 0 on 731 of its days. **Its evaluation should be treated as stale.**
- **Oracle predictions.** 252 `oracle_predictions` rows from 2026-03-26 to 04-04 cite `gdelt_article_count` (492 mentions) or `gdelt_conflict_count` (12) in their signals:
  - `news_energy`: 67 hit, 41 miss, 21 partial;
  - `options_flow`: 66 hit, 36 miss, 21 partial.
  These are journal records and stay untouched. Their cited values live in their own `signals` JSON.
- **Live reader.** `alpha_research/conviction_scorer.py` reads the latest `gdelt_article_count`.
- **One-off research scripts.** `scripts/granular_inflection_analysis.py` and `scripts/reverse_engineer_10000pct.py` list `gdelt_article_count` and `gdelt_conflict_count`.
- **No other references.** None found in `hypothesis_registry.feature_ids`, `discovered_hypotheses`, `backtest_results.report_json`, `feature_importance_log` or `regime_state_vectors`.

## Clean replacement source (already on disk)

- **Files.** `/data/gdelt/v1/events` on grid-svr holds GDELT 1.0 daily export files from 2013-04-01 to 2026-04-07: 4,733 files, none missing in 2024–2026 (810).
  - Measured on 2026-11-15: GDELT 1.0's daily file D is exactly the 96 GDELT 2.0 files stamped D (same 61,540 event IDs).
  - So a rebuild from it is on the same day basis as slice 1 (`day_basis = 'dateadded_v2'`), joins its rows without a seam, and counts countries by CAMEO code.
- **Dates.** Each day's `release_date` comes from a HEAD on the file's URL (Last-Modified, about 07:00 UTC on D+1): 4,733 requests, roughly 15 minutes.
- **No source for some rows.** G4 (conflict from GKG themes) has no event-file equivalent. The event-file `gdelt_conflict_count` (QuadClass 3+4) replaces it on the same name, which is what `parse_gdelt.py` meant it to be. G6 has a source in `/data/gdelt/v1/gkg`, but slice 3 re-designs the GKG features, so this proposal retracts G6 rather than rebuilding it.

## The choice

`store.pit` hides a retracted row only for reads `as_of` on or after the retraction. That is by design (`resolved_retractions_20260927`): replays reproduce what was served. The catch is that an append-only fix can't take the same-day lookahead out of backtests dated before the repair day.

**Option A — append-only (GRID's established path, as in `repair_full_panel_vintages.py`).**
- Steps:
  1. Retract all 23,270 rows (run tag `gdelt-history-20261116`).
  2. Insert the rebuilt history: 2013-04-01 .. 2026-03-19, 12 features, about 4,734 days × 12 ≈ 56.8k points, with `release_date = vintage_date =` the date of each file's Last-Modified (D+1).
- These are new vintages (D+1), so they coexist with the legacy D vintages.
- Effect:
  - Reads from the repair day on see only the rebuilt values.
  - Replays `as_of` before the repair day see the rebuilt value for every `obs_date < as_of`.
  - But they still see the legacy row for `obs_date = as_of`, which is the same-day lookahead and, for G1/G4, a zero.
  - FIRST_RELEASE replays keep the legacy rows outright.

**Option B — remove the legacy rows (owner action), then insert the same rebuilt history.**
- Steps:
  1. `DELETE` the 23,270 rows. No retraction row references them, so the FK allows it. Keep a full copy first in `resolved_series_gdelt_legacy_20261116` (a copy, not a view) so the evidence stays.
  2. Insert as in A.
- Effect: every replay and backtest, under both vintage policies, sees only publication-dated values.
- Cost: exact replay of what GRID served from these rows is lost. The decision journal and the 252 oracle predictions keep their own copies of what they cited.

**Recommendation: B.**
- The point of slice 2 is that backtests stop seeing GDELT before it was published, and A can't do that.
- A deletes nothing and leaves the 2024–2026 country history as zeros in every replay.
- A is the fallback if you'd rather keep the append-only rule without exception.

Either way:
- **What gets written:** retractions or deletions, plus `resolved_series` inserts and new `gdelt_daily_summary` rows for days < 2026-03-20, through a reviewed script. The script is dry run by default, `--apply --run-tag` required, and it prints these counts again before writing.
- **What is never touched:** `offline_research_proof.py`, the decision journal, and `oracle_predictions`.
- **Legacy summary rows:** the `gdelt_daily_summary` rows for 2014–2026-03-19 (SQLDATE basis) would be superseded by `dateadded_v2` rows. B replaces them; A keeps them and adds nothing (one row per date).

## Whether the data/thesis got stronger or weaker

**Weaker, for what was claimed.**
- No backtest, model or hypothesis that used these features before today had real geopolitical event data for the country counts or for `gdelt_conflict_count` in 2024–2026.
- The other series were dated a day early and carried late-report-only crashes on 78 days.
- The CANDIDATE regime model's GDELT inputs were partly constant.

**Stronger, for what can be tested next.** The rebuild gives 13 years of publication-dated daily event counts. Any thesis that used these features needs a re-run on the rebuilt history before its result counts.

## Reproduce the counts

The read-only queries are in the PR description.
