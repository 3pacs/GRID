# Market-state record (market-state/3)

One machine-readable row per session in `market_state_daily`: every state variable reported against its own trailing
baseline, so each reads as "this far above or below its normal". Code: `intelligence/regime/market_state.py`
(record), `intelligence/regime/baseline.py` (rule trailing-robust/1), `scripts/run_market_state.py` (writer),
`config/econ_release_dates.json` (scheduled events, built by `scripts/build_econ_release_dates.py`).

## Baseline rule (trailing-robust/1)

| Part | Rule |
|---|---|
| Window | the variable's own previous observations, strictly before the one read |
| Window length | daily fast 252 obs (min 120), daily slow 1,260 (min 500), weekly 260 (min 104), monthly 60 (min 36) |
| Robust z | (value − median) / (1.4826 × MAD), capped ±5; IQR/1.349 when MAD is 0; none when both are 0 |
| Percentile | mid-rank in the window, 0..1 |
| Label | normal \|z\|<1 · above/below 1–2 · high/low ≥2 (from the percentile when there is no z) |
| Too little history | `unavailable` with the reason, never a guess |

## Freshness (max-age/1)

market-state/2 adds one rule to market-state/1. An input whose newest observation is older than its maximum age reads
`stale`, not `ok` (#1303). The reading (value, z, label) is kept so a reader can see what was stale, and `freshness`
gives the age, unit, limit and basis. `stale` outranks both `ok` and `no_baseline`, and `coverage.stale` counts it.
`gamma_state` is `stale` when its row is not from the record's own session. Code: `MAX_AGE` in
`intelligence/regime/market_state.py`.

| Inputs | Max age | Why |
|---|---|---|
| VIX, VIX/VIX3M, HY, IG, curve, fed funds, real fed funds, DIX, short volume | 2 sessions | posted the next business day: 1 session old, 2 when a federal holiday falls on a trading day |
| SPY trend, the three news readings | 1 session | read for the session itself; one missed session allowed |
| dealer gamma, spot vs flip, `gamma_state` | 0 sessions | positioning changes every session with open interest and expiries |
| weekly and monthly inputs | period + declared lag − 1 days | claims 12, net liquidity 9, labor 72, CPI and industrial production 80, sentiment 90 |

Sessions are NYSE sessions after the observation, up to and including `as_of`. They are counted on the hash-pinned
XNYS manifests in `docs/reference/calendar-manifests` (the prescreen parts, then `factory_session_calendar.PINS`:
2018-04-02 to 2027-12-31), never on the approximate scheduler helpers in `ingestion/market_calendar.py`. Those
helpers close Juneteenth in 2018–2021, before it existed, and keep 2018-12-05 and 2025-01-09 open, so they miscount
ages in both directions. Each record names the calendar it used in `freshness_calendar`.
- A record for a day after 2027-12-31 is refused until the manifests are extended.
- An observation from before 2018-04-02 gets a lower-bound age (`age_is_lower_bound`).
- The 0-session limit is a same-date rule: dealer gamma is fresh only when its row's date is the record's own date.

The weekly and monthly limits come from
`LAGS`: the next observation is public one period plus its declared lag after the current one, so anything older means
it is overdue or was never published. Documented shutdown releases therefore read `stale`, which is what they are.

The limits were measured on every stored record from 2019-01-02 to 2026-11-19, and no normal reading exceeds them.
Counting on the pinned calendar instead of the scheduler helpers changes the status of none of those stored readings.
Dry runs of the new rule against griddb (2026-11-21, read-only) flag only real gaps:
- 2022-03-28: the news readings, 23 sessions old.
- 2025-12-19: CPI, 109 days old (shutdown).
- 2026-04-08: SPY, 5 sessions old.
- 2026-08-05: claims, 18 days old.
- 2026-10-19: dealer gamma, 4 sessions old.
- 2026-11-20: dealer gamma (the Cboe 403 day) and FINRA short volume, 3 sessions old.

Rows written before market-state/2 carry `rule = market-state/1` and no `freshness`; in them, `ok` never meant fresh.

## Provisional record at the close (#1312, display only)

The decision record for session D is written the evening after D (`grid-market-state.timer`, 23:15Z on D+1), under
#1191's previous-session rule. Anything that decides keeps reading that record. The post-close desk brief (22:20
America/New_York on D) runs before it exists, so a second record, computed at D's close, fills that gap. It is for
display only (owner's decision, bring-up plan rev 100).

- **Table**: `market_state_provisional` (as_of_date PK, rule, record JSONB, computed_at TIMESTAMPTZ, `provisional
  BOOLEAN NOT NULL DEFAULT TRUE CHECK (provisional)`). It comes from the alembic migration
  `migrations/versions/market_state_prov_20261122.py`, with the GRANT footer, and `schema.sql` declares the same
  statement. Production never creates it at runtime: the module's ensure runs on SQLite (tests) only, and on
  Postgres the store refuses to write while the table is missing. `store_market_state` refuses a provisional
  record, so one never lands in `market_state_daily`.
- **Availability cutoff**: the session's close, exactly. The record is `compute_market_state(..., known_by=close)`:
  the same record rule, with every read cut at that instant instead of the end of the UTC day. A rerun hours
  later reads the same inputs, so nothing that reached GRID after the close can appear in it (Codex HOLD on
  #1360). The decision record passes no cutoff and is unchanged. Each read is cut on the clock its store keeps:

  | Read | Cut |
  |---|---|
  | `raw_series` (`read_window_known_at(as_of_ts=)`) | `pull_timestamp` at or before the cutoff, and nothing else. There is no modeled path: a modeled known-at date never admits a row GRID first pulled later. A release override still delays a row until its UTC day has ended. |
  | `fred_vintages` (`load(fetched_by=)`) | `fetched_at` at or before the cutoff. Each version's live interval is rebuilt from the versions known then: it ends the day before the next one, or is open. The stored `realtime_end` is never read, because a later refresh extends it in place. |
  | `dealer_gex_daily` | `created_at` at or before the cutoff; a row without one is not known |
  | resolved `spy_full` | not read. `resolved_series` has no write clock (the resolver stamps `release_date` with the raw pull date), so SPY comes from `YF:SPY:close` by `pull_timestamp`. That is the record's own fallback basis, and `spy_basis` names it. |

  A read with no clock that can prove GRID held a row by the cutoff is not used. A later quarantine
  (`pull_status`) still removes a row; it adds nothing that arrived after the close.
- **Record**: each stored record adds the following:
  - `"provisional": true` and `"decision_use": "display_only"`.
  - A `label`: computed from the inputs GRID held at the close, with nothing that arrived later; the decision
    record is written the next evening.
  - `availability_cutoff`, the writer's clock `computed_at`, and `minutes_after_close`.
  - `source_lineage.availability_cutoff`, which names each read's cut.

  A late run is labelled, not refused. With the cutoff in place it cannot change the inputs, and a fixed limit
  would mark every early-close day late (21:15Z is 195 minutes after an 18:00Z close).
- **Writer**: `scripts/run_market_state.py --provisional`. It targets today's UTC session, and only at or after
  that session's `closes` in the pinned XNYS parts (`factory_session_calendar.PINS`). That close is 21:00Z, 20:00Z
  in summer, or 18:00Z on an early close.
  - Before the close, or on a day that is not a session, it refuses with exit 2.
  - It never takes `--from/--to`, `--as-of` or `--require-target-dealer`.
  - It never refreshes the FRED vintages, whatever flags are passed. `--no-refresh` is accepted and changes
    nothing, and no flag turns a refresh on (`--refresh` is an argparse usage error).
  - It is insert-only; `--replace` rewrites.
  - Its provenance says `mode: provisional`, `decision_use: display_only`, the cutoff, the minutes after the
    close, `vintage_refresh: never` and the calendar version.
  - Timer: `deploy/systemd/grid-market-state-provisional.*.template`, weekdays 21:15Z, `Persistent=false`, with its
    own lock. Installed on grid-svr 2026-11-22 (first run 2026-11-23), with the Python 3.14 drop-in
    `deploy/python314/dropins/grid-market-state-provisional.service.d-py314.conf`.
- **Reader**: only the post-close desk brief, and only while the session's decision record is not there yet
  (`docs/reference/DESK_BRIEF.md`). Every line starts with `PROVISIONAL (display only)` and states the cutoff and
  how long after the close the record was computed. A record whose cutoff is not the session's close is not shown.
- **Guard**: `tests/test_market_state_provisional_guard.py`.
  - Only the writer, this module, `analysis/desk_brief_sources.py` and the migration may name the table (docs aside).
  - Inside those files, only reviewed definitions may name it.
  - No reader of `market_state_daily` names it.
  - The provisional path has no refresh and always passes the cutoff.
  - Only the migration creates the table outside SQLite.
  - The decision writer still refuses today and targets the prior session.
  - `tests/test_market_state_provisional_cutoff.py` holds the delayed-input test.

## Variables (v1)

| Group | Variable | Input | Window |
|---|---|---|---|
| volatility | vix, vix_term (VIX/VIX3M) | CBOE:VIX, CBOE:VIX3M | daily fast |
| credit | hy_spread, ig_spread | BAMLH0A0HYM2, BAMLC0A0CM | daily fast |
| rates | yield_curve, fed_funds, real_fed_funds | T10Y2Y, DFF, DFF − T5YIE | daily slow |
| liquidity | net_liquidity, net_liquidity_4w | WALCL − WTREGEN − RRPONTSYD×1000 | weekly |
| labor | initial_claims; unemployment, payrolls_3m | ICSA; UNRATE, PAYEMS | weekly; monthly |
| inflation / growth | cpi_yoy, industrial_production_yoy, consumer_sentiment | CPIAUCSL, INDPRO, UMCSENT | monthly |
| trend | spy_vs_50d, spy_vs_200d | SPY close (spy_full, else YF:SPY:close) | daily fast |
| flow | short_volume_ratio | finra:short_volume_agg:market_ratio (GRID history from 2025-11) | daily fast |
| flow | dix, dix_change_5d | finra:dix_sp500: S&P 500 dollar-weighted dark-pool short share (`ingestion/altdata/finra_dix.py`, from 2018-08) | daily fast |
| positioning | dealer_gamma, spot_vs_gamma_flip | dealer_gex_daily (SPY; `physics/dealer_gex_daily.py`; registered captures from 2026-09-26) | daily fast |
| gamma_state | regime, gamma_flip, spot_vs_flip | latest dealer_gex_daily row | — |
| news | news_uncertainty, equity_news_uncertainty | USEPUINDXD, WLEMUINDXD: 7-day mean of days t-9..t-3, as FRED showed them on t | daily fast |
| news | news_first_print | USEPUINDXD: newest weekday first print, compared only with past first prints | daily fast |
| events | today, next, within_14d | FRED release dates (CPI, PPI, NFP, GDP, PCE, retail sales) + Fed calendar (FOMC) | — |
| news_shock | elevated_uncertainty / not_elevated | news_first_print z ≥ 2; calendar co-occurrence separate, attribution unverified | — |
| rates | ust_2y_change_5d (market-state/3) | DGS2, change over 5 sessions | daily fast |
| policy | fed_path_next_meeting, fed_path_change_5d (market-state/3) | Kalshi KXFED strike ladder for the next FOMC meeting (`intelligence/regime/policy_path.py`); change within one meeting's contract | daily fast |
| policy | fed_priced_move_next_meeting (market-state/3) | fed_path_next_meeting − DFEDTARU, refused until FRED prints the target after a decision | daily fast |
| policy_response | stress_front_end_eased / _partly_eased / _not_eased / no_stress / unknown | VIX z beside mean easing z of ust_2y_change_5d and fed_path_change_5d, one window of closes; descriptive | — |

Pending (listed in every record as `pending`): release surprise (no consensus source yet), earnings surprise.

A variable with some history but less than its baseline needs is `no_baseline`: value and observation date, no z or
label. Dealer gamma reads that way until ~120 sessions of registered captures exist (around spring 2027).

## News and vintages

The daily newspaper-based uncertainty indexes (Baker, Bloom, Davis) are revised for weeks: USEPUINDXD for
2022-06-02 first printed 60.23 and settled at 144.13. `fred_vintages` (`store/fred_vintages.py`, filled by
`ingestion/fred_vintages.py` from ALFRED) keeps every published version with the dates it was live, and answers
"what did FRED show for day X at the end of day D" for D within 45 days of X. Each past day's news reading is rebuilt
from the versions live on that day, so a first print is compared with past first prints, never with revised history.
First fill: `python -m ingestion.fred_vintages backfill` (vintages start 2014-03-27); the nightly writer refreshes it.

Checked on real data (read-only, 2026-11-13): 2020-03-16 and 2025-04-04/08 (tariffs) had elevated uncertainty
(z 5.0, 3.95, 5.0); 2022-06-13, the session after a hot CPI print, was not elevated (z −0.28).
These proxy readings do not themselves attribute the moves to news or establish that a session had no news.

## Availability

Every input goes through `store.observations.read_window_known_at` with a declared, deliberately late publication lag
(`market_state.LAGS`; documented shutdown releases override it). Backfilled history is revised vintage, not first
release. Monthly values may therefore arrive late, but a conservative lag does not remove later revisions.
Historical nightly equivalence is unproven without retained vintages and publication/capture evidence. Rows are insert-only; `--replace` is the only way to rewrite a date.

### News attribution boundary
The newspaper uncertainty proxy does not establish a market-moving shock, its cause, or a quiet/flow-only session. `calendar_context` reports only listed-release co-occurrence (or unknown when no calendar is loaded). `news_attribution` stays `unverified`; `supports_flow_only_classification` is false. Nonfinite or nonnumeric proxy scores are unavailable. The enclosing `news_shock` key is retained, but the new descriptive kinds intentionally replace the overclaiming scheduled/unscheduled/none categories. No production consumer currently reads those categories.

## DIX and dealer gamma

DIX_d = Σ ShortVolume×close / Σ TotalVolume×close over day d's S&P 500 members (`config/sp500_membership.json`, built
from fja05680/sp500 by `scripts/build_sp500_membership.py`), FINRA's CNMS file and same-day Tiingo closes. Share
weights track SqueezeMetrics' published DIX at only 0.90 correlation; dollar weights at 0.997 over 2,083 days. Checked
2026-11-05..11 against SqueezeMetrics to the third decimal. Backfill: `python -m ingestion.altdata.finra_dix --backfill
2018-08-01 <yesterday>`; nightly: the smart-scheduler entry `finra_dix`.

`dealer_gex_daily` (the godview table that had no writer) gets one SPY row per session from the latest registered
options capture completed by the end of that UTC day (`scripts/run_dealer_gex_daily.py`). Units: $M per 1 % SPY move.
Sessions without a registered capture, a spot receipt or a GEX zero crossing are skipped, not guessed.

## Raw-input lineage fingerprint (partial)

New records retain `source_lineage.raw_series` for each `_Reads.series` input. Each receipt hashes the exact
finite date-collapsed rows consumed, including value, source, pull clock, known-at date and known-at basis.
It also retains the source filter, declared publication policy, counts and latest selected observation.
The hash uses UTF-8 canonical JSON (sorted keys, compact separators, strict finite numbers), in observation-date
order. It fingerprints selection, not immutable custody: underlying source rows still need retained storage.
No raw payload, URL, credential or extra provider request is included. Values/selection/scoring are unchanged.

`modeled_lag` remains distinct from `pulled`, missing clocks remain missing, and a mixed-source refusal is
distinct from an empty successful read. Nothing is promoted to PIT-qualified. ALFRED selection/capture,
dealer capture, the selected SPY window and calendar publication/capture are explicitly unqualified paths.
This first slice does not wire catch-up, prove scheduled operation, revise historical rows or change timers.

## Policy path and policy response (market-state/3)

market-state/3 adds the policy-path variables and the `policy_response` block. Rows written earlier keep their
`rule`; a reader spanning the switch sees both, and a record is never rewritten to change its rule without `--replace`.

**The Kalshi ladder.** `ingestion/altdata/kalshi_markets.py` stores one row per open KXFED event (one per meeting) per
daily pull. The row's `value` is the price of whichever strike traded most that day, so it changes question when that
strike changes (KXFED-26DEC moved from "above 4.00%" to "above 4.25%" around 2026-11-12). It is never read. Every
strike's bid and ask are in `raw_payload.all_strikes`, and `policy_path.price_ladder` prices those:

- **Contract check, per strike.** The title must ask about the "upper bound of the federal funds rate" being "above
  X%". X must match the ticker and sit on the 0.25 grid. A meeting date named in the title must be the event's own
  (`strike_date`, New York date).
  - The ticker must belong to the row's event, and so must the strike's own `event_ticker` when stored.
  - A stored payout must be `strike_type: greater`, with `floor_strike` = X and no cap.
- **Quotes are bands, and missing stays missing.** Kalshi shows no bid as 0 and no ask as 1. The old writer also
  stored a missing or malformed quote as 0.0. So a side counts only strictly inside (0, 1), and only with a positive
  size where the size was stored.
  - Bands: [bid, ask] for a two-sided quote, [0, ask] with no bid, [bid, 1] with no ask.
  - Refused: no quote on either side, a crossed quote, or a band wider than 0.10.
  - Last trades are never used.
- **Shape.**
  - Strikes must be contiguous in 0.25 steps.
  - A book where a higher strike's bid is above a lower strike's ask fits no non-increasing ladder, and is refused.
  - Each strike's point is its band's mid, lowered by a running minimum where mids cross and held inside the bounds
    every fitting ladder must respect. The points moved are counted.
  - E = lowest strike + 0.25 × Σ P(above k).
  - `expected_bounds` gives the lowest and highest E the quotes allow.
- **Coverage.** Either tail that the quotes allow above 0.05 makes the snapshot unusable.
- **Odds.** Hike, hold and cut are given, with their bounds, only when both thresholds are quoted strikes: the
  current upper bound and one step below it. Otherwise `odds_withheld` says why. Odds are never extrapolated past
  the ladder.

**Clocks (`kalshi-acquired/1`).** There is no modeled path; a row counts for a cutoff only by when it was available.

| Rows | Clock | Available at |
|---|---|---|
| Written by the current writer | `clock.requested_at` (before the event's markets request) and `clock.acquired_at` (right after its response, also stored as `pull_timestamp`). The quotes were read between the two. The row is dated by the response's UTC day and commits in its own transaction straight after. | acquired + 1 min |
| Written earlier | Only `pull_timestamp`: the raw_series default, i.e. the start of one transaction that spanned the whole pull. The quotes came later. | `pull_timestamp` + 15 min |

The 15-minute bound is measured. pull_log 2026-05-27..11-23 has 289 Kalshi runs; the longest took 70 s, and inserting
runs take about 10 s. `source_lineage.kxfed` counts rows by clock basis.

**Schedule as known that day.** The next meeting is the earliest meeting after the day's newest quotes, among the
events GRID had read by then. An event first read later never rewrites an earlier day. If that event's row is missing
or unusable, the day has no reading; a later meeting is never substituted.

**The session's own snapshot.** The three policy variables have a maximum age of 0 sessions. The record reads only the
as_of session's snapshot. If that snapshot is missing or unusable, the variables read stale and
`next_meeting.reason` says why. An older ladder is never shown in its place.

The 5-session change is taken inside one contract. The priced move is set against DFEDTARU only when FRED's
observation postdates the last decision taking effect: right after a meeting the ladder already prices the next one
while FRED still prints the old target.

**policy_response (policy-response/1)** is Neon's PRFD ("is the Fed put alive?") with only the legs Codex's triage
admitted first (GRID-COMPOSITE-TRIAGE-20261122): VIX, plus the front end (2-year change, path change). There are no
EMPS or news-tone legs. It is **descriptive**: front-end repricing beside equity stress does not identify a policy
response or its cause.

- **One window of closes.** `windows` gives each leg's endpoints as XNYS closes, and the block is `unknown` unless
  they agree:
  - VIX: its close.
  - 2-year: DGS2's end close, and its start exactly five XNYS sessions earlier. `ust_2y_change_5d` now spans exactly
    five sessions; a bond-market holiday carries the previous close, and nothing older is carried.
  - Kalshi: a snapshot is placed at the close before it only when every instant its quotes can have been read lies
    after that close and before the next open.
- **The decision record lines up as published.** For session d: VIX for d−1 arrives about 12Z on d; DGS2 for d−1
  about 22:02Z on d; the d snapshot is read before d opens. So every leg ends at the d−1 close. The provisional record
  at d's close does not have the d−1 DGS2 print yet, so it reads `unknown`.
- stress = VIX robust z.
- easing = −(z of ust_2y_change_5d + z of fed_path_change_5d) / 2.
- score = stress − easing.
- Labels:
  - `no_stress` when stress < 1.5;
  - otherwise `stress_front_end_eased` when easing ≥ 1.0;
  - `stress_front_end_not_eased` when easing ≤ 0;
  - `stress_front_end_partly_eased` in between.
- Equal weights and round thresholds, declared and never fitted.
- `controls` carries VIX-only and front-end-only readings.
- The block is `unknown` when any of these fails:
  - all three legs read `ok` with a finite z (not stale, no-baseline, zero-spread or missing);
  - the session's own next-meeting snapshot is usable;
  - the windows agree.
- `decision_use: none`: nothing reads it to decide, and it has no registered test (#1311). The desk brief prints it,
  labelled descriptive research only.

Read against griddb on 2026-11-23. Dry run, read-only session, nothing written.

| Record | Window (closes) | VIX | 2y 5-session | Path 5-session | Reading |
|---|---|---|---|---|---|
| 2026-11-13 | 11-05 → 11-12 | 18.70, z +0.7 | +40 bp | +11.1 bp | `no_stress` |
| 2026-11-19 | 11-11 → 11-18 | 24.56, z +3.2 | +7 bp | −0.6 bp | `stress_front_end_not_eased` |
| 2026-11-20 | 11-12 → 11-19 | 28.44, z +4.8 | −10 bp | −0.9 bp | `stress_front_end_partly_eased` |
| 2026-10-29 | — | 16.91 | −14 bp | stale | `unknown`: the session's own snapshot has no same-contract change |

- 11-11 is Veterans Day: NYSE open, no DGS2 print. The 11-19 window carries the 11-10 close.
- Coverage: 149 of the 160 sessions from 2026-04-07 to 2026-11-20 have a next-meeting reading, the same as before
  the stricter quote and contract checks. 107 of those snapshots were read pre-open. The rest were pulled after the
  open, mostly in October, when the pull drifted to 11–15Z. On those days the block reads `unknown`.
- Every stored KXFED row so far is a transaction-start row.

The 2026-11-23 05:30Z snapshot prices the 2026-12-09 meeting with the upper bound at 4.00%:

| | Hike | Hold | Cut |
|---|---|---|---|
| Point | 93.5% | 6% | 0.5% |
| Bounds within the quotes | 93–94% | — | 0–1% |

E = 4.241 (4.228–4.255). The record shows these odds only once DFEDTARU is pulled.

## Composite register (Neon and Codex proposals, 2026-11-22)

Where each proposed composite stands. A composite enters the record only through #1311: one at a time, inputs read
point in time with a maximum age, and `unknown` when a leg is missing. Live use needs a registered test.

| Composite | Source | Status |
|---|---|---|
| PRFD (Fed put) | Neon global levers | **Tracked**: `policy_response` + four variables, market-state/3 |
| SRLI (custody / auction stress) | Neon; Codex second ticket | Data path started: WMTSECL1 in the FRED pull (raw only). Composite not built; needs release-dated custody plus auction tenors |
| EMPS (energy → inflation pass-through) | Neon; Codex first ticket | Queued: oil / lagged-breakeven / real-yield baseline, then release-aligned EIA Gulf Coast ULSD. SCFI parked (rights) |
| ICBD (insider vs. CFO behaviour) | Neon; Codex paper-only | Queued: needs a Form 4 + 8-K event pipeline with filing clocks |
| DBFL, FWAS (natural gas only), GCIR, WPCI, CSVL | Neon | Queued in Codex's order. Inputs exist in part (WLCFLPCL, DTB3 since #1347) |
| SSSF (settlement squeeze) | Neon | Rejected as written: FTD balances carry no age or cohort |
| RSPI, HCDW, RDFM, CDDS, FDTP, PBWG, ODFC, ACPB | Antigravity composites branch | Held library code, #1341 (Codex HOLD: as_of does not filter inputs) |

Neon's uncommitted `global_levers.py` (branch `claude/global-machinery-levers-20261122`, never pushed) has three
problems:

- It reads the KXFED `value` column. Its "3% December cut odds" was the price of "above 4.25%", i.e. a second hike.
- It reads `obs_date <= as_of` with the latest pull, which is not known-at.
- It replaces `tests/test_global_levers.py`, which covers the existing `intelligence/global_levers.py`.

None of it is used here except the PRFD idea.
