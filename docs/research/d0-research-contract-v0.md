# D0 research contract — event scanner (D1) and SPY volatility lab (D2), v0 DRAFT

<!-- D0-CONTRACT-BODY-BEGIN -->

| Field | Value |
|---|---|
| Issue | #1129 (roadmap #1128; extends #960, does not replace it) |
| Version | `d0-contract-v0` — **DRAFT, unsigned** |
| Drafted | 2026-11-12, Claude, from origin/main `b27a2065` |
| Research owner (signs) | _unassigned_ |
| Independent reviewer (signs the admission manifest) | _unassigned_ |
| Body hash | computed at signing over the text between the `D0-CONTRACT-BODY` markers (LF-normalised sha256), then pinned the house way: a `D0_CONTRACT_BODY_SHA256` constant checked by a test, as `analysis/gex_intraday_prereg.py:36` does |
| Machine-readable manifest | `docs/research/d0-coverage-manifest-v0.json` |

**Status of every number in this file: proposed default.** Nothing here is frozen until the research owner
signs and the body hash is pinned. Changing a threshold after any outcome is read makes this a new version
with fresh evidence, never a free retry.

**Not authorised by this document:** provider collection, spend, schema changes, scheduler/timer changes,
historical repair, merge, deploy, empirical scoring, promotion, brokerage orders. Each keeps its own gate.

---

## 1. Existing-work map (extend, do not rebuild)

Protocol machinery D0 reuses. Every row is on main unless marked.

| Need | Existing owner on main | What it already enforces | Live? |
|---|---|---|---|
| Discovery/holdout split, purge-by-construction, block-permutation null, BH-FDR over all declared trials | `analysis/offline_research_proof.py` (S08/S09) | write-once `discovery-frozen.json` / `holdout-result.json`; holdout refuses a changed manifest | no wiring |
| Embargoed CV, PBO/DSR | `alpha_research/validation/overfitting.py`, S11 overfit gate | purge + embargo | no |
| Alpha spending across sequential families, single-use windows | `analysis/ledger_steered_exploration.py` (S11) | α_k = q/(k(k+1)), q ≤ 0.10; Holm within run; `touched_windows()` | ledger path unset in config (`HYPOTHESIS_TRIAL_LEDGER_PATH=""`) |
| Candidate recorded before data is read; abandonment = failure; screen refuses touched windows | `evals/e3` E3A (e3-v1) | `screen` checks S11 `touched_windows` overlap | files only; E3B (e3-v2) code present but RELEASED.json says draft |
| One forward look per scientific pair | `analysis/research_forward_log.py` (S10) | single pre-registered look; prereg sha pinned | timer template not installed |
| Forward scoreboard, PIT price resolution, cost model, `registered_at` rule | `evals/e2` | outcomes before `registered_at` go to `pre_registration` | timer template only |
| Look-ahead canary, vintage immutability | `evals/e1` | PR-time gates | CI |
| SPY GEX forward log | `paper_log/gex_levels/` (#647), prereg `docs/paper_log/gex-levels-v1-preregistration.md` | one evaluation at 60 valid sessions; Bonferroni 0.0167 | install script prints a crontab only |
| SPY GEX intraday families | `analysis/gex_intraday_prereg.py`, `docs/paper_log/gex-intraday-v1-preregistration.md` | 5 sub-families, fixed k, stage-0 power gate | **every hypothesis BLOCKED_INPUT / BLOCKED_PRICE_CONTRACT** (no pre-open capture, no certified auction prices) |
| Intraday context / vol-repricing controls | #813 / #916, `scripts/intraday_lab/` | exact contract identity, first-receipt clocks, bid/ask with size | synthetic, UNVALIDATED |
| Spent ensemble record | #1065 (open) | six looks v1–v6 FAIL; 492 cumulative trials | paper only |
| Price-basis repair | #1084 / #1067 / #1072 / #1098 / #1100 / #1096 / #1099; #1086 open | retractions by run_tag; PIT reads before the retraction reproduce old values | applied 2026-11-09/10 |

**Window registry decision.** D0 does not invent a window format. Inspected periods are written as S11
allocations / E3 `stage_entered(screen, window=…)` records keyed by `identity_sha256`. Two gaps need an
owner-approved S11/E3 version bump before D2 can use them natively: (a) no slot for option-contract identity
or intraday horizon (horizons are integer sessions), (b) no record kind for "data inspected but not tested".
Until then the manifest's `inspected_windows` list (§4) is the authoritative record, and E3 `screen` cannot
be relied on to refuse D2 overlaps.

D1 modules to extend (none replaced):

| Module | Writes | Live caller | First-availability clock | Blocking defect for causal use |
|---|---|---|---|---|
| `intelligence/business_news_parser.py` | `business_events` | scheduler, every 6 h | **missing** (only article `published_at`) | cross-outlet stories not deduped |
| `intelligence/deal_detector.py` | `deal_pipeline` | scheduler, every 6 h | **missing**; `detected_at` = article time or NOW() | `updated_at` overwritten per stage (no stage history) |
| `intelligence/event_sequence.py` | — (read-only) | API, forensics, pattern_engine | **missing** | reads `r[8]` from an 8-column row (`:256`/`:277`), so every `signal_sources` event is silently dropped; stamps SEC filings at `period_end`, not filing date (`:656`) = weeks of lookahead |
| `intelligence/causal_links.py` (+ `causation*`) | `causal_links` | systemd template | **present** (`known_at` + basis) | accepts `earnings_calendar` stub rows (below) as real releases |
| `intelligence/actor_network.py` + `actors/` | `actors`, `wealth_flows`, `actor_connections` | Hermes, 6 h | missing | CIK→ticker from the current `company_tickers.json` only |
| `intelligence/sleuth_intersections.py` | `investigation_leads` | Hermes, daily | proxied by `signal_sources.created_at` | — (keeps #1071 construction rules) |
| `intelligence/forced_flow_monitor.py` | `forced_flow_briefings` | Hermes 06:00–06:40 **UTC** (docstring says ET) | n/a | ignores holidays; FOMC dates hard-coded for 2026 |
| `people_events` (migration `people_events_20260927`) | `people_events` | **no scheduled materializer** | **present** (`known_at`, `known_at_basis`, `dedup_key`, supersede/retract) | stale since 2026-07-01 |
| `intelligence/security_master.py` | `security_master` (22,401 rows), `security_identifiers` (`valid_from`/`valid_to`) | — | — | no D1 module uses it; seed never flips `is_active` |
| #1050 / #1051 / #1090 (open drafts) | SEC periods, AV transcripts, company documents | — | present in design | unmerged; #1051 stops new earnings stubs but does not repair existing ones |

---

## 2. Clocks every admitted input must carry

An observation enters causal evaluation only if all five are known; otherwise it is `EXCLUDED_UNKNOWN_AVAILABILITY`.

1. **observation / event time** — what the value describes;
2. **first public availability** — when anyone outside could first have known it;
3. **retrieval / ingestion time** — when GRID received it (`pull_timestamp`, `registered_at`, `vintage`);
4. **calculation time** — when the derived value was computed;
5. **decision time** — the as-of the research rule acts on (must be ≥ 2 and ≥ 3, plus registered latency).

Retrieval time may stand in for first availability only for the forward period, never for history
reconstructed today. Today's corrected data is never used to reconstruct what was knowable then; a repaired
series is read `as_of` the decision time, which returns the pre-repair value when that is what was served.

---

## 3. Coverage matrix (summary; full detail in the JSON manifest)

### 3.1 D2 — SPY inputs

| Input | Custody | Coverage | Clocks | Status for D2 |
|---|---|---|---|---|
| `options_capture_batches` / `options_snapshots_all`, SPY, Cboe delayed (~15 min) | GRID griddb, append-only | **2026-10-01 .. 2026-11-11, 30 sessions, 359 batches** (measured 2026-11-12) | batch `registered_at`, `capture_started/completed_at`; per-contract quote time absent; spot clock only from 2026-11-10 | forward-only; too short for any historical claim; OCC symbol not stored, no bid/ask size |
| `options_daily_signals` / `spy_iv_*` features | GRID | same as above | obs = release = snap date | diagnostic; "25-delta" is a ±10 % moneyness proxy |
| CBOE VIX / VIX9D / VIX3M | GRID `raw_series` | VIX 1990-01-02 .. 2026-11-10; VIX9D from 2011-01-04; VIX3M from 2009-09-18 | daily close; pull_timestamp | admitted for regime conditioning (close known after 16:15 ET) |
| CBOE SKEW | GRID | ends **2026-03-25** (stale) | — | excluded until the puller is fixed |
| Schwab chains / quotes / 1-min bars | process-local only, `persist_allowed=False`; no redistribution | live only | provider quoteTime + receipt time | live inputs for forward shadows; no history; no aggressor side |
| Polygon `/v3/snapshot/options` | — | 403, puller SKIPPED | — | unavailable |
| SPY 1-min bars, Polygon | offline research copy (GEX repo scratch) | rolling ~2 yr (2024-11 ..) | bar end | inspected (§4); not GRID custody |
| SPY 1-min bars, XNAS.ITCH | offline research copy | 2022-11 .. 2026-11 | bar end | inspected (§4) |
| OPRA cbbo-1m, near-spot 0DTE strikes | offline research copy | 2022-11-14 .. 2026-11-09 fetched; vendor history from 2013 | quote in force at minute end | inspected 2022-11-14 .. 2026-11-09; **pre-2022-11 not fetched, untouched** |
| OPRA open interest (statistics, stat_type 9) | offline research copy | 2022-11-14 .. 2026-03-25 rebuilt books | published ~06:30 ET from the prior close (PIT-safe) | inspected |
| SPY trades with aggressor side (XNAS.ITCH) | offline research copy | 2022-11 .. 2026-11 | trade time | dev 2024-11 .. 2026-11 inspected for edge8; 2022-11 .. 2024-11 unread for edge8 only |
| Tiingo SPY adjusted daily | GRID key; offline copy for edge3 | 1993 .. | daily | inspected for calendar rules 1993 .. 2026-11-10 |

The longest SPY options history lives outside GRID custody. Moving the qualified subset into GRID (#1132,
#1108 snapshots) is the first D2 gap; D2 results computed on research copies are admissible only with the
copy's file hash recorded in the admission manifest.

### 3.2 D1 — business-event inputs (measured on griddb 2026-11-12, read-only)

| Input | Rows / coverage | First availability | Identity | Status for D1 |
|---|---|---|---|---|
| `people_events` channel `form4` | 4,461,947 live rows; known_at 2006-01-04 .. **2026-07-01**; 16,599 CIKs | `known_at_basis = filing` (filing date; no EDGAR acceptance time) | CIK + ticker + `security_id` | best substrate; stale 4.5 months |
| `signal_sources` `insider` / `quiverquant:insider` | 20,806 (2018-08 .. 2026-11-10) / 60,408 (2002-03 .. 2026-11-10) | `created_at` (ingest) | ticker | already scored NO_EDGE in edge-report-forward-20261106 |
| `insider_trades` | 20,013 rows, 2,253 tickers; `filing_date` filled on **2,897** | partial | ticker | not admissible without filing date |
| `earnings_events` (SEC) | 8-K 4,623 (1,354 tickers), 10-Q 1,556, 10-K 81 … filing dates **2026-02-19 .. 2026-11-10** only | `filing_date` (date-only; no acceptance time) | CIK, accession | forward-only; no history |
| `earnings_calendar` (yfinance + compat stubs) | 10,508 rows, 1,203 tickers, 2002-07 .. 2027-04; **2,131 rows whose `earnings_date` equals a SEC `period_end`** | none for estimates (overwritten, `pull_timestamp` reset) | ticker | stub rows refused; estimates not PIT |
| `business_events` | 1,944 rows, 2026-03-12 .. 2026-11-12 | missing | regex ticker | evidence-only (secondary source on cards) |
| `deal_pipeline` | 1,659 rows, 2026-03-12 .. 2026-11-11 | missing | name strings | evidence-only |
| `supply_chain_edges` | 857 rows, as_of 2015-12-31 .. 2026-07-14 | 10-K filing date of the **latest** 10-K only | name slugs | not admissible historically (no change history) |
| `causal_links` | 2,358 rows, 2026-09-01 .. 2026-11-11 | present | ticker | forward-only |
| Index additions/deletions | **no source** | — | — | family unavailable |
| Liquid-equity universe | **no point-in-time universe** (hard-coded ~100 names; Tiingo tier snapshot 2026-03-31) | — | — | blocks every historical D1 look |

---

## 4. Inspected / spent periods (binding)

A family may not be tested again on a window listed here; a revisit is a new registration on new data.

| Ledger | Family | Window inspected | Verdict | Record |
|---|---|---|---|---|
| GEX panel_trials row 10 | volume-climax fade | Polygon SPY 1-min 2025-05 .. 2025-12 (and 2026 in-sample) | FAIL | #1133 |
| GEX range-offsides (41d2181) | range fakeout fade | 2026-03-25 .. 2026-11-04 | FAIL | #1133 |
| GEX edge 1 DAYSIDE | late-day prior-close side | 2025-04-02 .. 2026-03-24 | FAIL | #1133 |
| GEX rules-2 level fade | shadow level fade | first 30 entries from 2026-11-04 | FAIL | #1133 |
| GEX edge2 (c5051ea) | option flow, skew shock, vol-gamma magnet, overnight shares | dev 2024-11 .. 2026-11; holdout 2022-11-14 .. 2024-11-08 | FAIL ×4 | #1133 |
| GEX edge3 (2ee3eff) | turn of month, pre-FOMC, VIX spike, pre-holiday (daily) | dev 1993 .. 2012; holdout 2013-01-02 .. 2026-11-10 | FAIL ×3, UNDERPOWERED ×1 | #1133 |
| GEX edge4 (7c6cc9a) | pin-range/2, gamma-break/1, em-exhaustion/1 replay | 2022-11-14 .. 2026-03-25 | FAIL ×3 | #1133 |
| GEX edge5 (9f664a8) | day-type levers (gamma sign, iv drift) | dev 2024-11-11 .. 2026-11-09; holdout 2022-11-14 .. 2024-11-08 | FAIL, UNDERPOWERED ×2 | #1133 |
| GEX edge6 (cb7b213) | 0DTE short iron fly | 2022-11-14 .. 2026-11-09 | **PASS 12:00/14:00/15:00**, FAIL 09:35 | #1133 |
| GEX edge7 (6b90351) | pin setups | 2022-11-14 .. 2026-11-09 | FAIL ×3, UNDERPOWERED ×1 | #1133 |
| GEX edge8 (dev only, 73ffc1d) | trade-flow turn markers | dev 2024-11 .. 2026-11 | not yet registered | #1133 |
| #1065 v1–v6 | ETF rotation / trend / vol-managed / mean reversion / inverse-vol / regime top-3 | weekly 2017-01 .. 2026-10 (v6 also Kenneth French 1926 .. 2026) | FAIL ×6; 492 trials | #1065 |
| #647 gex-levels-v1 | SPY GEX levels | forward from its registration | interim only | #647 |

Consequence for D2: **no untouched historical intraday SPY 0DTE window exists from 2022-11-14 onward.**
Untouched options history exists before 2022-11-14 (OPRA from 2013), where SPY had Monday/Wednesday/Friday
expiries rather than daily ones.

Equity and event looks already run or reserved:

| Ledger | Family | Window | State |
|---|---|---|---|
| VS1 insider density v1–v3 | Form 4 buyer density vs sector-ETF-relative return | discovery windows in each prereg | Stage-0 power FAIL |
| VS1 v4, v5, v7 | same | — | superseded unopened |
| VS1 v6 | same, ticker-reuse bound | — | STOPPED, power 0.480 |
| **VS1 v8** | same | discovery 2008-01-01 .. 2020-01-01, **holdout 2020-01-01 .. 2026-07-01 reserved** | registered; no outcome in repo |
| VS1 sectors v2–v4, v6 | 10 other sectors | discovery 2012–2019, holdout 2020+ | registered, unopened (v6 bound to v8 windows) |
| trade-edge-v2 | insider buys ≥ $500k, 30-session excess vs SPY | forward from 2026-10-07 | no result in repo |
| hypothesis-forward-v1 (S10) | frozen scan candidates | forward from 2026-09-26 | no result in repo |
| edge-report-forward-20261106 | every `signal_sources` source | to 2026-11-06 | all NO_EDGE (insider 47.9 %) |
| astrogrid-celestial-equity-v1 | index levels | 2017 .. 2026-10 | no comparison passed |

Consequence for D1: **Form 4 insider tests belong to the VS1 lane.** D1 opens no new insider look on
2008 .. 2026-07; insider evidence may appear on cards as labelled context only.

---

## 5. Track D1 — event-and-relationship scanner, v1

### 5.1 Frozen scope (proposed)

- **Universe:** US-listed common stocks whose trailing 60-session median dollar volume is ≥ $20 M **as of
  the event's first-availability date**, computed from Tiingo EOD including delisted names (existing key,
  no new vendor). Identity through `security_master` / `security_identifiers` valid at the event date;
  ticker reuse resolved by CIK. Until this universe exists (gap G3) no historical D1 look can run.
- **Families (k = 2 evaluated, 1 evidence-only).** Chosen for source quality, not for expected return:

| ID | Family | Event source and first-availability clock | Evaluated? |
|---|---|---|---|
| D1-F1 | Earnings release | SEC 8-K **Item 2.02** (Results of Operations), first availability = EDGAR acceptance datetime from the filing header; replaces `earnings_calendar` dates entirely | yes |
| D1-F2 | Material agreements and completed deals | SEC 8-K **Items 1.01 / 2.01 / 1.02**, same clock; `deal_pipeline` / `business_events` headlines attached as secondary evidence with their clock gap shown | yes |
| D1-F3 | Insider open-market purchases | `people_events` form4, `known_at` = filing | **evidence-only**; evaluation stays with VS1 v8 / trade-edge-v2 |

  Not in v1: customer-supplier links (no change history), index additions/deletions (no source),
  OPEX/FOMC forced flows (calendar defects; SPY flows are D2's), headline-only deal news.
- **Event time and entry:** the first regular-session open strictly after acceptance time + 5 minutes.
  An after-close filing enters at the next open; the announcement-day move before entry is recorded on the
  card as "already moved" and is never claimed.
- **Horizons (2):** [entry, +5 sessions] and [entry, +20 sessions], open-to-close.
- **Outcome:** return minus the sector ETF return over the same span (sector from `analysis/sector_map`
  as of the event date), with SPY-relative reported alongside.
- **Variants:** direction = sign of the [−1, entry) reaction (continuation); its mirror (reversal) is the
  control, not a second chance.
- **Controls:** (a) declare-all baseline — every eligible event, unranked; (b) placebo — the same issuers on
  dates shifted by ±60 sessions; (c) shuffled-relationship control for any card using a link.
- **Ranking rule (frozen before D3):** data quality tier, then event strength (item type; for F1 the
  absolute day-0 reaction in units of the issuer's trailing 60-session volatility), then liquidity. No
  composite probability. "Priced-in" is shown as unknown when there is no PIT consensus or option-implied move,
  which is the default in v1 (`earnings_calendar` estimates are not PIT).

### 5.2 Windows (fixed before the EDGAR fetch)

- Development: acceptance dates 2012-01-01 .. 2018-12-31.
- Holdout: 2019-01-01 .. 2025-12-31, one look per family × horizon.
- Forward: from the signing date, through the existing E2/S10 forward chain.

No D1 family above has been tested on these windows by any ledger in §4 (VS1 covers insider density only).

### 5.3 Success thresholds (fixed before any outcome)

- **Statistical:** for each family × horizon (4 cells), the top ranking quintile's mean net excess return
  minus the declare-all mean is > 0 with a block-bootstrap lower bound (clusters = event week and issuer;
  B 10 000, seed 20261107) above 0 at one-sided 1 − 0.05/4 = 0.9875. Costs 10 bp per side; 25 bp
  sensitivity reported.
- **Sample floor:** ≥ 200 events in the top quintile and ≥ 60 distinct event weeks per cell in the holdout,
  else UNDERPOWERED.
- **Power rationale:** with per-event excess-return SD of about 4 % at +5 and 8 % at +20 sessions (to be
  measured on development data and recorded before the holdout opens), n = 200 gives SE ≈ 0.28 % / 0.57 %,
  so the minimum detectable difference at 0.9875 and 80 % power is about 0.85 % / 1.7 %. If the measured
  development SD implies power < 0.5 at the literature-plausible effect, the cell is STOP_UNDERPOWERED before
  the holdout, as in the VS1 Stage-0 gate.
- **Economic:** top-quintile mean net excess ≥ 0.5 % at +5 or ≥ 1 % at +20; capacity reported at 1 % of
  median dollar volume.
- **Operational (per event):** acceptance datetime present; CIK resolves to one security valid at that time;
  entry and exit prices present `as_of` the decision time; sector known. Otherwise `EXCLUDED_*`, counted.
  Exclusions > 10 % of eligible events in a cell → HOLD with the cause.

### 5.4 GO / HOLD / RETIRE

- **GO** for a family: both horizons pass, or one passes and the other has a positive point estimate.
- **HOLD:** UNDERPOWERED, exclusions > 10 %, or the EDGAR/universe gaps (G2, G3) are unresolved.
- **RETIRE:** both horizons fail. A changed family is a new registration on the forward period only.

### 5.5 Budget

EDGAR 8-K header backfill 2012–2025 is free but rate-limited (10 requests/s): roughly 1 M filings,
two to three days at half the limit. It is a new collection job and needs operator approval. Storage in griddb
under an append-only table with accession as key. Compute small.

### 5.6 Examples required before admission

- **Traceable:** one 8-K Item 2.02 from `earnings_events` (accession, filing date) → acceptance datetime →
  entry open → card with "already moved", sector-relative outcome slot left PENDING. Binds to the #958 chain
  shape (input → materialization row → reasoning output or unavailable reason → retained bytes); M0 does not
  prove accuracy.
- **Refusal:** one of the 2,131 `earnings_calendar` rows whose `earnings_date` equals a SEC `period_end`
  (compat stub, `reported = TRUE`), refused as `EXCLUDED_UNKNOWN_AVAILABILITY`.

---

## 6. Track D2 — SPY volatility lab, v1

### 6.1 Frozen scope (proposed)

- **Instrument:** SPY and its listed options only. No SPX, no other underlying.
- **Calendar:** NYSE regular sessions from `ingestion/market_calendar.py` plus the local corrections already
  in #1100; half days excluded from intraday families; sessions with thin open interest excluded by the edge4
  rule (fixed before data, recorded per session).
- **Separate questions, scored separately:** (a) forecast quality of a volatility measure (diagnostic);
  (b) after-cost tradability of a fixed structure (economic). A pass on (a) never implies (b).

### 6.2 Hypotheses (k = 3, one family)

| ID | Hypothesis | Data | Window | Look |
|---|---|---|---|---|
| D2-H1 | The 0DTE variance premium found in edge6 replicates out of sample: the 14:00 and 15:00 ET ATM short iron fly (wings and strike rule exactly as edge6, frozen code hash) has mean net > 0 at bid/ask on **pre-2022-11-14 Monday/Wednesday/Friday SPY expiries** | OPRA cbbo-1m, not yet fetched | 2018-01-02 .. 2022-11-11 expiry days (exact start = first full year with M/W/F expiries in the vendor data; fixed before fetch) | one look, after the fetch manifest is hashed |
| D2-H2 | fly-shadow/1 forward: the 15:00 fly exited at 15:55 (shorts at ask, wings at bid) has mean net > 0 | live Schwab chain, ledger `fly_days` | forward from 2026-11-12 | one look at 100 traded sessions (already registered fe8318a; D0 adopts it unchanged) |
| D2-H3 | bounce-shadow/1 forward: long SPY 15:29→15:59 after a negative-gamma late-day bounce | live dte0 regime + bars | forward from 2026-11-12 | one look at 50 qualifying sessions (registered f8b3874; adopted unchanged). **Not independent of history:** chosen after edge7 P3 (short the same bounce, −0.391, n 83) was read |

Diagnostic only (no pass/fail, cannot promote): realised-vs-implied variance by time of day on GRID's own
`options_capture_batches` once it reaches 120 sessions.

Not in v1: anything requiring trade-side live data (Schwab has none), any SPX family, any new 0DTE
intraday rule on 2022-11 .. 2026-11.

### 6.3 Execution and costs (fixed)

- Option fills: enter at the ask (buy) / bid (sell) in force at the end of minute t+1 after the signal; exit
  at the opposite side at the end of the exit minute; fee $0.03 per contract per side. No mid fills.
- Shares: $0.04 per share round trip (D2-H3), plus a 6 bp round-trip sensitivity (`evals/e2` `e2-costs-v1`).
- **Gap:** no options cost class exists in `evals/e2/cost_model.json`. D2 must add `us_equity_option_0dte`
  to the cost model (versioned) before D3 can score through E2.
- Settlement-graded results (15:59 settle) are reported but are not the judged metric for any live-exit
  hypothesis; pin risk is reported as the share of entries settling within $0.25 of a short strike.

### 6.4 Success thresholds (fixed before any outcome)

**Statistical:** per hypothesis, mean net per trade > 0 with a seeded percentile-bootstrap lower bound
(B = 10 000, seed 20261107) above 0 at one-sided level 1 − 0.05/k, k = 3 → 0.9833 for D2-H1. D2-H2 and
D2-H3 keep their registered levels (0.95) because they were registered first; the family-wise
correction across D2 is reported alongside.

**Sample floor:** D2-H1 n ≥ 300 sessions per entry time; D2-H2 n = 100; D2-H3 n = 50. Fewer eligible
sessions than the floor = UNDERPOWERED = HOLD, never PASS.

**Power rationale (D2-H1/H2):** edge6 at 15:00 gives mean +0.195, bootstrap LB(0.9875) +0.131 at n = 986,
so SD ≈ 0.90 per trade. At n = 100 (D2-H2), the one-sided 95 % bound clears 0 when the sample mean
exceeds ≈ 0.148: power ≈ 0.70 if the true mean is 0.195, ≈ 0.50 if a 15:55 exit cuts it to 0.15. The
distribution is left-skewed (many small wins, rare losses near −4), so the bootstrap at n = 100 is likely
optimistic; D3 reports a block-bootstrap and a worst-decile sensitivity. At n = 300 (D2-H1, level 0.9833, bound clears 0 above ≈ 0.111), power ≈ 0.95
for a true mean of 0.195 and ≈ 0.77 for 0.15. These use the 2022–2026 SD; M/W/F-only history may differ.

**Economic:** mean net ≥ 2 % of capital at risk (5 − credit) per trade; max drawdown reported in units of
mean monthly net; positive in a majority of calendar years covered.

**Operational (per session, before inclusion):** chain captured within 2 minutes of each grading mark;
both short strikes and both wings quoted (bid > 0, ask ≥ bid); spot clock present; otherwise the session is
`EXCLUDED_INPUT` and counted. If excluded sessions exceed 10 % of eligible sessions at any fixed review
(the #647 stop rule), the hypothesis halts as HOLD with the cause recorded.

### 6.5 GO / HOLD / RETIRE

- **GO** (eligible for D4 prospective paper): D2-H1 PASS **and** D2-H2 PASS at their looks.
- **HOLD:** either is UNDERPOWERED, or operational exclusions exceed 10 %, or D2-H1 cannot be fetched
  (spend not approved). Calendar time alone never converts HOLD to GO.
- **RETIRE:** D2-H1 FAIL (the premium did not replicate) **or** D2-H2 FAIL. A retired structure returns only
  as a new registration on new data.
- D2-H3 decides only its own GO/RETIRE; it cannot rescue or sink the fly.

### 6.6 Budget

Compute: single host, < 2 h per look. Storage: research copies stay outside griddb until #1108 defines
snapshots. Spend: the D2-H1 OPRA fetch is a usage-based purchase and needs the operator's explicit approval
with a quoted cap before it starts; no subscription.

### 6.7 Examples required before admission

- **Traceable:** one fly-shadow/1 graded session — chain capture id, quotes at 15:00 and 15:55, fills,
  net, ledger row hash.
- **Refusal:** one session excluded for missing quotes or a stale spot clock, with the exclusion reason as
  stored. (The pre-registration rows from 2026-11-04 .. 11-11 are graded but outside the judged sample;
  they may serve as the mechanics example, labelled pre-registration.)

---

## 7. Rules common to both tracks

1. **Count everything.** Every hypothesis, variant, horizon and instrument in §5–§6 is declared here; the
   S11 ledger carries them as one allocation per family. Tests outside this list are exploratory and cannot
   be promoted.
2. **Record before reading.** Each candidate gets an E3A candidate record before its data is read;
   abandonment counts as a failure.
3. **One look per cell.** Interim reads are labelled interim and cannot change a threshold.
4. **Missing is not zero.** Unavailable inputs produce `EXCLUDED_*` or `UNAVAILABLE`, counted and published
   with the result.
5. **Publish negatives.** Every verdict, including FAIL and UNDERPOWERED, is recorded on the issue and in
   the ledger.
6. **No reconstruction from corrected data.** Historical inputs are read `as_of` the decision time.
7. **Repaired prices.** The #1084 repair fixed GRID's own pipeline error (mixed raw/adjusted vintages), not
   information the market lacked, so historical research reads the repaired series (current `as_of`) and
   discloses it. Because adjusted closes are back-adjusted with later corporate actions, they are used only as
   **returns** (ratios), never as price levels or level thresholds. Revisable *knowledge* (estimates, filings,
   relationships, availability) is always read `as_of` the decision time.

## 8. Admission manifest and independent review

Admission happens per hypothesis, never project-wide. A hypothesis is admitted when the manifest
(`d0-coverage-manifest-v0.json`) shows, for every input it uses: status `ADMITTED`, a file or query hash,
and coverage meeting its sample floor. The independent reviewer then:

- [ ] re-runs the coverage queries and matches counts within 0.5 %;
- [ ] injects late, revised, missing and duplicate observations into a fixture and confirms each is
      excluded or deduplicated as stated;
- [ ] reproduces the traceable example and the refusal example for each track;
- [ ] checks §4 against the GEX records and #1065 for any window this contract would reuse;
- [ ] signs the manifest (`reviewer`, `signed_at`, `manifest_sha256`).

Gaps block only the hypotheses named in the gap list below.

## 9. Prioritised gap list

| # | Gap | Blocks | Proposed owner | Needs |
|---|---|---|---|---|
| G1 | Research owner and independent reviewer not named | signing; everything | operator | a decision |
| G2 | No EDGAR acceptance-datetime history; `earnings_events` starts 2026-02-19 | D1-F1, D1-F2 history | existing SEC lane (#1050 author) | approval of a free backfill job |
| G3 | No point-in-time liquid-equity universe | all D1 history | — | Tiingo EOD incl. delisted + `security_master` validity |
| G4 | No options cost class in `evals/e2/cost_model.json` | D2 scoring through E2 | E2 owner | versioned cost-model bump |
| G5 | Pre-2022-11 OPRA quotes not fetched | D2-H1 | GEX research lane | operator spend approval with a cap |
| G6 | `event_sequence.py` drops all `signal_sources` events (`r[8]`) and stamps filings at `period_end` | any D1 card that uses event_sequence | intelligence owner | small fix PR + tests |
| G7 | 2,131 `earnings_calendar` compat stubs read as real releases by `causal_links` | D1 (excluded by rule meanwhile) | #1051 author | repair or quarantine, separately authorised |
| G8 | S11/E3 windows cannot express contract identity, intraday horizon or "inspected, not tested" | native D2 window checks | S11/E3 owner | version bump |
| G9 | GRID holds 30 sessions of SPY chains; long history is offline research copies; OCC symbol and bid/ask size not stored | D2 GRID custody (#1132, #1108) | options ingestion owner | schema change, separately authorised |
| G10 | `people_events` materializer unscheduled (stale since 2026-07-01) | D1-F3 evidence currency | people_events owner | schedule approval |
| G11 | CBOE SKEW stale since 2026-03-25 | SKEW as a D2 conditioner (excluded meanwhile) | altdata owner | puller fix |
| G12 | `forced_flow_monitor` ignores holidays; FOMC dates hard-coded for 2026 | forced-flow family (not in v1) | intelligence owner | fix before any v2 |

<!-- D0-CONTRACT-BODY-END -->
