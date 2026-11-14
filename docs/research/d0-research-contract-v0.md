# D0 research contract — event scanner (D1) and SPY volatility lab (D2), v0 DRAFT

<!-- D0-CONTRACT-BODY-BEGIN -->

| Field | Value |
|---|---|
| Issue | #1129 (roadmap #1128; extends #960, does not replace it) |
| Version | `d0-contract-v0`, **revision 5** (revision 2 answered the reviewer's first pass; revision 3 added the G11 SKEW receipts and the G6 status, §10.1; revision 4 adds the D2 daily-conditioner rule, §10.2; revision 5 answers the reviewer's re-checks of 88768412 and 217d9c91, §10.3; revision 6 makes historical D1 results diagnostic-only, §10.4) — **DRAFT, unsigned** |
| Drafted | 2026-11-12, Claude, from origin/main `b27a2065` |
| Research owner (signs) | **Claude** (named by the operator 2026-11-12); signature pending the reviewer's first pass |
| Independent reviewer (signs the admission manifest) | **Codex** (named by the operator 2026-11-12). First pass received 2026-11-13: **REQUEST_CHANGES** on head `bf03b8e0`; revision 2 answers it (§10); revision 3 adds measurements only (§10.1); revision 4 adds one D2 rule (§10.2). Re-checks of `88768412` (2026-11-13) and `217d9c91` (2026-11-14): **REQUEST_CHANGES**; revision 5 answers both (§10.3). Re-check of `222e9083` (2026-11-14): **REQUEST_CHANGES** on historical availability only; revision 6 answers it (§10.4) |
| Signing order | reviewer's first pass on this draft → owner revises and signs (body hash pinned) → reviewer runs §8 and signs the admission manifest per hypothesis |
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
| `intelligence/event_sequence.py` | — (read-only) | API, forensics, pattern_engine | **missing** | G6. #1145 (merged 2026-11-12) reads `signal_sources` by column name and times filings by `filing_date`. Codex's reader guards (8befc05e, integrated as `25467893`, **merged and deployed in #1167 (`99dbd52a`, 2026-11-13)**) go further: signal events carry a timing hold, and calendar/filing rollups are withheld until release/version receipts exist |
| `intelligence/causal_links.py` (+ `causation*`) | `causal_links` | systemd template | **present** (`known_at` + basis), but `known_at` = end of `earnings_date` for calendar rows | G7: accepts `earnings_calendar` compat stubs as releases. 8befc05e holds that timing in the three direct `causal_links` readers (deployed in #1167, `99dbd52a`). `causation_graph` calendar reads and persisted `causal_chains` are **not** covered and remain unqualified |
| `intelligence/actor_network.py` + `actors/` | `actors`, `wealth_flows`, `actor_connections` | Hermes, 6 h | missing | CIK→ticker from the current `company_tickers.json` only |
| `intelligence/sleuth_intersections.py` | `investigation_leads` | Hermes, daily | proxied by `signal_sources.created_at` | — (keeps #1071 construction rules) |
| `intelligence/forced_flow_monitor.py` | `forced_flow_briefings` | Hermes 06:00–06:40 **UTC** (docstring says ET) | n/a | ignores holidays; FOMC dates hard-coded for 2026 |
| `people_events` (migration `people_events_20260927`) | `people_events` | **no scheduled materializer** | **present** (`known_at`, `known_at_basis`, `dedup_key`, supersede/retract) | stale since 2026-07-01 |
| `intelligence/security_master.py` | `security_master` (22,401 rows), `security_identifiers` (`valid_from`/`valid_to`) | — | — | no D1 module uses it; seed never flips `is_active` |
| #1050 / #1051 / #1090 (open drafts) | SEC periods, AV transcripts, company documents | — | present in design | unmerged; #1051 stops new earnings stubs but does not repair existing ones |
| Foundation source kit `25f7a982` (Codex, unmerged) | — | — | — | **SOURCE-READY evidence only**: scoped independent source approval for its offline tests; includes the `commit_visibility_unproven` refusal (§2). Not research admission, not release approval |
| #1141 (merged) native-volume calendar | — | — | — | pins one calendar for the GEX native-volume reader. It does **not** certify product-expiration, FOMC or roll schedules, and grants no new source or capture authority. Holiday and calendar-estimate defects elsewhere (`forced_flow_monitor`, G12) are not fixed by it |

---

## 2. Clocks

### 2.1 Input clocks: every admitted input carries all six

An input enters a decision only if all six are known; otherwise it is `EXCLUDED_UNKNOWN_AVAILABILITY`.

1. **Observation / event time.** What the value describes.
2. **First public availability.** When anyone outside could first have known it. When only a proxy is
   available, the proxy is named. Example: the EDGAR acceptance datetime is first **SEC-public**
   availability. It is not necessarily the first public announcement, since a company release or a news
   wire can come earlier. Rules built on it use a conservative SEC-delayed entry and never claim to capture
   the earlier move.
3. **Retrieval / ingestion time.** When GRID's process received it (`pull_timestamp`, `registered_at`,
   `vintage`).
4. **Durable system visibility.** When the exact version became visible to readers **after commit**. This is
   not the ingestion timestamp:
   - `people_events.ingested_at` is the transaction start;
   - revision `recorded_at` is written before commit;
   - `known_at` can tighten later.

   Until a durable exact-version visibility receipt exists, covering corrections and retractions, clock 4 is
   `UNPROVEN`. Historical feature admission is refused, as source kit `25f7a982`'s
   `commit_visibility_unproven` refusal does. A receipt prototype is not a qualified producer (gap G13).
5. **Calculation time.** When the derived value was computed.
6. **Decision cutoff.** The instant the research rule acts. It must be ≥ clocks 2, 3 and 4 for every input,
   plus the registered order latency. Only prices **observed at or before** the cutoff may set direction,
   eligibility or ranking. A track may fix an earlier price-observation cutoff than its decision time; D1
   does (§5.1: prices observed by the day-0 close O, decision at the later deadline D).

### 2.2 Outcome clocks are separate

- **An outcome is not an input.** It is the entry and exit prices and the horizon return.
- **Each outcome has its own clock and provider contract.** An outcome stays `PENDING` until its horizon has
  matured and its price is available under that contract.
- **A late, missing or revised outcome stays `PENDING` / `UNAVAILABLE`.** It is never backdated into the
  decision, and it never changes a decision already made.
- **Forbidden:** requiring that an outcome be "present as of the decision time", or reading one before its
  horizon matures.

### 2.3 Corrected history

- **Retrieval time stands in for availability only in the forward period.** It may stand in for first
  availability or visibility there, never for history reconstructed today.
- **Revisable knowledge is read as of the decision cutoff.** This covers estimates, filings, relationships,
  availability, sector and universe membership.
- **The single exception is the pipeline-repair rule in §7.7.**

---

## 3. Coverage matrix (summary; full detail in the JSON manifest)

### 3.1 D2 — SPY inputs

| Input | Custody | Coverage | Clocks | Status for D2 |
|---|---|---|---|---|
| `options_capture_batches` / `options_snapshots_all`, SPY, Cboe delayed (~15 min) | GRID griddb, append-only | **2026-10-01 .. 2026-11-11, 30 sessions, 359 batches** (measured 2026-11-12) | batch `registered_at`, `capture_started/completed_at`; per-contract quote time absent; spot clock only from 2026-11-10 | forward-only; too short for any historical claim; OCC symbol not stored, no bid/ask size |
| `options_daily_signals` / `spy_iv_*` features | GRID | same as above | obs = release = snap date | diagnostic; "25-delta" is a ±10 % moneyness proxy |
| CBOE VIX / VIX9D / VIX3M | GRID `raw_series` | VIX 1990-01-02 .. 2026-11-10; VIX9D from 2011-01-04; VIX3M from 2009-09-18 | daily close; pull_timestamp | **CANDIDATE_UNREVIEWED**. Proposed for regime conditioning as the prior session's close, read under the §6.1 daily-conditioner rule. "Published after the close" is an assertion, not a checked historical first-availability receipt. Measured 2026-11-13 (`d0-receipts/vix_*`, `vintage_20260409`): `vix_spot` carries real Cboe prints on non-session days (from 2022-05-30, every NYSE holiday except New Year's Day, Good Friday and Christmas; also the 2004-06-11 and 2025-01-09 closures); CBOE, FRED and yfinance agree on them. The 2026-04-09 yfinance vintage holds 1,640 rows more than 5 % off the other sources, superseded on 2026-04-10. The whole vintage (9,134 rows) was retracted on 2026-11-13 as bookkeeping (run_tag `vix-spot-yf-20260409`, `d0-receipts/vix_0409_retraction*`); no date was served from it before or after. Only one of them (obs 2026-04-09) is a first release. VIX9D/VIX3M were not measured |
| CBOE SKEW (`skew_index`) | GRID `resolved_series` | **1990-01-02 .. 2026-11-11**, 9,268 dates (measured 2026-11-13, query receipt `d0-receipts/g11_skew_coverage`). Since 2026-03-26 the only `vix_spot` dates without SKEW are 4 US market holidays; 50 such dates over the full history, not itemised | daily close; first resolved vintage 1 day after obs for 150 of 160 dates since 2026-03-26, 2–6 days for 10. Rows for 2026-03-26 .. 11-11 were committed by the 2026-11-13 backfill (clock 4 **UNPROVEN**) | **CANDIDATE_UNREVIEWED**, on the same terms as VIX: "after the close" is an assertion, not a first-availability receipt (G11 coverage closed) |
| Schwab chains / quotes / 1-min bars | process-local only, `persist_allowed=False`; no redistribution | live only | provider quoteTime + receipt time | live inputs for forward shadows; no history; no aggressor side |
| Polygon `/v3/snapshot/options` | — | 403, puller SKIPPED | — | unavailable |
| SPY 1-min bars, Polygon | offline research copy (GEX repo scratch) | rolling ~2 yr (2024-11 ..) | bar end | inspected (§4); not GRID custody |
| SPY 1-min bars, XNAS.ITCH | offline research copy | 2022-11 .. 2026-11 | bar end | inspected (§4) |
| OPRA cbbo-1m, near-spot 0DTE strikes | offline research copy | 2022-11-14 .. 2026-11-09 fetched; vendor history from 2013 | quote in force at minute end | inspected 2022-11-14 .. 2026-11-09. Pre-2022-11 not fetched by this lane: a **candidate** replication window, not untouched by assertion (§6.2, G5) |
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

- **Universe:** US-listed common stocks whose trailing 60-session median dollar volume is ≥ $20 M, measured
  over the 60 sessions **ending the session before acceptance**. Computed from Tiingo EOD including delisted
  names (existing key, no new vendor). No future-informed liquidity or universe selection. Identity through `security_master` / `security_identifiers` valid at the event date;
  ticker reuse resolved by CIK. Until this universe exists (gap G3) no historical D1 look can run.
- **Families (k = 2 evaluated, 1 evidence-only).** Chosen for source quality, not for expected return:

| ID | Family | Event source and first-availability clock | Evaluated? |
|---|---|---|---|
| D1-F1 | Earnings release | SEC 8-K **Item 2.02** (Results of Operations), first availability = EDGAR acceptance datetime from the filing header; replaces `earnings_calendar` dates entirely | yes |
| D1-F2 | Material agreements and completed deals | SEC 8-K **Items 1.01 / 2.01 / 1.02**, same clock; `deal_pipeline` / `business_events` headlines attached as secondary evidence with their clock gap shown | yes |
| D1-F3 | Insider open-market purchases | `people_events` form4, `known_at` = filing | **evidence-only**; evaluation stays with VS1 v8 / trade-edge-v2 |

  Not in v1: customer-supplier links (no change history), index additions/deletions (no source),
  OPEX/FOMC forced flows (calendar defects; SPY flows are D2's), headline-only deal news.
- **Clocks (one price-observation cutoff O and one decision deadline D per event; daily data only in v1).**
  - **Acceptance A:** the EDGAR acceptance datetime. This is the SEC-public proxy (§2.1).
  - **Session closes are the actual ones.** Every close below is the session's actual regular close from
    `ingestion/market_calendar.py` plus the #1100 corrections: 16:00 ET, or 13:00 ET on an NYSE early close.
    16:00 is never assumed.
  - **Day 0:** the first regular session whose actual close is **strictly after A + 5 minutes**. So an
    acceptance more than 5 minutes before that day's close (before 15:55 ET, or 12:55 ET on an early close)
    falls on that day; a later or after-close acceptance falls on the next session.
  - **Reference price P₋₁:** the last regular-session close **before** A.
  - **Price-observation cutoff O:** the actual close of day 0. Direction, eligibility and ranking use only
    prices **observed** at or before O: P₋₁ and the day-0 close C₀.
  - **Operational decision deadline D:** 20:00 ET on the calendar day of day 0, which is later than O.
    The decision is made at D, never at O. 20:00 is a fixed margin chosen before any data is read, not a
    measured publication time. Every input, C₀ included, must be available by D under §2.1 (clocks 2, 3
    and 4 at or before D):
    - **Forward period:** C₀'s retrieval time and durable-visibility receipt must both be at or before D.
    - **Historical windows: availability by D is unknown.** GRID did not retrieve these closes at the
      time, and nothing held shows that a given close, in the version read today, existed by 20:00 ET
      ("the same evening" is not a 20:00 bound). A historical run may read C₀ only under the label
      `PROXY_EOD_BY_DEADLINE`, which names the gap and does not fill it:
      - its clocks 2, 3 and 4 stay **unknown**, never treated as timely;
      - its late-input exclusion cannot be applied, because no historical observation shows which closes
        missed D, so no historical event is `EXCLUDED_LATE_INPUT` and the miss rate is reported as unknown;
      - every result built on it is **DIAGNOSTIC_ONLY / NOT_ADMITTED**. It cannot satisfy any D1 GO,
        RETIRE or HOLD verdict (§5.4), any admission (§8) or any claim beyond a description.
      - §7.7's repaired-price exception covers the price **basis** only; it does not establish publication
        time.

      The only route to a judged historical D1 result is gap G15: a price-only availability exception tied
      to a named source, version and basis and to retained evidence. It must state which clocks it does not
      establish and which claims stay forbidden, and be qualified before any judged look. Until G15 is
      closed, the D1 holdout (§5.2) stays unopened.
    - **Late or missing C₀:** an event whose C₀ is not available by D is `EXCLUDED_LATE_INPUT` and counted
      (§7 rule 4). It is skipped, never deferred to a later session, and C₀ is never backdated to D.
  - **Order latency (registered):** the entry is a market-on-open order for day +1, submitted no earlier
    than D + 15 minutes and before the listing exchange's opening-auction order cutoff. An event whose
    order cannot meet that cutoff is `EXCLUDED_LATE_INPUT`.
  - **Entry:** the open auction of day +1, the first regular session after day 0.

  The entry open is an outcome-clock observation (§2.2). It never decides its own fill. The day-0 move
  (C₀ / P₋₁ − 1) is recorded on the card as "already moved" and is never claimed.
- **Horizons (2):** [entry, +5 sessions] and [entry, +20 sessions], open-to-close.
- **Outcome:** return minus the sector ETF return over the same span (sector from `analysis/sector_map`
  as of the event date), with SPY-relative reported alongside.
- **Variants:** direction = sign of C₀ / P₋₁ − 1 (continuation). Its mirror (reversal) is the control, not
  a second chance. A zero or missing C₀ or P₋₁ makes the event `EXCLUDED_INPUT`.
- **Controls:** (a) declare-all baseline — every eligible event, unranked; (b) placebo — the same issuers on
  dates shifted by ±60 sessions; (c) shuffled-relationship control for any card using a link.
- **Ranking rule (frozen before D3):**
  - Order: data quality tier, then event strength, then liquidity. No composite probability.
  - **Event strength (F1 and F2):** |C₀ / P₋₁ − 1| divided by the issuer's 60-session close-to-close
    volatility ending at P₋₁. Item type does not set F2's strength: three item types cannot form five
    reproducible quintiles. F2's item set is recorded on every card and reported per quintile.
  - **One event per accession.** An 8-K that carries Item 2.02 is an F1 event only, whatever else it
    reports. Otherwise an 8-K carrying any of Items 1.01, 2.01 or 1.02 is one F2 event, with its item set
    recorded.
  - **Ranking bucket:** a quintile is not formed across events whose cutoffs fall later. Each event is placed
    in a quintile by four fixed strength breakpoints b₁ < b₂ < b₃ < b₄, computed per family on the
    development window only and frozen before the holdout opens. So an event's bucket is known at its own
    deadline D.
  - **Ties:** an event's quintile is 1 + the number of breakpoints at or below its strength, so a strength
    equal to a breakpoint goes to the higher quintile. Within a bucket, any ordering ties are broken by
    liquidity (higher first), then by accession number ascending.
  - **"Priced-in"** is shown as unknown when there is no PIT consensus or option-implied move,
  which is the default in v1 (`earnings_calendar` estimates are not PIT).
- **Ranking implementation, frozen in source and tests before admission** (requirements, not yet built):
  - **Quantile estimator:** one named estimator computes b₁ .. b₄ from the development strengths. It is
    fixed in source, with tests, before the development data is read.
  - **Zero or missing volatility:** an issuer whose 60-session volatility is zero or missing at P₋₁ has no
    strength; the event is `EXCLUDED_INPUT` and counted, never given a substitute volatility.
  - **Duplicate breakpoints:** if development strengths tie so that strict b₁ < b₂ < b₃ < b₄ do not exist,
    the family is HOLD (cause recorded). Cutoffs are never invented, and never chosen after the holdout
    is seen.
  - **Liquidity (tie-break):** the universe measure, trailing 60-session median dollar volume ending the
    session before A (§5.1 universe).

### 5.2 Windows (fixed before the EDGAR fetch)

- Development: acceptance dates 2012-01-01 .. 2018-12-31.
- Holdout: 2019-01-01 .. 2025-12-31, one look per family × horizon. It stays unopened until G15 is closed (§5.1).
- Forward: from the signing date, through the existing E2/S10 forward chain.

No D1 family above has been tested on these windows by any ledger in §4 (VS1 covers insider density only).

### 5.3 Success thresholds (fixed before any outcome)

- **Statistical:** for each family × horizon (4 cells), the top ranking quintile's mean net excess return
  minus the declare-all mean is > 0 with a block-bootstrap lower bound (clusters = event week and issuer;
  B 10 000, seed 20261107) above 0 at one-sided 1 − 0.05/4 = 0.9875. Costs 10 bp per side; 25 bp
  sensitivity reported.
- **Sample floor:** ≥ 200 events in the top quintile and ≥ 60 distinct event weeks per cell in the holdout,
  else UNDERPOWERED.
- **Power rationale.** These numbers are placeholders, not the gate.
  - Assuming per-event excess-return SDs of about 4 % at +5 sessions and 8 % at +20, n = 200 gives a naive
    SE of about 0.28 % / 0.57 %. The minimum detectable difference at 0.9875 and 80 % power would then be
    about 0.85 % / 1.7 %.
  - **Clustering:** the test clusters by issuer and event week, so the naive per-event SE overstates power.
    Before the holdout opens, the development data alone is used to estimate the effective sample size (the
    design effect from those clusters) and the variance of the **paired** statistic (top quintile minus
    declare-all).
  - **The power gate is frozen from those estimates before any untouched outcome is read.** If power is
    below 0.5 at the literature-plausible effect, the cell is STOP_UNDERPOWERED before the holdout, as in
    the VS1 Stage-0 gate.
- **Economic:** top-quintile mean net excess ≥ 0.5 % at +5 or ≥ 1 % at +20; capacity reported at 1 % of
  median dollar volume.
- **Operational, inputs:** each event needs all of the following, observed by O and available by D:
  - an acceptance datetime;
  - a CIK that resolves to one security valid at A;
  - P₋₁ and C₀;
  - a trailing liquidity and volatility window;
  - a sector as of A.

  Otherwise the event is `EXCLUDED_*` and counted.
- **Operational, outcomes:**
  - The entry open and the +5 / +20 exit prices are outcome-clock observations (§2.2), read under the price
    contract once the horizon has matured.
  - Until then the event is `PENDING`. A late or missing outcome stays `PENDING` / `UNAVAILABLE` and never
    changes the decision.
  - Exclusions over 10 % of eligible events in a cell → HOLD with the cause. Inputs and outcomes are counted
    separately.

### 5.4 GO / HOLD / RETIRE

Each horizon cell gets one verdict:
- **UNDERPOWERED:** below its sample floor, or STOP_UNDERPOWERED.
- **PASS:** it meets its floor and its statistical threshold (§5.3).
- **FAIL:** it meets its floor but not its statistical threshold.

Only results whose every input has clocks 2, 3 and 4 shown at or before D enter this table. A result
built on `PROXY_EOD_BY_DEADLINE` (§5.1) is DIAGNOSTIC_ONLY and gets no verdict here.

A family's verdict is the first of these rules that applies, in this order:
1. **HOLD:** the EDGAR/universe gaps (G2, G3) are unresolved, or exclusions exceed 10 % in either cell.
2. **HOLD:** either cell is UNDERPOWERED. This takes precedence over any point estimate: PASS +
   UNDERPOWERED is HOLD, and so is FAIL + UNDERPOWERED.
3. **RETIRE:** both cells FAIL.
4. **GO:** both cells PASS, or one PASSes and the FAIL cell's point estimate is > 0. Either way the §5.3
   economic threshold must also be met; otherwise the verdict is HOLD (below the economic threshold).
5. **HOLD (horizons disagree):** one PASSes and the FAIL cell's point estimate is ≤ 0.

A HOLD from rule 2, 4 or 5 is never converted to GO by more calendar time on the same windows. A changed
family is a new registration on the forward period only.

### 5.5 Budget

EDGAR 8-K header backfill 2012–2025 is free but rate-limited (10 requests/s): roughly 1 M filings,
two to three days at half the limit. It is a new collection job and needs operator approval. Storage in griddb
under an append-only table with accession as key. Compute small.

### 5.6 Examples required before admission

- **Traceable:** one 8-K Item 2.02 from `earnings_events` (accession, filing date) → acceptance datetime →
  entry open → card with "already moved", sector-relative outcome slot left PENDING. Binds to the #958 chain
  shape (input → materialization row → reasoning output or unavailable reason → retained bytes); M0 does not
  prove accuracy.
- **Refusal:** one `earnings_calendar` compat-stub row (`earnings_date` equals a SEC `period_end`,
  `reported = TRUE`), refused as `EXCLUDED_UNKNOWN_AVAILABILITY`.
  - "2,131 rows" is the author's measurement from 2026-11-12. Date equality alone does not prove every
    matching row is a stub, so no row is deleted, rewritten or quarantined on that count.
- **Leakage fixture (synthetic, no outcomes):** five filings, all with outcomes left `PENDING`:
  - pre-open, accepted 07:00 ET;
  - intraday, accepted 11:30 ET;
  - after-close, accepted 16:30 ET.

  - early close: accepted 12:58 ET on an NYSE early-close day, so day 0 is the next session;
  - delayed C₀ (forward case): C₀ becomes visible at D + 1 minute, so the event is `EXCLUDED_LATE_INPUT`,
    not decided at a later time.

  The test mutates every price observed **after** each event's O and asserts that direction, eligibility,
  strength and quintile are unchanged. It also mutates C₀ and asserts that they do change.

---

## 6. Track D2 — SPY volatility lab, v1

### 6.1 Frozen scope (proposed)

- **Instrument:** SPY and its listed options only. No SPX, no other underlying.
- **Calendar:** NYSE regular sessions from `ingestion/market_calendar.py` plus the local corrections already
  in #1100; half days excluded from intraday families; sessions with thin open interest excluded by the edge4
  rule (fixed before data, recorded per session).
- **Daily conditioners (VIX, VIX9D, VIX3M, SKEW).** For decision session *t*, a conditioner is the official
  close dated on the last session of this calendar strictly before *t*, read `as_of` the decision time
  (§7 rule 6). The conditioner's own row dates never define sessions:
  - prints dated on non-session days are ignored, never used as "the prior session" and never averaged in.
    Cboe publishes VIX on most NYSE holidays since 2022-05-30 (§3.1); SKEW is not published on them;
  - if the value for that session is missing, the cell is excluded for session *t* and counted (§7 rule 4).
    It is never filled from an earlier session or from another conditioner;
  - a read that would serve the bad 2026-04-09 yfinance `vix_spot` vintage (`as_of` on 2026-04-09) is
    excluded and counted. A retraction cannot replace this rule: `store/pit.py` hides a retracted row only
    for `as_of` on or after the retraction date, so replays at `as_of` 2026-04-09 would still serve it.
    The vintage was retracted on 2026-11-13 (`d0-receipts/vix_0409_retraction*`); this rule stays;
  - sessions are checked against `ingestion/market_calendar.py` plus the #1100 corrections, never against
    the dates of another series. Calendar comparisons exclude retracted rows (`spy_full` holds retracted
    rows on the closed days 2025-01-09 and 2025-07-04).
- **Separate questions, scored separately:** (a) forecast quality of a volatility measure (diagnostic);
  (b) after-cost tradability of a fixed structure (economic). A pass on (a) never implies (b).

### 6.2 Hypotheses: D2-H1 has two cells; D2-H2 and D2-H3 are adopted unchanged

| ID | Hypothesis and exact estimand | Data | Window | Look |
|---|---|---|---|---|
| D2-H1a / D2-H1b | edge6 replication at **14:00** (H1a) and **15:00** (H1b) ET, two separate cells. Short ATM iron butterfly exactly as edge6 (registered cb7b213), with K = the strike nearest spot that has both a call and a put quote. **Entry:** sell the K call and K put at their bids and buy the K+5 call and K−5 put at their asks, at the quote mark T (§6.3); spot = the close of the 1-minute bar ending at T. **Exit: a synthetic intrinsic-payoff proxy, not settlement.** Each leg is valued at its intrinsic value at S, the close of SPY's 15:59 1-minute bar (the edge6 formula, `vrp_edge6.py` at 35fa182); no early exit. SPY options are American-style and physically settled, so this proxy makes no claim about exercise, assignment, after-hours moves or the official expiry value. **Fees, bound to edge6:** $0.0003 per share per leg, which is $0.03 per contract at the 100-share multiplier, charged on the four entry legs only and never at the proxy exit (edge6 protocol cb7b213: "Fees 0.0003 per share per leg"; code `FEE = 0.0003`, net = credit − payoff − 4 × FEE). Net in dollars per share; risk per trade = 5 − credit, per share. | OPRA cbbo-1m plus SPY 1-minute bars, not yet fetched | Monday/Wednesday/Friday SPY expiry days 2018-01-02 .. 2022-11-11. The exact start, contract list and strike/target scope are fixed in a fetch manifest **before** the fetch | one look per cell after the fetch manifest is hashed |
| D2-H2 | fly-shadow/1 forward: the 15:00 fly **exited at 15:55** (shorts bought back at the ask, wings sold at the bid) | live Schwab chain, ledger `fly_days` | forward from 2026-11-12 | one look at 100 traded sessions. Registered fe8318a; adopted **unchanged**, including its own test and level |
| D2-H3 | bounce-shadow/1 forward: long SPY 15:29 → 15:59 after a negative-gamma late-day bounce | live dte0 regime plus bars | forward from 2026-11-12 | one look at 50 qualifying sessions. Registered f8b3874; adopted unchanged. **Not independent of history:** chosen after edge7 P3 (short the same bounce, −0.391, n 83) was read |

**The estimands differ:**
- **D2-H1** is graded on the synthetic payoff proxy and is historical.
- **D2-H2** exits at 15:55 and runs forward.

A D2-H1 pass does not show that the 15:55 exit earns the same, and a D2-H2 pass does not replicate the
proxy result. The only comparison D0 makes is the GO rule in §6.5, frozen here before either look.

**Pre-2022 window, before fetching (G5):**
- **Reconcile the full inspection ledger.** This covers GEX edge1–edge8, #647, VS1, #1065 and the daily-SPY
  calendar looks in §4. The aim is to confirm that no SPY option-premium family, entry time or contract scope
  has been tested on 2018 .. 2022-11.
- **Protected reservations.** The daily-SPY reservations (edge3 holdout 2013 .. 2026-11-10, calendar rules
  only) and the insider reservations stay protected. They are not opened by this fetch.
- **If reconciliation finds an overlap,** that cell is struck before the fetch and recorded here as a new
  revision.

**Excluded entry times:** 09:35 (edge6 FAIL) and 12:00 (edge6 PASS, but not chosen) are not tested, and are
counted in the selection disclosure in §6.4.

Diagnostic only (no pass/fail, cannot promote): realised-vs-implied variance by time of day on GRID's own
`options_capture_batches` once it reaches 120 sessions.

Not in v1: anything requiring trade-side live data (Schwab has none), any SPX family, any new 0DTE
intraday rule on 2022-11 .. 2026-11.

### 6.3 Execution and costs (fixed)

- **Quote marks.** Every signal, fill and exit mark uses the quote **in force at or before** the mark: the
  latest update at or before it, never one after.
- **Maximum quote age:** 60 s per leg.
- **Maximum spread:** $0.20 per leg, or 50 % of that leg's mid, whichever is larger.
- **Simultaneous availability:** all four legs must meet the age and spread rules at the same mark.
  Otherwise the session is `EXCLUDED_INPUT` for that cell.
- **Size:** one contract per leg, full fill at the quoted price. Displayed size and partial fills are not
  modelled.
- **D2-H1 (replication):** fills at mark T, exactly as edge6.
  - Spot and quotes share the mark, so each fill is labelled **MODELED_BBO_SAME_MARK**.
  - A conservative sensitivity, reported and not judged, fills at the quote in force at T + 1 minute.
- **D2-H2:** keeps fe8318a's registered fills.
- **Fees:** D2-H1 uses the edge6 fee in §6.2: $0.0003 per share per leg, entry legs only. D2-H2 keeps fe8318a's registered fees and units. No mid fills in any judged result.
- **Labels.** Every option result is labelled **MODELED_BBO**, not observed execution; D2-H2's
  Schwab-quote-graded fills are modelled too. A quote snapshot within two minutes with positive bids is
  **not** proof that the trade could be executed.
- **What missing execution proof blocks.** Without it, no economic or execution claim can be made, in
  particular the §6.4 economic threshold for anything beyond MODELED_BBO. Descriptive and statistical
  diagnostics can still be reported.
- Shares: $0.04 per share round trip (D2-H3), plus a 6 bp round-trip sensitivity (`evals/e2` `e2-costs-v1`).
- **Gap:** no options cost class exists in `evals/e2/cost_model.json`. D2 must add `us_equity_option_0dte`
  to the cost model (versioned) before D3 can score through E2.
- **D2-H1 is judged on the synthetic payoff proxy** (§6.2). No D2-H1 result is a claim about exercise, assignment or after-hours outcomes.
- **For the live-exit hypothesis (D2-H2),** the proxy-graded result is reported but never judged.
- **Pin risk** is reported for every fly cell, as the share of entries whose proxy mark S is within $0.25 of a short
  strike.

### 6.4 Success thresholds (fixed before any outcome)

**Statistical:**
- **D2-H1a and D2-H1b** are each tested separately: mean net per trade > 0, with a seeded
  percentile-bootstrap lower bound (B = 10 000, seed 20261107) above 0 at one-sided **1 − 0.05/2 = 0.975**
  (Bonferroni over the two H1 cells).
- **D2-H1 PASS** requires **both** cells to pass (an intersection–union rule), and each cell's mean must be
  positive in a majority of the calendar years covered.
- **If one cell passes,** that cell's own claim stands, but D2-H1 is not a PASS.
- **D2-H2 and D2-H3** keep their registered tests and levels (0.95) unchanged. D0 does not regrade them.

**Program-level selection, disclosed and not corrected by these per-look levels:**
- edge6 was chosen as the one PASS among the SPY option and intraday looks of 2026-11-06 .. 11-12 (#1133: 10
  looks, about 40 rules and cells, all others FAIL or UNDERPOWERED).
- edge6's own four entry times included 09:35 FAIL and 12:00 PASS.
- D2-H1 is a replication on a candidate-untouched window for that reason. Its result is reported together
  with the full program count, and never as a standalone discovery.

**Sample floor:** D2-H1a and D2-H1b n ≥ 300 sessions each; D2-H2 n = 100; D2-H3 n = 50. Fewer eligible
sessions than the floor = UNDERPOWERED = HOLD, never PASS.

**Power rationale (D2-H1/H2):** edge6 at 15:00 gives mean +0.195, bootstrap LB(0.9875) +0.131 at n = 986,
so SD ≈ 0.90 per trade. At n = 100 (D2-H2), the one-sided 95 % bound clears 0 when the sample mean
exceeds ≈ 0.148: power ≈ 0.70 if the true mean is 0.195, ≈ 0.50 if a 15:55 exit cuts it to 0.15. The
distribution is left-skewed (many small wins, rare losses near −4), so the bootstrap at n = 100 is likely
optimistic; D3 reports a block-bootstrap and a worst-decile sensitivity. At n = 300 per H1 cell (level 0.975, bound clears 0 above ≈ 0.102), power ≈ 0.96
for a true mean of 0.195 and ≈ 0.82 for 0.15. Because both cells must pass, joint power is about their product
if they are independent. The 14:00 and 15:00 cells share sessions, so joint power is higher than that product. These use the 2022–2026 SD; M/W/F-only history may differ.

**Economic:** mean net ≥ 2 % of capital at risk (5 − credit) per trade; max drawdown reported in units of
mean monthly net; positive in a majority of calendar years covered.

**Operational (per session, before inclusion):**
- all four legs meet the §6.3 quote rules at every judged mark: quoted at or before the mark, aged ≤ 60 s,
  within the spread cap, bid > 0 and ask ≥ bid;
- the spot clock is present.

Otherwise the session is `EXCLUDED_INPUT` and counted. D2-H2 keeps fe8318a's own inclusion rule. If excluded sessions exceed 10 % of eligible sessions at any fixed review
(the #647 stop rule), the hypothesis halts as HOLD with the cause recorded.

### 6.5 GO / HOLD / RETIRE

- **GO** (frozen here, before either look): D2-H1 PASS **and** D2-H2 PASS at their own looks.
  - This cross-program rule is D0's. It reads D2-H2's verdict as registered and does not change it.
  - **D4 does not run another fly pilot.** D2-H2 is already the prospective paper test, so its immutable
    evidence maps directly onto D4's acceptance rule.
  - A D4 step is added only for a genuinely new validation question, such as executable fills. It is then
    registered separately, without touching fe8318a.
- **HOLD:** either is UNDERPOWERED, or operational exclusions exceed 10 %, or D2-H1 cannot be fetched
  (spend not approved). Calendar time alone never converts HOLD to GO.
- **Precedence (fixed before either look):** RETIRE first, then HOLD, then GO. A FAIL in any judged cell
  (H1a, H1b or H2) is RETIRE even when another cell is UNDERPOWERED, because a FAIL is evidence against
  the structure and an UNDERPOWERED cell is not evidence for it. Without a FAIL, any UNDERPOWERED cell or
  other HOLD cause gives HOLD.
- **RETIRE:** D2-H1 FAIL **or** D2-H2 FAIL. D2-H1 FAIL means either H1 cell FAILs; an UNDERPOWERED cell gives HOLD, not FAIL. A retired structure returns only
  as a new registration on new data.
- D2-H3 decides only its own GO/RETIRE; it cannot rescue or sink the fly.

### 6.6 Budget

Compute: single host, < 2 h per look. Storage: research copies stay outside griddb until #1108 defines
snapshots. Spend: the D2-H1 OPRA fetch is a usage-based purchase and needs the operator's explicit approval
with a quoted cap before it starts; no subscription.

### 6.7 Examples required before admission

- **Traceable:** one fly-shadow/1 graded session. It shows:
  - chain capture id;
  - quotes at 15:00 and 15:55, with each leg's quote time and age;
  - fills labelled MODELED_BBO;
  - net;
  - ledger row hash.
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
7. **Repaired prices: the one narrow exception to rule 6.** The #1084 repair fixed GRID's own pipeline
   error (mixed raw and adjusted vintages), not information the market lacked. So historical research may
   read the repaired price series at the current `as_of`, under these conditions:
   - **Fixed repair manifest.** The read is bound to the repair run_tags (`full-basis-20261109`,
     `full-basis-20261110-nine`, `full-basis-20261110-xlre`, `tiingo-foreign-20261109`) and the
     retraction-table state as of the contract's signing. A later repair is a new revision.
   - **Basis.** Tiingo adjusted closes, used **only as returns** (ratios within one series), never as price
     levels or level thresholds, because they are back-adjusted with later corporate actions.
   - **Corporate actions.** Splits and dividends inside a return span are handled by the adjusted series
     itself. A span containing a delisting, symbol change or merger is resolved through `security_master`
     validity at the event date, or excluded.
   - **What it does not admit:**
     - revised knowledge: estimates, filings, relationships, availability;
     - current sector membership (sector is read as of the event);
     - any liquidity or universe selection computed with data after the decision cutoff.

     Those are always read as of the decision cutoff (§2.3).

## 8. Admission manifest and independent review

Admission happens per hypothesis, never project-wide.

**While this contract is unsigned, no input may carry `ADMITTED`.** The strongest allowed label is
`CANDIDATE_UNREVIEWED`. An input label is never a hypothesis admission.

An input may be marked `ADMITTED` only when all of these hold:
- the contract is signed and the body hash is pinned;
- the reviewer has signed the manifest;
- the input has a measured missingness entry;
- it has a first-availability receipt, not an assertion such as "after close";
- it has a durable-visibility status other than `UNPROVEN`, where clock 4 applies;
- it has a file or query receipt.

`analysis/d0_manifest.py` is a **provisional label checker, not an admission authority** (Codex's
cf879835, integrated in revision 5). Until an authenticated admission verifier exists (G14), it refuses
**every** `ADMITTED` label, input or hypothesis, even one whose listed prerequisites look complete. Its
prerequisite checks are diagnostics only. Its test runs in CI. So signing this contract cannot admit
anything until G14 is closed.

A hypothesis is admitted when every input it uses is `ADMITTED` and coverage meets its sample floor.

**Reproducibility record,** per input:
- exact query text and its sha256;
- source cutoff or snapshot identity;
- source version;
- receipt hash;
- exclusion list.

A later live re-query that differs is a **new coverage observation**, recorded with its own receipt. It is
not a failure, and it does not overwrite the earlier one.

The independent reviewer then:

- [ ] re-runs each coverage query from its recorded text against the recorded snapshot, or records a new
      observation as above;
- [ ] runs the §5.6 leakage fixture and the clock-4 visibility refusal;
- [ ] injects late, revised, missing and duplicate observations into a fixture and confirms each is
      excluded or deduplicated as stated;
- [ ] reproduces the traceable example and the refusal example for each track;
- [ ] checks §4 against the GEX records and #1065 for any window this contract would reuse;
- [ ] signs the manifest (`reviewer`, `signed_at`, `manifest_sha256`).

Gaps block only the hypotheses named in the gap list below.

## 9. Prioritised gap list

| # | Gap | Blocks | Proposed owner | Needs |
|---|---|---|---|---|
| G1 | ~~Research owner and independent reviewer not named~~ resolved 2026-11-12: owner Claude, reviewer Codex | signing | — | Codex pickup + first pass |
| G2 | No EDGAR acceptance-datetime history; `earnings_events` starts 2026-02-19 | D1-F1, D1-F2 history | existing SEC lane (#1050 author) | approval of a free backfill job |
| G3 | No point-in-time liquid-equity universe | all D1 history | — | Tiingo EOD incl. delisted + `security_master` validity |
| G4 | No options cost class in `evals/e2/cost_model.json` | D2 scoring through E2 | E2 owner | versioned cost-model bump |
| G5 | Pre-2022-11 OPRA quotes not fetched | D2-H1a, D2-H1b | GEX research lane | First, reconcile the full inspection ledger and fix the exact contract and target scope in a fetch manifest (§6.2). Then the operator's spend approval with a cap |
| G6 | `event_sequence.py` dropped all `signal_sources` events (`r[8]`) and stamped filings at `period_end` | any D1 card that uses event_sequence | Claude (owner integration) | #1145 merged and deployed 2026-11-12. Codex's stricter guards (8befc05e) are integrated with #1145 as `25467893` (rebased onto `6e0b78b7`) in #1167, **merged and deployed 2026-11-13 as `99dbd52a`** (CI green on all 11 checks). Event-sequence timing is now held at the reader; lead times stay on HOLD for D1 until release/version receipts exist |
| G7 | `earnings_calendar` compat stubs (`earnings_date = period_end`; 2,131 by date equality, the author's count) read as releases | D1 (excluded by rule meanwhile); earnings causal-link timing on HOLD | readers: done in #1167 (`99dbd52a`, deployed 2026-11-13); producer: #1051 author | The reader-side hold covers the three direct `causal_links` readers, not `causation_graph` or persisted `causal_chains`. Stored rows are never mass-rewritten on the date-equality count. Any repair is separately authorised |
| G8 | S11/E3 windows cannot express contract identity, intraday horizon or "inspected, not tested" | native D2 window checks | S11/E3 owner | version bump |
| G9 | GRID holds 30 sessions of SPY chains; long history is offline research copies; OCC symbol and bid/ask size not stored | D2 GRID custody (#1132, #1108) | options ingestion owner | schema change, separately authorised |
| G10 | `people_events` materializer unscheduled (stale since 2026-07-01) | D1-F3 evidence currency | people_events owner | schedule approval |
| G11 | ~~CBOE SKEW stale since 2026-03-25~~ coverage closed 2026-11-13 | — (SKEW is now `CANDIDATE_UNREVIEWED`, like VIX) | — | #1146 (`entity_map`), #1148 (resolver series filter) and #1149 (obs-date floor) merged and deployed. A scoped backfill wrote 7,696 rows; `skew_index` resolves through 2026-11-11 with 0 conflicting values and 0 vintages before their obs date (receipts in `docs/research/d0-receipts/`). First availability and clock 4 stay open, as for every Cboe input |
| G12 | `forced_flow_monitor` ignores holidays; FOMC dates hard-coded for 2026 | forced-flow family (not in v1) | intelligence owner | Fix before any v2. Not fixed by #1141 |
| G13 | No durable exact-version post-commit visibility receipt (clock 4), including corrections and retractions | historical admission of `people_events` and any table whose ingestion time is not commit visibility | people_events owner | Source-only design that links durable receipts to the canonical `people_events` pipeline, plus a disposable-PostgreSQL concurrency test plan. No second materializer, no historical repair, no runtime activation |
| G14 | No authenticated admission verifier. The provisional checker refuses every `ADMITTED` (§8) | every admission, so every look | Claude (research owner), reviewed by Codex | Exact booleans and validated signature, hash and date formats; explicit required-input ids per hypothesis, each `ADMITTED`, with the sample floor measured and unknown dependencies refused; a durable-visibility state of applicable / qualified / not_applicable with reason and receipt, never null as an exemption; the pinned contract body digest verified at signing, and a manifest digest scope with no self-referential signature field. Source and synthetic tests only |
| G15 | No qualified historical price-availability exception for D1: historical C₀ availability by D is unknown (§5.1) | every judged historical D1 result; the D1 holdout stays unopened | Claude (research owner), reviewed by Codex | A price-only exception tied to a named source, version and basis and to retained evidence. It states the clocks it does not establish and the claims it forbids, and is qualified before any judged look. No clock backfill and no outcome read. Until then historical D1 runs are DIAGNOSTIC_ONLY |

## 10. Revision 2: response to the reviewer's first pass (2026-11-13)

| Reviewer item | Where answered |
|---|---|
| 1. D1 decision and ranking leakage | §5.1. One decision cutoff (day-0 close; split in revision 5 into the observation cutoff O and the deadline D, §10.3), reference prices observed by then, frozen development-only quintile breakpoints, entry at the next open as an outcome observation. §5.6 adds the pre-open / intraday / after-close leakage fixture |
| 2. Input clocks versus outcome clocks | §2.2 and §5.3. Outcomes stay `PENDING` until mature; late exits never change the decision |
| 3. Multiplicity and D2-H1 aggregation | §6.2 and §6.4. Two H1 cells at 0.975, both required; H2 and H3 unchanged; program-level selection disclosed |
| 4. Manifest looked admitted while unsigned | §3.1 and §8. `CANDIDATE_UNREVIEWED`; since revision 5, `analysis/d0_manifest.py` refuses every `ADMITTED` until the G14 verifier exists |
| 5. Durable knowledge visibility | §2.1 clock 4 and G13. `UNPROVEN` until receipts exist; reference to `25f7a982` `commit_visibility_unproven` |
| 6. D2 exit contract and executable-versus-simulated labels | §6.2 and §6.3. Exact entry and exit per cell; quote-at-or-before, age, spread, four-leg and size rules; MODELED_BBO labels; missing execution proof blocks economic claims only |
| EDGAR acceptance is SEC-public, not first announcement | §2.1 and §5.1 |
| Narrow pipeline-repair exception | §7.7 |
| Clustered power | §5.3 |
| Reproducibility beyond count matching | §8 |
| Pre-2022 OPRA is a candidate window | §3.1, §6.2 and G5 |
| No second fly pilot in D4 | §6.5 |
| Unmerged foundation fixes; #1141 does not certify calendars | §1, G6, G7, G11 and G12 |

### 10.1 Revision 3: measurements only (2026-11-13)

No threshold, window, hypothesis or rule changed. No outcome was read.

| Change | Where |
|---|---|
| SKEW coverage re-measured with two read-only query receipts (SQL text plus result file, sha256 in the manifest): `d0-receipts/g11_skew_coverage` and `d0-receipts/g11_skew_detail` | §3.1, G11, manifest `grid.raw_series.CBOE:SKEW` |
| SKEW moves from `EXCLUDED_STALE` to `CANDIDATE_UNREVIEWED`; clock 4 recorded as `UNPROVEN`, because the 2026-03-26 .. 11-11 rows were committed by the 2026-11-13 backfill | §3.1, manifest |
| Side finding, not checked further: resolved `vix_spot` carries rows on 4 US market holidays since 2026-03-26 (2026-05-25, 06-19, 07-03, 09-07). VIX missingness stays unmeasured | §3.1 |
| G6 status corrected: revision 2 said "PR pending" when no PR existed. The integration is #1167, merged and deployed 2026-11-13 as `99dbd52a` | §1, G6 |

### 10.2 Revision 4: one D2 rule (2026-11-13)

No threshold, window or hypothesis changed. No outcome was read. The rule only fixes which value a daily
conditioner takes; it is set before any D2 data is scored.

| Change | Where |
|---|---|
| Daily-conditioner rule: the prior-session close on the §6.1 session calendar; non-session prints ignored; missing → excluded, never filled; reads at `as_of` 2026-04-09 excluded (a retraction would not hide them, since it applies only from its own date) | §6.1 |
| VIX holiday prints and the 2026-04-09 vintage measured with five read-only query receipts (`d0-receipts/vix_holiday_rows`, `vix_vs_spy_calendar`, `vintage_20260409`, `served_20260409`, `retractions_check`) | §3.1, manifest `grid.raw_series.CBOE:VIX` |
| The revision 3 side finding is answered: the holiday rows are real Cboe prints, not a GRID defect | §3.1 |
| Note added after revision 4, no rule changed: the 2026-04-09 yfinance `vix_spot` vintage was retracted on 2026-11-13 10:31Z (9,134 rows, run_tag `vix-spot-yf-20260409`, operator-approved). Script, runner, dry-run and apply outputs, and a separate read-only check are in `d0-receipts/vix_0409_retraction*`. The §6.1 exclusion of reads at `as_of` 2026-04-09 stays | §3.1, §6.1, manifest `grid.raw_series.CBOE:VIX` |

### 10.3 Revision 5: response to the re-checks of 88768412 and 217d9c91 (2026-11-14)

No threshold, window, horizon, level or sample floor changed. No outcome was read. Every change below
fixes a definition, a label or a precedence before any look.

| Reviewer item | Change | Where |
|---|---|---|
| 1. Admission checker fails open (three synthetic witnesses) | Codex's cf879835 integrated as-is (authorship kept): the checker is a provisional label checker and refuses every `ADMITTED`. All three witnesses are now refused. The real verifier is gap G14 | §8, §9 G14, `analysis/d0_manifest.py`, manifest `admission_rule` |
| 2. D1 conflated the observation cutoff with usable-data time | The price-observation cutoff O (the actual day-0 close) is separate from the operational deadline D (20:00 ET). C₀ must be available by D; historical availability is the named proxy `PROXY_EOD_BY_DEADLINE`; a late C₀ is `EXCLUDED_LATE_INPUT`, skipped and never backdated or deferred. Registered order latency: D + 15 minutes, before the opening-auction order cutoff. Actual closes, early closes included; the leakage fixture adds early-close and delayed-C₀ cases | §5.1, §5.3, §5.6 |
| 3. D2-H1 called the 15:59 intrinsic mark "cash settlement"; fee units ambiguous | Relabelled as a synthetic intrinsic-payoff proxy, with no exercise, assignment or after-hours claim. Fee bound to edge6: $0.0003 per share per leg ($0.03 per contract at the 100-share multiplier), four entry legs only. H2 and H3 unchanged | §6.2, §6.3, manifest D2-H1a/b |
| Mixed-horizon precedence | D1: an UNDERPOWERED cell gives HOLD over any point estimate; FAIL + UNDERPOWERED is HOLD; PASS + FAIL with a point estimate ≤ 0 is HOLD (horizons disagree); GO also needs the §5.3 economic threshold. D2: RETIRE, then HOLD, then GO | §5.4, §6.5 |
| F2 strength and ties | F2 uses the same scaled day-0 move as F1 (item type alone cannot form quintiles); one event per accession, Item 2.02 → F1; fixed per-family breakpoints, a value equal to a breakpoint goes up, then liquidity and accession number break ties | §5.1 |

### 10.4 Revision 6: historical D1 availability (2026-11-14)

This answers the re-check of `222e9083`. No threshold, window, horizon, level or sample floor changed, and
no outcome was read.

| Reviewer item | Change | Where |
|---|---|---|
| Historical `PROXY_EOD_BY_DEADLINE` asserted availability by 20:00 without receipts, against §2 and §8 | The proxy now names a gap and does not fill it. Historical clocks 2–4 stay unknown; no historical late-input exclusion is claimed; every result on the proxy is DIAGNOSTIC_ONLY / NOT_ADMITTED and gets no §5.4 verdict and no admission. A judged historical result needs a qualified price-only exception (new gap G15); until then the D1 holdout stays unopened | §5.1, §5.4, §9 G15 |
| Implementation items before admission | Recorded as requirements, not built: one named quantile estimator fixed in source and tests before the development data is read; zero or missing volatility → `EXCLUDED_INPUT`; tied breakpoints → HOLD, never invented cutoffs; liquidity = the universe's 60-session median dollar volume | §5.1 |

<!-- D0-CONTRACT-BODY-END -->
