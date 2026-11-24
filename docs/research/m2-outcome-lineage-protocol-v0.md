# M2 outcome lineage and staged research protocol, v0 (draft, unreviewed)

Issue: [#960](https://github.com/3pacs/GRID/issues/960). Milestone: M2, "Evidence-based outcome learning protocol".
Machine-readable companion: [`m2-outcome-lineage-v0.json`](m2-outcome-lineage-v0.json). The companion is held to
the code by `tests/test_m2_outcome_lineage_map.py`.

**Status: DRAFT v0.** It is unreviewed and unsigned, and it authorises nothing. It adds no research job, production
scoring or learning, evaluation run, DDL, historical rewrite, promotion or trading action. Only the research owner
can adopt it, and only after the independent reviewer's pass (§6). Adoption is a separate commit that pins this
file's hash. The D0 contract ([#1129](https://github.com/3pacs/GRID/issues/1129), draft PR #1142) is the first
application of this protocol. Where D0 already fixes a rule, this document cites it and does not restate or amend it.

## 1. What already exists (reuse, do not rebuild)

The 2026-11-05 inventory on #960 found that the protocol was mostly code already. The open problem was binding the
pieces together. Since then, the publication chain has gained its own claim and outcome records:

| Piece | Code | State on main `0d0c4b9c` |
|---|---|---|
| Publication capture | `scripts/daily_publication_packet.py`, runbook `docs/runbooks/DAILY-PUBLICATION-PACKET.md` | scheduled on grid-svr at 10:35Z; publication id + text sha256 + chained receipts |
| Claim extraction and grading | `scripts/grade_publication_review.py` (schema `publication-review-verdicts-v1`) | scheduled at 11:35Z; claim span + `claim_sha256` + verdict + dated evidence |
| Verdict store | `scripts/ingest_publication_review.py`, migration `pub_review_ingest_20261108` | `publication_review_runs` / `publication_claim_verdicts`, append-only by trigger; regrades supersede runs and never edit them |
| Candidate accounting | `evals/e3` (`e3-v1`, E3A candidate ledger) and `analysis/ledger_steered_exploration.py` (S11) | released |
| Funnel and holdout custody | `evals/e3` `e3-v2` (E3B) | released, **fixture-only**. This answers the 11-05 open question on E3B's status. |
| Leakage gates | `evals/e1` (`e1-v1.4`), `store/pit.py` `assert_no_lookahead()`, `store/observations.py` `read_window_known_at` | released |
| Forward scoreboard | `evals/e2` (`e2-v1`; streams read through pinned adapters; `e2-stream-v1` log format) | released |
| Single forward look | `analysis/research_forward_log.py` (S10), `alpha_research/edge_stream/` | released / installed |
| Dependence-aware nulls | `analysis/offline_research_proof.py` (`block_permutation_pvalue`, `bh_adjusted`), `intelligence/hypothesis_trials.py` (BH per batch) | released |
| Co-firing clusters | `analysis/factory_stats.py` `cofiring` (#1156): single-linkage clusters on fire overlap, one representative per cluster | released |
| Spent looks | `analysis/research_findings.py` (`desk-findings/1` catalogue) | loaded nightly |
| Regime state | `intelligence/regime/market_state.py` | deployed |

Naming: `evals/e2/README.md` calls E2 "milestone M2 of the evals plan". That is a different M2 from this milestone.
This document leaves E2 unchanged. Publications enter E2 only as a new registered stream (§2.3, gap L6).

## 2. Outcome lineage

### 2.1 The chain

A publication outcome is usable for learning only when every link below resolves. If any link fails, the outcome
keeps a named non-success state (§2.2); it is never dropped and never filled in.

| Link | Identity | Where it lives now | State |
|---|---|---|---|
| P. Publication | `publication_id` = `<channel>:<row id>`, `publication_sha256` = sha256 of the captured text, `packet_sha256`, `published_at` | packet `publications[]`, `receipt.json` chain | **EXISTS** |
| C. Claim | (`publication_id`, `claim_sha256`); `claim_span` is a verbatim substring of the captured text; `claim_type` ∈ {direction, level, event, causal, data_quality} | `publication_claim_verdicts` | **EXISTS**, per extraction setup (L2) |
| I. Inputs the publication used | raw row / vintage / pull timestamp of every input the generator read | not recorded. The packet sets `capture_receipt.missing_decision_vintages: true` | **MISSING** (L1) |
| T. Target | instrument, session, horizon end, price basis | `instrument` and `horizon_end` are typed; the price basis appears only inside `evidence` | **PARTIAL** (L3) |
| O. Matured outcome | `verdict` with evidence dated no later than `graded_at`; `horizon_end` must lie in (`stated_at`, `graded_at`] | `publication_claim_verdicts` + `publication_review_runs.id` | **EXISTS**; non-success states partial (L4) |
| R. Run identity | `publication_review_runs.review_sha256`, `supersedes_run_id`, grader version | `publication_review_runs` | **EXISTS**; learning must pin one run (rule 2.4.3) |

### 2.2 Outcome states (every state is kept and counted)

| Protocol state | Current encoding | Meaning |
|---|---|---|
| `CONFIRMED` / `REFUTED` / `PARTIAL` | `confirmed` / `refuted` / `partially_confirmed` | matured, graded against dated evidence |
| `PENDING` | `unresolved` with a future `horizon_end` | not matured yet. It is never read as a negative. |
| `UNRESOLVABLE_BY_RULE` | `unresolved` for `event` / `causal` | v1 cannot grade the claim. A causal claim is never confirmed from co-movement. |
| `UNAVAILABLE` | `unscorable`, distinguished by `root_cause` text only | a matured target whose price or evidence is missing |
| `UNMEASURABLE` | `unscorable`, distinguished by `root_cause` text only | no gradable target, e.g. an alias level ("SPX above 6800") or a narrative that was withheld |

Negatives (`REFUTED`) and non-success states are published with every result. A rate computed over graded claims
must also report the pending, unavailable and unmeasurable counts beside it.

### 2.3 Lineage gaps (each blocks only what it names)

| # | Gap | Blocks | Proposed owner |
|---|---|---|---|
| L1 | The generators do not record which input vintages they read | any claim that an outcome reflects the information available when the publication was written; any input-attribution learning | generator owners (`ollama/market_briefing.py`, `scripts/run_market_diary.py`, `intelligence/thesis_tracker.py`); design only until approved |
| L2 | Claim identity depends on the extraction setup (prompt version, tier, provider). A re-extraction can produce a different claim set. | comparing claim counts across grader versions | grader owner. Rule 2.4.2 applies meanwhile. |
| L3 | The price basis is not a typed field | pooling outcomes across instruments or bases | grader owner (next schema version) |
| L4 | `UNAVAILABLE` and `UNMEASURABLE` share `unscorable` | missingness accounting by cause | grader owner (typed reason code in the next schema version) |
| L5 | M0 (#958) proved one designed refusal, not a reasoned use of an input | any accuracy claim that depends on lineage | research owner. A reasoned-use proof (e.g. the 2026-10-26 briefing 4756) needs the receipt-format decision noted on #958. |
| L6 | Verdict rows are not E2 predictions. E2 needs a logged entry before the outcome and registration in `rules.json`. | scoring publications on the E2 scoreboard | owner decision. A publication stream would be a new E2 version, never an edit of `e2-v1`. |

### 2.4 Lineage rules

1. **Read, never rewrite.** Learning reads the append-only verdict tables and the retained packets. It never edits
   them, and it never backfills a claim, a vintage or an outcome from today's data.
2. **Freeze the claim set before reading outcomes.** A study fixes its extraction setup (grader version, prompt
   version, tier, provider) and its `review_sha256` set before it reads any verdict. A claim that the frozen setup
   did not extract is out of scope; it is not added later.
3. **Pin the run.** A study names the `publication_review_runs.id` values it reads. A later superseding run is a new
   observation with its own result; it never silently replaces the one read.
4. **Day eligibility.** A publication day is eligible only when its three runtime receipts exist (packet
   `receipt.json`, grader receipt `WRITTEN`/`UNCHANGED`, ingest receipt `INGESTED` with a bound
   `ingest_confirmation`). Hand-produced late captures keep their `late_capture: true` flag. They are reported
   separately and never pooled silently.
5. **No accuracy from lineage that M0 has not proven.** Until L1 and L5 close, publication outcomes describe how
   often a published sentence held. They do not show that GRID's inputs or reasoning produced it.

## 3. Staged protocol (S0 to S8)

Every empirical study goes through these stages in order, and each stage leaves a record. "Existing" names the code
that enforces the stage. A study may reuse an application's frozen numbers (D0 §5–§6) but may not skip a stage.

| Stage | Requirement | Existing |
|---|---|---|
| S0 Explore | Anything goes, labelled `EXPLORATORY`. It can propose a candidate but never supply evidence for one. The data it read is recorded as spent. | `analysis/research_findings.py` catalogue; D0 §4 spent periods |
| S1 Register | One candidate record **before any data is read**: rule, variants, horizons, instruments, controls, outcome definition, thresholds and the fixed look. Abandonment counts as a failure. | `evals/e3` E3A ledger, S11 allocation (`record_hypothesis_batch`), edge-stream registration |
| S2 Custody | A holdout or walk-forward split declared at S1, never chosen after a look. Holdout reads go through custody. | E3B `holdout.py` (fixture-only), factory pre-screen (one older-data look), D0 §5.2 / §8 |
| S3 Leakage | Every input passes the six clocks of D0 §2.1. The study runs a look-ahead canary and a late/revised/missing/duplicate fixture. | `evals/e1`, `assert_no_lookahead()`, `read_window_known_at`, D0 §5.6 |
| S4 Costs | Results are net of a cost model declared at S1. A gross-only result is labelled gross and cannot pass. | E2 cost model, edge-stream cost floor, D0 §6.3 |
| S5 Uncertainty | Block-permutation or block-bootstrap intervals that respect the serial and cross-sectional dependence; BH-FDR across the declared family. Effect size ± interval comes first, then the verdict. | `offline_research_proof.py`, `hypothesis_trials.py`, E2 bootstrap |
| S6 Regimes | Regime splits are declared at S1 from the market-state record as of each decision. Results are reported per regime. A regime is never picked after the look. | `intelligence/regime/market_state.py` |
| S7 Fixed look | One look per registered cell. Interim reads are labelled interim and change nothing. Verdicts: PASS / FAIL / UNDERPOWERED / HOLD, all published. | S10 `research_forward_log.py`, edge-stream one look, E2 pre-registered looks |
| S8 Promotion | Manual only. A PASS is a candidate report for the owner. Nothing changes a production score, weight, prompt or allocation automatically. | `governance/registry.py` lifecycle (CANDIDATE → SHADOW → STAGING → PRODUCTION); packet `promotion_allowed: false`; edge-stream PASS = candidate report |

### 3.1 Co-firing signals count once

1. Before S1 closes, the study builds the co-firing clusters of its candidate signals from **fire overlap only**
   (never from returns), using `factory_stats.cofiring` at a threshold declared in the registration.
2. A cluster is **one trial** in the multiple-testing denominator and takes at most one forward slot.
3. The cluster representative is fixed by the existing rule (the tested trial with the highest t, ties by ledger
   order).
4. Any other member earns credit only for **incremental evidence**: its effect on the sessions where the
   representative did not fire, or its effect after conditioning on the representative. That incremental test is
   itself declared at S1 and counted.

### 3.2 Second- and third-order chains

A chain (A moves → B responds → C follows) is registered as its links, each with an observable, a clock and a
declared lag window.

1. A chain is `CONFIRMED` only when every intermediate link is observed, in order, inside its window.
2. A link that fires without its successor is recorded as `BROKEN_LINK` at that link. It counts as a negative for the
   chain and is never dropped.
3. Counter-cases are counted and published: the terminal outcome occurring without the chain, and the chain
   occurring without the terminal outcome.
4. A terminal outcome alone never confirms a chain.

## 4. Budget

- Existing data and the retained packets and verdict tables only. No new provider, timer, Hermes job or spend.
  Spend keeps its own approval (D0 G5).
- Computation runs on grid-svr, read-only against griddb (sessions set read-only on the connection). Output is files
  with sha256 receipts.
- A study states its expected rows read, runtime and output size at S1. A run that exceeds the declaration by more
  than 2× stops and is recorded as `HOLD_BUDGET`.

## 5. Empirical acceptance (fixed before any outcome is read)

A study's S1 record fixes all of the following. Choosing them after a look voids the study.

- The minimum matured sample per cell, with a power rationale (the minimum detectable effect at the declared level).
  A cell below its floor is `UNDERPOWERED`, not FAIL.
- The effect measure, its sign and the economic threshold net of costs, e.g. the mean net bootstrap lower bound > 0
  (the GEX expectancy rule).
- GO / HOLD / RETIRE precedence in the form of D0 §5.4 / §6.5.

For publication outcomes specifically, nothing is reported as an accuracy rate until each reported cell (claim type ×
instrument × horizon) has at least the declared number of matured graded claims, under a pinned run set, on eligible
days (rule 2.4.4). The pending, unavailable and unmeasurable counts are always shown beside the rate.

## 6. Review and adoption

1. **Owner and reviewer: proposed, not assumed.** Following the D0 ownership (#1129), the proposal is Claude as research owner and Codex as independent reviewer. The operator confirms or names others.
2. The reviewer checks §2 against the code it cites, using the companion JSON and its test. It checks that §3 adds
   no rule that conflicts with a released eval suite or with the signed D0 body.
3. After the reviewer's pass, the owner adopts v0 in its own commit, which pins this file's sha256 with a test. Any
   later edit is a new version, never a re-pin.
4. No evaluation run under this protocol starts before adoption. D0 runs keep their own admission (D0 §8).
