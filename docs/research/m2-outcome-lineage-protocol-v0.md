# M2 outcome lineage and staged research protocol, v0 (draft, unreviewed)

Issue: [#960](https://github.com/3pacs/GRID/issues/960). Milestone: M2, "Evidence-based outcome learning protocol".
Machine-readable companion: [`m2-outcome-lineage-v0.json`](m2-outcome-lineage-v0.json). The companion is held to
the code by `tests/test_m2_outcome_lineage_map.py`.

**Status: DRAFT v0, revision 6** (answers the reviewer's HOLDs 6033027329 on `393cde73`, 6033670709 on
`4aa18aee`, 6033699395 on `bc0e85db`, 6033821128 on `3c0c79a9` and 6033941569 on `c7be57b6`; the change maps
are §7–§11). It is unreviewed and unsigned, and it authorises nothing. It adds no research job, production
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
| O. Matured outcome | `verdict` with evidence dated no later than `graded_at`; for a resolved verdict, `horizon_end` must lie in (`stated_at`, `graded_at`] | `publication_claim_verdicts` + `publication_review_runs.id` | **PARTIAL**. Maturity and missing data are not separate fields (§2.2, L4). The close evidence is dated by observation date: it does not show when the selected vintage became available (L7). |
| R. Run identity | `publication_review_runs.review_sha256`, `supersedes_run_id`, grader version | `publication_review_runs` | **EXISTS**; learning must pin one run (rule 2.4.3) |

### 2.2 Outcome states: three derived axes, then one state

The grader's `verdict` mixes three questions, so a study must not read it alone:

- `unresolved` covers both a horizon still in the future and a horizon that has passed with no endpoint close.
- A missing **baseline** close is graded `unscorable` straight away, before the horizon matures.
- The grader's own `claims_graded` count is total minus `unscorable`. It includes immature rows, so it is **not** a
  matured denominator.

A study therefore derives three axes for each row, at a fixed read instant `R` (the study's own, recorded at S1),
and only then assigns a protocol state. Two clocks of the row's run decide whether the row can be used at all:

- **Visible by R, by a post-commit witness.** `publication_review_runs.ingested_at` is `DEFAULT NOW()`, which is
  PostgreSQL's transaction-start time. A run stamped before `R` can commit after it, so `ingested_at` never shows
  visibility, and neither does the ingest wrapper's own stamp. A row is visible by `R` only when a **witness**
  exists. A witness is an observation made after commit, bound to the run's `run_id` and `review_sha256`, and timed
  at or before `R`. Two kinds qualify:
  - **the study's own read at `R`** (forward studies): the study records the `run_id` and `review_sha256` of every
    run its read returned, with the read time. A reader sees only committed rows.
  - **a timed grader confirmation**: the grader's `ingest_confirmation` already binds `review_sha256`,
    `packet_sha256` and `run_id`, but it records **no time**. Until it does (gap L8), it cannot serve as a
    historical witness.

  A positive witness timed after `R` proves nothing about `R`: a first sighting at `R+5` fits a commit at `R−1`
  as well as one at `R+1`. So a row is `NOT_VISIBLE` only on **negative evidence**, of one of two kinds:
  - **a cutoff-negative witness.** Every condition below must hold; if any one fails, or any field is missing or
    errored, the result reverts to `VISIBILITY_UNKNOWN`:
    - **Trusted source:** the primary database, not a replica (replica lag can hide a committed run).
    - **Snapshot timing:** the read's snapshot was **acquired** at or after `R`. The response or completion time
      does not count, since a snapshot taken before `R` can finish after it.
    - **Complete lookup:** either an exact-identity lookup of this `run_id` that completed without error and
      returned no row; or a scoped list of `publication_review_runs` for the run's publication day, marked
      complete (no filter, limit or pagination), whose `returned_run_ids` is present and omits the `run_id`.

    A read before `R` that misses the run proves nothing about `R`.
  - **an authoritative commit time after `R`:** a commit timestamp recorded by a trusted database source (not
    `ingested_at`), bound to this exact `run_id` and `review_sha256`. None exists today.

  Every other row without a positive witness by `R` is `VISIBILITY_UNKNOWN`. That covers a positive witness only
  after `R`, no witness at all, and a negative read before `R` or scoped elsewhere. Every historical run is in this
  state today. Both states are excluded from the rate, each has its own count, and neither is ever read as an
  outcome.

  **No substitution after freezing.** The study freezes its run set at `R`. A run that becomes visible later,
  including a superseding run, never replaces a frozen run and is never added to the frozen set.
- **Assessed after the horizon.** Only a grading made at or after the horizon can say an endpoint, or any other input,
  was missing at maturity. A non-resolved row whose run `graded_at` < `horizon_end` is `NOT_ASSESSED` once the
  horizon passes. That means matured but not graded at maturity. It is never `UNAVAILABLE`, and the study waits for
  the superseding run graded after the horizon.

**Maturity**, from `horizon_end` and `R`:

| Value | Rule |
|---|---|
| `NO_HORIZON` | `horizon_end` is null: event, causal and data_quality claims, and level claims without a horizon |
| `IMMATURE` | `horizon_end` > `R` |
| `MATURE` | `horizon_end` ≤ `R` |

**Data**, from `verdict` and `root_cause`. v1 has no typed reason code (L4), so the derivation reads the grader's
fixed `root_cause` prefixes, and anything it does not recognise is `UNKNOWN_CAUSE`:

| Value | Rule |
|---|---|
| `PRESENT` | verdict is confirmed, refuted or partially_confirmed |
| `MISSING_BASELINE` | unscorable, `root_cause` starts `no usable ` (grader: no usable close on the baseline session) |
| `UNUSABLE_ENDPOINT` | unscorable, `root_cause` starts `unusable ` |
| `NO_SOURCE` | unscorable, `root_cause` starts `no price series for instrument` |
| `MISSING_ENDPOINT` | unresolved, `MATURE`, claim type direction or level |
| `AWAITING_ENDPOINT` | unresolved, `IMMATURE` |
| `NOT_GRADABLE` | unresolved for event / causal (v1 rule); any other unscorable with a recognised no-target cause (alias level, withheld narrative, no direction, no checkable claim) |
| `UNKNOWN_CAUSE` | unscorable with an unrecognised `root_cause` |

**State**:

| Protocol state | Axes | Counted in the matured denominator? |
|---|---|---|
| `CONFIRMED` / `REFUTED` / `PARTIAL` | `MATURE`, `PRESENT` | yes |
| `PENDING` | `IMMATURE`, any data value, including `MISSING_BASELINE`, which is known before maturity | no. It is never read as a negative. |
| `NOT_ASSESSED` | `MATURE`, not resolved, run `graded_at` < `horizon_end` | no; reported beside the result |
| `UNAVAILABLE` | `MATURE`, run `graded_at` ≥ `horizon_end`, with `MISSING_BASELINE`, `MISSING_ENDPOINT`, `UNUSABLE_ENDPOINT` or `NO_SOURCE` | yes, as a non-success. It is never dropped. |
| `UNMEASURABLE` | `NO_HORIZON`, or `NOT_GRADABLE` | no; reported beside the result |
| `UNKNOWN` | `UNKNOWN_CAUSE`, at any maturity (checked after visibility) | no; reported beside the result, and a non-zero count is a grader-owner defect |
| `NOT_VISIBLE` | negative evidence: a scoped read at or after `R` that did not return the run, or an authoritative commit after `R` (checked first) | no; counted on its own line |
| `VISIBILITY_UNKNOWN` | no positive witness by `R` and no negative evidence (checked first) | no; counted on its own line. Every historical run is here until L8 closes. |

A matured rate's denominator is the **visible, assessed, gradable, mature** rows: `CONFIRMED`, `REFUTED`, `PARTIAL`
and `UNAVAILABLE`, and nothing else. Every other state has its own count, shown next to the rate: `PENDING`,
`NOT_ASSESSED`, `UNMEASURABLE` (a mature `NOT_GRADABLE` row included), `UNKNOWN`, `NOT_VISIBLE` and
`VISIBILITY_UNKNOWN`. None of these
can pass as observed missing data. The companion JSON carries one fixture
per derivation rule; the test applies the rules to those fixtures, as the executable form of this table.

### 2.3 Lineage gaps (each blocks only what it names)

| # | Gap | Blocks | Proposed owner |
|---|---|---|---|
| L1 | The generators do not record which input vintages they read | any claim that an outcome reflects the information available when the publication was written; any input-attribution learning | generator owners (`ollama/market_briefing.py`, `scripts/run_market_diary.py`, `intelligence/thesis_tracker.py`); design only until approved |
| L2 | Claim identity depends on the extraction setup (prompt version, tier, provider). A re-extraction can produce a different claim set. | comparing claim counts across grader versions | grader owner. Rule 2.4.2 applies meanwhile. |
| L3 | The price basis is not a typed field | pooling outcomes across instruments or bases | grader owner (next schema version) |
| L4 | `UNAVAILABLE` and `UNMEASURABLE` share `unscorable` | missingness accounting by cause | grader owner (typed reason code in the next schema version) |
| L5 | M0 (#958) proved one designed refusal, not a reasoned use of an input | any accuracy claim that depends on lineage | research owner. A reasoned-use proof (e.g. the 2026-10-26 briefing 4756) needs the receipt-format decision noted on #958. |
| L6 | Verdict rows are not E2 predictions. E2 needs a logged entry before the outcome and registration in `rules.json`. | scoring publications on the E2 scoreboard | owner decision. A publication stream would be a new E2 version, never an edit of `e2-v1`. |
| L7 | Close evidence is dated by observation date (`obs_date`, session close). It records neither the selected vintage nor its availability time. | any claim that an outcome was knowable at the grading instant from first-release data; revision-sensitive outcomes | grader owner (record the vintage and pull timestamp of each close used) |
| L8 | No timed post-commit witness of run visibility. `ingested_at` is transaction start, and the grader's `ingest_confirmation` has no time. | any historical study: its rows are `VISIBILITY_UNKNOWN` | grader owner: stamp `observed_at` on `ingest_confirmation` when the ingest's committed result is returned (bound to `run_id` and `review_sha256`). Forward studies can use their own read. |

### 2.4 Lineage rules

1. **Read, never rewrite.** Learning reads the append-only verdict tables and the retained packets. It never edits
   them, and it never backfills a claim, a vintage or an outcome from today's data.
2. **Freeze the claim set before reading outcomes.** A study fixes its extraction setup (grader version, prompt
   version, tier, provider) and its `review_sha256` set before it reads any verdict. A claim that the frozen setup
   did not extract is out of scope; it is not added later.
3. **Pin the run.** A study names the `publication_review_runs.id` values it reads. A later superseding run is a new
   observation with its own result; it never silently replaces the one read.
4. **Day eligibility.** A publication day is eligible only when all three hold:
   - the producer's `receipt.json` exists;
   - the grader's receipt is `WRITTEN` or `UNCHANGED` and carries an `ingest_confirmation`. This is the grader's own
     check that the ingest's returned result binds the same `review_sha256`, `packet_sha256` and a positive `run_id`,
     with no rejected rows.
   - that run passes the visibility rule of §2.2.

   The ingest's own receipt (`state: INGESTED` or `PRESENT`) is the underlying result. It is necessary, but on its
   own it is not a confirmation: the grader records `ingest_confirmation_error` when the result does not bind its
   file. Hand-produced late captures keep their `late_capture: true` flag; they are reported separately and never
   pooled silently.
5. **No accuracy from lineage that M0 has not proven.** Until L1 and L5 close, publication outcomes describe how
   often a published sentence held. They do not show that GRID's inputs or reasoning produced it.

## 3. Staged protocol (S0 to S8)

Every empirical study goes through these stages in order, and each stage leaves a record. "Existing" names the code
that enforces the stage. A study may reuse an application's frozen numbers (D0 §5–§6) but may not skip a stage.

Binding, per stage, says what actually makes a study comply today:

- `ENFORCED`: a production code path refuses the violation;
- `CI_GATE`: a released suite or test fails CI on it;
- `FIXTURE_ONLY`: the code exists and is tested on fixtures, with no production caller;
- `POLICY`: a written rule that nothing refuses;
- `MISSING`: nothing yet.

| Stage | Requirement | Existing | Binding today |
|---|---|---|---|
| S0 Explore | Anything goes, labelled `EXPLORATORY`. It can propose a candidate but never supply evidence for one. The data it read is recorded as spent. | `analysis/research_findings.py` catalogue; D0 §4 spent periods | `POLICY`: the catalogue records spent looks, and nothing refuses re-reading them |
| S1 Register | One candidate record **before any data is read**: rule, variants, horizons, instruments, controls, outcome definition, thresholds, the fixed look and the read instant `R`. Abandonment counts as a failure. | `evals/e3` E3A ledger, S11 allocation (`record_hypothesis_batch`), edge-stream registration | `ENFORCED` inside the edge stream and hypothesis scoring v2; E3A is a released library with no production caller verified here; elsewhere `POLICY` |
| S2 Custody | A holdout or walk-forward split declared at S1, never chosen after a look. Holdout reads go through custody. | E3B `holdout.py` (`e3-v2`), factory pre-screen (one older-data look), D0 §5.2 / §8 | E3B `FIXTURE_ONLY`; the D0 admission verifier is inert (`VERIFIER_ACCEPTED = False`); elsewhere `POLICY` |
| S3 Leakage | Every input passes the six clocks of D0 §2.1. The study runs a look-ahead canary and a late/revised/missing/duplicate fixture. | `evals/e1`, `assert_no_lookahead()`, `read_window_known_at`, D0 §5.6 | `CI_GATE` for E1's integrity gates. `assert_no_lookahead()` is a date-grain check on the callers that use it: it is `ENFORCED` there, but it does not enforce D0's six clocks. Clock 4 visibility, and everything outside those callers, is `POLICY` |
| S4 Costs | Results are net of a cost model declared at S1. A gross-only result is labelled gross and cannot pass. | E2 cost model, edge-stream cost floor, D0 §6.3 | `ENFORCED` inside the edge stream; E2 applies its cost model whenever it scores; elsewhere `POLICY` |
| S5 Uncertainty | Block-permutation or block-bootstrap intervals that respect the serial and cross-sectional dependence; BH-FDR across the declared family. Effect size ± interval comes first, then the verdict. | `offline_research_proof.py`, `hypothesis_trials.py`, E2 bootstrap | `ENFORCED` in hypothesis scoring v2 (BH per batch); E2 applies its bootstrap whenever it scores; library functions otherwise, so `POLICY` |
| S6 Regimes | Regime splits are declared at S1 from the market-state record as of each decision. Results are reported per regime. A regime is never picked after the look. | `intelligence/regime/market_state.py` | `MISSING`: the record exists, but nothing binds a study's splits to its registration |
| S7 Fixed look | One look per registered cell. Interim reads are labelled interim and change nothing. Verdicts: PASS / FAIL / UNDERPOWERED / HOLD, all published. | S10 `research_forward_log.py`, edge-stream one look, E2 pre-registered looks | `ENFORCED` in S10 and the edge stream; elsewhere `POLICY` |
| S8 Promotion | Manual only. A PASS is a candidate report for the owner. Nothing changes a production score, weight, prompt or allocation automatically. | `governance/registry.py` lifecycle (CANDIDATE → SHADOW → STAGING → PRODUCTION); packet `promotion_allowed: false`; edge-stream PASS = candidate report | `POLICY` for promotion decisions; the packet flag is a constant, not a gate |

### 3.1 Co-firing signals count once

1. Before S1 closes, the study builds the co-firing clusters of its candidate signals from **fire overlap only**
   (never from returns), using `factory_stats.cofiring` at a threshold declared in the registration.
2. A cluster is **one trial** in the multiple-testing denominator and takes at most one forward slot.
3. **The representative is chosen without outcomes.** The registration fixes an outcome-independent rule before any
   outcome is read. Default: the member with the most fires in the discovery window, ties by ledger order (fires are
   events, not returns).
4. **The alternative is to account for the selection.** `factory_stats.cofiring` today picks the tested trial with
   the highest t, which is selected by outcome. A study that keeps that rule must:
   - compute its cluster-level null on the **maximum** statistic over every tested member, under the same
     permutation or bootstrap;
   - send the representative to a **separate frozen forward check** before any verdict is quoted.

   Without both, its cluster verdict is exploratory (S0).
5. Any other member earns credit only for **incremental evidence**: its effect on the sessions where the
   representative did not fire, or its effect after conditioning on the representative. That incremental test is
   itself declared at S1 and counted.

### 3.2 Second- and third-order chains

A chain (A moves → B responds → C follows) is registered as its links. Each link has an observable, a clock, a
declared lag window and the coverage it needs.

1. A chain is `CONFIRMED` only when every intermediate link is observed, in order, inside its window.
2. A successor that is absent counts as `BROKEN_LINK`, a negative for the chain, **only when both hold**:
   - its lag window has fully matured at `R`;
   - the successor's observable has **verified coverage** over that whole window: the source was collecting, with
     no gap, outage or missing session in the window, shown by a receipt.
3. If the window has not matured, the link is `LINK_PENDING`. If coverage is unverified or has gaps, it is
   `LINK_UNOBSERVABLE`. Both are kept and counted, and neither is a negative.
4. Counter-cases are counted and published, under the same maturity and coverage rule: the terminal outcome
   occurring without the chain, and the chain occurring without the terminal outcome.
5. A terminal outcome alone never confirms a chain.

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

## 7. Revision 2 change map (answers HOLD 6033027329 on `393cde73`)

| Reviewer point | Change |
|---|---|
| (1) `unresolved` covers both a future and a due-but-missing endpoint; a missing baseline is `unscorable` before maturity; `claims_graded` is not a matured denominator | §2.2 rewritten as three derived axes (maturity, data, verdict) and one state, with the matured denominator defined. Fixtures for each rule are in the JSON (`derivation.fixtures`), applied by the test. |
| (2) Dated close evidence is an observation date, not vintage availability; separate production enforcement, fixture-only checks, operator policy and missing bindings per stage | Link O is now `PARTIAL`, and new gap L7 covers outcome-vintage lineage. Each stage in §3 has a "Binding today" column (`ENFORCED` / `CI_GATE` / `FIXTURE_ONLY` / `POLICY` / `MISSING`). |
| (3) Highest observed t is an outcome-selected representative | §3.1: an outcome-independent representative is chosen at S1, or the selection is accounted for with a max-statistic cluster null plus a separate frozen forward check; otherwise the result is exploratory. |
| (4) An absent successor is a negative only on a mature window with verified coverage | §3.2: `BROKEN_LINK` requires a matured window and receipted coverage; otherwise the link is `LINK_PENDING` or `LINK_UNOBSERVABLE`. |

## 8. Revision 3 change map (answers HOLD 6033670709 on `4aa18aee`)

| Reviewer point | Change |
|---|---|
| Endpoint-missing came from the study's later read clock `R`, without requiring that the unresolved verdict was graded at or after the horizon, or that its run was visible by `R` | §2.2 binds two run clocks. A row is used only if `ingested_at` ≤ `R` (otherwise `NOT_VISIBLE`). A non-resolved mature row counts as `UNAVAILABLE` only if its run's `graded_at` ≥ `horizon_end`; otherwise it is `NOT_ASSESSED`. |
| The denominator prose (every mature claim) disagreed with the fixtures (which excluded mature `NOT_GRADABLE` / `UNKNOWN_CAUSE`) | The prose now names exactly the four states in the denominator and lists each excluded state's own count. The JSON `matured_denominator` and the test agree with it. |
| Add paired pre-/post-horizon cases with the same cause, and a recorded-after-`R` case | Fixtures: unresolved graded before vs after the horizon; missing baseline graded before vs after the horizon, both read after it; one row whose run was ingested after `R`. |

## 9. Revision 4 change map (answers HOLD 6033699395 on `bc0e85db`)

| Reviewer point | Change |
|---|---|
| `ingested_at DEFAULT NOW()` is the transaction-start time, not commit or read visibility; the ingest wrapper's stamp is not a post-commit witness | §2.2: visibility needs a post-commit witness bound to `run_id` and `review_sha256` and timed by `R`. Without one the row is `VISIBILITY_UNKNOWN` (every historical run today); with a witness after `R`, `NOT_VISIBLE`. New gap L8, for a timed grader confirmation. |
| Test a transaction straddling `R`, and a superseding run before and after the cutoff | Fixtures: a run with `ingested_at` before `R` and its only witness after `R` (`NOT_VISIBLE`); a run with no witness (`VISIBILITY_UNKNOWN`); a day whose superseding run is witnessed after `R`, so the study reads the earlier run; and the same day read after both witnesses, so it reads the superseding run. |
| §2.4.4 must distinguish the grader's hash-bound `ingest_confirmation` from the underlying ingest result | §2.4.4 now names each separately; the ingest receipt alone is not a confirmation. |
| S3's production assert is date-grain on covered callers, not enforcement of all six D0 clocks | S3's binding says so. |

## 10. Revision 5 change map (answers HOLD 6033821128 on `3c0c79a9`)

| Reviewer point | Change |
|---|---|
| A first positive read at `R+5` fits an `R−1` or an `R+1` commit, so it does not prove `NOT_VISIBLE` at `R` | §2.2: `NOT_VISIBLE` needs negative evidence. That is either a read at or after `R`, scoped to the run's table and day, that did not return the run, or an authoritative commit time after `R`. A positive witness only after `R` is `VISIBILITY_UNKNOWN`. |
| Add paired cases, with genuine negative evidence | Fixtures: a positive witness only after `R` (`VISIBILITY_UNKNOWN`), and the same with a scoped negative read at `R` (`NOT_VISIBLE`); an authoritative commit after `R` (`NOT_VISIBLE`); a negative read before `R` (`VISIBILITY_UNKNOWN`); a negative read scoped to another day (`VISIBILITY_UNKNOWN`). |
| Keep both states excluded; no dynamic substitution after the run set is frozen | Both stay out of the denominator, with their own counts. §2.2 adds "No substitution after freezing", and the supersession test checks that a superseding run first seen after `R` is never read. |

## 11. Revision 6 change map (answers HOLD 6033941569 on `c7be57b6`)

| Reviewer point | Change |
|---|---|
| A pre-`R` snapshot completing after `R`, replica lag, and a filtered, incomplete or missing list can falsely certify `NOT_VISIBLE` | §2.2: a negative witness needs a trusted primary source, a snapshot acquired at or after `R` (not the response time), and either a complete exact-identity lookup or a complete scoped list with `returned_run_ids` present. An error or a missing field reverts to `VISIBILITY_UNKNOWN`. |
| Bind the authoritative commit proof to the exact run and hash, from a trusted source | §2.2: the commit time must come from a trusted database source and name the same `run_id` and `review_sha256`. |
| Add stale-snapshot and partial or missing list cases | Fixtures: a stale snapshot (acquired before `R`, completed after); a replica read; a partial list; a missing list; an errored read; an exact-identity lookup with no row (`NOT_VISIBLE`); a commit proof bound to another run. |

