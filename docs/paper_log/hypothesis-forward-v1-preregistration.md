# Pre-registration: hypothesis-loop forward log v1

Status: registered 2026-09-26 (the git commit that adds this file is the registration record).
Owner: Anik. Author: Claude (Opus 5.5), slice S10 of the 2026-09-26 GRID plan.
Nothing below may change after the first record is written. Any change is a new version (v2)
with a new log file; the v1 log is kept as-is.

## Why

The research loop (`analysis/offline_research_proof.py`, `analysis/research_real_panel.py`,
`scripts/run_real_panel_scan.py`) freezes candidates from a latest-vintage historical scan. A
historical scan, however careful, is hindsight: the data are the latest vintage, and the
researcher has already seen them. The only evidence a frozen candidate can earn is forward
evidence, taken on observations that did not exist when it was frozen, under rules fixed before
the first one arrived. This log collects that evidence. It changes no weight, promotes nothing,
and writes nothing to the GRID database.

## What may enter the log (eligibility)

The log starts empty (a header record only). A candidate enters only through `admit`, from the
output directory of one `scripts/run_real_panel_scan.py` run, and only if every check below
passes. A scan is admitted all-or-nothing: if any of its candidates fails a check, none is admitted.

1. The scan's code includes PR #661 (S09b): the `code_sha` recorded in `summary.json` descends from
   `4506ce4828f5a43a29d46f58569d82e42628b78d` (the #661 head, merged at `a8b3a400`), checked with
   `git merge-base --is-ancestor` in a named clone, and the `file_sha256` values the scan recorded
   equal the files at that commit.
2. Origin `latest_vintage_read` with a read receipt, in the manifest protocol, the summary and every
   candidate specification. `synthetic_fixture`, `exploratory_replay`, `pit_vintage_read` and any
   other origin are refused.
3. The manifest is intact (its sha256 matches), carries S09b structure (`self_lag` in the protocol,
   `block_basis`), is `candidate_eligible` and `horizon_spaced`, and the holdout result and
   `frozen-candidates.json` are the ones that manifest produced.
4. Every candidate is intact (sha256 over its specification), in state `FORWARD_EVIDENCE_PENDING`,
   `promotion_allowed: false`, a discovery trial that was `tested` and `selected`, and a holdout
   retrospective survivor with the same sign.
5. No candidate is a proxy of its own target: its feature series is not in the target's
   declared proxy group (`research_real_panel.PROXY_GROUPS`) and the pair is not in the
   protocol's `self_lag`. States `SELF_LAG_NEVER_A_CANDIDATE` and `RESCAN_REQUIRED` are refused,
   and so is any scan directory holding a `frozen-candidates.relabelled.json`.
6. The first real-panel scan (`ef0d564b`, discovery manifest
   `b7515b2b4def28c21630d330a19c042b269eb1651ab457bd4dff6b41d6c63fbc`) is refused by name, in
   addition to failing checks 1, 2 and 5.
7. Admission happens at or after the candidate's `forward_start_not_before`, and the same
   candidate or scan is never admitted twice.
8. The publication schedules the scan recorded for the feature and the target equal the ones in
   the running code.
9. Each scientific pair is forward-tested at most once in this log, ever. The pair is the target
   series, label kind, horizon, feature series and feature transform (`chg5`/`chg20`/`z60`). A
   candidate whose pair is already admitted, whether its test is open or decided, is refused,
   whatever scan, manifest or direction it comes from.

## Error control across scans

- Within one scan, the K admitted candidates share alpha 0.05 (Bonferroni, 0.05 / K each), so
  the chance that the scan yields any false `FORWARD_SUPPORTED_REVIEW_REQUIRED` verdict is at most
  0.05.
- Across scans, rule 9 means no pair is ever re-tested with fresh alpha on overlapping future data.
  By the union bound, which does not need the scans' forward windows to be independent, the chance
  of any false supported verdict in the whole log is at most 0.05 times the number of admitted
  scans. `STATUS.md` reports that count and the bound.
- There is no global cap on the number of scans. A supported verdict is not a promotion. It is a
  request for independent review, which must weigh this bound.

## Per-candidate plan (frozen in the admission record)

Each admission record carries a plan, and the plan's sha256. The admission time is the freeze
time.

- Target: the candidate family's target series, labelled as the scan labelled it (`change` or
  `return`) over `h` sessions (the family's `fwd<h>`).
- Feature: the candidate's feature (series, `chg5`/`chg20`/`z60`, the scan's transform, publication
  source and carry-forward limit).
- Sessions: weekdays that are not US federal holidays, at 00:00Z. This is the calendar the
  adapter uses for business-day publication lags. (The scan's index keeps holidays as sessions
  with no observation; here they are skipped, so a decision or a label end never falls on one.)
  The first decision is the first session strictly after the freeze time. Decision `k` is
  `k * s` sessions later, where `s = max(scan step, h)`, so outcome windows never overlap.
- Prediction: at decision `k` the feature value as GRID held it at that instant, read through
  `store.observations.read_window` via `research_real_panel.load_latest_vintage_panel` with
  `as_of_ts` equal to the decision instant, and usable only from its declared publication time. It
  is logged before its outcome can be known.
- Outcome: the target label over `[decision k, decision k + h sessions]`, known at the declared
  publication time of the observation dated `decision k + h sessions`, read after that time.
- Decision rule: one look, on the first `min_n` valid pairs in decision order (`min_n` = the scan
  protocol's `min_n`, at least 30). The statistic is the scan's (`spearman` or `pearson`). The
  p-value is one-sided in the candidate's direction, against 9,999 block permutations of the
  outcomes (seed 20260926). The block is the frozen discovery block for the family, capped so at
  least 8 blocks remain (a declared protocol block is used as is).
- Alpha: 0.05 divided by the number of candidates admitted from the same scan (Bonferroni over the
  scan's forward family).
- Supported: `direction * rho > 0` and `p <= alpha`. State `FORWARD_SUPPORTED_REVIEW_REQUIRED`.
  Promotion stays not allowed: a supported candidate still needs an independently reviewed
  promotion policy, which this log does not provide.
- Failure: `FORWARD_FAILED` if the look finds `direction * rho <= 0`, `p > alpha` or a constant
  input. `FORWARD_INCONCLUSIVE_STOPPED` if `2 * min_n` decisions are resolved with fewer than
  `min_n` valid pairs. Both mean "not supported", and both close the candidate.
- Stop: a candidate closes at its verdict record and gets no further predictions.

## Exclusions (logged with a reason code, never silently)

- `feature_abstained`: the feature has no usable value at the decision (missing or stale beyond its
  carry-forward limit).
- `late_prediction`: the prediction would be logged at or after its outcome's publication time.
- `target_missing`: a target level at the decision or the label end is absent 5 days after the
  label's publication time.

A pair also counts only if its decision is after the freeze, its feature was known at or before
the decision, its prediction was logged before the outcome's publication time, and its outcome
was logged at or after it. The evaluation re-checks all four on every run.

## Until the look

`status` reports activity only: decisions due, predictions and outcomes logged, exclusions by
reason, valid pairs against `min_n`. It never reports a correlation or a p-value before a
candidate's verdict record exists.

## Integrity

- **Log.** Append-only JSONL on grid-svr under `/data/grid/paper_log/hypothesis_forward_v1/`.
  Every record carries the SHA-256 of the previous record's exact canonical JSON line, the code
  commit that wrote it, and its run time. The first record is a header that carries the SHA-256
  of this file (LF line endings).
- **Code commit.** The commit comes from the installed archive's `VERSION` file, or from `HEAD`
  of a checkout with no modified tracked files. No caller can override it.
- **What the chain shows.** The chain shows that no line was edited, reordered, inserted or
  removed from the middle. On its own it cannot show that trailing lines were not truncated, or
  that the whole file was not recomputed.
- **Anchor file.** After every append the job appends the record count and the head hash to a
  chained anchor file (`hypothesis_forward_v1.anchors.jsonl`). `verify` checks every anchored
  prefix against the log, locally and against a copy given with `--anchor`.
- **What the anchor file shows.** The log is tamper-evident only relative to an anchor copy held
  where the log's writer cannot edit it, such as an off-host mirror like the GEX paper log's
  vault mirror. That mirror is a follow-up that needs the owner's approval. Until it exists, the
  anchor file detects accidents such as truncation or a stray rewrite, not a deliberate recompute
  on the same host.
- **STATUS.md.** `STATUS.md` is regenerated from the log each run, after the chain and anchors
  verify. It is a view, not an anchor. A failed check stops the job from appending and is
  reported in `STATUS.md`.
- **Verdict records.** Each verdict record carries the candidate id and its scientific pair
  (target, label, horizon, feature). It also carries the direction, the plan hash, the windows
  (first decision, first and last paired decision, last label end), n, rho, p, alpha, the
  pre-registration hash, and the hash of the log head it follows. A consumer can therefore check
  the verdict against the chain.
- The job opens database sessions read-only (`default_transaction_read_only=on`, statement timeout
  at most 60 s) and reads only through the latest-vintage adapter. It never reads or writes
  `hypothesis_registry`, `discovered_hypotheses` or `scanner_weights`.
