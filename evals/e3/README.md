# E3: hill-climb harness (`e3-v1`, slice E3A: the candidate ledger)

E3 is the staged funnel a hill-climber runs through: propose, screen on discovery data, E0/E1 gates, a reusable holdout spent through S11, the forward log, then promotion. This slice (E3A) ships only the **candidate ledger**. That is the record that makes every multiple-testing denominator honest. E3B (funnel and holdout), E3C (bandit allocation and retirement) and E4 (proposers) build on it.

## Why the ledger exists

S11 (`analysis/ledger_steered_exploration.py`) already counts every declared trial inside an allocation, abandoned runs included. A hill-climber, though, produces many candidates that never reach an allocation:

- ideas screened out on discovery data;
- variants an agent tried and dropped;
- code changes that E0 or E1 rejected.

If those vanish, the effective number of tests is understated, and selection bias leaks in. The E3 ledger records each candidate **before any data is read** and records every stage transition after that. Nothing is ever edited or deleted.

## Storage

The ledger is files only. It has no DB and no migrations.

| What | Where |
|---|---|
| Ledger | `$GRID_E3_LEDGER_DIR/candidates.jsonl`, one canonical JSON record per line, hash-chained (`seq`, `prev_sha256`) |
| Anchor | `$GRID_E3_ANCHOR_DIR/candidates.anchor.jsonl`. This must be a **different directory** from the ledger. |
| Lock | `$GRID_E3_LEDGER_DIR/candidates.lock` (an `O_EXCL` lock file, as in `analysis/research_forward_log.py`) |
| Off-host witness | Vault `05-GRID/Paper-Log/evals/e3/ledger.anchors.jsonl`, using the EVAL-E0H2 pattern on the EVAL-E2F2 cadence. It is checked with `verify(external_anchors=...)`. |

Storage, hash chain and anchor are S11's own `Ledger`, `Anchor` and `verify_chain`, imported unchanged. The S11 module is not edited.

Each open and each append does the following under the lock:

1. Re-read both files.
2. Verify the chain and the anchor.
3. **Replay every invariant**, so a well-chained record that breaks a rule is still refused.

Each of these is refused:

- an edited, truncated or reordered ledger;
- a recomputed ledger;
- a second genesis against an existing anchor.

If a tamperer rewrites both local files consistently, the local check can't tell. The off-host witness catches it.

A crash between the ledger write and the anchor write leaves one unanchored line. The ledger then refuses to open (fail closed) until the owner reviews it.

## Records

| Kind | Written by | Rules |
|---|---|---|
| `genesis` | owner/judge | `ledger_id`, `e3_version`, `created_at`, and `s11_ledger_id`, the S11 ledger whose alpha the holdout spends |
| `proposed` | proposer | `candidate_id` (the sha256 of the canonical proposal core), `family`, `candidate_kind` (feature, parameter, machinery-change or generator-change), `identity` (S11 `scientific_identity` per declared trial) and `identity_sha256`, `declared_trials`, `proposer`, `engine_version`, `spec_sha256`, `expected_sign` (-1, 0 or 1), `rationale_sha256`, `retest_of`, `proposed_at`. The spec and the rationale are stored as **hashes only**, so no data references and no free payload enter the ledger. |
| `stage_entered` | judge | Stages run in order: `screen` → `gates` → `holdout` → `forward` → `promotion`. Each is entered once, and only after the previous stage passed. `screen` records its label window and the S11 head it was checked against. `holdout` must name S11's **open** allocation, which must declare the candidate's identities. One allocation pays for each identity's holdout look only once. The record stores the allocation's alpha. |
| `stage_result` | judge | `result` is `pass`, `fail`, `untestable` or `inconclusive`. Also `p_values` (one per declared trial, or null), `alpha_spent` (0 for every stage except holdout, which spends exactly its allocation's alpha), `s11_allocation_sha256` (holdout only) and `receipt_sha256` of the stage artifact. |
| `abandoned` | judge, or the proposer for its own candidate before any allocation | Allowed at any point before a terminal record. It **counts as a failure.** After an S11 allocation, the allocation must first be closed in S11 (`abandon()`, so alpha stays spent), and the ledger stores that S11 record's sha. If the S11 run was already recorded, it stores the `run_result` sha instead. |
| `withdrawn_pre_data` | judge, or the proposer for its own candidate | Only before any `stage_entered`. Still counted in `proposed`. No alpha spent. |
| `promoted_research`, `suspended`, `retired` | judge (E3B/E3C) | Promotion needs a passed `promotion` stage. Suspension and retirement need forward admission. |

A re-test is a **new candidate**, with `retest_of` pointing at the earlier one and a new id, so it pays again. Proposing an identical candidate twice is refused.

At `screen`, an identity is refused if S11's window registry (`touched_windows`) says it already touched an overlapping window.

## Counts API

`CandidateLedger.family_counts(family)` returns these fields:

- `proposed` and `declared_trials`;
- `withdrawn`, `screened`, `abandoned`;
- `failed` (closed by a non-pass result) and `failures` (`abandoned` + `failed`);
- `holdout_looks`, and `alpha_spent` (summed once per distinct S11 allocation that the family's holdout looks used, counted at holdout entry, so an abandoned look still counts);
- `forward_admitted`, `promoted`, `suspended`, `retired`.

E3B and E3C use these as the honest denominator, and E4B uses them for yield per trial.

## Who may write what

- `CandidateLedger` is the **judge**. It covers genesis, stages, results, abandonment, promotion, suspension and retirement.
- `client_for_proposer(proposer_id)` returns a `ProposerClient` that exposes only three methods:
  - `propose()`;
  - `abandon()`, for the proposer's own candidates, before any S11 allocation;
  - `withdraw()`, for its own candidates, pre-data.
- Judge methods don't exist on the client, so calling one raises `AttributeError`.
- Appends carry a module-private capability (the VS1 `_WITNESS_TOKEN` pattern). The proposer capability is refused every judge record kind, and every record's `actor` is replayed on open.

**Limit.** In-process Python is not a security boundary. A proposer that runs in the judge's process could reach module internals. In v1 the client writes the ledger, anchor and lock files itself, so it only works where those directories are writable. EVAL-E4A must provide the binding boundary:
- proposers run as a separate OS user, with no write access to `GRID_E3_LEDGER_DIR` or `GRID_E3_ANCHOR_DIR` and no view of holdout data;
- their records reach the ledger through a judge-owned writer (a spool directory or IPC) that exposes this same client API.

The judge only accepts the S11 **file** ledger with its anchor, never an in-memory one.

**Scale.** Every append and every read re-verifies and replays the whole file under the lock. That is O(n) per call, which is fine at hill-climb volumes. E3B or E4 may cache a verified head if volumes grow.

## Usage

```python
from analysis.ledger_steered_exploration import CANONICAL_LEDGER_ID, Ledger
from evals.e3.ledger import CandidateLedger, client_for_proposer

judge = CandidateLedger.genesis(s11_ledger_id=CANONICAL_LEDGER_ID)  # once, by the owner
client = client_for_proposer("agent-7")
cid = client.propose(family="macro-rates", candidate_kind="feature",
                     trials=[("SPY|change|fwd5", "DGS10|chg5")],
                     spec={...}, rationale="...", engine_version="e4b-v1")
s11 = Ledger(s11_path, anchor=s11_anchor)
judge.stage_entered(cid, "screen", s11=s11, window={"start": ..., "end": ...})
judge.stage_result(cid, "screen", "pass", receipt_sha256=..., p_values=[...])
judge.family_counts("macro-rates")
```

## Versioning

Every file in `evals/e3/` is pinned in `MANIFEST.sha256`. It uses the E0 "versioned" format: a `# version:` header, then LF sha256 lines.

- `python -m evals.e3.manifest --check` verifies the pins.
- Any change is a new suite version. Bump `evals.e3.VERSION`, run `python -m evals.e3.manifest --write`, and append a new `e3` entry to `evals/RELEASED.json` (EVAL-E0H1). Never edit a released entry.

## Owner decisions (approved 2026-10-01)

- **S11 binding: the canonical `grid-hypothesis-loop` ledger.** One global error budget is shared with the hypothesis loop. `genesis(s11_ledger_id=...)` still has no default, so the binding is explicit at genesis. As of 2026-10-01, the canonical S11 ledger has not had its genesis on grid-svr; that is a separate, owner-gated step.
- **Release:** e3-v1 is approved, enrolled by appending to `evals/RELEASED.json`.
- **Directories:** the ledger and anchor directories on grid-svr are approved. They are separate, owned by `grid`, and the anchor directory is not writable by proposers. They are created only after this suite is merged and deployed, outside the backup window.


## E3B (`e3-v2`): fixture evaluation funnel

`Funnel.screen` records E3 entry before a discovery-only reader call and uses S11's next run alpha with the existing ORP Holm machinery. `Funnel.gates` requires successful current GRID CI checks tied to the exact candidate commit and verifies E0 against every released version when machinery changes. Feature/parameter candidates with an unchanged machinery fingerprint take the documented trivial E0 pass. Baselines, CI receipts and benchmark runners are judge-owned inputs.

After SCREEN and GATES, the judge creates an S11 allocation using existing policy. HOLDOUT accepts exactly the screened trial universe, windows and alpha, binds the frozen discovery artifact to that allocation, and delegates label access to `Custodian.check` with the judge capability. S11 spends before the reader runs; failures close the allocation through S11 and retain the E3 family denominator. There is no separate E3 holdout budget. Custodian entry is durable: a fresh instance cannot silently retry a previously entered holdout. Proposers receive only a boolean pass/fail view; never give them the judge adapters or label reader. In-process Python capabilities are not an OS security boundary.

`Admission` writes an exclusive, hashed `e3_admitted` fixture receipt before recording forward admission. It uses S10's first session strictly after admission, freezes the judge's forward-check specification, and binds promotion evidence to that specification and the exact decision rows. Promotion needs at least 40 decisions, 120 elapsed sessions from the first prospective session, positive net return at twice costs, a passed preregistered check, and no retirement trigger. It writes only research receipts and the existing candidate ledger's `promoted_research` record. A failed promotion is a single look; it cannot be retried under the same candidate.

Source authority: owner-supplied binding acceptance text for `EVAL-E3B-staged-funnel-and-holdout.md` (2026-10-05). Owner-reported original SHA256: `d7f246435f5b2208c25b19e9b0b01b13426d36eed8f5f7c2bc82a1301187ff82`; original bytes were unavailable in this execution environment, so that original hash is not independently verified.

Owner accepted 40 decisions, the 120-session floor, and the judge-only extract directory on grid-svr (2026-10-05). Exact holdout segment boundaries remain proposed for review; activation is separate. This slice uses synthetic/file-backed fixtures only. Canonical S11 creation/activation, live holdout, scoring-hold release, deploy, merge, migrations and trading are outside its scope. Canonical S11 was reported absent; its separate controller-owned dry-run/execute-once/verify/vault-witness procedure is required before live use.


### Approved Exact-Block Supplement & Ledger Compatibility (E3B / E0)

- **Approved Exact-Block Supplement**: Incorporates exact-block exchangeability serial null control (`exact-block-v1`, repeats DGP) as a supplementary diagnostic gate in `evals/e3/gates.py`. Runs fresh candidate machinery across 300 simulated worlds (60 blocks repeated $H=4$ times to 240 rows across 40 entities) and validates FDR, FWER, and strict KS uniformity calibration.
- **Immutable v1 Compatibility**: Historical `e3-v1` ledgers remain immutable and replay verbatim. `candidate_core` binds `e3_version` from `state.genesis["e3_version"]`, preserving hash symmetry across write and replay without retroactive migrations or anchor ID rewrites.
- **Asymmetric Forward Compatibility**: The historical `e3-v1` reader accepts `e3-v2` genesis records (as the original v1 validator checked only that `e3_version` was a string, without rejecting unknown genesis versions), but fails on `e3-v2` proposals due to candidate-ID hash mismatch (since the v1 reader hardcodes `VERSION="e3-v1"` in `candidate_core`).
- **Adaptive Block Limit**: Validity of this supplementary null DGP is strictly established for the declared fixed block size ($H=4$); it does not establish calibration for adaptive block length selection.
- **Source Criteria Unchanged**: Released E0 baseline card checks, power curve monotonicity, and manifest integrity checks remain enforced without relaxation or bypass.
- **Canonical Operations Separate**: Owner-accepted minima and judge-only extraction remain subject to separate controller activation; no live merge, deployment, or trading operations are executed.

Historical callers of `candidate_core` must pass the journal genesis version explicitly (for example, `candidate_core(record, e3_version=state.genesis["e3_version"])`); the default uses the current package version. Journal writer and replay already pass it explicitly.
