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
