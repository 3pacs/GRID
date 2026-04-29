# Task Priority Convention (TAF-OBS2)

Every task subject in the GRID task queue must begin with one of three priority tokens. The convention is mechanical, not aspirational — it gates dispatcher behavior, wave bundling, and review urgency.

## Tokens

| Token | Meaning | Dispatch behavior |
|-------|---------|-------------------|
| **P0** | Blocking — production is broken, an alpha loop is severed, or another task can't start until this lands | Run first. Bundle with other P0s if file claims permit. Escalate review immediately on failure. |
| **P1** | Important — unlocks new alpha or removes recurring drag, but the platform survives without it for a day | Run after the P0 wave is green. Default for most feature work. |
| **P2** | Governance / housekeeping — docs, refactors, drift cleanup, stale-data prunes | Run when P0/P1 queue is empty, or in batched maintenance waves. Never blocks a wave. |

A task without a priority token in its subject is treated as **P1** by the dispatcher, but the linter will flag it for relabeling.

## Subject format

```
<TOKEN> <CODE>: <one-line summary>
```

Examples:

- `P0 SYNTH-A: Wire handlers for PredictionScored + PostmortemCompleted`
- `P1 GEX-3: Port 7 BS Greeks from Gex Grok MD.md`
- `P2 OBSIDIAN-3: Vault drift reconciliation`

The `CODE` is a short slug (`SYNTH-A`, `INTEL-1`, `TAF-OBS3`) so the task can be referenced verbally without the database id.

## Wave bundling

When a wave is composed, the dispatcher groups tasks by priority:

1. All P0 tasks ship in the first wave (parallel where file claims allow).
2. P1 tasks ship in the second wave, after P0 verification is green.
3. P2 tasks ship in a separate maintenance pass — never mixed with P0/P1.

This is why every task subject **must** carry a token: without one the bundler treats it as P1 and may run it ahead of governance work it was meant to follow.

## Why it matters

In the 2026-04-12 session the queue had 75+ tasks and no priority field. The main session bundled P0 alpha loops with P2 cleanup tasks in the same wave, which:

- delayed alpha rollouts behind doc edits
- ran governance work in parallel with active feature dispatches (file conflicts)
- made wave summaries impossible to scan ("3 of 8 done" tells you nothing about which 5 are blocking)

The fix is the priority field plus a dispatcher rule that refuses to bundle across priorities. A wave is now homogeneous in priority by construction.

## Linter

`scripts/dispatch_agent.py` validates the priority token at compose time:

```bash
python3 scripts/dispatch_agent.py compose --task-id 99 ...
# fails with:
#   error: task 99 subject does not start with P0/P1/P2 — see docs/TASK_PRIORITY_CONVENTION.md
```

Re-run after fixing the task subject. The linter does NOT auto-correct — operator visibility is the point.
