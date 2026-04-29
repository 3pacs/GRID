# GRID Agent Prompt Template (2026-04-13, TAF-OBS1 revision)

Every agent prompt that creates or modifies backend code MUST begin with the preamble below. The main dispatcher (`scripts/dispatch_agent.py`) composes the full agent prompt by concatenating the preamble fragments in `docs/agent_preamble/` (in lexicographic order) and injecting the current `pre_create_check.py` output for the task's primary concept. **The main session should call `dispatch_agent.py` to produce prompts — never hand-roll Agent() calls for backend work.**

The preamble is now stored as numbered fragments under `docs/agent_preamble/` so each section can be edited in isolation. The composite below is regenerated from those fragments and kept here as a human-readable mirror — DO NOT edit this composite directly; edit the fragment file then re-run the regen check.

| Fragment | Section |
|---|---|
| `01_pre_create_check.md` | Pre-flight coverage check |
| `02_deployment.md` | Deployment via `scripts/deploy.py` |
| `03_smoke_test.md` | Smoke test regression gate |
| `04_migration_pattern.md` | Migration GRANT footer + griddb target |
| `05_sql_safety.md` | Parameterized SQL only |
| `06_return_contract.md` | `<agent-return>` JSON envelope |

`dispatch_agent.py::load_preamble()` reads the fragment directory directly, so editing a single fragment is enough to update every future agent prompt — no regen step needed for the dispatcher itself.

---

## PREAMBLE — copy verbatim, do NOT remove or reword

### 1. Pre-flight coverage check (mandatory for new files)

Before creating any new module, script, ingestor, or intelligence component:

1. The dispatcher has already run `scripts/pre_create_check.py "<concept>"` for your primary concept and embedded the output below. READ IT. If it shows existing coverage, the default is to EXTEND the canonical module, not create a new one.
2. Cross-reference `docs/MODULE_INVENTORY.md` for every module that touches the same table or signal.
3. Only create new files when `pre_create_check` exits 1 AND inventory shows no coverage. Document your decision in the return JSON.

### 2. Deployment — use `scripts/deploy.py` ONLY

The two server trees are now a **symlink pair**: `/data/grid_v4/grid_repo` → `/data/grid_v4/astrogrid_dedup`. Physical drift is impossible.

But you still must use the deploy helper — it hash-verifies every write, snapshots pre-images for bisectable rollback, and logs to `.grid_backups/deploy_log.jsonl`:

```bash
# One file
python3 scripts/deploy.py path/to/file.py

# Multiple files + snapshot + restart + smoke test (the safe full sequence)
python3 scripts/deploy.py --snapshot --restart --smoke path/to/file.py path/to/other.py

# Deploy all staged-in-git files
python3 scripts/deploy.py --staged --restart --smoke
```

**Forbidden:** raw `scp`, `rsync`, `ssh cp`. They bypass hash verification + audit logging. Any agent report that claims a successful deploy without running through `deploy.py` is rejected.

### 3. Smoke test — `scripts/smoke_endpoints.sh` is the regression gate

A wave is not "done" until this script exits 0:

```bash
bash scripts/smoke_endpoints.sh          # runs on server, ~8 seconds
python3 scripts/deploy.py --smoke <file> # runs the script after the deploy
```

The script tests: sector_map load (3,533 actors), supply_chain, capital_flow (with percentile enrichment), contagion (with scenarios), actor_detail, sector_health, contagion→trade_tickets, explain. Each has its own exit code (1-8) so failures localize instantly.

### 4. Migration pattern

Every new migration must include the GRANT footer (see `migrations/_TEMPLATE.sql`):

```sql
GRANT ALL ON <new_table> TO grid;
GRANT USAGE, SELECT ON SEQUENCE <new_table>_id_seq TO grid;
```

Migrations without this footer break the `grid` runtime role.

### 5. SQL safety

Parameterized queries only. No f-string SQL for user-provided values. Use `text(...)` + `.bindparams(...)`. Regression guard at `tests/test_no_sql_fstrings.py` blocks regressions.

### 6. Return-value JSON contract

At the end of your final message, emit a single JSON object wrapped in `<agent-return>` tags:

```
<agent-return>
{
  "task_id": 83,
  "files_modified": ["api/routers/foo.py", "tests/test_foo.py"],
  "files_deleted": [],
  "files_created": [],
  "loc_delta": -42,
  "tests_passed": 12,
  "tests_failed": 0,
  "endpoints_verified": ["supply_chain", "capital_flow"],
  "deploy_hash_verified": true,
  "smoke_passed": true,
  "drift_check_clean": true,
  "pre_create_check_result": "exit 0 — extending intelligence/chain_contagion.py",
  "errors": [],
  "notes": "one-line summary of anything unusual"
}
</agent-return>
```

The dispatcher parses this to auto-close TaskUpdate, update `docs/WAVE_LOG.md`, and reject any agent that doesn't fill in `deploy_hash_verified` or `smoke_passed`.

---

## PREAMBLE ends

Below the preamble, the task-specific body follows. Keep it terse — the preamble handles shared context. A good body is:

- 1 line: what to do
- 1-3 lines: which files to read first (absolute paths)
- 3-5 lines: the specific algorithm or design choice
- 1 line: return with the JSON contract

Most prompts should fit under 40 body lines.

---

## Why this matters

In the 2026-04-12 session, the main agent dispatched ~35 parallel agents. Every prompt repeated deployment/preamble/verify boilerplate. CLAUDE.md listed 14 intelligence modules (the real count was 143), so every agent operated on a false inventory. Session-created duplicates included chain_contagion, supply_chokepoints, cross_lens, sector_health, fundamental_divergence, holder_deal_overlap, news_contagion_listener, supply_chain_edge_validator — each one a potential overlap with pre-existing canonical modules.

100+ modules that don't know about each other is worse than 14 that do. Duplicated work at best, contradictory predictions at worst when two modules score the same signal differently.

This preamble + `pre_create_check.py` + `dispatch_agent.py` + `deploy.py` + `smoke_endpoints.sh` prevent that class of failure mechanically, not via agent discipline.

## Enforcement

- **Main session** uses `dispatch_agent.py` to compose every backend Agent() prompt. Pasting the JSON output from the dispatcher into the Agent() tool is the contract.
- **Agent return messages** must contain the `<agent-return>` JSON block. Missing block = review-blocking defect, auto-rejected by the dispatcher's return parser.
- **First review check**: did the agent run `pre_create_check`, did it honor the result, did it use `deploy.py`, did `smoke_endpoints.sh` exit 0?
- **If an agent creates a new file without running the check:** revert via the snapshot in `.grid_backups/deploy_YYYYMMDD_HHMMSS/` and re-dispatch with the preamble.

## Related tooling

- `scripts/pre_create_check.py` — coverage search, called automatically by dispatcher
- `scripts/deploy.py` — atomic dual-write with hash verify / snapshot / restart / smoke
- `scripts/smoke_endpoints.sh` — 8-endpoint regression gate
- `scripts/dispatch_agent.py` — prompt composition wrapper (main session's Agent() entry point)
- `docs/MODULE_INVENTORY.md` — authoritative 649-module catalog
- `docs/MODULE_DEDUPE_PLAN.md` — consolidation status
- `docs/DEPLOY.md` — full deploy reference
- `.grid_backups/deploy_log.jsonl` — every deploy's audit trail
- `docs/WAVE_LOG.md` — dispatcher's wave-level rollup (append-only)
