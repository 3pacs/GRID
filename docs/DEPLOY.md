# GRID Deploy — canonical reference

**Every backend file change must be deployed via `scripts/deploy.py`.** Ad-hoc `scp` calls are forbidden going forward — they have caused drift between the two server trees at least six times this session and were the root cause of TOP ASAP FIX #1.

## The two trees

`grid-svr` (Tailscale `100.75.185.36`) hosts the backend from two distinct paths:

| Path | Services | Purpose |
|---|---|---|
| `/home/grid/grid_v4/grid_repo` (symlink → `/data/grid_v4/grid_repo`) | `grid-hermes`, `grid-extractor`, `grid-intelligence`, `grid-realtime`, `grid-spider`, `grid-backlinker`, `grid-breaking-news` | Ingestors + schedulers |
| `/data/grid_v4/astrogrid_dedup` | `grid-api` | FastAPI service the public URL hits |

Both are on the same filesystem (`/data/sdc1`) and owned by `grid:grid`. Neither is a Python venv — the venv is at `~/grid_v4/venv` (shared).

**Why two trees:** historical — the repo was forked into a second path during the V5 astrogrid dedup work and the two have drifted. Unifying is task #82 wave-2 (the present deploy.py is wave-1, stop the bleeding).

## Systemd unit drift verifier

`scripts/verify_systemd_units.py` compares every `*.service` and `*.timer` in `server_setup/` against the live systemd state on the host it's run on. It checks the runtime-relevant keys (`WorkingDirectory`, `EnvironmentFile`, `ExecStart`, `ExecStartPost`, `User`, `Restart`, …) and prints a per-unit drift report.

```bash
# Run on grid-svr — exits 0 if repo == live, 1 if any drift
python3 scripts/verify_systemd_units.py

# Show a sudo-able patch to push repo state into live
python3 scripts/verify_systemd_units.py --fix-direction repo-to-live

# Capture live state back into the repo (when an operator hand-edited live)
python3 scripts/verify_systemd_units.py --fix-direction live-to-repo
```

**Canonical paths for ingestor/scheduler units** (everything except `grid-api`): `WorkingDirectory=/home/grid/grid_v4/grid_repo` and `EnvironmentFile=/home/grid/grid_v4/grid_repo/.env`. The earlier `…/grid_repo/grid/` paths in five units were typos — `grid_repo/grid/` exists but contains only a vestigial `migrations/` subdir, not the canonical scripts. `grid-hermes`, `grid-coordinator`, `grid-worker` were already corrected in live by hand; `grid-assimilator` and `grid-tao-miner` still drift in live and need a `daemon-reload + restart` to pick up the repo paths.

**`grid-api` exception:** intentionally lives on `/data/grid_v4/astrogrid_dedup` per the two-tree split documented above. The verifier does not flag this.

## `scripts/deploy.py` — what it does

1. **Resolves local paths** relative to the repo root.
2. **Computes SHA256** of each local file.
3. **scps to `/tmp/grid_deploy_staging/` on the server**, one staged file per target.
4. **Verifies the staging-side hash** matches the local hash (catches network corruption).
5. **For each file × each tree:** atomically install via `cp → mv` into the target path, then re-read the file and verify the post-install SHA256 equals the local SHA256. Any mismatch = the operation failed; no silent drift.
6. **Records the run** to `.grid_backups/deploy_log.jsonl` with both per-file hashes, snapshot dir (if any), restart status, smoke-test status.

## Common usage

```bash
# Deploy a single file to both trees with hash verify
python3 scripts/deploy.py api/routers/flows.py

# Deploy multiple files
python3 scripts/deploy.py api/routers/flows.py analysis/sector_map.py

# Deploy with pre-image snapshot (safety net for destructive waves)
python3 scripts/deploy.py --snapshot api/routers/flows.py

# Deploy + restart grid-api
python3 scripts/deploy.py --restart api/routers/flows.py

# Full flow: snapshot + restart + smoke test
python3 scripts/deploy.py --snapshot --restart --smoke api/routers/flows.py

# Deploy all staged-in-git files (great for commit-driven workflows)
python3 scripts/deploy.py --staged

# Dry run (show what would happen, touch nothing)
python3 scripts/deploy.py --dry-run api/routers/flows.py
```

## Exit codes

| Code | Meaning |
|---|---|
| 0 | All files deployed, both trees verified, (optional smoke test passed) |
| 1 | Local file not found |
| 2 | Remote write failed (scp, mkdir, or mv) |
| 3 | Post-install hash mismatch (drift detected, run halted) |
| 4 | Smoke test failed (deploy succeeded, service may be broken) |
| 5 | SSH connection failure |

## Snapshots

Passing `--snapshot` captures the pre-image of every target file into:

```
/data/grid_v4/_backups/deploy_YYYYMMDD_HHMMSS/
├── grid_repo/<relative path>
└── astrogrid_dedup/<relative path>
```

If a deploy later turns out to be broken, `ssh grid@... "ls /data/grid_v4/_backups/"` shows every timestamped snapshot so you can roll back with a single `cp`.

The full dual-tree tarball backup (for whole-repo rollbacks) lives at `/data/grid_v4/_backups/dual_tree_YYYYMMDD_HHMMSS/`.

## Agent prompts

Every agent prompt that writes code to the server MUST invoke this tool instead of doing its own `scp` + `ssh cp` boilerplate. The canonical preamble at `docs/AGENT_PROMPT_TEMPLATE.md` has been updated (2026-04-13) to reference this document. Agent return-value contract (task #88) includes `deploy_hash_verified: bool` so the dispatcher can reject any agent that claims a successful deploy without running through `deploy.py`.

## What this fix does NOT address

- **The historical drift between the two trees is NOT resolved.** `diff -rq` still shows ~100+ diverged files (unique-to-one-side, plus wiki/docs/CLAUDE.md). A separate task (TAF-1 wave 2) will pick a per-file winner and either merge or replace one tree with a symlink to the other once content is identical.
- **Legacy bash scripts.** `deploy_to_grid_svr.sh` still exists for the [[Trial Gem Hunter]] install flow; it's annotated as LEGACY and should not gain new deploy steps.
- **Non-file state.** [[migrations|Migrations]], systemd unit reloads, and venv updates still need manual intervention. Deploy.py is for file writes only.

## Related tasks

- TAF-1 wave 2: historical drift reconciliation (per-file winner pick + unify to one tree)
- TAF-2: `scripts/dispatch_agent.py` enforces this tool in every agent call
- TAF-5: `scripts/smoke_endpoints.sh` — called by `deploy.py --smoke`
- TAF-8: timestamped backups before destructive waves (`--snapshot` flag is the foundation)
