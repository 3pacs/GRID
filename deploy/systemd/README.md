# GRID systemd units

This directory ships the systemd unit template for the idle-fleet goal
worker (Day 1 of `docs/planning/IDLE-FLEET-AGENT-LOOP.md`).

## `grid-goal-worker@.service`

A *templated* unit. One instance per Tailnet node, each with its own
env file. The `%i` placeholder identifies the instance and selects the
env file path — it has no effect on the worker's runtime identity.
The worker's actual `node_id`, `hardware_tier`, and `max_duty_cycle`
come from the env file.

### Per-node env file

Create `/etc/grid/goal-worker-<node>.env` on each node. Required keys:

```ini
# Identity reported to goal_queue.claimed_by and goal_results.node_id.
GRID_GOAL_WORKER_NODE_ID=gridz4

# One of: cpu, medium_gpu, large_gpu, vision.
# Determines which goals this node is eligible to claim.
GRID_GOAL_WORKER_HARDWARE_TIER=large_gpu

# Optional: restrict to specific goal_type values (comma-separated).
# Useful while only one handler exists (Day 1: score_active_hypothesis).
# GRID_GOAL_WORKER_GOAL_TYPES=score_active_hypothesis
```

Optional overrides (defaults shown in the unit file):

```ini
GRID_GOAL_WORKER_POLL_SECONDS=30
GRID_GOAL_WORKER_LEASE_SECONDS=600
GRID_GOAL_WORKER_MAX_DUTY_CYCLE=0.5
GRID_GOAL_WORKER_DUTY_WINDOW_S=300
GRID_GOAL_WORKER_HEARTBEAT_SEC=60

# Locked decision #1: cloud LLMs are refused by default. Set to 1 only
# when Anik explicitly approves a cloud-using goal class for this node.
GRID_GOAL_WORKER_ALLOW_CLOUD=0
```

GRID DB connection comes from the standard `config.py` settings
(`GRID_DB_*` env vars); no duplication here.

### Suggested per-node config

| Node       | Hardware              | tier         | duty | goal_types (Day 1)         |
|------------|-----------------------|--------------|------|-----------------------------|
| gridz4     | Blackwell 24G + A2000 | `large_gpu`  | 0.5  | score_active_hypothesis     |
| ocr-node   | RTX 2070S + 3050      | `vision`     | 0.5  | score_active_hypothesis     |
| z400       | A2000 12G             | `medium_gpu` | 0.5  | score_active_hypothesis     |
| redbox     | GTX 1060/1650         | `medium_gpu` | 0.3  | score_active_hypothesis     |
| koala      | CPU only              | `cpu`        | 0.5  | score_active_hypothesis     |
| p9d        | Blackwell 16G         | `large_gpu`  | --   | -- (Day 1: skip per plan)    |

p9d intentionally skipped on Day 1 — ComfyUI co-scheduling is empirical
and lives in Day 2-4 (locked decision #2).

### Install & enable

Day 1 PR is build-only. **Do not deploy yet.** Once approved, on each
node:

```bash
sudo install -m 0644 grid-goal-worker.service.template \
    /etc/systemd/system/grid-goal-worker@.service

sudo install -d -m 0750 -o root -g grid /etc/grid
sudoedit /etc/grid/goal-worker-$(hostname).env   # fill in values above
sudo chmod 0640 /etc/grid/goal-worker-$(hostname).env
sudo chown root:grid /etc/grid/goal-worker-$(hostname).env

sudo systemctl daemon-reload
sudo systemctl enable --now grid-goal-worker@$(hostname).service
sudo systemctl status grid-goal-worker@$(hostname).service
journalctl -u grid-goal-worker@$(hostname).service -f
```

### Verify

After a few minutes the worker should be claiming or polling. Check
the queue depth from any node with a GRID checkout:

```bash
psql "$GRID_DB_URL" -c "
  SELECT state, hardware_tier, COUNT(*) AS n
  FROM goal_queue
  GROUP BY state, hardware_tier
  ORDER BY state, hardware_tier;"
```

### Stop

```bash
sudo systemctl stop grid-goal-worker@$(hostname).service
```

The worker handles `SIGTERM` cleanly — in-flight goals complete, then
the next `claim_goal` returns the loop. No forcible kill is needed
unless the lease has to be reaped (it will be, automatically, on the
next worker startup or by the Day 2 reaper).

## `grid-analytics-snapshots` (service + timer) — NOT installed

Daily run of `scripts/run_analytics_snapshots.py`, which refreshes the
`analytical_snapshots` categories behind the discovery, associations,
snapshots and models views: `feature_engineering`, `orthogonality`,
`clustering` (global), `clustering_sector` (one row per sector of
`analysis/sector_map.py`, `subcategory` = sector name), `feature_importance`
and `options_scan` (a summary of the scan grid-scheduler already persists).
It writes nothing else: no ingestion, no resolver, no email, no
`feature_importance_log`, no hypothesis/weights/autoresearch tables, no LLM
calls. Every run tags each payload with a `provenance` block (release SHA,
as-of date, vintage policy, gate result, stale inputs it excluded).

Schedule: 07:15 UTC daily (`OnCalendar`, `Persistent=true`), after
`grid-resolved-series-backfill.timer` (06:30 UTC). Runs from the deployed
release `/data/grid_v4/grid_release` as `User=grid` with the repo `.env`,
under `flock -n`, `TimeoutStartSec=3h`.

### Readiness gate (checked by the job on every run)

The job exits 3 without computing or writing anything unless **all** pass
(`SuccessExitStatus=3`, so a not-ready day is a clean skip):

| id | check |
|----|-------|
| G1 | table `resolved_series_retractions` exists (PR #683 migration) |
| G2 | it holds rows with `run_tag = $GRID_ANALYTICS_REQUIRED_RUN_TAG` (default `reresolve_20260927`) |
| G3 | the newest of those rows is older than `$GRID_ANALYTICS_SETTLE_MINUTES` (default 60) — the retraction insert has finished |
| G4 | the operator flag file `$GRID_ANALYTICS_READY_FLAG` (default `/data/grid/state/analytics-snapshots.ready`) exists and its first line is the run tag |
| G5 | `spy_full` has a PIT observation within `$GRID_ANALYTICS_MAX_RESOLVER_LAG_DAYS` (default 4) of the as-of date — the resolver refresh landed |

G4 is the explicit go: write it only after the re-resolve run order is
complete and `07_verify_retractions.sql` passed
(`GRID-RERESOLVE-PLAN-20260927` §5, step 8):

```bash
sudo install -d -m 0755 -o grid -g grid /data/grid/state
echo reresolve_20260927 | sudo -u grid tee /data/grid/state/analytics-snapshots.ready
```

Check the gate without computing anything (read-only):

```bash
cd /data/grid_v4/grid_release && set -a && . /home/grid/grid_v4/grid_repo/.env && set +a
python3 scripts/run_analytics_snapshots.py --check-only     # exit 0 = ready, 3 = not ready
python3 scripts/run_analytics_snapshots.py --dry-run        # compute, write nothing
```

### Install (owner decision — not done by the PR)

```bash
sudo install -m 0644 deploy/systemd/grid-analytics-snapshots.service.template \
    /etc/systemd/system/grid-analytics-snapshots.service
sudo install -m 0644 deploy/systemd/grid-analytics-snapshots.timer.template \
    /etc/systemd/system/grid-analytics-snapshots.timer
sudo systemctl daemon-reload
sudo systemctl start grid-analytics-snapshots.service   # one gated run first
journalctl -u grid-analytics-snapshots.service -n 200 --no-pager
sudo systemctl enable --now grid-analytics-snapshots.timer
```

Stop / roll back: `sudo systemctl disable --now grid-analytics-snapshots.timer`.
Rows it wrote are ordinary `analytical_snapshots` rows (identifiable by
`payload->'provenance'->>'job' = 'run_analytics_snapshots'`).
