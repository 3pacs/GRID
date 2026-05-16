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
