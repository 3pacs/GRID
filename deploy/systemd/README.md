# GRID systemd units

This directory ships systemd unit templates. None of them is installed by
any code path, CI job or deploy workflow.

* `grid-goal-worker@.service`: the idle-fleet goal worker (Day 1 of
  `docs/planning/IDLE-FLEET-AGENT-LOOP.md`).
* `grid-godview-{fed,cftc}.{service,timer}`: the god view pillar writers
  (materialization plan slice G7). See the section at the end.

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

## `grid-causal-links.service` / `.timer` (templates, not installed)

Slice N2's scheduled writer for `causal_links`
(`scripts/run_causal_links.py` -> `intelligence/causal_links.py`). Each run
links recent insider / congressional trades to public events on the same
ticker that were knowable *before* the trade day, upserts them keyed on
`edge_key` with run id, code sha and known_at, and records the run in
`causal_link_runs`. The Timeline / Causal Map / Why views show those rows
with the last finished run's as-of time.

Prerequisite: `alembic upgrade head` has applied
`causal_links_provenance_20260927` (the script exits 2 without writing
otherwise). Installing the timer is the activation decision; the install
commands are in the service template's header. Try it first with
`python3 scripts/run_causal_links.py --dry-run --json`.

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

## `grid-regime-state-vectors` (service + timer) — NOT installed

Wave 3 W3.2's scheduled writer for `regime_state_vectors`
(`scripts/run_regime_state_vectors.py` -> `intelligence.regime.state_vector`).
It is the *only* writer of that table: the `/regime` and `/regime/analogs`
GET routes (`api/routers/intelligence_regime.py`) call
`get_or_compute_state_vector(..., persist=False)` and never write, computing
a vector in memory (`cached: false`) when nothing is cached for the
requested date instead of caching a possibly-partial or same-day one. Each
run computes and persists **only the prior completed trading session's**
vector (`resolve_target_date()`: `last_trading_day(today - 1 day)`, so it
can never pick up today's still-open or just-closed session regardless of
what time it runs), and only if the vector clears
`intelligence.regime.state_vector.MIN_CACHE_COMPLETENESS` (0.4).

Readers go through `store.observations.read_window` (SUCCESS-only,
vintage-collapsed, PIT) instead of raw `raw_series` queries, and the SPY
momentum/RSI dimensions prefer the resolved `spy_full` feature (via
`alpha_research.realized_alpha.resolve_spy_feature` + `store.pit.PITStore`)
over the raw `YF:SPY:close` series when it's available — the vector's
`price_basis` field records which one was actually used, and is `null` when
neither had data (an honest "unavailable", not a silent stale read).

Schedule: weekdays 23:00Z (`OnCalendar=Mon..Fri`, `Persistent=true`),
comfortably after the US market close.

**Deliberately excluded from this PR:** rebuilding the 1,927 existing
`regime_state_vectors` rows (computed 2026-05-08, before the YF quarantine)
through `compute_state_vector_series` into a scratch table and diffing/
swapping them in — per the Wave 3 triage report, "any history rebuild is a
data write that needs a separate GO" from the operator. Those rows are
untouched; only new rows (from the prior session onward, once this timer is
installed) use the PIT readers.

Try it first, read-only:

```bash
cd /data/grid_v4/grid_release && set -a && . /home/grid/grid_v4/grid_repo/.env && set +a
python3 scripts/run_regime_state_vectors.py --dry-run --json      # compute, write nothing
python3 scripts/run_regime_state_vectors.py --as-of 2026-09-24 --dry-run --json  # backfill preview
```

### Install (owner decision — not done by the PR)

```bash
sudo install -m 0644 deploy/systemd/grid-regime-state-vectors.service.template \
    /etc/systemd/system/grid-regime-state-vectors.service
sudo install -m 0644 deploy/systemd/grid-regime-state-vectors.timer.template \
    /etc/systemd/system/grid-regime-state-vectors.timer
sudo systemctl daemon-reload
sudo systemctl start grid-regime-state-vectors.service   # one real run first
journalctl -u grid-regime-state-vectors.service -n 50 --no-pager
sudo systemctl enable --now grid-regime-state-vectors.timer
```

Stop / roll back: `sudo systemctl disable --now grid-regime-state-vectors.timer`.
Rows it wrote are ordinary `regime_state_vectors` rows; there is no
provenance column to filter on (the table predates that convention), but
new rows carry a `price_basis` key inside the `vector` JSONB blob that the
2026-05-08 backfill's rows never had.

## `grid-godview-fed` / `grid-godview-cftc` (god view writers)

Oneshot services plus weekly timers that run
`scripts/run_godview_writers.py --pillar fed|cftc --code-sha-from-git` from
the deployed release tree `/data/grid_v4/grid_release`, as `grid`, with the
GRID env file `/home/grid/grid_v4/grid_repo/.env`, under a shared
`flock /tmp/grid-godview-writers.lock` (one writer at a time).

| Timer | Calendar (America/New_York) | UTC (EDT / EST) | Why |
|---|---|---|---|
| `grid-godview-fed.timer` | Thu 17:30, retry Fri 09:00 | 21:30 / 22:30 | H.4.1 publishes Thu ~16:30 ET; FRED lands WALCL/WTREGEN ~30 min later |
| `grid-godview-cftc.timer` | Fri 16:00, retry Sat 14:00 | 20:00 / 21:00 | COT publishes Fri 15:30 ET; the puller moves to Fri >= 19:45 UTC + a Saturday retry (plan A1) |

Every run writes a `godview_runs` row (`complete`, `noop`,
`partial_blocked_by_legacy`, `inputs_missing` or `failed`); exit codes are in
the script's docstring. `partial_blocked_by_legacy` exits 0: keys held by
legacy (NULL-provenance) rows are skipped until the A2 archive clears them.

**Activation is the owner's step (plan A3). Do not install before:** the G2
migration is applied (A2), the CFTC backfill under the new ids is done (A1)
for the cftc pillar, and a manual dry run on the host looks right:

```bash
cd /data/grid_v4/grid_release
set -a; . /home/grid/grid_v4/grid_repo/.env; set +a
python3 scripts/run_godview_writers.py --pillar fed  --code-sha-from-git --dry-run
python3 scripts/run_godview_writers.py --pillar cftc --code-sha-from-git --dry-run
systemd-analyze calendar 'Thu *-*-* 17:30:00 America/New_York'   # needs systemd >= 235
git -C /data/grid_v4/grid_release rev-parse HEAD                 # must work as grid
```

Then:

```bash
sudo install -m 0644 deploy/systemd/grid-godview-fed.service  /etc/systemd/system/
sudo install -m 0644 deploy/systemd/grid-godview-fed.timer    /etc/systemd/system/
sudo install -m 0644 deploy/systemd/grid-godview-cftc.service /etc/systemd/system/
sudo install -m 0644 deploy/systemd/grid-godview-cftc.timer   /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl start grid-godview-fed.service && journalctl -u grid-godview-fed.service -n 50
sudo systemctl enable --now grid-godview-fed.timer    # fed can go before cftc
sudo systemctl enable --now grid-godview-cftc.timer
```
