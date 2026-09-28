# GRID systemd units

This directory ships systemd unit templates. None of them is installed by
any code path, CI job or deploy workflow.

* `grid-goal-worker@.service`: the idle-fleet goal worker (Day 1 of
  `docs/planning/IDLE-FLEET-AGENT-LOOP.md`).
* `grid-godview-{fed,cftc}.{service,timer}`: the god view pillar writers
  (materialization plan slice G7). See the section at the end.
* `grid-hypothesis-forward-log.{service,timer}`: the S10 hypothesis-loop
  forward log's daily run. See its section below.

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

## `grid-dollar-flows.service` / `.timer` (templates, not installed)

Wave 3 item W3.3's scheduled writer for `dollar_flows`
(`scripts/run_dollar_flows.py` -> `intelligence/dollar_flows.py`'s
`normalize_all_flows`). Each run scans `signal_sources` + `raw_series` for
the last 7 days, converts every signal to an estimated USD amount, and
persists via a DELETE-then-INSERT over the touched date range. The
`#/geo-flows` view reads the result.

Three honesty guards (GRID-WAVE3-HELD-WRITERS-TRIAGE-20260927 §4.3, plus the
staleness bound added per PR #712 review), all counted in the run's summary
rather than silently absorbed:
  - A dark-pool row with no real VWAP observation (via
    `store/observations.py::read_latest` on `YF:{ticker}:close`,
    SUCCESS-only, PIT) is dropped, never fabricated from the old
    `_DEFAULT_VWAP_ESTIMATE` ($50 flat).
  - A dark-pool row whose only VWAP observation is older than 5 trading
    days (`intelligence.dollar_flows._VWAP_MAX_AGE_TRADING_DAYS`, via the
    real NYSE calendar in `ingestion/market_calendar.py`) is dropped rather
    than priced off a stale close.
  - A row whose `signal_date` (or, for 13F/ETF flows, `obs_date`) is in the
    future is dropped, never persisted.

Try it first with `python3 scripts/run_dollar_flows.py --dry-run --json` —
no DELETE/INSERT against `dollar_flows` happens in a dry run. Installing
the timer is the activation decision; the install commands are in the
service template's header.

## `grid-market-diary.service` / `.timer` (templates, not installed)

Wave 3 slice W3.1's scheduled writer for `market_diary`
(`scripts/run_market_diary.py` -> `intelligence/market_diary.py`). Each run
writes one entry: rule-based market-move / active-actor sections, an LLM
narrative over them, and a pre-open thesis verdict read from the last
`thesis_snapshots` row before 13:30Z that day (never computed at write
time — that was the look-ahead this slice fixed). The Market Diary view
shows the result.

**Before installing:** confirm the YF quarantine
(`raw_series_quarantined_20260926`) has actually been run against
`raw_series` on this host, then set `GRID_MARKET_DIARY_PRICES_ENABLED=true`
in `/home/grid/grid_v4/grid_repo/.env`. Until that flag is set, the job
still runs and still writes a diary entry (actors, pre-open thesis verdict,
narrative) — it just reports every price-dependent field as unavailable
instead of reading `raw_series`. Setting the flag does not bypass the
per-series freshness check: every price read still requires the newest
accepted observation to be dated exactly the target trading date, or that
entry is reported "no close for date" rather than a stale value.

Try it first with `python3 scripts/run_market_diary.py --dry-run --json`.

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

## `grid-hypothesis-forward-log` (service + timer)

Daily run of `scripts.research_forward_log run` (S10; see
`analysis/research_forward_log.py` and
`docs/paper_log/hypothesis-forward-v1-preregistration.md`). Runs from the
deployed release `/data/grid_v4/grid_release` as `User=grid` with the repo
`.env`, under `flock -n`. Opens a read-only, short-statement-timeout DB
connection, reads only through the latest-vintage adapter
(`store/observations.py`), appends due prediction/outcome/verdict records
to the hash-chained, append-only JSONL log at
`/data/grid/paper_log/hypothesis_forward_v1/` (a persistent path outside
the per-commit release tree), and rewrites `STATUS.md`. No orders, no
weights, no promotion — every record carries `promotion_allowed: false`.
On an empty log (no admitted candidates yet) it writes only the header
record.

This unit supersedes the crontab + `git archive` install path proposed in
`deploy/paper_log/hypothesis_forward_v1.sh` for grid-svr (that script is
kept for reference / other hosts, but grid-svr's actual copy of the code
already lives at `/data/grid_v4/grid_release`, so a separate per-commit
archive under `/data/grid/paper_log/code/<sha>/` is unnecessary here).

Schedule: 11:30 UTC daily (`OnCalendar`, `Persistent=true`), well clear of
the nightly encrypted Postgres backup window (`grid-pg-backup.timer` fires
03:30 UTC and recent runs have taken until roughly 10:05-10:30 UTC;
`grid-analytics-snapshots.timer` was moved off 07:15 UTC for the same
reason — see its `timer.d/override.conf` on grid-svr).

### Verify before installing (read-only, no DB, writes nothing)

```bash
cd /data/grid_v4/grid_release
PYTHONPATH=/data/grid_v4/grid_release /usr/bin/python3 -m scripts.research_forward_log status --log-dir /tmp/some-throwaway-dir
```

`status` and `verify` never open a database connection and never write to
the log directory (`run` and `admit` are the only writing commands) — see
the module's own docstring before running anything else by hand.

### Install (owner decision)

```bash
sudo install -m 0644 deploy/systemd/grid-hypothesis-forward-log.service.template \
    /etc/systemd/system/grid-hypothesis-forward-log.service
sudo install -m 0644 deploy/systemd/grid-hypothesis-forward-log.timer.template \
    /etc/systemd/system/grid-hypothesis-forward-log.timer
sudo systemctl daemon-reload
sudo systemctl enable --now grid-hypothesis-forward-log.timer   # timer only — let the job fire on its own schedule, don't `systemctl start` the service by hand
systemctl list-timers grid-hypothesis-forward-log.timer
```

Stop / roll back: `sudo systemctl disable --now grid-hypothesis-forward-log.timer`.
Never delete `/data/grid/paper_log/hypothesis_forward_v1/hypothesis_forward_v1.jsonl`
— it is the permanent, append-only research record; archive it first if
v1 is ever abandoned (see the pre-registration's Integrity section).
