# Fleet-Hermes audit (`scripts/fleet/`)

Read-only fleet audit for GRID compute + GPU hosts. Pairs an SSH probe with the
compute coordinator, emits a JSON/Markdown report, and optionally persists a
snapshot row to `fleet_state` per host (+ one `host='intelligence'` row per
pass).

This directory ships v0.5 of the audit:

- `../fleet_audit.py` — the audit CLI.
- `fleet_state_init.sql` — schema + idempotent indexes + retention notes.
- `poll_hosts.sh` — thin JSON polling wrapper.
- `run_audit.sh` — full-report wrapper with optional `--write-db` and agent-hub fan-out.
- `finance.stepdad.fleet-audit.plist` — LaunchAgent template (15-min cadence, `RunAtLoad=false`).

## What v0.5 covers (vs v0)

v0.5 adds three things on top of v0 (PR #179):

1. **Wider `SERVICE_NEEDLES`** (`fleet_audit.py:30`) — now includes `hermes`,
   `oracle`, `prefect`, `redpanda`, `minio`, `langfuse`, `postgres`, `micro`,
   `embed-worker`, `embed-enqueue`, `gem-hunter`, `permutation-worker`,
   `kill-predictor`, `llm-groundtruth`, `prefect-trust-scores`. These are the
   units that actually matter on `grid-svr`.
2. **Intelligence-layer reads** — each `--write-db` pass appends a single
   `host='intelligence'` row to `fleet_state` whose `state` JSONB carries
   counts/timestamps for:
   - `discovered_hypotheses` — status counts + `active` rows whose
     `last_tested` is NULL or older than 7d (this is the gap that bit us on
     2026-05-16; fleet-Hermes is the thing that should yell about it).
   - `hypothesis_asic_decisions` — counts by `predictor_version`, count in the
     last 1h, max `decided_at` (confirms the kill-predictor is producing).
   - `gem_alerts` — counts by `subject_kind` over the last 24h, max
     `detected_at`, and a `stale_alert` boolean (`> 1h` since last fire).
   - `hypothesis_pvalue_history` — new rows in the last 24h + max
     `computed_at` (confirms the permutation engine is producing).
   - `hypothesis_asic_shadow` — decisions in the last 24h + max `decided_at`
     (confirms the shadow A/B is running).

   Each section is fault-isolated: a missing table or a transient error gets
   captured as `{"error": "..."}` against that section instead of breaking the
   row.

3. **Retention prune** — every `--write-db` pass ends with
   `DELETE FROM fleet_state WHERE ts < NOW() - INTERVAL '<keep_days> days'`.

## Retention

The default retention window is **90 days** (`--prune-keep-days 90`). At the
~15-minute LaunchAgent cadence:

- `~768 rows/day` of per-host/per-GPU snapshots (8 hosts × ~96 passes/day,
  with multi-GPU hosts producing extra rows).
- `+96 rows/day` of intelligence snapshots (1 per pass).

90 days retains roughly **70k–80k rows total** — small enough that the
`(ts DESC)` index stays hot, big enough to spot week-over-week drift.

To change the window, pass `--prune-keep-days <N>` to `fleet_audit.py` or
hard-set a different default and document it here. The prune is the **only**
DELETE the audit ever runs; everything else is INSERT/SELECT.

## Safety posture

- v0.5 is still read-only against services. No `systemctl restart`, no
  `launchctl`, no service rebinds, no producer-cadence changes.
- The only DB mutations are the schema `CREATE TABLE/INDEX/ALTER ... IF NOT
  EXISTS`, the per-host/intelligence `INSERT`s, and the retention `DELETE`.
- `--apply` remains a hard `SystemExit`. v0.5 does not unlock auto-remediation.

## Running

Local smoke (no DB, no SSH):

```bash
python3 scripts/fleet_audit.py --skip-ssh --from-json /tmp/sample.json
```

Live pass with DB write + retention:

```bash
set -a; source ~/.config/grid/live-db.env; set +a
python3 scripts/fleet_audit.py \
  --timeout 6 \
  --output /tmp/fleet.json \
  --markdown /tmp/fleet.md \
  --write-db
```

Wrapper (preferred — picks up `~/.config/grid/live-db.env` if present):

```bash
GRID_FLEET_WRITE_DB=1 scripts/fleet/run_audit.sh
```
