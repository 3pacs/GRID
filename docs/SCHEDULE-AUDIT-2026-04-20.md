# Hypothesis + Backtest Schedule Audit — 2026-04-20

Session goal: make sure GRID is generating new hypotheses and running
backtests whenever it is supposed to.

## Findings

1. **`scripts/walk_forward_validate.py` was orphaned** — a full [[Walk-Forward Backtesting|walk-forward]]
   backtest harness with its own CLI, but no systemd unit, cron, or in-process
   schedule entry invoked it. Nothing was running it automatically.

2. **Daily 2 AM batch in `scripts/hermes_operator.py` had a 10-minute window
   only** (`now.hour == 2 and now.minute < 10`). If the 2:00–2:10 UTC window
   was missed (cycle timeout, hermes restart, long-running prior cycle), the
   daily batch — which runs `backtest_scanner.run_full_scan`,
   `review_existing_hypotheses`, and `hypothesis_engine.auto_discover` —
   would be skipped entirely for that day. No catch-up.

3. **`OperatorState` was in-memory only.** On every `grid-hermes` restart
   the `last_*` timestamps reset to `None`, so `_hours_since(None) == inf`
   and every gate passed immediately. You could not tell from state whether
   work actually ran or was re-fired after a restart.

4. **Double-scheduled hypothesis generation at 02:00 UTC.** Both
   `intelligence/scheduler.py` (grid-intelligence service, `_nightly_research`
   → `research_agent.run_full_research` → `generate_hypotheses`) AND
   `scripts/hermes_operator.py` (grid-hermes service,
   `hypothesis_engine.auto_discover` + `backtest_scanner.run_full_scan`)
   fired at 02:00.

## Fixes (this commit)

### (1) Wired walk-forward backtest into systemd

New unit files in `server_setup/`:

- `grid-walk-forward-daily.service` + `.timer` → 03:30 UTC daily,
  `python3 -m scripts.walk_forward_validate --days 90 --horizon-days 7`
- `grid-walk-forward-weekly.service` + `.timer` → Sunday 04:00 UTC,
  `python3 -m scripts.walk_forward_validate --days 365`

Both use `Persistent=true` so a missed run catches up on boot, and a
`flock` to prevent overlapping runs.

### (2) Widened the 2 AM daily gate in `scripts/hermes_operator.py`

The daily batch now fires if either:
- `now.hour == 2 and now.minute < 10` (original 10-min window), OR
- `now.hour >= 2` and no daily batch has run today yet (catch-up).

The `_hours_since(last_daily_intel) >= 20` guard still prevents
double-runs inside the same UTC day.

### (3) Persisted `OperatorState` across restarts

Added `OperatorState.hydrate_from_snapshot(engine)` in
`scripts/hermes_health.py`. It reads the most recent
`analytical_snapshots` row with `subcategory='hermes_operator'` and
restores all `last_*` timestamps plus cumulative counters. Called once
from `scripts/hermes_operator.py::main()` on startup. Silent-fail;
first-boot (no snapshot) is a normal fresh start.

Also extended `to_dict()` to include `last_signal_forecasts`,
`last_enrich_connections`, `last_contagion_backtest`,
`last_contagion_feedback`, `last_sector_health`, `last_options_scoring`
so they round-trip through the snapshot.

### (4) De-duplicated 02:00 UTC hypothesis generation

Moved `intelligence/scheduler.py` entries:
- `_nightly_research`: 02:00 → **02:45**
- `_taxonomy_audit`: 02:30 → **03:15**

This keeps `hermes_operator`'s 02:00–02:10 batch as the single source of
daily hypothesis discovery + backtest scan, and prevents DB contention
and duplicate hypothesis writes against `hypothesis_registry`.

## Final UTC schedule (hypothesis + backtest surface)

| Time       | Service            | Work                                                         |
|------------|--------------------|--------------------------------------------------------------|
| 02:00–02:10| grid-hermes        | backtest_scanner.run_full_scan → new TACTICAL hypotheses     |
| 02:00–02:10| grid-hermes        | backtest_scanner.review_existing_hypotheses                  |
| 02:00–02:10| grid-hermes        | hypothesis_engine.auto_discover                              |
| 02:00–02:10| grid-hermes        | postmortem.batch_postmortem                                  |
| 02:45      | grid-intelligence  | research_agent.run_full_research (sector research + hyps)    |
| 03:15      | grid-intelligence  | taxonomy_audit                                               |
| 03:30      | timer (daily)      | walk_forward_validate --days 90 --horizon-days 7             |
| Sun 02:30  | grid-scheduler     | hypothesis_engine.cleanup_hypotheses                         |
| Sun 04:00  | timer (weekly)     | walk_forward_validate --days 365                             |

Autoresearch (`scripts/autoresearch.py`) still runs opportunistically
inside hermes cycles with a ≥12h cooldown — unchanged.

## Deployment (on grid-svr)

```bash
# 1. pull latest
ssh grid@grid-svr 'cd /home/grid/grid_v4/grid_repo && git pull origin main'

# 2. install new timers
ssh grid@grid-svr 'sudo cp /home/grid/grid_v4/grid_repo/server_setup/grid-walk-forward-*.{service,timer} /etc/systemd/system/ && sudo systemctl daemon-reload'

# 3. enable + start timers
ssh grid@grid-svr '
  sudo systemctl enable --now grid-walk-forward-daily.timer
  sudo systemctl enable --now grid-walk-forward-weekly.timer
'

# 4. restart affected daemons to pick up code changes
ssh grid@grid-svr 'sudo systemctl restart grid-hermes grid-intelligence'
```

## Verification checklist

Run these on grid-svr after deploy. All should return the expected
value — if any fail, the fix did not take.

```bash
# Timers are active and show next fire time
systemctl list-timers 'grid-walk-forward-*'
#   Expect: NEXT shows the upcoming 03:30 UTC / Sun 04:00 UTC fires.

# State persistence confirmed — restart hermes, confirm it hydrates
sudo systemctl restart grid-hermes
sleep 10
grep -E "hydrated from snapshot|no prior snapshot found" /data/grid/logs/hermes-operator.log | tail -3
#   Expect: "Hermes state hydrated from snapshot (last_daily_intel=..., ...)"
#           on restarts after first. First-ever restart prints the fresh-start
#           message — still fine.

# Daily batch catch-up — check log for either window or catch-up trigger
grep -E "Running daily intelligence batch" /data/grid/logs/hermes-operator.log | tail -5
#   Expect: "(window=True catch_up=False)" between 02:00–02:10 UTC
#           "(window=False catch_up=True)" on any day where hermes started
#           after 02:10 UTC and hadn't run yet that day.

# 02:45 research no longer colliding with 02:00 hermes batch
grep -E "Nightly research complete" /data/grid/logs/intelligence-loop.log | tail -5
#   Expect: log timestamps around 02:45 UTC, not 02:00 UTC.

# Walk-forward ran
tail -50 /data/grid/logs/grid-walk-forward-daily.log
#   Expect: "walk_forward_validate done: walked=... trades=..." at the most
#           recent 03:30 UTC tick.

# DB-level confirmation — hypothesis + backtest rows landed
psql -U grid_user -d grid -c "
  SELECT MAX(created_at) AS last_hypothesis FROM hypothesis_registry;
  SELECT MAX(created_at) AS last_backtest   FROM backtest_results;
"
#   Expect: both timestamps within the last 26 hours under normal load.
```

## Notes for future agents

- **Single source of truth for daily hypothesis generation is
  `scripts/hermes_operator.py`.** If you need to add a new hypothesis
  generator, hang it off the daily 2 AM block there — do not re-add one
  to `intelligence/scheduler.py`.
- **Never schedule anything else at exactly 02:00 UTC** without
  accounting for the 02:00–02:10 hermes batch already landing there.
  Prefer 02:45+ or any hour other than 2 UTC.
- **[[Walk-Forward Backtesting|Walk-forward]] backtest is systemd-timer-driven, not in-process.**
  If `OnCalendar` fires but the unit errors out, `journalctl -u
  grid-walk-forward-daily.service` will show why; the hermes operator
  log will NOT.
- **If you restart grid-hermes**, the hydrate step restores
  `last_daily_intel` from the most recent `analytical_snapshots` row
  with `subcategory='hermes_operator'`. If you ever wipe that table
  or clear snapshots older than a few cycles, the operator will act
  like a fresh start and may re-fire the daily batch. This is the
  intended trade-off (cheap to re-run vs. risk of missing a run).
- **Gate pattern to copy** for any new daily task:
  ```python
  is_window   = (now.hour == HH and now.minute < 10)
  is_catch_up = (now.hour >= HH and
                 (state.last_X is None or state.last_X.date() < now.date()))
  due = (is_window or is_catch_up) and _hours_since(state.last_X) >= 20
  ```
  This combo gives a clean on-time firing window PLUS catch-up on
  restarts, without risking double-runs inside a UTC day.
