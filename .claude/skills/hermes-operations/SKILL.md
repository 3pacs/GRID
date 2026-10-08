---
name: hermes-operations
description: Operate the Hermes autonomous daemon — the 24/7 self-healing scheduler for background tasks. Use when adding scheduled steps or debugging why a scheduled step or puller did not run (cycle structure, time gates, timeouts); for source freshness thresholds, API keys, or NaN quality use data-health.
---

# hermes-operations

Operating the Hermes autonomous daemon — the 24/7 self-healing system that runs all GRID background tasks. Covers the cycle structure, step scheduling, data gathering, and troubleshooting.

## When to Use This Skill

- Adding new scheduled tasks to Hermes
- Debugging why a puller or pipeline step isn't running
- Understanding the Hermes cycle structure
- Monitoring system health and freshness
- Diagnosing stale data or failed cycles

## Cycle Structure

Every 5 minutes, Hermes runs one cycle:

```
0. Git pull (sync latest code/config)
1. Health check (DB, connection pool, Hermes health, alert thresholds)
1b. Obsidian vault sync + agent cycle
2. Fix broken pulls (diagnose_and_fix_pulls with cooldown + smart retry)
2b. Proactively re-pull stale sources (up to 15 per cycle)
3. Smart ingestion (SmartScheduler runs due/stale pullers)
3b. Conflict resolution (raw_series → resolved_series via canonical resolver)
4. Data gaps (skipped; handled by SmartScheduler)
5. Self-diagnostics (every 6th cycle / 30 min)
6. Autoresearch (every 12th cycle / 1 hour, bounded and fenced)
7. Specialized periodic tasks:
   7.      UX Audit (every 72nd cycle / ~6 hours)
   7b.     Daily digest email (once per day)
   7c.     100x Digest (every 4 hours)
   7c-ii.  Solana top-volume snapshot (every 4 hours)
   7c-iii. Supply Chain Pulse watchdog (every 6 hours)
   7c-iv.  News contagion listener (every 15 minutes)
   7c2.    AstroGrid celestial cycle (hourly, ahead of oracle; sky snapshot + interpretation)
   7d.     Oracle prediction cycle (every 6 hours)
   7d-ii.  TimesFM forecast cycle (every 6 hours)
   7d-iii. AutoBNN changepoint detection (every 12 hours)
   7d-iv - 7d-vi. Gemma micro classification, narration, knowledge mapping
   7e.     Alpha research heartbeat + signal publishing (every cycle)
   7f.     Sector health snapshot & intelligence modules (trust scoring, cross-reference, etc.)
   7g.     Rotation paper trading (daily after 17:00 UTC)
   7h.     Tiingo bulk data pull (overnight 02:00-06:00 UTC)
8. Git push (commit analytical outputs)
8b. LLM Task Queue status
9. Save cycle snapshot & Obsidian cycle report
```

## Key Files

| File | Purpose |
|------|---------|
| `scripts/hermes_operator.py` | Main daemon (~4700 lines) |
| `ingestion/scheduler.py` | Pull schedule definitions |
| `scripts/hermes_operator.py:run_cycle()` | One cycle logic |
| `scripts/hermes_operator.py:OperatorState` | Persistent state across cycles |

## Adding a New Scheduled Task

1. Find the appropriate step number (7a-7z)
2. Add a time-gated block in `run_cycle()`:
   ```python
   # 7x. My new task — daily at HH:MM UTC
   try:
       now_utc = datetime.now(timezone.utc)
       if HH <= now_utc.hour < HH+1:
           last_run = getattr(state, "_last_mytask_date", None)
           if last_run != now_utc.date():
               if not dry_run:
                   # ... run task ...
                   state._last_mytask_date = now_utc.date()
   except Exception as exc:
       log.warning("My task failed: {e}", e=str(exc))
   ```

3. Use `getattr(state, attr, None)` for new state fields (backwards compatible)

4. If the task is slow enough to need a `_run_with_timeout` budget, check the
   cycle's remaining budget before starting it, and advance the state marker
   only on a completed run:

   ```python
   elapsed = time.monotonic() - cycle_start
   if CYCLE_TIMEOUT_SECONDS - elapsed < MY_TASK_TIMEOUT_SECONDS:
       # defer; leave the marker alone so the next cycle retries
   ```
   The budgeted steps ahead of a task can consume cycle budget, so a slow
   cycle can reach a step with insufficient time left — and a budgeted step
   that starts without enough budget takes the whole cycle down rather than
   just itself. Cycles run every 5 minutes, so deferring costs one cycle, not
   the slot. See step 7c2 (AstroGrid celestial cycle) for a worked example.

## Service Management

```bash
# Status
sudo systemctl status grid-hermes

# Restart (after code changes)
sudo systemctl restart grid-hermes

# Logs (if journalctl has permission)
sudo journalctl -u grid-hermes --since "1 hour ago" --no-pager

# Manual single cycle
python3 scripts/hermes_operator.py --once

# Dry run (diagnose without fixing)
python3 scripts/hermes_operator.py --once --dry-run
```

## Common Issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| Puller returns 0 rows | API key expired or rate limited | Check .env, test API manually |
| Cycle takes >15 min | Resolver scanning full raw_series | Use lookback_days=7 |
| Git push fails | Merge conflict or auth | Manual git pull/push |
| LLM unavailable | llama.cpp crashed | `sudo systemctl restart grid-llamacpp` |
| Stale data (>26h) | Puller blacklisted | Check source_catalog, clear blacklist |

## Monitoring

Health endpoint: `GET http://localhost:8000/api/v1/system/health`

Key fields:
- `recent_data: true` — data pulled within 26h
- `pool_healthy: true` — DB connection pool OK
- `llm_available: true` — llama.cpp responding
- `thread_ingestion: true` — background ingestion running

Cycle snapshots stored in `analytical_snapshots` table — query for history.
