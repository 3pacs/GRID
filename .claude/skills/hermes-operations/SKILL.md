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
1. Health check (DB, data freshness, LLM availability)
2. Pull fixer (retry failed ingestion with diagnosis)
3. Pipeline runner (full pipeline on 6h schedule)
4. Data gatherer (fill historical gaps)
5. Autoresearch (hypothesis generation when healthy)
6. Self-diagnostics (read own error logs, propose fixes)
7. Specialized tasks:
   7a-7d. Scheduled pullers (FRED, yfinance, BLS, altdata)
   7e.    Alpha research heartbeat + signal publishing
   7f.    Intelligence modules (trust scoring, forensics, etc.)
   7g.    Rotation paper trading (daily 17:00-17:30 UTC)
   7h.    Tiingo bulk data pull (overnight 02:00-06:00 UTC)
8. Git push (commit analytical outputs)
9. Save cycle snapshot
```

## Key Files

| File | Purpose |
|------|---------|
| `scripts/hermes_operator.py` | Main daemon (~1800 lines) |
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
