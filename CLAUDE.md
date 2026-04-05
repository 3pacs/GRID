# GRID Trial Gem Hunter — AutoAgent Branch

## What This Is

This is a branch of **GRID** (Universal Market Intelligence Engine) that adds
clinical trial data as an orthogonal signal domain for biotech/pharma equity prediction.

The agent autonomously discovers underpriced small-cap biotech equities where
an imminent clinical trial readout hasn't been priced in by the market.

Called "the truffle pig" — it sniffs out gems before the market smells them.

## Architecture Position in GRID

```
GRID Signal Domains (orthogonal by design):
├── Macro        (FRED, M2, Fed balance sheet)
├── Energy       (EIA, crude, natgas)
├── Sentiment    (NewsAPI, GDELT, OSINT)
├── Momentum     (price/volume, Alpha Vantage)
└── Trial Gems   ← THIS MODULE
    ├── grid/ingestors/trial_ingestor.py   (cron job, 6am daily)
    ├── grid/signals/trial_signal.py       (signal generator)
    └── tasks/trial-gem-hunter/            (AutoAgent self-improvement harness)
```

## GRID Infrastructure (grid-svr)

- **Server**: Supermicro X9DR3-F, 10.254.111.80 / Tailscale 100.75.185.36
- **DB**: PostgreSQL 14, `griddb`, credentials `grid/grid2026`
- **Repo**: `~/grid_v4/grid_repo` (branch `claude/grid-foundation-build-rjlxI`)
- **Cron**: 8 existing jobs; add trial_ingestor as job #9

## New DB Tables

```sql
trial_signals      -- agent output, scored picks
trial_cache        -- raw CT.gov JSON, 24h TTL
catalyst_calendar  -- upcoming readout dates
```

Views:
- `trial_gems`             — active BUY signals in favorable regimes
- `trial_signal_performance` — historical hit rate by regime/indication
- `upcoming_catalysts`     — next 180 days of readout events

## Signal Logic

1. Fetch Phase 2/3 trials where enrollment is complete (ACTIVE_NOT_RECRUITING)
2. Filter: readout within 30–180 days, industry sponsor, market cap < $2B
3. Score: endpoint clarity × phase × disease priority × enrollment × FDA flags
4. Penalize: slow enrollment, high short interest, terminated trials
5. **Regime gate**: only BUY in GROWTH/NEUTRAL (reads `regime_states` table)
6. Write to `trial_signals`, surface via `trial_gems` view

## AutoAgent Self-Improvement Loop

The AutoAgent harness (`tasks/trial-gem-hunter/`) hill-climbs on:
- **Score**: mean 30-day forward return of top-5 picks (0.0–1.0)
- **Agent edits**: screening thresholds, feature weights, disease priorities
- **Regime awareness**: correct WATCHLIST-only behavior in FRAGILE/CRISIS

Run a single task:
```bash
cd ~/grid_v4/grid_repo
uv run harbor run -p tasks/ --task-name trial-gem-hunter -l 1 -n 1 \
  --agent-import-path agent:AutoAgent -o jobs --job-name latest
```

Run parallel (10 workers):
```bash
uv run harbor run -p tasks/ -n 10 --agent-import-path agent:AutoAgent \
  -o jobs --job-name trial-gems-$(date +%Y%m%d)
```

## Setup (one-time on grid-svr)

```bash
# 1. Apply DB migration
psql -U grid -d griddb -f grid/scripts/migrations/add_trial_signals.sql

# 2. Run ingestor manually to seed data
python -m grid.ingestors.trial_ingestor

# 3. Add cron job
echo "0 6 * * * cd ~/grid_v4/grid_repo && python -m grid.ingestors.trial_ingestor >> /var/log/grid/trial_ingestor.log 2>&1" | crontab -

# 4. Test signal generator
python -m grid.signals.trial_signal --output table

# 5. Write to DB
python -m grid.signals.trial_signal --output db
```

## Key Files

| File | Purpose |
|------|---------|
| `grid/signals/trial_signal.py` | Main signal class — import into GRID pipeline |
| `grid/ingestors/trial_ingestor.py` | Daily CT.gov ingestor |
| `grid/scripts/migrations/add_trial_signals.sql` | DB schema |
| `tasks/trial-gem-hunter/task.toml` | AutoAgent task config |
| `tasks/trial-gem-hunter/instruction.md` | Agent prompt (the truffle nose) |
| `tasks/trial-gem-hunter/tests/test.py` | Scoring harness |
| `tasks/trial-gem-hunter/environment/Dockerfile` | Task container |

## Scoring Notes

- `trial_strength_score` ≥ 0.65 + GROWTH regime → BUY signal
- `trial_strength_score` ≥ 0.40 + any regime → WATCHLIST
- FRAGILE/CRISIS → never BUY, WATCHLIST only above 0.70
- Position sizing: Kelly-inspired, max 5% per trial bet
- Orthogonality bonus: novel tickers not in existing GRID features get +0.05

## Documented Edge Cases

- **FDA Fast Track** designation is the single strongest multiplier (1.0 score on designation axis)
- **Single-drug company** + Phase 3 + oncology = maximum move potential (historically)
- **Enrollment completed early** (actual > target) = trial going better than expected, market doesn't know
- **Why Stopped field populated** = immediate disqualify, hard filter
- **Results already posted** = too late, penalty ×0.1

## Failure Modes to Avoid

- Do NOT optimize for enrollment_pct alone — a trial at 100% for 2 years is dead
- Do NOT surface FRAGILE/CRISIS regime picks as BUY — regime gate is non-negotiable
- Do NOT rely on title keyword matching for FDA designation — check EDGAR filings too
- Treat composite endpoints with skepticism (×0.7 vs binary)
