# TODO — Next Session & Ongoing

## IMMEDIATE (before anything else)

```bash
# On server:
sudo systemctl restart grid-api grid-hermes
sudo fallocate -l 8G /swapfile && sudo chmod 600 /swapfile && sudo mkswap /swapfile && sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab

# Verify Qwen is running:
curl -s http://localhost:8080/health

# Verify API:
curl -s http://localhost:8000/api/v1/system/health

# Run first intelligence cycle:
cd ~/grid_v4/grid_repo
~/grid_v4/venv/bin/python3 -c "
from intelligence.trust_scorer import run_trust_cycle
from intelligence.cross_reference import run_all_checks
from analysis.flow_thesis import generate_unified_thesis
from intelligence.thesis_tracker import snapshot_thesis
from sqlalchemy import create_engine
engine = create_engine('postgresql://grid:gridmaster2026@localhost:5432/griddb')
print(run_trust_cycle(engine))
print(run_all_checks(engine))
thesis = generate_unified_thesis(engine)
print(thesis['overall_direction'], thesis['conviction'])
snapshot_thesis(engine, thesis)
"

# Check LLM task queue is running:
curl -s http://localhost:8000/api/v1/system/llm-status
```

## DATA GAPS TO CLOSE

| Family | Current | Target | Action |
|--------|---------|--------|--------|
| systemic | 0% | 80%+ | Re-pull OFR data (endpoint fixed), run resolver |
| alternative | 6% | 50%+ | New alt features registered but need ingestion runs |
| trade | 33% | 80%+ | Comtrade data exists, needs resolver mapping |
| rates | 60% | 85%+ | New features registered, need entity_map entries |
| macro | 67% | 85%+ | Run resolver with new mappings |

## FRONTEND TESTING CHECKLIST

- [ ] 7 world view tabs all load
- [ ] Ask GRID chat responds
- [ ] Command palette (Cmd+K) works
- [ ] Watchlist: add (search), delete (swipe + X), click → detail
- [ ] Detail page: AI overview, price chart, options intel, GEX, insider edge
- [ ] Earnings calendar loads
- [ ] Correlation matrix renders
- [ ] Strategies view shows paper trading data
- [ ] Predictions scoreboard loads
- [ ] Settings: service status, API keys, hermes schedule
- [ ] Pipeline health: source table, coverage heatmap
- [ ] Mobile: bottom tabs, swipe delete, pull refresh, collapsible sections
- [ ] Push notifications (need VAPID keys)

## QWEN 24/7 WORK PRIORITIES

The LLM task queue keeps Qwen busy. Priorities:

### P1 — Never wait:
- Trade rec sanity reviews
- User chat (Ask GRID)
- Convergence alert narratives
- Regime change explanations

### P2 — Hermes cycle:
- Thesis narratives (every 4h)
- Cross-reference narratives (weekly)
- Post-mortem analysis (daily)
- Hypothesis review (daily)
- Earnings reaction predictions

### P3 — Background (never idle):
- Investigate sleuth leads (follow rabbit holes)
- Write market diary entries
- Generate feature interpretations
- Research actor profiles
- Detect anomalies and explain them
- Discover new correlations
- Generate new hypotheses
- Refine active predictions
- Summarize news headlines
- Assess actor motivations

## ITERATION GOALS

### Week 1: Validate
- Run all intelligence cycles for a full week
- Score thesis accuracy daily
- Score trade recommendation outcomes
- Build trust score baselines
- Fill data gaps

### Week 2: Improve
- Run improvement cycle on scanner weights
- Post-mortem every failed trade
- Identify which models are working, which aren't
- Adjust thesis model weights from outcomes
- Add more actors to the network from 13F data

### Week 3: Scale
- If Qwen is bottlenecked, add GPU or switch to 72B model
- If data gaps persist, add more ingestion sources
- If thesis accuracy < 60%, investigate why
- If UI is slow, add Redis caching layer

### Week 4: Monetize
- Document API for external consumers
- Add rate limiting + API key management
- Create pricing tiers
- Launch beta to 5 users

## AGENT IMPROVEMENTS (iterate until better than humans)

### Trust scoring:
- Current: Bayesian with recency decay
- Next: add Kelly-weighted sizing (trusted sources get more capital allocation)
- Next: add network effects (if trusted source A is connected to B, B gets a trust boost)

### Sleuth:
- Current: generates leads and investigates with LLM
- Next: auto-follow up on unresolved leads weekly
- Next: cross-reference leads with each other (lead A and lead B might be related)
- Next: generate "investigation reports" that connect multiple leads into a narrative

### Thesis:
- Current: 10 models combined by confidence weight
- Next: adaptive weights from accuracy tracking (monthly rebalance)
- Next: regime-conditional weights (some models work better in GROWTH vs CRISIS)
- Next: add new models as we discover them

### Trade recommender:
- Current: scanner + GEX + 5-layer sanity
- Next: incorporate trust scorer convergence as a 6th sanity layer
- Next: add earnings proximity check (don't trade into earnings unless we have edge)
- Next: add regime-conditional strike selection (different gamma behavior in each regime)

### Cross-reference:
- Current: government stats vs physical reality
- Next: add company-level cross-reference (earnings vs capex vs hiring vs revenue guidance)
- Next: add analyst consensus vs insider actions (when analysts say buy but insiders sell = red flag)
- Next: track lies over time and score which governments/companies are most dishonest
