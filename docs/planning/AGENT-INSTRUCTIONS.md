# Agent Instructions — Keep the Machine Running

These instructions are for ALL agents working on GRID: Claude Code, Codex, Hermes, Qwen on the server.

---

## For Hermes Operator (on server, 24/7)

You run the intelligence loop. Here's your expanded job:

### Every 4 hours:
1. `trust_scorer.run_trust_cycle(engine)` — score pending signals, update trust scores, detect convergence
2. `options_recommender.generate_recommendations(engine)` — fresh trade recs with 5-layer sanity
3. `cross_reference.run_all_checks(engine, skip_narrative=True)` — check stats vs reality
4. `flow_thesis.generate_unified_thesis(engine)` — build the market thesis
5. `thesis_tracker.snapshot_thesis(engine, thesis)` — archive it

### Every 6 hours:
1. `options_tracker.score_expired_recommendations(engine)` — P&L on expired trades
2. `lever_pullers.identify_lever_pullers(engine)` — who's moving markets
3. `actor_network.track_wealth_migration(engine)` — where's money going
4. `thesis_tracker.score_old_theses(engine)` — was our thesis right?

### Daily 2:00 AM:
1. `source_audit.run_full_audit(engine)` — which sources are most accurate
2. `backtest_scanner.run_full_scan(engine)` — find new trading signals (with LLM sanity)
3. `postmortem.batch_postmortem(engine)` — autopsy every failed trade
4. `options_tracker.run_improvement_cycle(engine)` — adjust scanner weights from outcomes
5. `backtest_scanner.review_existing_hypotheses(engine)` — LLM review of all hypotheses
6. `thesis_tracker.run_thesis_cycle(engine)` — full thesis cycle with post-mortems
7. Run resolver: `Resolver(engine).resolve_pending()` — map raw data to features

### Weekly Sunday 3:00 AM:
1. Full cross-reference with LLM narrative
2. `lever_pullers.generate_lever_report(engine)` — who did what this week
3. `trust_scorer.generate_trust_report(engine)` — who's reliable
4. `actor_network.generate_actor_report(engine)` — power structure changes
5. Email digest to operator with all reports

### On every run:
- Log timing for each task
- If a task fails, log the error but DON'T stop the loop
- Broadcast important events via WebSocket: regime changes, convergence, recommendations
- Push notifications for high-priority alerts

---

## For Qwen 32B (on server, GPU inference)

You are the reasoning engine. Here's what you get asked to do:

### Thesis narratives (every 4h):
- Input: all 10 model states + current data
- Output: 1 paragraph unified market thesis
- Be specific: name tickers, cite numbers, make predictions
- Don't hedge everything — take a position

### Trade recommendation review (every 4h):
- Input: candidate trades with scanner scores + GEX + regime
- Output: PASS/FAIL for each with 1-line reason
- Be harsh — reject anything that smells like noise

### Hypothesis sanity check (daily):
- Input: list of backtest winners
- Output: KEEP/REJECT for each
- Reject: trivially correlated, survivorship bias, wouldn't survive costs

### Post-mortem analysis (daily):
- Input: failed trade + all data at decision time + what actually happened
- Output: root cause + what we should change
- Be specific: "we should have weighted FOMC proximity higher"

### Cross-reference narrative (weekly):
- Input: all divergence checks with z-scores
- Output: "What the data is telling us that the headlines aren't"
- Name names: which governments, which statistics, what implications

### Actor motivation assessment:
- Input: actor + their action + context
- Output: "likely_informed" / "routine" / "hedging" + explanation
- Think about: committee jurisdiction, timing relative to events, pattern history

### Lever-puller report (weekly):
- Input: all lever-puller actions this week
- Output: narrative connecting the dots — who's doing what and why it matters

### Earnings reaction prediction:
- Input: upcoming earnings ticker + IV + historical surprise % + sector context
- Output: predicted direction + magnitude + confidence

### PRIORITIES:
1. Thesis narrative — this is the system's voice
2. Trade review — this prevents bad trades
3. Post-mortems — this is how we learn
4. Everything else

---

## For Next Claude Code Session

### Immediate (server must be restarted first):
1. `sudo systemctl restart grid-api grid-hermes` — activate everything
2. Add swap: `sudo fallocate -l 8G /swapfile && ...`
3. Run first intelligence cycle manually to verify:
   ```python
   from intelligence.trust_scorer import run_trust_cycle
   from intelligence.cross_reference import run_all_checks
   from analysis.flow_thesis import generate_unified_thesis
   ```

### Test every view:
- Walk through all 7 world view tabs with real data
- Test Ask GRID chat with real questions
- Test watchlist flow end-to-end
- Test command palette (Cmd+K)
- Test mobile responsiveness

### Fix what's broken:
- Any D3 viz that crashes on empty data → add EmptyState
- Any endpoint that returns 500 → add error handling
- Any view that shows "Building..." → replace stub with real component

### Data pipeline:
- Run resolver to populate new features
- Trigger each new puller manually once to verify
- Check which alt data sources need API keys

### Performance:
- watchlist.py is 1400+ lines — split into modules
- intelligence router is huge — split by domain
- Add Redis caching if memory caches aren't enough

### More GPU:
- If adding GPUs, consider:
  - Dedicated GPU for Qwen inference (don't share with TAO mining)
  - Second GPU for embeddings (Hyperspace)
  - Or: bigger model (Qwen 72B) if single GPU has enough VRAM

---

## For Codex Agent (AstroGrid)

Check .coordination.md before touching any files.
Your branch: `codex/astrogrid-dedup`
Your domain: `astrogrid/`, `astrogrid_web/`, `astrogrid_shared/`

When ready to merge:
1. Update .coordination.md
2. Create PR targeting main
3. Claude Code will review and merge

---

## Principles for ALL Agents

1. **Every data point has a confidence label**: confirmed/derived/estimated/rumored/inferred
2. **Every failed prediction gets a post-mortem**: what went wrong, why, what to change
3. **Every thesis gets archived and scored**: we track if we're getting better
4. **Trust scores are earned, not assigned**: accuracy over time determines weight
5. **The system improves itself**: scanner weights, thesis models, source priorities all evolve
6. **Never present estimates as facts**: LLM guesses are clearly labeled
7. **Check .coordination.md before every commit**: don't step on other agents
8. **Graceful degradation**: if an LLM/API/DB is down, continue with what you have
