# Next Session Priorities

**Last session:** 2026-03-28 (55 commits, massive build night)
**Tests:** 594 passed, 0 failed, 10 skipped
**Modules:** 22/22 new modules import clean
**Data coverage:** FX 88%, Macro 67%, everything else 85%+

---

## MUST DO (server)

1. `sudo systemctl restart grid-api` — pick up all new backend endpoints
2. Add swap to prevent OOM:
   ```
   sudo fallocate -l 8G /swapfile && sudo chmod 600 /swapfile && sudo mkswap /swapfile && sudo swapon /swapfile
   echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
   ```
3. Check TAO miner: `journalctl -u grid-tao-miner -n 50`
4. Rotate GitHub PAT (old one was exposed in git remote URLs)
5. Generate VAPID keys for push notifications

## HIGH PRIORITY — Fix & Polish

### Data gaps to close
- **systemic** 0/3 — OFR endpoint was fixed but data needs re-pull
- **alternative** 2/35 (6%) — many new features registered but no data flowing yet
- **trade** 1/3 (33%) — Comtrade data exists in raw_series, needs resolver mapping
- **rates** 25/42 (60%) — new features registered, need entity_map entries
- Run resolver after deploying

### Frontend testing
- Walk through EVERY view on mobile and desktop
- Test the 7 world view tabs
- Test Ask GRID chat
- Test watchlist full flow: add, search, delete, click detail, back
- Test push notifications (need VAPID keys first)

### Integration gaps
- Hermes operator needs restart to pick up new intelligence tasks
- Options recommender needs first live run
- Trust scorer needs first cycle
- Cross-reference needs first full run with data

## MEDIUM PRIORITY — Next Features

### Phase 3 Options Edge
- First live recommendation generation
- Monitor outcomes, run improvement cycle after 1 week

### Intelligence refinement
- Actor network: more connections from real 13F data
- Trust scorer: first scoring cycle for baselines
- Cross-reference: historical backfill for lies ledger
- Lever-puller convergence: needs enough signal_sources rows

### UI polish
- Test all D3 visualizations with real data
- GEX profile needs options_snapshots data
- Globe needs Comtrade bilateral data for real trade flows
- Risk treemap needs historical risk scores for timeline

## LOW PRIORITY — Future
- Extract reusable viz components (DivergenceMatrix, ComparisonOverlay)
- API rate limiting + documentation for external consumers
- Performance profiling (watchlist.py is large — consider splitting)
- Merge Codex AstroGrid de-dup branch
