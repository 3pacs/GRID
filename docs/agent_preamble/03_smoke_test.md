### 3. Smoke test — `scripts/smoke_endpoints.sh` is the regression gate

A wave is not "done" until this script exits 0:

```bash
bash scripts/smoke_endpoints.sh          # runs on server, ~8 seconds
python3 scripts/deploy.py --smoke <file> # runs the script after the deploy
```

The script tests: sector_map load (3,533 actors), supply_chain, capital_flow (with percentile enrichment), contagion (with scenarios), actor_detail, sector_health, contagion→trade_tickets, explain. Each has its own exit code (1-8) so failures localize instantly.
