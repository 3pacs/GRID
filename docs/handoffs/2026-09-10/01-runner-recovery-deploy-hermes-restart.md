# 01 — Land the merged fixes on grid-svr and prove the error log went quiet

Branch: `claude/handoff-01-hermes-land` (only needed if you have to fix something).
Lane: ops-exec / gemini-task. Depends on the runner being alive (see 00-COMMON).

## Goal

PRs #411 (Hermes repairs) and #412 (Robinhood connector) are merged to main
(commits 06a89a6, d1bbfa9). Deploy runs #565 and #566 passed pre-deploy
verification and are queued for the grid-svr runner. Make sure they land, bring
the Hermes tree and its daemons onto the same code, and verify each repaired
error pattern is gone.

## Status update — 20:40 UTC 2026-09-10 (supersedes step 1)

The runner was restarted by the operator and is claiming jobs again. Deploy runs
#565 (06a89a6) and #566 (d1bbfa9) **failed as designed**: they sat queued for
40 minutes while main advanced, so the release tree fetched main, landed on
`ae003d2`, and the `deploy.yml` guard rejected the mismatch against each run's
own commit. Ignore both. **Deploy #567 (run 34527331707, commit `ae003d2` =
current main) is the one that matters** — it carries #411, #412 and #413
together. Start at step 2.

## Steps

1. Check the runner: `actions_list list_workflow_runs deploy.yml`. If the Deploy
   job is still "queued" the runner is stalled — tell the operator to run
   `sudo systemctl restart actions.runner.3pacs-GRID.grid-svr` and wait. Do not
   re-dispatch jobs into a dead queue.
2. When the deploy for current main is green (its verify step checks health, PWA
   assets and three OpenAPI routes), run an ops-exec that merges the Hermes tree from the release
   tree (command in 00-COMMON) and restarts:
   `grid-hermes grid-extractor grid-worker grid-coordinator grid-scheduler grid-intelligence`.
   Print `git log --oneline -1` of both trees and `systemctl is-active` of each unit.
3. After at least two Hermes cycles (10 min) and one trust cycle (the trust cycle
   runs every 4 h — trigger one instead:
   `PYTHONPATH=. python3 -c "from db import get_engine; from intelligence.trust_scorer import run_trust_cycle; print(run_trust_cycle(get_engine())['scoring'])"`
   in the Hermes tree, detached), verify:
   - journal: `journalctl -u grid-hermes --since '<restart time>' -o cat | grep -c 'Failed download'` is near zero
     (was 11,109/day) and the scoring summary shows `skipped_unpriceable` and `tickers dead this cycle`.
   - `journalctl -u grid-extractor --since '<restart>' -o cat | grep -c QueryCanceled` is 0.
   - `journalctl -u grid-worker --since '<restart>' -o cat | grep -c 'Failed to report'` is 0 and
     the coordinator log shows `already recorded` acks instead of 400s if any retry happens.
   - `python3 scripts/audit_error_log.py --hours 2 --top 20` shows none of:
     `run_claimed_job`, `_compute_btp_bund_spread UniqueViolation`, `run_extractor QueryCanceled`,
     `build_redundancy_map`, `BLS daily threshold`, `IMF IFS pull failed`.
4. Check `GET /api/v1/trading/robinhood/status` through the API (bearer token
   from the login flow; never print the master password) returns
   `mode: UNCONFIGURED` — the connector is deployed and waiting for keys.
5. Report counts before/after in the PR-less agent report (this hand-off has no
   code unless something regressed). If something regressed, fix it on your
   branch with a test and a PR.

## Known follow-ups you will see but must not chase here

Dead FRED ids (hand-off 02), options tapes scoring zero hits (03), the CPU-only
Qwen unit on :8080 (04), insider "Unknown motivation" narratives (05).
