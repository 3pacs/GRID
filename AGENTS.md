# AGENTS.md

Append-only running log for GRID system work.

## 2026-09-11 - correction: the `GitHubActions-WSL-Start` task exists (supersedes the boot-contract claim below)

What was done:

- Corrected `docs/SERVER-SERVICES.md` "Alien Runner": the Windows scheduled task `GitHubActions-WSL-Start` **does exist** on alien. The entry below, and the doc text it shipped, said it was absent and told the operator to re-register it. Both were wrong, and following that guidance would have duplicated or overwritten a working task.
- Root cause of the false negative: `ssh alien 'schtasks /query /tn ...'` runs under Git Bash, where MSYS rewrites `/query` into `C:/Program Files/Git/query`; the resulting argument error reads like "task not found". The correct probe is now documented: `ssh alien 'MSYS_NO_PATHCONV=1 schtasks /query /tn GitHubActions-WSL-Start /v /fo LIST'`, or PowerShell `Get-ScheduledTaskInfo`.
- Documented the task's actual failure mode and the operator fix. It has only boot and logon triggers with `RestartOnFailure` Count 5 / Interval PT1M; it ran on 2026-09-02, exited 1, exhausted its five retries, and — with Windows `LastBootUpTime` at 2026-08-19 — nothing re-fired it for 22 days, which is why all 19 runners were down. The fix is `Set-ScheduledTask` adding a 15-minute repetition trigger to the existing task (safe: `MultipleInstancesPolicy` is already `IgnoreNew`), plus enabling `Microsoft-Windows-TaskScheduler/Operational`, which is off and is why the 2026-09-02 failure left no record.
- Reworded the `TEST_RUNNER` warning to match reality (gate on the distro being `Running` and the runner `online`, not on the task existing) and fixed the same MSYS bug in the Health / recovery probe.

Non-obvious decisions:

- Added as a new entry rather than editing the 2026-09-11 entry below, which stays as written — this log is append-only, and the wrong claim is worth leaving visible next to its correction.
- `schtasks /run` on the existing task was used to bring the distro up. That is an ops start of operator-authored infrastructure, not persistence creation; no task was created, modified, or enabled by an agent.

Broken or TBD:

- The repetition trigger still needs an operator's elevated PowerShell on alien. Until it lands, one transient failure can take CI down repo-wide again for as long as the box stays up.
- Vault `05-GRID/Infrastructure/Servers.md` still carries the same "absent" claim.

Next pick-up:

- Operator: run the `Set-ScheduledTask` snippet in `docs/SERVER-SERVICES.md`, then confirm `wsl.exe -l -v` reports `Running` after a deliberate `schtasks /end`.
- Full investigation log: `Sessions/S-2026-09-11-opus-alien-runner-followup.md` in the Obsidian vault.

## 2026-09-11 - alien self-hosted CI runner for GRID activated (handoff-09), TEST_RUNNER left unset pending WSL keep-alive

What was done:

- Registered runner `alien` (labels `self-hosted, alien, tests`) for 3pacs/GRID inside the Dell's `GitHubActions` WSL2 distro: `/opt/github-actions/GRID/alien`, unit `actions.runner.3pacs-GRID.alien.service` (drop-in: egress guard, `user@1000`, rootless `DOCKER_HOST`, `TZ=UTC`). Installed PostgreSQL 15.19 + TimescaleDB 2.28.3 as a persistent service, `grid/testpass/griddb_test`, cluster + DB pinned to UTC; Python 3.11 (deadsnakes), build-essential, libpq-dev.
- Verified on alien via PR #450: Frontend Build 1m26s green, Lint 1m56s green, Backend Tests ran all 8,061 tests in 14m12s with `Reset persistent Postgres (alien)` executed and the ephemeral-Postgres step skipped (ON_ALIEN true). Fallback verified: deleting `TEST_RUNNER` and re-running sends the jobs back to `ubuntu-latest` (run 34544223388, green).
- Fixed two alien-only failures: `test_regime_history_writer` off-by-one day (Postgres was America/Los_Angeles; now UTC) and `TestGauntlet::test_run_gauntlet` needing ~90 s on the E5-2698 v3 (`test.yml`: pytest `--timeout` 60 -> 180, backend `timeout-minutes` 15 -> 25).
- Rewrote the `docs/SERVER-SERVICES.md` "Alien Runner" section with the real install path (WSL2 layout, pgdg/Timescale repos and package names, missing `svc.sh` in the 2.337.0 tarball, MSYS path-conversion gotcha, UTC pin, measured timings).

Non-obvious decisions:

- **`TEST_RUNNER` is deliberately unset.** The documented boot contract (Windows scheduled task `GitHubActions-WSL-Start`) does not exist on alien, so the distro idles out ~1 min after the last `wsl.exe` session and every Linux runner on the box goes offline (all 19 siblings were offline on 2026-09-10 for this reason). With the variable set that blocks CI repo-wide and cancels foreign PRs' jobs mid-run. Creating the task needs an operator's elevated PowerShell on alien; the exact snippet is in SERVER-SERVICES.md. Flip `gh variable set TEST_RUNNER --body alien -R 3pacs/GRID` only after `wsl.exe -l -v` says Running unattended.
- Did not replace the distro's system Node 22 with Node 20 (shared with sibling runners); `actions/setup-node`/`setup-python` provision 20 / 3.11 into the runner tool cache.
- deploy.yml, ops-exec.yml, gemini-task.yml, ops-bringup.yml untouched; alien has no deploy credentials.

Broken or TBD:

- Backend Tests on alien is ~2x slower than hosted (single-process pytest on a 2014 Xeon). Needs `pytest-xdist` with per-worker DB isolation to meet the handoff's 4-minute bar.
- A second consecutive alien run with warm cache was not completed because the distro cannot be kept alive without the scheduled task.

Next pick-up:

- Operator: create `GitHubActions-WSL-Start` on alien (snippet in SERVER-SERVICES.md), confirm `wsl.exe -l -v` Running and all runners online, then set `TEST_RUNNER=alien` and watch the next two PRs' test jobs.

## 2026-05-30 - stepdad.finance: natural-language home composer (Phase 1, LIVE)

What was done:

- Rebuilt the PWA home page (`pwa/src/views/Home.jsx`, lazy-loaded as route `home` via `app.jsx`) into **stepdad.finance** — a plain-language composer for the operator's dad. Type a request → it assembles a live dashboard. Brand is "stepdad.finance", not GRID.
- New backend `POST /api/v1/chat/compose` (in `api/routers/chat.py`): planner LLM maps NL → a layout `{spoken_reply, widgets[], allocation[]}` against a fixed 6-widget catalog (verdict, ticker_pulse, watchlist, macro_regime, news, money_flow). Validates/coerces output, always returns ≥1 widget (verdict fallback) so the page is never blank. Smoke-tested: "Show me Apple and Tesla, what's gold doing, should I worry" → 3 ticker_pulse (AAPL/TSLA/GLD, gold→GLD mapped) + verdict, 12s.
- New fast endpoint `GET /api/v1/watchlist/{ticker}/quote` (in `api/routers/watchlist_overview.py`): LLM-free price/options snapshot (DB reads + rule-based sentiment). **Replaces the 27s `/overview` for the pulse tile → 0.5s cold / ~15ms warm.**
- Frontend: new `pwa/src/components/home/widgets.jsx` (6 self-fetching cards + registry + WidgetGrid) and `answerFormat.jsx` (parses VERDICT/WHY/CONFLICTS/ACTION and renders the verdict as a real headline — fixes the old `formatAnswer` that buried the verdict in tiny gray monospace). Added `api.compose()` + `api.getTickerQuote()`.

Non-obvious decisions:

- **Two live code trees.** grid-api (`:8000`, serves the PWA) runs from `/data/grid_v4/grid-api-main` (detached, 365 commits behind `main`, 37 uncommitted local patches), NOT from `/home/grid/grid_v4/grid_repo` (main) where dev happens. `scripts/deploy.py` dual-writes to `grid_repo` + `astrogrid_dedup` — **neither is the live grid-api tree**, so the deploy tool is misconfigured. Per operator's call, did a **surgical copy**: applied only the new/changed files to `grid-api-main` (backups in `/tmp/sd_deploy_backup_20260530T084415Z`), rebuilt its `pwa_dist`, restarted. Did NOT pull main or touch the 37 patches.
- money_flow→`/flows/sectors` is 40s cold but 20ms warm (876KB, daemon-cached). Kept as-is.

### UPDATE — streaming verdict shipped; capacity concern was a MISDIAGNOSIS

- Added **`POST /api/v1/chat/ask/stream`** (SSE) in `api/routers/chat.py` + `api.askStream()` + VerdictCard now renders tokens live with a caret. Deployed both trees, verified: tokens flow immediately ("1. VERDICT: No, the market is not risk-on…").
- **CORRECTION to my earlier "verdict = 57s on the contended grid-svr 3090 :8081" claim — that was WRONG.** `_get_llm_client()` (chat.py) resolves `Tier.ORACLE` to **`http://gridz4:8080`** running **Qwen3.6-35B-A3B (Opus-distill)** — the bigger brain, NOT grid-svr's 27B. The `"qwen3.6-27b"` backend label is a stale hardcoded string. gridz4 is NOT saturated and streams instantly. The `:8081` saturation I chased (both slots busy, hermes) is a DIFFERENT server irrelevant to the chat/verdict path. **No capacity change needed.** The one 80s hang during testing was a transient (gridz4 mid-generation right after a restart).
- Net: verdict now streams live from the 35B; the streaming A/B-Opus + firewall are skipped on this path (private tile, not published).

Broken or TBD:

- **Favicon/mascot blocked**: operator's `stepdad.png` is on his Mac (`/Volumes/...`), unreachable from grid-svr. Needs scp to the server, then wire as favicon + empty-state mascot.
- `/quote` prices are `as_of 2026-05-15` (DB cache ~2wks stale) and `change_pct` is null. Data-freshness, not lag.
- Voice input (local Whisper `/transcribe`) = Phase 2. Profiles (named layout+allocation, hundreds, arena battle via existing `trading.py` wallets) = Phase 2/3.

Next pick-up:

- Get `stepdad.png` onto the server → favicon + mascot. Then Phase 2: profiles + Whisper voice. TradingView/thinkorswim-via-Playwright is a candidate enrichment data source (operator wants the "stealthier" one) — wire behind GRID feeds, not primary.
- Login `dad` / `mom` (contributor, `grid_users` id 14) created and verified against live API.

**Note (2026-09-10):** The "two live code trees" / "deploy tool is misconfigured" situation above is superseded. Starting with the first `.github/workflows/deploy.yml` commit (2026-06-15), every push to `main` runs an automated deploy: a `verify` job, then a `deploy` job on the self-hosted grid-svr runner that resets `/data/grid_v4/grid_release` to `main`, builds the PWA, runs migrations, and points grid-api's systemd `WorkingDirectory` at that tree via a drop-in. `grid-api` now runs from `/data/grid_v4/grid_release`, kept in sync with `main` automatically — not from `grid-api-main`, and not manually via `scripts/deploy.py`. See `docs/SERVER-SERVICES.md` for the current topology.

## 2026-05-29 - Oracle llama-server "inference hang" = queue saturation death spiral

What was done:

- Diagnosed the grid-svr oracle (:8081, Qwen3.6-27B) "inference hangs / times out with empty body" report. /health and /v1/models were fine and fast; only inference timed out. Root cause was NOT hardware, chat-template, or mmproj — it was queue saturation on the single inference slot.
- Found `scripts/hermes_operator.py` (PID, ~13h uptime) holding ~116 connections to :8081 (76 ESTAB, 36 CLOSE-WAIT, 4 FIN-WAIT-2). With `--parallel 1` the server serves one request at a time; new requests (incl. diagnostic curls) queued past their timeout → looked like a hang. Throughput was ~25 tok/s with prompt-processing time ≈ generation time (SWA prompt-cache invalidation reprocesses every prompt full).
- Immediate unjam: `systemctl restart grid-llamacpp-oracle` (drains queue) + `grid-hermes` (drops leaked conns). Inference recovered: /completion 90s-timeout → 0.3s.
- Code/config fixes: `.env` + `config.py` `LLAMACPP_ORACLE_NUM_PREDICT 15000→6000`, `MIN_NUM_PREDICT 15000→0`. Oracle drop-in `/etc/systemd/.../big-ctx.conf` `LLAMACPP_PARALLEL 1→2`. Verified: 2 slots, a quick request returns in 0.3s while a 2000-tok gen runs on the other slot; conns to :8081 back to 0.

Non-obvious decisions:

- The real engine of the spiral: `min_num_predict=15000` forced EVERY oracle call (even tiny ones) up to 15000 tokens via `max(num_predict, min_num_predict)` in `llamacpp/client.py`. At ~27 tok/s that's ~555s — but `LLAMACPP_ORACLE_TIMEOUT_SECONDS=300`. So full-length calls ALWAYS orphaned mid-generation while the server kept generating, holding the one slot ~555s each. The 2026-05-07 "timeout < cycle" fix didn't help because per-CALL gen time exceeded the per-CALL HTTP timeout. Capped to 6000 tok (~220s) to fit inside 300s; dropped the forced floor so short tasks finish fast.
- `--parallel 2` is defense-in-depth: `--ctx-size 32768` is split across slots (16384 each), ample for ~1500-tok prompts + 6000-tok gen. VRAM safe (3090 ~11GB free, Blackwell ~4.5GB free after).

Broken or TBD:

- Prompt-cache reuse does not work for this model (every request logs "forcing full prompt re-processing due to lack of cache data"). TESTED `--swa-full` (via `LLAMA_ARG_SWA_FULL=1` in the drop-in) — it does NOT help and was reverted: the model reports `n_swa = 0` (no sliding-window layers), so the cause is the "hybrid/recurrent memory" path [[llama.cpp]] can't reuse, not SWA. MEASURED not worth chasing: over 589 real calls (2h), median prompt=1156 tok, median gen=1289 tok; clean bench = 790 tok/s prompt vs 25.7 tok/s gen. So a typical call is ~1.5s reprocess + ~50s generation = prompt reprocess is only **~3% of wall-clock** even though 93.5% of calls force-reprocess. The real bottleneck is GENERATION throughput (25.7 tok/s), not the cache. (Exception: rare large-context calls — one 208K-tok prompt seen, truncated to 32K ctx = ~41s reprocess; only matters for the long-prompt tail, p90 prompt is still 1156.)
- `_run_with_timeout` in hermes_operator still abandons worker threads on timeout (orphan keeps its HTTP request open). The token cap makes orphans rare now, but the cleaner fix is cooperative cancellation / closing the session on timeout.

Next pick-up:

- If saturation recurs: `ss -tn | grep :8081 | awk '{print $1}' | sort | uniq -c` (watch for CLOSE-WAIT pile-up) and `curl -s localhost:8081/slots`.
- DONE (2026-05-29): consolidated the oracle onto the 3090 alone (`LLAMACPP_DEVICE=CUDA0` in the drop-in; was `CUDA0,CUDA1`). The 27B Q4 + mmproj + full 32K KV (parallel 2) fits in 19.8GB with ~4.3GB headroom. **Generation 19.36 → 39.5 tok/s = 2.04x**; typical call ~68s → ~34s. The cross-GPU layer split onto the slower Blackwell RTX PRO 2000 was halving throughput (token gen is bandwidth-bound; the 3090 alone wins, and PCIe sync per token hurt). Prompt processing dropped 790→658 tok/s (negligible, ~5% of a call). Bonus: freed ~7GB on the Blackwell (now 10.8GB free) for other models. Concurrency (2 slots) stable, no OOM. Rollback = set `LLAMACPP_DEVICE=CUDA0,CUDA1` and `daemon-reload && restart`.
- Speculative decoding: NOT FEASIBLE on current setup. The Qwen3.6 family ships only 27B (dense, arch `qwen35`) + 35B-A3B (MoE), both with a 248320-token vocab — no small vocab-compatible draft exists. And this [[llama.cpp]] build (v500, 2026-05-05) has no MTP head support (only `--spec-draft-*` for a separate draft model). Future path: newer llama.cpp build + `unsloth/Qwen3.6-27B-MTP-GGUF` for self-speculation (~1.5-2x), but that's a rebuild affecting all servers.
- Think-trimming (`enable_thinking=false` / `chat_template_kwargs`): WORKS technically (~25%+ fewer tokens, 34.8→25.7s) but REJECTED — strips the reasoning chain. User directive: quality over speed every time. Oracle keeps thinking ON. See [[feedback-quality-over-speed]].
- Minor: oracle restarts take ~90s to stop (llama-server ignores SIGTERM until the stop-timeout SIGKILL). Could add `TimeoutStopSec=15` + `KillSignal=SIGINT` to the unit.
- [[llama.cpp]] main-push from this clone is broken: local main is ~317 commits ahead (stranded hermes "analytical outputs") / ~10 behind origin. My oracle fix was cherry-picked straight onto origin/main (0c810ef7) to avoid the backlog. Untangling the divergence is a separate task.

## 2026-05-09 - Cluster reshape + LLM routing overhaul

What was done:

- Rebuilt gridz4 Blackwell (RTX PRO 4000 24GB) with CUDA [[llama.cpp]] (sm_120). Deployed Qwen3.6-35B-A3B Claude-Opus-Reasoning-Distilled at ~145 tok/sec. Promoted gridz4 to ORACLE primary in `llm/router.py` and updated `LLM_ORACLE_PROVIDER=llamacpp_z4` in `.env`. Updated systemd unit `/etc/systemd/system/z4-llama.service` to source `LLAMA_BIN` from `/etc/default/z4-llama`; kept Vulkan binary as rollback.
- Upgraded panda [[Ollama]]: `qwen2.5:32b` → `qwen3.6:27b-q4_K_M`. Purged old qwen2.5 models (38GB freed; /home: 13G → 31G free). GPU 0 already pinned.
- Expanded ocr-node (2× 8GB Ampere): set up `anikd` SSH user with key auth, grew LVM disk from 100GB → 462GB using `lvextend +100%FREE`, wiped SATA drive and mounted as `/scratch` ext4 (110GB), successfully pulled `gemma3:12b-it-q4_K_M` after restart.
- Provisioned koala (2× GTX TITAN X Maxwell 12GB sm_5.2): Card 0 = [[Ollama]] with `gemma3:12b-it-q4_K_M` + `nomic-embed-text`; Card 1 = Whisper service (whisper.cpp, :8092) + Kokoro TTS (CPU, :8091). Round-trip voice test passed.
- Demoted grid-svr ORACLE to fallback. Wired new providers (`ollama_panda`, `ollama_ocr`, `ollama_koala`) into `llm/router.py` with defaults in `config.py`. Restarted all services (grid-hermes, grid-intelligence, grid-extractor, grid-api) to pick up new environment. PR #104 (+106 lines).

Non-obvious decisions:

- Promoted gridz4 as ORACLE primary despite being a remote node: Blackwell + 35B-A3B distill outperform grid-svr's Pascal + 27B at 3–5× speed with comparable output quality, justifying the latency trade-off for long-horizon reasoning tasks.
- Kept Vulkan [[llama.cpp]] binary as rollback instead of deletion in case future CUDA auto-updates cause issues.
- Pinned koala Card 1 to CPU for both Whisper and Kokoro rather than splitting GPU: Whisper.cpp Vulkan is lightweight, and TTS can comfortably run CPU; reserves GPU for future workloads.

Broken or TBD:

- grid-api and grid-hermes may need router chain tuning if fallback to grid-svr ORACLE becomes frequent; monitor response times.
- No load-balancing metrics instrumented yet for the new provider chain; consider adding Prometheus metrics for llm/router.py provider selection.
- ocr-node `/scratch` mount is ext4 on SATA; consider upgrading to faster media if I/O becomes a bottleneck.

Next pick-up:

- Monitor gridz4 z4-llama.service uptime and error logs over 24h; verify no CUDA memory exhaustion or timeout issues.
- Instrument llm/router.py with provider selection metrics (Prometheus counters per provider).
- If panda qwen3.6 proves stable, retire qwen2.5 binary entirely and reclaim additional [[Ollama]] disk space.
- Benchmark koala Whisper + Kokoro latency with grid-api in production load; consider GPU acceleration if CPU bottleneck emerges.

## 2026-05-20 - Hermes storage maintainer subagent

What was done:

- Added a bounded, report-first GRID storage curator for active data roots on `grid-svr`: `/data/gdelt`, `/data/bulk_data`, `/data/datasets`, `/data/grid/bulk`, and `/data/archive`, with `/mirror` as the cold-storage target.
- Added [[Hermes Scheduler|Hermes]] repair skill `CHECK_STORAGE[:target_id]` and dedicated subagent role `storage_maintainer`, backed by the new `hermes_storage_maintenance` goal-worker handler.
- Wired `scripts/hermes_operator.py` so the daily [[Hermes Scheduler|Hermes]] intelligence batch queues the storage maintainer subagent instead of relying on a manual command.
- Deployed the updated files to both server runtime trees with `scripts/deploy.py --snapshot`, restarted `grid-hermes` and `grid-goal-worker@grid-svr`, and verified both services active.
- Ran a live dispatch through the daily helper; goal `#4` completed and wrote `outputs/storage_maintenance/storage_maintenance_20260520T075333Z.{json,md}`.

Non-obvious decisions:

- No files were deleted or moved. The cleanup plan sets `delete_source=false` until DB ingest proof and checksum manifests exist.
- The scan found `/data` at about 77.5% used and `/mirror` at about 48.3% used. `/data/gdelt` is the dominant active footprint, but `/mirror/gdelt` already contains same-size copies for at least the largest sampled [[GDELT]] archive.
- The [[GDELT]] archive set appears downloaded, but ingest is not proven: live DB lacks `gdelt_events` and `gdelt_daily_summary`, while `scripts/parse_gdelt.py` only covers `/data/grid/bulk/gdelt` rather than the much larger `/data/gdelt/v1/events` and `/data/gdelt/v2/...` roots.

Broken or TBD:

- Extend or replace the [[GDELT]] parser so it ingests the real `/data/gdelt` archive roots, then build a checksum/ingest manifest before active-disk cleanup.
- Add a manifest-backed copy/remove executor after parser proof; the current subagent is intentionally inventory/plan only.
- The live `/home/grid/grid_v4/grid_repo` checkout is on `fix/hermes-active-hypo-scoring-dedent-2026-05-17`, ahead of origin with many dirty files. Do not hard-reset it; reconcile as a separate server-worktree cleanup.

## 2026-11-06 - explicit daily publication packet handoff job (source only)

What was done:

- Added `docs/runbooks/DAILY-PUBLICATION-PACKET.md`: Codex owns end-to-end daily
  closure and Claude owns capture production. Named the UTC-day key, existing
  scheduler, stage receipts, admission/A10 handoff, private custody/dedup,
  privacy screening, local/Gemini review and visible failure/deadline handling.
- Added a stdlib-only offline status reporter and operational regression tests.
  It summarizes supplied metadata, never authorizes execution, and does not
  replace the external producer, its one-use registry or the existing scheduler.
- Reconciled scope with M0 issue #958 and research protocol issue #960; neither
  is completed by this operational handoff. No `.agents/skills` directory was
  present; inspected relevant `.claude/skills` and the module inventory instead.

Non-obvious decisions:

- A missing capture/admission record does not establish diary execution failure.
  The status explicitly distinguishes diary evidence from packet preparation and
  admission review. `COMPLETE_REPORTED` is a declaration, not custody verification
  or scientific acceptance.
- A10's existing conditions remain required. Spent/started/unknown capture
  attempts require reconciliation and cannot be automatically replayed.

Broken or TBD:

- Existing external automation/producer owners must adopt the observation
  contract and retain runtime receipts at the existing private ledger. This
  source PR does not prove today's diary ran or repair historical packets.
- No capture, production query, Gemini call, timer/schedule change, merge or
  deployment is included. Full repository CI and native runtime acceptance
  remain separate from the offline controls.

Next pick-up:

- Follow the runbook's adoption/operational acceptance checklist in the existing
  automation. Retain one naturally scheduled run's two output statuses and
  failure notification before declaring the pipeline repaired.
## 2026-11-07 - summary-first Oracle and operator email reporting (source only)

What was done:

- Added a prominent Oracle activity headline and concise recap before the supplied
  prediction detail. New prediction rows, model votes and update operations remain
  separate; absent or incomplete store counts are unknown, never substituted.
- Replaced the Oracle email's misleading P/L percentage with a labeled historical
  signed-move sum in percentage points. It is an unreconciled diagnostic, not a
  portfolio/option return. No scoring, historical data or model weights changed.
- Added cycle timestamps, nullable/non-finite display handling, expiry status,
  full supplied signal/anti-signal detail and readable plain text.
- Operator issue totals now aggregate the full bounded 24-hour interval before
  the 100-record detail limit. The recap and subject say historical issue records,
  not current open incidents. Retained diagnosis, fix text and recorded statuses.
- Long Plays displays the saved board/price dates and marks past catalysts as
  outcome-unverified. Snapshot age never implies revalidated eligibility.

Verification and limits:

- 197 offline tests passed on Python 3.12 with outbound sockets blocked and
  SQLAlchemy 2.0.57. Oracle report line coverage 100%; operator whole-module 76%;
  combined 82%. Ruff, compilation and diff checks passed for changed reporting
  code/tests. Synthetic Oracle/operator HTML previews inspected in Chromium.
- Independent review's incomplete-update-count and malformed-candidate findings
  were fixed and covered by regression tests.
- Draft source change only: no SMTP send, production query, historical repair,
  merge, deployment, schedule change or new external-agent communication.
- PostgreSQL integration, full repository CI and deployed email/client acceptance
  remain separate. This does not reconcile the historical prediction track record.

## 2026-11-07 - expand summary-first reporting to all email alert families

What was done:

- Inventoried both SMTP transports and every repository email template in
  `docs/reference/EMAIL_ALERT_INVENTORY.md`. Extended the same draft change rather
  than adding alternate senders or changing enablement, schedules or recipients.
- Added source-specific headline counts, scores, states or verdicts and concise
  recaps to all 12 shared public sender types, specialized options, supply-chain
  and Waterfall reports, and legacy generic/backtest/daily notifications.
- Kept full supplied narratives, commands, findings, calendar events and bounded
  option ledger detail. Removed old presentation truncation without changing the
  underlying scanner gates or historical scoring.
- Clarified samples versus totals, overlapping findings, hypothetical results,
  missing inputs, saved-board age, source dates and expired/unconfirmed catalysts.
  Failed or unavailable sources never become a fabricated zero or all-clear.
- Added readable multipart plain text, preserving exact code indentation and
  repeated spaces in preformatted HTML and explicit plain text.
- Left the pre-existing raw-attempt autoresearch notification gate and missing
  circuit-breaker sender import dormant. Their wiring is documented separately;
  this presentation update does not turn on extra notifications.

Verification and limits:

- 494 combined offline tests passed on Python 3.12, using SQLAlchemy 2.0.57,
  mocked SMTP / SQLite and an outbound-socket block. The existing pure mock_engine
  fixture was loaded without executing the repository's live-DB conftest setup.
- Synthetic health, research, options, supply-chain, Waterfall and backtest HTML previews were inspected in Chromium; all led with a visible headline and recap.
- Ruff (E/W/F, excluding E501), compilation and git diff --check passed for all
  changed reporting code/tests. Independent review found no remaining blockers
  after fixes for saved Long Plays freshness and plain-text command whitespace.
- The first draft's GitHub checks passed, including backend tests on both Python
  versions, frontend build, lint, migrations, Quiver and G3/G5 matrix checks.
  The expanded source revision requires its own CI run.
- No production queries, SMTP delivery, merge, deployment or external-agent
  communication. Repository coverage is not proof of deployed email delivery,
  historical data reconciliation or actual investment performance.

### Same-day integration with the updated main branch

- Main advanced to `8fbed48c` while the expanded draft was being published.
  Integrated it into the isolated draft branch, without merging the PR.
- Resolved the sole `scripts/notify.py` overlap by retaining main's creation-date
  cutoff and legacy-label exclusions together with the email's bounded update
  window. The recap discloses this filtered PASSED-count scope.
- Strengthened the SQLite regression with recently updated legacy, labeled and
  future rows; only the current unlabelled row is counted. All 494 combined
  offline tests passed again; Ruff and diff checks passed.

## 2026-11-07 - make the daily research recap scannable (source only)

- Daily research email now uses separate HTML/plain-text recap bullets, while
  existing string callers retain their original paragraph/plain-text output.
  Its normal decision-count headline is no longer repeated in the recap; an
  incomplete/unknown headline still keeps the available count or unknown label.
- Long Plays is explicitly a weekly saved snapshot, with a compact data-as-of
  date in the recap and the exact generation time, age caveats, saved stances,
  capped rows, catalyst caveats and proxy labels retained below. Reused the
  existing purple accent class instead of an unsupported hex class name.
- No data query, collection window, board refresh/cadence, research audience,
  sending/dedup path, other email family, deployment or production setting changed.
- Validation: 508 offline tests across 17 email-related suites passed on Python
  3.12 with network, SMTP and PostgreSQL connections blocked. Coverage includes
  HTML/plain rendering, capped samples, absent/degraded inputs, escaping, and
  stale/missing/invalid/future/naive/offset board timestamps. Existing SQLite
  datetime deprecation warnings remain.
- Changed-file Ruff E/W/F (excluding E501), compilation and diff checks passed.
  Repository-wide blocking Ruff rules passed with the supported Python 3.11
  target; default-target Ruff reported two pre-existing BaseExceptionGroup
  name warnings in tests/test_resolver_transaction_pg.py.
- Synthetic local Chrome previews passed at 1000px and 390px, with no horizontal
  overflow and the computed purple accent. Preview files remain outside the
  repository. This is not live delivery or pixel-identical Gmail validation;
  full repository CI, publication, merge and deployment remain separate.
- Visual-review follow-up: separated the current-regime recap fields with
  semicolons and made a one-row sample singular. Stale/unverified recaps and
  full regime cards are unchanged. All 508 offline tests and the same lint,
  compile, diff and affected desktop/mobile preview checks passed again.

## 2026-11-07 - Alpha Vantage transcript coverage (isolated source only)

- Extended the existing earnings-events puller with durable symbol/quarter
  rotation, explicit local pacing and optional daily budgets, and provider-notice
  stop behavior. Kept all live wrappers, schedules and account allocations unchanged.
- Reused stored SEC fiscal metadata matched to exact filings rather than guessing
  calendar quarters. Missing/ambiguous metadata and legacy transcript provenance
  are reported as coverage gaps; no historical rows are repaired automatically.
- Added `docs/reference/ALPHAVANTAGE_COVERAGE_ROLLOUT.md` with prerequisites,
  milestones and minimum data contracts. AV bulk adapters remain unchanged/unrun.
- 56 focused offline tests passed with socket access blocked; changed-file lint,
  compilation and diff checks passed. Independent source review's legacy-date
  finding was fixed and regression-tested. PostgreSQL integration, account-wide
  budget allocation, legacy reconciliation and date-aware consumers remain gates.
- Local branch only. No provider requests, production writes, push, PR, merge or
  deployment. This does not establish live coverage or complete valuation inputs.

### Follow-on qualification (2026-11-07, source still local)

- Verified current upstream work and no overlapping open AV/fiscal PR. Continued
  the existing branch without changing Claude's daily-packet/Hermes work.
- Bounded production READ ONLY aggregates found 373 recent candidate SEC fact
  rows across 21 tickers, reducing to 40 verified period pairs across 20 tickers
  when matched to stored SEC filings. One legacy overlap leaves 39 potentially
  requestable pairs; this is not fresh provider or full-universe qualification.
- All 240 legacy AV transcript rows (160 tickers) lack new provenance fields;
  all have filing_date=period_end, including three future-dated rows. No repairs.
- Added five actual PostgreSQL 15.19 tests in a separate private Unix-socket
  cluster. Together with 56 offline tests, 61 passed with provider calls blocked.
- Documented eight independent AV client modules/scripts and requirements for
  real account-wide budgeting. The current local ledger is not a global limiter.
- No production writes, paid API calls, push, PR, merge, deployment, or scheduler
  changes. Legacy date semantics, metadata breadth and shared budgets remain gates.

## 2026-11-07 - SEC financial duration prevention (source-only candidate)

- Reproduced the NVDA quarter/YTD collision: the old form-only classifier and
  input-order tie breaker admitted a six-month amount as a quarter. Added
  explicit duration admission and exact filing/period/tag provenance.
- Quarterly/annual facts are selected deterministically; missing, unsupported
  and conflicting contexts remain unavailable. Debt netting requires matching
  reported contexts and resolves amendments before positive-inflow filtering.
- Added a versioned source marker and bounded replacement of newly validated
  economic keys, without sweeping unmarked legacy rows or changing schema.
- Offline parser, provenance, writer and adjacent-reader tests use synthetic
  inputs, in-memory SQLite or mocks with sockets and PostgreSQL blocked.
- Production repair is not done. Legacy rows, omitted/withdrawn revisions,
  structured revision identity, PostgreSQL validation and dependent rollup
  reconciliation are explicit rollout gates in
  `docs/reference/SEC_FINANCIAL_PERIOD_INTEGRITY.md`. No provider calls, DB
  corrections, publication, merge or deployment.


## 2026-11-07 - qualify SEC snapshots and historical reconciliation proposal

- Continued the isolated period candidate with original archived SEC Company
  Facts for NVDA, Apple and TSMC plus the NVDA filing. Real source bytes confirm
  $96.221B quarter versus $177.837B six-month revenue, a 371-day Apple fiscal year,
  a 98-day Apple quarter and explicitly limited IFRS annual coverage.
- Added content-bound source snapshots, a separate current pointer, strict
  admission and an actor-serialized transactional projection writer. Valid
  empty/unsupported current projections remove only this producer's old marked
  rows. Failed captures and older/regressed revisions cannot replace current data.
  Original bytes, normalized outputs, FX inputs and receipt times are preserved.
- Added proposed evidence/head tables without historical rewrites. Application
  archive writes are append-only; actual deployment ownership/default/inherited
  grants still require review. Downgrade deliberately retains evidence.
- Distinguished attempted versus successful checkpoint progress, so a permanently
  failing issuer cannot starve later tickers or freeze healthy revision refreshes.
- Prepared a local-only, no-execution reconciliation manifest from a frozen
  read-only export of 798 NVDA rows: 252 value differences, 338 provenance-only
  review cases and 208 unavailable cases. These are review groups, not approved
  corrections. Exact IDs and lossless before-image hashes guard a future repair.
  Private operational artifacts remain outside git; public SEC fixtures are saved.

Verification and limits:

- 270 selected offline tests passed, 3 dependency-limited tests deselected; 14
  real PostgreSQL tests passed on a new disposable native PostgreSQL 15.19 cluster.
  Checked actual uniqueness, rollback, archive bytes, idempotency, empty updates,
  concurrent capture ordering and clean-role evidence UPDATE/DELETE/TRUNCATE denial.
- Private cluster was stopped and removed. No existing database/service or denied
  WSL instance was accessed. No production writes, migration, checkpoint changes,
  paid-provider calls, external-agent communication, push, PR, merge or deployment.
- Independent review findings on malformed source admission, Decimal preservation
  and failed-issuer liveness were fixed and regression-tested; final review found
  no additional actionable source defect. Ruff, compilation and diff checks passed.
- Full CI, deployment schema/permissions, legacy reconciliation, dependent rollup
  invalidation and consumer acceptance remain required. Existing FX/concept/sign
  and fiscal-label policies are not made authoritative by this duration fix.
  See docs/reference/SEC_FINANCIAL_PERIOD_INTEGRITY.md for evidence and repair gates.
## 2026-11-07 - daily publication packet: in-repo producer replaces the per-day capture ceremony

What was done:

- Merged current main into #1002 (AGENTS.md conflict: both appended sections kept).
- Added `scripts/daily_publication_packet.py`: one scheduled run per completed UTC day
  on grid-svr reads `market_briefings`, `thesis_snapshots` and `market_diary` rows
  published in the window (plus diary context, audio metadata and grounding receipts)
  in a single read-only `REPEATABLE READ` snapshot, screens texts (credentials,
  connection strings, private keys, health identifiers, hidden characters; whole-record
  quarantine, never truncation), and writes `capture.json` (0600), `packet.json`
  (`publication-review-v1`, the shape the M0 checker reads), three chained stage
  receipts, `observation.json` (producer stages declared) and `receipt.json`.
  A produced day is never rewritten. Diary execution evidence comes from the rows the
  writer produced plus `systemctl show` of its unit; Sun/Mon report a verified
  non-session, a scheduled-but-missing diary reports `failed`.
- Reordered the status reporter's stages so the producer's three come first
  (`diary_run → candidate → capture`), then the daily-review owner's five; the
  `not_started` capture action now names the producer job.
- Added `deploy/systemd/grid-publication-packet.{service,timer}.template` (06:15Z daily,
  previous UTC day) and documented the precision5520 pull mirror into the Codex pickup
  directory (screened files only). Runbook rewritten accordingly; `.env.example` gained
  the two optional settings.

Why:

- The r3–r13 lineage needed a fresh source bundle, independent review, sealed admission,
  reviewer runtime acceptance and prior-day dispositions for every single day; nothing
  completed after Nov 3 and even the completed Oct 31 candidate was never delivered.
  The operator's standing instruction (A10, 2026-11-02) is that the daily capture class
  runs automatically. The data was always in griddb; the job now reads it there.

Verification and limits:

- 35 offline tests (status + producer, fake engine) pass; ruff F-gates clean.
- Runtime acceptance (a naturally scheduled run, mirror delivery, the daily review's
  own stages) is recorded in the runbook's consumer checklist; the producer cannot
  supply those receipts. Gemini/provider input remains the daily review's own gate.

## 2026-11-07 - market_briefings 5007: 1.5 MB hourly briefing was an unbounded evidence appendix

Cause:

- `ollama/market_briefing.py::_publication_evidence_footer` (added 2026-10-26 in
  54c1c6e9) appends a "Feature evidence declarations" JSON block to every briefing with
  one ~1.5 KB declaration record per snapshot feature whose latest value equals a number
  in the narrative. `_publication_stage_diagnostics` listed every equal-value match per
  mention across three roles. With ~2,700 model-eligible features in the snapshot, a
  value shared by many features fans out without bound.
- Row 5007 (hourly, 2026-11-06 14:27:48Z): narrative 1,617 chars; `0%` matched 961
  zero-valued features, `0.6` matched 40, `0.54` matched 12 (two mentions) -> 1,005
  records, 1,527,952 chars of `content`, 30.8 MB `snapshot_data` text (1.5 MB TOASTed).
  Not an LLM loop (num_predict=800), not a retry, not a grounding dump.
- Same mechanism, smaller: 69 rows over 100 KB between ids 4731 and 5035 (2026-10-26 to the
  2026-11-07 20:42Z read, only 5007 over 1 MiB); typical hourly 210-550 KB with 125-346 records, e.g. 4984 at 555 KB had `0.125`
  matching 189 features). Before 2026-10-26 the daily max was 15 KB.

Fix (writer, this branch):

- `_publication_stage_diagnostics(..., match_list_cap=8)`: every match is still counted
  (`snapshot_match_count`), at most 8 listed per mention, `snapshot_matches_complete`
  flag. Cap is a keyword default, not a module constant, so the AST-loaded contract
  tests still execute the function.
- `_publication_evidence_footer`: an incomplete listing gets
  `attribution: "ambiguity_overflow"`, its match count, and NO records (equal-value
  coincidence with that many observations is not attribution). New `rendering`
  metadata. Backstop `rendered_cap_bytes=262_144`: an appendix still over the cap is
  withheld whole with a declared one-line note and a `log.warning`; records stay in the
  stored payload. The narrative is never cut. Mentions with <=8 matches render exactly
  as before.
- Tests: `tests/test_market_briefing_evidence_footer_bound.py` (11, real module, no
  network/DB/model; reproduces the 5007 shape: 961 zero features + `0%`, content <10 KB,
  snapshot <150 KB). Related suites: 534 passed, 21 skipped; ruff E/W/F clean.

Packet cap decision:

- `scripts/daily_publication_packet.py::TEXT_FIELD_CAP_BYTES` stays 1 MiB (comment added
  at the constant). A legitimate briefing is now <=15 KB narrative + <=256 KiB appendix;
  raising the cap would admit the runaway shape, not a real briefing. Row 5007 and any
  other pre-fix row over the cap remain EXCLUDED_TEXT_OVER_FIELD_CAP by id/hash.

Not done here:

- Row 5007 and the other oversized rows are untouched (read-only inspection via
  `psql -d griddb` with `default_transaction_read_only=on`). Whether to compact or
  re-render historical appendices is an owner decision; the data summary / narrative
  in those rows is intact and small, only the appendix is large.
- Not merged, not deployed. Takes effect for new briefings once the release tree is
  updated and grid-intelligence restarts.

## 2026-11-08 - AV date-aware consumers and draft reconciliation

- Integrated current main into draft #1051. Kept its rendering-date helper and
  both fresh/stale digest tests; no main push, PR merge or deployment.
- Added forward source migration removing fabricated period-end earnings dates
  from the compatibility view and disabling unverified calendar stub insertion.
  Historical rows remain untouched; unknown dates stay NULL.
- Wired provenance/availability-gated AV read-through transcript analyses without
  writing the incompatible legacy analysis table. Historical as-of output is
  explicitly recomputed from current evidence, not an immutable PIT snapshot.
- 155 offline tests passed with outbound sockets blocked; six real PostgreSQL
  contracts passed in a disposable private-socket cluster, then it was stopped.
  Changed-file Ruff and diff checks passed. New-head remote CI remains separate.
- No provider calls or production access. Historical reconciliation, metadata
  breadth, immutable revision history and shared-account quotas remain gates.


### AV draft CI follow-up

- The raw-read guard exposed an obsolete allowance for analyzer docstring prose.
  Verified both source heads: the actual EDGAR SQL already filters SUCCESS.
  Removed only that allowlist entry; scanner/enforcement unchanged.
- 157 focused offline tests including both repository-wide guard scans passed
  with outbound sockets blocked. New full CI remains pending after draft push.


## 2026-11-08 - SEC current-consumer integrity continuation (draft only)

- Integrated main 3102e7b0, resolving the independent digest-clock test conflict
  through main's clock seam while retaining fresh and stale coverage (49 tests).
- Added a snapshot-aware read projection: supported direct financial facts must
  match the current normalized evidence; missing facts cannot resurrect legacy,
  seed or actor-case variants. Direct annual/quarter facts are deduplicated before
  aggregation. Original ledger rows and evidence remain unchanged.
- Bound TTM visibility and recompute state to the current SEC snapshot identity.
  First empty heads and same-values/new-identity captures trigger reconciliation.
  A controlled real PostgreSQL overlap proves a late old rollup is hidden until
  the next current recompute, without relying on wall-clock timestamps.
- Fixed downstream missing-as-zero ratios and financial scores. Incomplete
  fundamental scores do not emit new trading verdicts; explicit zeros remain valid.
- 418 selected offline tests passed, 8 optional/integration skips, 2 subtests;
  24 tests passed in a fresh private PostgreSQL cluster that was stopped/removed.
  Independent review, changed-file Ruff, compilation and diff checks passed.
- Public NVDA fixture: 587 qualified rows beside 587 synthetic legacy duplicates;
  final relational evidence join annual aggregate about 17 ms locally. This is
  not production-scale qualification. No production database or provider access.
- Remaining gates: exact-head full CI, deployed schema/ownership/grants and query
  plans, persisted fundamental-divergence and report invalidation, TTL-cache
  invalidation, reviewed historical repair. No repair executor, merge, deployment,
  production migration, historical correction or production rollup was performed.


### SEC continuation CI correction

- Clean replay exposed the separate raw-SQL capital_flows dependency. The new
  migration now creates that canonical empty ledger only if absent; it does not
  run raw 0024's historical backfill or deduplication against existing data.
- Parallel test collection exposed one unordered parameter set. Sorted it and
  verified all 98 missing-value test IDs match across three Python hash seeds.
- 25 disposable PostgreSQL tests pass, including empty-schema initialization,
  grid-role view/insert access and populated-ledger preservation. Fresh head CI
  remains required; no production state changed.


## 2026-11-08 - Source-bound report and cache continuation (draft only)

- Added SEC/TTM global generation binding and current views for divergence and
  sector-health reports. Legacy/stale report rows remain retained evidence.
  Computations check before/after reads; gated writes and current views reject
  stale publication, including a late writer after a newer source commits.
- Capital-flow, sector-health, divergence and percentile TTL caches validate
  generation on every hit/build, isolate engines and fail closed on source churn.
- Disabled new divergence signal fanout. Existing signal source families,
  registry/materialized outputs and journal rows still require end-to-end
  provenance/invalidation before rollout is complete. No evidence was erased.
- Missing current-generation historical baselines produce unavailable trends,
  never a fabricated stable trend. Regular source reobservations make historical
  trends generally unavailable until explicitly rebound/recomputed.
- 398 offline financial/scheduler tests passed (9 optional skips); 35 isolated PostgreSQL tests
  passed with role grants, raw preservation, peer/TTM invalidation and a real
  late-writer race. Full exact-head CI remains required after the draft push.
- No production access, mutation, historical repair executor, deployment or merge.
  Remaining operational gates are documented in SEC_FINANCIAL_PERIOD_INTEGRITY.md.

- Independent review caught retry-status ambiguity: source-version failures and
  rejected writes now raise retryable errors rather than marking Hermes due work
  done as empty/superseded. Postcommit checks and 22 dedicated regressions cover it.


## 2026-11-08 - First derived-signal containment slice (draft only)

- Added an explicitly incomplete source inventory and staged non-destructive
  lineage/quarantine plan, including executable proof of lost ancestry.
- Blocked financial event replay at all three SignalFired handlers before any
  DB/provider effects. Existing producer suppression remains active.
- Guarded 12 current regime/feedback reads against explicit provisional journal
  records. Original history, operator actions and outcomes remain unchanged.
  Empty synthesis returns 503 before LLM use, rather than inventing a regime.
- 676 offline tests passed (9 optional skips); 71 private PostgreSQL tests passed,
  including 36 actual-predicate and raw-evidence preservation cases. Lint,
  compilation and diff checks passed. A date-sensitive test uses the existing
  UTC seam; production clock behavior is unchanged.
- Mixed historical registry/prediction/materialized/learned state still needs
  qualified publications and clean learning epochs. This is not complete repair.
  No production access, migration, deletion, merge or deployment. Remote
  exact-head qualification remains required after any draft publication.


### Derived-signal qualification correction

The first G3/G5 run correctly rejected an outdated forward thesis digest before
executing tests. The 12 guarded reads now use fixed-column SQLAlchemy clauses and
bound values, preserving predicate/history semantics. Only the forward thesis
pin and new helper dependency are updated; all 14 HOLD seals, other forward
seals, test counts, isolation and rollback checks stay unchanged. Expanded local
validation passes 759 offline tests (9 optional skips) and 71 disposable PG tests.
All 15 forward and 14 HOLD source blobs verify. Fresh exact-head CI is required;
the failed predecessor is not accepted. No production access or deployment.
## 2026-11-08 — Daily review becomes machine-readable: `review.jsonl` ingest (Claude)

Scope: `scripts/ingest_publication_review.py`, `migrations/versions/pub_review_ingest_20261108.py`
(`publication_review_runs`, `publication_claim_verdicts`, append-only), unit templates
`deploy/systemd/grid-publication-review-ingest.{service,timer}.template` (13:05Z),
`tests/test_ingest_publication_review.py`, runbook section "Review ingest", `.env.example`
(`GRID_PUBLICATION_REVIEW_ROOT`).

Why: the Oct 5–22 daily reviews were documents in the grader's Library that the operator
never saw and nothing in GRID consumed ("0 measured outcomes" every day). Operator
direction 2026-11-07: "everything needs to be machine readable first … the machines will
be doing the research." The deliverable is now one `review.jsonl` per day
(`publication-review-verdicts-v1`), bound to the producer's packet by hash and to each
publication by a verbatim span; this job verifies and ingests it. The file is treated as
hostile input (another agent writes it): regular-file and size checks before reading,
per-row isolation, no NUL/NaN/lone-surrogate content reaches the driver, receipts written
through O_EXCL temp files, no look-ahead grading (evidence and horizons must predate
`graded_at`).

Decision: no projection into `thesis_postmortems`. An independent review of the first
draft showed that table is the thesis tracker's wrong-thesis analysis (enum root causes,
under `THESIS_SCORING_HELD`), and a grader row there would both pollute it and block the
tracker's own postmortem for that snapshot. `publication_claim_verdicts` is the single
record; wiring it into the thesis learning loop is the hold owner's decision.

Verification and limits:

- 101 offline tests (fake engine that enforces bind names, int4 range, jsonb validity, NUL,
  span length and the UNIQUE review hash, with rollback on failure) + alembic single-head +
  systemd invariants pass locally.
- Not verified here: the grader's first `review.jsonl` (its owner adopts the schema), the
  reverse mirror leg, the first scheduled ingest on grid-svr. Those are the runbook's
  acceptance chain. No row is ever updated: a correction is a new review file.

## 2026-11-08 — Daily review grader moves in-repo; chain reports on itself (Claude)

Anik, after the whole packet → ingest chain was live but idle waiting for the external
daily-review agent to be nudged: "what are the weak link bottle necks that we can harden so i
dont need to nudge anything" → "yeah do that". The only link that needed a nudge was the only
one that was not a scheduled job.

Changes:

- `scripts/grade_publication_review.py` (+ `tests/test_grade_publication_review.py`): the
  grader is a oneshot on grid-svr (11:35Z, `deploy/systemd/grid-publication-review-grader.*`).
  Deterministic core: thesis direction calls graded against SPY split-adjusted closes with the
  thesis tracker's own 3-day/±0.5 % rule; records whose narrative was withheld get one
  `unscorable` `data_quality` row (the real Nov 4–7 packets are 100 % such records for
  briefings and the diary — that is now measured, not hidden). Records with a narrative go
  through claim extraction on `llm.router` (tier configurable; "better models until the
  workflow is tight" is an env var, not an agent); extracted direction/level claims are graded
  from closes, causal/event stay unresolved. Every row is built to the ingest's contract and the
  tests push each produced file through the real ingest validators. Re-grades for 10 days so
  every horizon up to a week resolves; a file is rewritten only when verdicts change; the grader hands the file to
  the ingest in-process. Smoke-run on the real Nov 7 packet: 31 rows, 0 rejected.
  An independent review (code-reviewer subagent) found and this PR fixed before merge: the
  narrative window cut at the generator's own narrative headings; level claims on index aliases
  compared against ETF prices; list-valued model JSON crashing a day; a failed re-ingest never
  retried; neutral-vs-move not matching the tracker's "partial"; week horizons outside the
  lookback; a stale baseline close silently accepted; the cache never invalidating on model change.
- `scripts/ingest_publication_review.py`: `MAX_PACKET_BYTES` (64 MB) for packet.json. The 8 MB
  review-file cap also applied to packets and would have refused every real day (7–24 MB);
  found by the smoke run above, never by the 101 fixture tests.
- `store/publication_review_status.py` + `api/routers/system.py::health`:
  `checks.publication_review` with the latest packet day on disk and the latest ingested review
  day; a named `degraded_reasons` entry when either is older than two days. Table-existence
  probe first, never raises, missing tree/table = not configured.
- `scripts/check_systemd_unit_drift.sh` + a push-deploy step (`--sync --strict`): installed
  units whose template opts in with `# deploy-sync: managed` and differ from it are reinstalled and
  daemon-reloaded; every other installed unit is compared and reported only (grid-e2-scoreboard's
  deliberate dedicated-user deviation stays). Never installs a new unit, never touches drop-ins,
  never restarts a running service, reports `name@.service` without syncing; remaining drift is a
  workflow warning.
  Closes the class behind the packet unit running a day without its public-tree env.

Decisions and limits: the external agent's reverse mirror leg (`grid-publication-review-push`)
is retired — two writers of `review.jsonl` would race; the pull mirror is retired with it
(agents read the public tree on grid-svr). First install of the grader timer is the operator's
`sudo install` (unchanged rule). Not verified here: the first scheduled run on grid-svr, the
LLM extraction path against a live provider (no narrative exists in any current packet to
extract from), and the deploy-time unit sync on the real host.

## 2026-11-09 - Offline company document intake

Added ingestion/company_documents.py, a bounded no-network raw-document archive,
plus scripts/ingest_company_document.py and an explicit consumer/runbook contract.
This complements existing SEC financial and AV transcript work without modifying
those collectors. Every snapshot binds raw bytes, metadata, parsed text and parser
version; atomic no-replace publication preserves revisions and supports retries.
Publication time remains distinct from retrieval and unknown timestamps stay unknown.
Unsupported PDF/encoding and empty text are visible missingness, not usable evidence.

Validation: 39 offline intake tests pass, including concurrent capture, corruption,
PIT boundaries and CLI roundtrip; changed Python Ruff passes. No live downloads,
paid providers, production DB, schedule, migration, repair, merge or deployment.
Document authenticity, complete coverage and semantic fact extraction remain outside
this primitive. Full repository CI has not been run for this local candidate.

Integrated the separate evidence-quality gate and archive-verified reader wrapper.
Combined final local validation: 62 tests and 9 subtests passed, including true
archive-to-quality roundtrip, tamper rejection and high-precision numeric conflict
regressions. Independent intake review found no critical defect under the documented
trusted-archive assumption. Quality is not truth/authenticity or complete coverage.

## 2026-11-09 — Review visible to every agent; briefing figures graded (Claude)

Anik: "yeah fix it, codex still cant see it". Two causes, two fixes in `scripts/grade_publication_review.py`:

- Visibility: the review root is under `/home/grid/research` (0700, home 0750), so Codex on
  Alien could read the packets but never the review. The grader now copies `review.jsonl`,
  `grader-receipt.json` and `ingest-receipt.json` (allow-list, bytes unchanged, 0644) to
  `/data/grid/outputs/publication-reviews/<D>/` on every WRITTEN or UNCHANGED day. The 10-day
  lookback re-publishes the backlog on the next scheduled run; no manual step.
- Every briefing was unscorable: the narratives state current levels and never forecast
  (gridz4 extraction returns `[]` correctly). Each stated level on a series the generator reads
  is now a `data_quality` row graded against what GRID knew at the publication instant (the
  generator's own Yahoo reader, `read_latest(as_of_ts=stated_at)` for FRED). Nov 4-7 narratives
  carry 59 such figures. `oracle/claim_verifier.py` was not reused: it reads the latest value,
  the wrong clock for grading a past publication.

## 2026-11-09 - Deterministic briefing guard regression clock

Full Python 3.14 CI found a pre-existing test false positive: forbidden financial
text 8.96 happened to occur inside assembly timestamp 08:16:38.960504. Pin the
single regression's briefing clock while preserving all numeric, semantic, file
and DB-persistence assertions. No production code or guard changes.

Local Windows run: 241 briefing tests passed; 15 cannot execute because the
existing social sentiment import requires Unix fcntl. The targeted regression
is among those platform-blocked tests. Hosted Linux full CI is the acceptance
check. Company intake/quality remains 62 tests and 9 subtests passing.


## 2026-11-09 - prepare private GEX projection parity in GRID (not activated)

- Audited the GEX dashboard/GRID API split and documented it in `docs/reference/GEX_GRID_API_PARITY.md`. Shared hosts and raw stores do not make publication guards universal.
- Added a default-off, admin-only nine-view read bridge under `/api/v1/gamma-watch/private/{view}`. It preserves upstream guarded payloads and source clocks, rejects unknown view/schema/status, and exposes no private state, raw contracts or write route. No production setting or service was changed.
- Hardened the shared upstream JSON reader against finite-token overflow such as `1e400`.
- Validation: 121 focused synthetic bridge/private-auth tests passed, changed-file E/W/F and repository blocking Ruff passed with Python 3.11 target. Full backend/PostgreSQL, live parity, entitlement review, browser checks and release remain unrun.
- Next: verify actual private audience and paired endpoint evidence before activation; consolidate quality/calculation contracts behind GRID and migrate GEX consumers incrementally, retaining volume containment and existing release ownership.


## 2026-11-09 - reconcile and qualify the private GEX API bridge locally

- Direct GitHub main check confirms base 2945e95c is current; no rebase is required. Open PR search found no overlapping bridge owner; #982 is separate browser acceptance.
- Require the literal boolean True for the private-views gate, because mutable Settings assignment is not validated; add the disabled flag to .env.example and negative controls. Disabled or failed reads now have a null receipt timestamp, separate from bridge evaluation time.
- Final relevant offline validation: 1,139 passed, no skips, after supplying the existing historical engine-reference commit. Python outbound sockets were blocked and no live DB was used. Python 3.11-target lint, wikilink and whitespace checks pass. Exact CI-default Ruff has two pre-existing BaseExceptionGroup target errors in an unchanged test file. Bandit is absent; full backend/PG, browser, entitlement and live parity gates remain unrun.
- Local branch/commit and draft PR text only. Publication needs user approval; no push, merge, deployment or feature activation.

## 2026-11-09 - Bound Oracle actor evidence and preserve source lineage

The actor bridge now rejects aggregate/feed identities, normalizes only explicit
bull/bear directions, applies observation-date and ingestion-availability bounds,
and selects one latest-date vote per actor/source channel. Unknown or conflicting
latest-day directions withhold the group; zero-trust actors cannot add coherence
votes. No historical rows or runtime settings are changed.

Actor freshness remains unknown at exact-hour precision. Reports distinguish the
stored observation DATE, calendar-age lower bound and ingestion availability.
Used row/source refs and scoring effects are kept in a small receipt in the same
signals JSON value that the existing upsert replaces atomically. Candidate detail
is explicitly sampled, complete used support is bounded, and an oversized receipt
fails closed before any weight mutation. The existing confidence/target upsert
policy and other signal families' freshness behavior remain outside this patch.

Synthetic regression coverage includes direction, repeated/conflicting rows,
identifier rejection, clock boundaries, actual SQL selection, size limits,
atomic no-mutation failure, scoring/coherence and insert/update receipt retention.
Focused bridge/Oracle/report tests passed locally. Broader compatible Oracle tests
also passed; full Linux/PostgreSQL CI remains the aggregate acceptance check.
No production access, send, generator, migration, service change, merge or deploy.
See docs/reference/ORACLE_ACTOR_EVIDENCE.md for the explicit contract and limits.

## 2026-11-16 - Version and label fixed-moneyness IV wings (source only)

- Replaced misleading future `iv_25d_*` writes with shared, explicitly named
  90%/110%-spot band-mean proxies. Added nullable method/expiry/provider/spot,
  capture and sample lineage on daily signals and each primary resolved value.
  Original legacy columns, registry history and research are not rewritten.
- Term structure uses the observed spot from one immutable capture; Watchlist
  and derivatives expose deprecated 25d keys as NULL and use the proxy names.
  Unknown legacy values remain separately labeled. Mixed/unknown methods,
  providers and tenors cannot enter the new spread percentile; missing wings
  stay missing. Bulk writes new raw IDs with payload lineage, kept unmapped
  until a lineage-aware resolver is reviewed. New primary features are not
  model-eligible and do not forward-fill. Surfacer clears incompatible wings.
- Separately labeled the existing ATM-IV Black-Scholes delta-strike surface
  approximation, removed its silent fixed-wing fallback and out-of-support
  clamping, and propagated its method to scanner receipts/digest descriptions.
  Existing scanner thresholds are unchanged; loaded legacy wings are not scored.
- Verification: 455 offline tests passed with outbound sockets blocked; one
  disposable-PostgreSQL test skipped (no dedicated DSN). Shared helper coverage
  100%. E/W/F lint (excluding E501/E402), compilation, single Alembic head and
  derivatives production build passed. API functions and JSON serialization
  were tested; sandbox ASGI TestClient hung, so full HTTP acceptance is unproven.
- Added `docs/reference/OPTIONS_WING_IV_METHODS.md` with compatibility/release
  boundaries and an opt-in PostgreSQL migration/upsert/lineage preservation test.
  Dedicated source branch only. No provider request, production query, data
  repair, holdout access, service/schedule change, merge or deploy. Anik alone
  applies releases; migration precedes code and API/UI ship together.

### Same task - exact schema conformance and retained-review follow-up

- Both backend CI jobs on a0a13c3a failed only the exact resolved-series column
  pin (21,545 passed in each). Added the nullable `options_iv_metadata` column
  explicitly to that exact set, preserved the equality assertion, tested its
  JSONB/no-default declaration against the real migration operation, and added
  the options publisher to guarded SQL modules with mandatory SQL coverage.
- Read Claude's exact-head REQUEST_CHANGES before editing. Preserved that
  verdict and v1 kit. Also retained existing BRK-B bulk IDs for untouched
  metrics and tested their preservation; clarified latest-capture abstention,
  unknown-provider missingness and the sector analytics legacy-suffix boundary.
- Expanded offline selection includes the schema guard: 480 passed, one
  explicit disposable-PostgreSQL skip; shared helper still 100% covered. No
  claim that old-head CI or the old REQUEST_CHANGES verdict is cleared by this
  local result. The same draft PR needs new-head CI and retained verification.
- No production/holdout access, provider request, credential/service change,
  merge or apply. Anik retains release authority.

### Same task - close the real PostgreSQL and HTTP qualification gaps

- Reconciled retained Claude ACCEPT_WITH_NOTES and green exact-head CI on
  9efefc1b. Fresh main c24b418b and current #1226 216714b7 each gave clean
  non-mutating merge-tree analysis; no combined #1226 changes were published.
- A real run in a uniquely named database on an already-running local test
  PostgreSQL container reached Surfacer after passing migration retry,
  producer upsert, legacy preservation and resolved lineage. It exposed a
  fixture-only missing created_at column (present in the declared schema).
  Added that column to the fixture. The failed run and verified database
  cleanup are retained in the v3 qualification evidence, not overwritten.
- Added six in-process ASGI HTTP regressions. All six pass with outbound
  socket connections blocked. The earlier hang was independently reproduced
  by a trivial worker-thread control: the restrictive sandbox denies AsyncIO's
  self-pipe socket.send. HTTP runs outside that restriction need no server.
  Successful-auth routing uses a synthetic override; unauthenticated routing
  is tested without it. Deployed app/auth-store acceptance remains separate.
- Qualification-only tests/documentation delta; no provider or IV calculation
  implementation, migration DDL, historical data or holdout changes. New-head
  CI and the existing verifier's updated disposition remain separate from the
  old accepted-head evidence. No new Claude handoff or release action.

## 2026-11-14 — Integrate accepted offline NVDA desk report

Additive #1085 report mode joins the existing frozen ledger, historical cash
reconciliation and #1204 evidence labels. The accepted kit's five files are
byte-identical; its bundle availability floor is preserved. 111 offline tests,
Ruff and exact report reproduction pass on the integration base c19920e9.
No recognition/valuation/trade result is fabricated: seven candidates remain
unusable. Frozen v2, legacy scenarios and the Nov18 comparison are unchanged.
See docs/research/r3-nvda/desk-report-v1.md. Full CI pending; Claude verifies final
kit and Anik applies releases. No production or holdout changes.

## 2026-11-17 - per-ticker social series: Bluesky cashtags, ApeWisdom Reddit boards, dormant Reddit Data API (source only)

What was done:

- `SocialSentimentPuller.pull_ticker_series` and a SmartScheduler entry `social_ticker_series` (12 h, timeout 900 s, catalog row `SocialSentiment`). It writes numeric `BSKY:*`, `APEWISDOM:*` and (with an owner app) `REDDIT:*` raw_series per UTC fetch date; FinBERT (local) scores up to 100 cashtag posts per ticker.
- entity_map: `REDDIT:<sub>:active_users` / `subscribers` map to the 2026-03-21 `reddit_<sub>` / `reddit_<sub>_subs` features. `tests/test_hermes_sec_only.py` scheduler tail re-pinned with a lineage comment.

Non-obvious decisions:

- Reddit closed logged-out access on 2026-11-17 (~17:50Z): `.rss` answers an Atom "Not Found" (404), `.json` 403, old.reddit redirects to login; seen from grid-svr and a second network. smart_money and reddit_options_pulse have failed since. No scraping workaround: the Data API (owner app) is the only sanctioned route; ApeWisdom is the working Reddit-derived feed.
- Bluesky logged-out `searchPosts` refuses every cursor page with 403 "forbidden by administrative rules". The puller reads one page and does not slice by `until` to get around it; a page that fills inside 24 h is stored as a rate (`posts_per_hour`) over its span, never as a 24 h count.
- Bluesky search ignores `$` (a `$KO` query returned no Coca-Cola posts), so only literal cashtags count.

Broken or TBD:

- Needs merge + deploy; the job then runs on the next SmartScheduler tick. Reddit board stats need REDDIT_CLIENT_ID/SECRET from an owner-registered app.
- smart_money / reddit_options_pulse / social_port_activity Reddit legs stay dead until the Data API is configured; not changed here.

## 2026-11-18 - Reddit back on free community APIs: Arctic Shift (posts/comments) + Tradestie (WSB sentiment) (source only)

What was done:

- New `ingestion/altdata/arctic_shift.py`: an HTTP-only client for the Arctic Shift archive API (free, no key, minutes behind Reddit when probed from grid-svr). Fixed host, redirects refused, 4 MB body cap, strict `{"data": [...]}`, typed and bounded fields, one cross-process lock spacing requests 2 s apart, a single bounded wait on 429 (`X-RateLimit-Reset`) and on the "Timeout" 422.
- `smart_money` (SmartScheduler now hourly, was 10 min), `reddit_options_pulse` (daily thread + newest 500 comments) and `social_sentiment.pull_all` (10 newest posts per board) read Arctic Shift instead of the 404 Reddit RSS feeds.
- `pull_ticker_series` gains `ARCTICSHIFT:*` (per-board posts/comments volume, per-board ticker post counts, FinBERT per ticker over Reddit posts) and `TRADESTIE:wallstreetbets:*` (Tradestie's top-50 comment counts and its own sentiment score for the previous US/Eastern day). `tests/test_hermes_sec_only.py` scheduler tail re-pinned with a lineage comment.

Non-obvious decisions:

- Candidates checked from grid-svr on 2026-11-18: PullPush answers 429 "does not provide free scraping resources for agents" (rejected: it refuses this use); SwaggyStocks has no public API (a JS dashboard; scraping it would mean browser automation); Apify/RapidAPI wrappers need an account; GitHub "no auth Reddit" wrappers call the dead `.json` endpoints. Arctic Shift's aggregate endpoints read 0 for every recent hour (they lag days), so counts come from records; its full-text title search on r/wallstreetbets mostly times out, so the daily thread is picked from the newest posts by title prefix.
- Arctic Shift re-reads scores about 36 h after a post; younger posts report `score` None rather than the archive's 0/1 (or an early large number), so no score logic ever sees an unsettled post. A security review flagged that this leaves smart_money's score gate inert (one throwaway account could seed a signal). Gates now: (a) at least two distinct authors per (ticker, direction) in the pull, and (d) unscored signals are stored in raw_series with `trust_tier: "unscored"` but kept out of `signal_sources` (the downstream smart_money / social streams) until corroborated across time (`trust_tier: "unscored_promoted"`). A second review found the first version split "prior" from "now" by row `pull_timestamp`, so re-pulled posts could count twice. Promotion now works on post creation time: fresh posts are this pull's posts no earlier run stored (by `post_id`, now kept in the payload, or author + text hash for older rows); prior authors come from stored posts created at least 6 h before the earliest fresh post (within 48 h); promotion needs two or more prior authors and a fresh post by someone else. Arctic Shift exposes no account creation time, so there is no account-age gate.
- reddit_options_pulse picked the daily thread by title prefix alone, newest first, so a user post with the same title could be taken. It now also requires the posting account: u/verified-trader (the board's moderator account; live check 2026-11-18 showed it posted the last three daily threads, `distinguished="moderator"` on the "Moves Tomorrow" ones, none on the daily one) or AutoModerator (earlier years), and takes the first such post of the newest day. AutoModerator alone would match nothing today. `stickied` cannot be requested as a field from Arctic Shift, and `distinguished` is absent on the daily thread, so neither is required.
- Tradestie is asked for the finished previous US/Eastern day and stored once per Tradestie day (`tradestie_date` in the payload); obs_date stays the fetch date.

Broken or TBD:

- Needs merge + deploy (SmartScheduler picks up smart_money's hourly cadence and the new legs; intelligence scheduler's 22:00 options pulse uses the new fetch). No activation step.
- Arctic Shift publishes no uptime guarantee; a 429/422 burst shows as PARTIAL/FAILED in pull_log, never as zero rows passed off as data.

## 2026-11-09 — Bind daily grader ingestion confirmations to actual bytes

The unchanged `--no-ingest` path could invent `ingested_review_sha256`, causing the
next normal grader run to skip a review that had never reached the database. The
same path could suppress recovery after a failed hand-off. The grader also trusted
an ingest status without retaining the hash of the bytes the ingest actually read.

- Require an explicit confirmation containing the actual review hash, packet hash
  and positive database run ID returned by a fully accepted ingest. Preserve those
  hashes through the hand-off wrapper. Missing or mismatched evidence is visible
  through `ingest_confirmation_error` and exit 3, with no certified ingestion.
- Ignore legacy bare hash claims when deciding whether to skip ingestion. The next
  normal run inside the existing lookback reconciles through the unique-hash,
  idempotent ingest. Older days remain a separate historical reconciliation task.
- Preserve genuine bound confirmations for unchanged reviews and retain the
  producer, sole grader ownership, existing schedules and append-only DB history.
- Add ten offline regression tests, including actual file replacement at hand-off,
  seven malformed result cases, failed recovery, legacy claims and success reuse.
  Update existing mocks to report the actual review and packet identities.

Validation: 202 tests and 16 subtests pass across grader, receipt recovery, ingest,
packet producer and packet status suites with socket connections blocked. Changed
Python lint, compilation and diff checks pass. Independent synthetic review passed.
Full backend/CI results are separate. No production access, service changes,
scheduler changes, historical backfill, merge or deployment performed. Hash-bound
receipts detect wrong-byte hand-off but do not serialize concurrent writers.

## 2026-11-08 - refresh daily recap draft against current main

- Integrated main `1d0a84912` into draft #1032. The only conflict was this
  append-only log; both complete sections were preserved. Email code and tests
  are unchanged from reviewed head `e9f3d853`.
- All 508 guarded offline email tests passed after integration. Changed-file
  Ruff, compilation and diff checks passed. Synthetic quiet/active previews at
  1000px and 390px retained recap bullets, purple accents and no overflow.
- No production query, SMTP send, service restart, PR merge or deployment.
  The new draft head requires its own hosted CI and later delivery validation.

## 2026-11-09 — Fail closed on incomplete gateway evidence (Codex)

- Extended existing decision_gateway and freshness_guard; no new pipeline.
- Failed/invalid red-team or stress checks and failed gateway stages cannot
  produce a positive verdict or ticket. Empty red-team fallback reports no
  longer turn model failure into zero risk. Valid zero-risk counterarguments
  and sufficient-data behavior remain supported.
- Future observations are INVALID rather than FRESH. Existing callers only log
  this status; end-to-end source admission remains unqualified.
- Gateway opts into strict raw counter-score parsing; other consumers retain
  legacy tolerant defaults. Model-supplied grade overrides are ignored there.
- 252 relevant mocked/injected-client tests passed before publication; full remote
  CI and runtime acceptance remain separate. No live providers, production DB,
  scheduler, migration, deployment or trading changes.
- See docs/reference/SYNTHESIS_EVIDENCE_FAILURE_BOUNDARY.md for the contract,
  limits and relationship to #1083/#1090/#960.

## 2026-11-09 - AV draft integration after publication-review landing

- Integrated main cd55457 into existing draft #1051 and preserved both sides of
  the append-only log conflict. No source consumer contract was changed.
- Found sibling AV and publication-review migration heads. Added a no-op merge
  revision preserving both parent histories and refusing unsafe downgrade.
- 144 focused offline AV/analyzer/date/digest/raw-read and migration-graph tests
  passed, with 2 subtests and outbound socket connections blocked. The new graph
  test initially missed annotated metadata; corrected it before the passing run.
- Changed-file Ruff and authored-diff checks passed. No actual PostgreSQL replay
  or full repository suite was rerun locally; new-head hosted CI is still needed.
- Shared-account budgeting, delayed-dispatch pacing, historical reconciliation,
  metadata breadth and separate canary approval remain rollout gates. No provider
  calls, production access, scheduler changes, merge to main or deployment.

## 2026-11-21 — SEC #1050 applied-FX identity correction (Codex)

- Owner-directed source-only continuation from `1d0b3a1b`; merged main
  `427dbc247` with main's append-only entries first and recomputed 16 forward
  pins. All 14 HOLD pins remain unchanged. No other PR or runtime was touched.
- Snapshot identity now retains only supplied FX rates the normalizer applies.
  USD bypasses FX; foreign conversion, fallback confidence and overflow replay
  retain their existing behavior. Unrelated rate movement is identity-neutral.
- 56 snapshot/admission/manifest tests passed with the requested light venv,
  serial nice priority and two-thread BLAS limits. Fixtures only; no providers,
  SSH, production access, PR merge or deployment.
- The supplied worktree's shared Git metadata is read-only. Task Git metadata
  is isolated in `.codex-task-git` inside this worktree; use
  `git --git-dir=.codex-task-git` here to inspect the actual task branch.
- Generation identity and the migration-head join remain for the next commits;
  Claude reviews the pushed head before Anik decides whether to merge.

### SEC #1050 content generation correction

- Generation now hashes ordered actor/snapshot identities and TTM source IDs /
  quarter fingerprints. Request/receipt/compute times remain metadata. Identical
  reobservations and TTM recomputation preserve the generation; changed content,
  peer membership and readiness still invalidate reports and caches.
- Updated the old reobservation/compute-time expectations and added a private-PG
  regression proving an unchanged refetch with moving unused FX advances head
  metadata without duplicating the archive or hiding either current report.
- Focused output/cache/PG files: 38 passed, 194 PostgreSQL tests skipped locally.
  A local test fence blocks sockets and PostgreSQL connections. Native SQL/view
  execution remains for CI, as requested. No assertions were relaxed for the
  light venv's out-of-range SQLAlchemy 2.1.8.

### SEC #1050 migration join and local qualification

- Added the requested no-op join of `sec_pub_merge_20261109` with
  `merge_av_wing_20261120`. Fetched main `427dbc247` already included its child
  `research_findings_20261121`, so a second no-op join leaves exactly one head:
  `merge_sec_main_20261121`. All IDs fit 32 characters. The graph regression
  checks one head and retained ancestry without pinning a particular tip.
- Ran all 26 PR test files plus `tests/test_alembic_single_head.py`, serially
  with `nice -n 10`, two-thread BLAS limits and socket/PG fences. The supplied
  dependency path shadows the venv's SQLAlchemy 2.1.8 with 2.1.9 and caused 35
  SQL-text assertion failures. No assertions or installed packages were changed.
- Re-ran the same 27 files using existing cached SQLAlchemy 2.0.58, within the
  repository's `<2.1` constraint: 679 passed, 201 skipped, one missing-yfinance
  failure in `TestTiingoNews::test_default_tickers_drop_untaggable_symbols`.
  PostgreSQL tests intentionally skip; actual database/view behavior remains
  for exact-head CI. No provider call or network dependency installation.
- Final strengthened migration-edge checks and all single-head/version/logging
  tests: 4 passed. Changed-file Ruff, Python parsing, all 16 forward / 14 HOLD
  source digests and `git diff --check` passed. Original base ancestry retained.
- Remaining review gates: Claude reviews the pushed head; Anik alone decides
  merge. No PR merge, deployment, production access, historical repair, SSH,
  outside-worktree report or coordination write was performed in this run.

## 2026-11-23 - 13F change rows: a re-run writes nothing new (draft only)

Branch `claude/13f-rerun-dedupe-20261123`. Anik: "fix the 13F duplicates".

- **Cause:** the 13F change-row loop in
  `ingestion/altdata/institutional_flows.py` used `BasePuller._row_exists`,
  whose window is one hour. Every `etf_flows` run more than an hour after the
  last appended every row again. grid-svr, 2026-11-23 23:30Z, source 280:
  166,760 `13F:%` rows for 41,690 distinct `(series_id, obs_date)`, exactly four
  identical copies each, all written since 17:24Z (the first run after #1393).
- **Fix:** the writer reads the newest stored vintage of each change row
  (one `IN` lookup per 500 series) and skips the row only when that vintage is
  the same observation: same `filing_accession` and `prev_filing_accession`,
  same value, identical payload. Anything else is stored as a new vintage.
  `BasePuller` is unchanged; no other puller is affected.
- **Not changed:** filing receipts (`13F-FILING:...`) stay one per run (#1393:
  the holdings reader ranks filings by receipt time). No reader is touched.
- **Readers and the existing copies:** `holdings_observations_13f` and
  `dollar_flows` take the newest vintage per key, so nothing is counted twice;
  the holdings' `received_at` is the newest copy's time. The two 13F counts in
  `intelligence/actor_discovery.py` (`COUNT(*)`, `SUM(value)`) count every copy.
- **Production rows:** untouched by this PR. Cleanup is a separate owner decision.
- **Codex HOLD 6031396853 (2026-11-24):** two overlapping writers could both
  look up before either committed and both insert. The lookup and the inserts
  now run under one transaction-scoped advisory lock per (source, manager,
  filing date) (`_lock_change_rows`, PostgreSQL only).
  `tests/test_13f_holdings_graph_pg.py` holds the first writer open inside its
  transaction on a second connection; without the lock both insert. Quarantined
  rows are left out before the ranking: an older identical SUCCESS under a newer
  QUARANTINED copy still counts as stored. A surviving change row is one stored
  observation of a filing comparison, not a filing.
