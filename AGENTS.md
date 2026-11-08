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
