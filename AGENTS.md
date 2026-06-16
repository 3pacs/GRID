# AGENTS.md

Append-only running log for GRID system work.

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
