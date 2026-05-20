# AGENTS.md

Append-only running log for GRID system work.

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
- Added Hermes repair skill `CHECK_STORAGE[:target_id]` and dedicated subagent role `storage_maintainer`, backed by the new `hermes_storage_maintenance` goal-worker handler.
- Wired `scripts/hermes_operator.py` so the daily Hermes intelligence batch queues the storage maintainer subagent instead of relying on a manual command.
- Deployed the updated files to both server runtime trees with `scripts/deploy.py --snapshot`, restarted `grid-hermes` and `grid-goal-worker@grid-svr`, and verified both services active.
- Ran a live dispatch through the daily helper; goal `#4` completed and wrote `outputs/storage_maintenance/storage_maintenance_20260520T075333Z.{json,md}`.

Non-obvious decisions:

- No files were deleted or moved. The cleanup plan sets `delete_source=false` until DB ingest proof and checksum manifests exist.
- The scan found `/data` at about 77.5% used and `/mirror` at about 48.3% used. `/data/gdelt` is the dominant active footprint, but `/mirror/gdelt` already contains same-size copies for at least the largest sampled GDELT archive.
- The GDELT archive set appears downloaded, but ingest is not proven: live DB lacks `gdelt_events` and `gdelt_daily_summary`, while `scripts/parse_gdelt.py` only covers `/data/grid/bulk/gdelt` rather than the much larger `/data/gdelt/v1/events` and `/data/gdelt/v2/...` roots.

Broken or TBD:

- Extend or replace the GDELT parser so it ingests the real `/data/gdelt` archive roots, then build a checksum/ingest manifest before active-disk cleanup.
- Add a manifest-backed copy/remove executor after parser proof; the current subagent is intentionally inventory/plan only.
- The live `/home/grid/grid_v4/grid_repo` checkout is on `fix/hermes-active-hypo-scoring-dedent-2026-05-17`, ahead of origin with many dirty files. Do not hard-reset it; reconcile as a separate server-worktree cleanup.
