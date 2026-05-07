# Session Handoff - 2026-04-17b

**Branch/main at handoff:** `3d7c652` (PR #43 squash-merged to main).
**Scope:** Bring up redbox as a GRID compute node; wire it into the router as an opt-in QUICK-tier [[llama.cpp]] provider.
**Result:** redbox is a live Tailscale-reachable GRID node. grid-svr offloads news-sentiment and UX-audit LLM calls to it. Both other compute nodes (grid-svr, gridz4) remain running; gridz4 is unchanged and not yet wired.

---

## Start Here

The user was trying to recover redbox (an older AM3+ workstation) that was failing to boot after a CPU fan replacement, then pivoted to enrolling it as a compute node once it was back up. "z4" in conversation = `gridz4` (Tailscale `100.68.9.27`), a separate HP Z4 workstation already running [[llama.cpp]] — not touched in this session.

Local repo path:

```bash
cd /c/Users/anikd/dev/GRID
```

SSH access:

```bash
ssh grid@10.254.111.84       # redbox, LAN
ssh grid@100.126.129.45      # redbox, Tailscale
ssh grid@100.75.185.36       # grid-svr, Tailscale
ssh grid@100.68.9.27         # gridz4, Tailscale (no key yet — pw auth)
```

Deploy helper:

```bash
python3 scripts/deploy.py --snapshot path/to/file.py
ssh grid@grid-svr 'sudo systemctl restart grid-hermes grid-scheduler grid-api'
```

Note: `/home/grid/grid_v4/grid_repo` is a symlink to `/data/grid_v4/astrogrid_dedup` on grid-svr — the "dual-tree" setup in `scripts/deploy.py` is effectively one tree. Deploys land in one place.

---

## What Shipped In This Session

### Redbox hardware recovery

- RAM reseated (DRAM LED triggered after the fan swap); ASUS "Overclocking failed" cleared by loading optimized defaults and re-pointing boot priority.
- Ubuntu 24.04.4 LTS reinstalled cleanly from USB after earlier EFI-stub boot hang (root cause was an inoperable kernel/initrd on the disk; live USB booted fine, confirming hardware OK).
- Secure Boot disabled in BIOS (OS Type → Other OS). Booting now clean.
- NVIDIA driver 580.126.09 installed. Initial install pulled `-open` (Turing+ only) which failed for the Pascal GTX 1070 (`NVRM probe failed`). Swapped to proprietary `nvidia-driver-580`; both GPUs now live (`nvidia-smi` sees both).

### Redbox LLM serving

- [[llama.cpp]] built from source with CUDA: `~/llama.cpp/build/bin/llama-server`.
- Model: `Qwen3-14B-Q5_K_M.gguf` (bartowski), staged at `/data/models/`.
- systemd unit `llama-server.service` at `/etc/systemd/system/`:
  - `--host 0.0.0.0 --port 8080 -c 8192 -ngl 99 --split-mode row --alias qwen3-14b --reasoning off`
  - `--reasoning off` is load-bearing: Qwen3 otherwise spends all `num_predict` budget on chain-of-thought and returns empty `content` with reasoning tokens in `reasoning_content`. With reasoning off, QUICK-style prompts get direct one-word answers.
  - Auto-starts on boot, enabled. Log: `/var/log/llama-server.log`.
- Dual-GPU split: GTX 1070 (8 GB) + GTX 1660 SUPER (6 GB). About 12.2 GB VRAM occupied with Q5_K_M loaded.

### GRID router wiring (PR #43)

Merged to main as commit `3d7c652`:

- `config.py` — new `LLAMACPP_QUICK_*` pydantic fields (disabled by default):
  ```python
  LLAMACPP_QUICK_BASE_URL: str = "http://100.126.129.45:8080"
  LLAMACPP_QUICK_ENABLED: bool = False
  LLAMACPP_QUICK_TIMEOUT_SECONDS: int = 120
  LLAMACPP_QUICK_CHAT_MODEL: str = "qwen3-14b"
  ```
- `llm/router.py` — `_create_llamacpp_quick_client` factory + `llamacpp_quick` case in `_create_client` dispatch. Mirrors the existing `_create_llamacpp_oracle_client` pattern. Callers opt in via `get_llm(provider="llamacpp_quick")`; no tier auto-routes here, so existing `Tier.LOCAL/REASON/ORACLE` callers are unchanged.
- `tests/test_llamacpp_quick.py` — 3 tests (dispatch routing, disabled→None, enabled→configured client). All pass locally and on grid-svr.
- `ingestion/altdata/news_scraper.py:216` — first QUICK opt-in (news sentiment).
- `scripts/ux_auditor.py:362` — second QUICK opt-in (UX summary).
- `.env` on grid-svr (NOT in the repo): `LLAMACPP_QUICK_ENABLED=true`, `LLAMACPP_QUICK_BASE_URL=http://100.126.129.45:8080`, `LLAMACPP_QUICK_CHAT_MODEL=qwen3-14b`. Backup at `.env.bak-before-redbox`.

Fallback chain (`gemma → llamacpp → ollama → llamacpp_oracle → openrouter → openai`) still applies automatically when redbox is unreachable, so enabling this is zero-risk.

### Live verified

- `get_llm(provider="llamacpp_quick")` from grid-svr returns a `LlamaCppClient` pointed at `http://100.126.129.45:8080`, `is_available=True`.
- End-to-end test: system prompt `"Classify as bullish/bearish/neutral. One word only."`, user prompt `"Fed cuts rates by 50bp in emergency move"` → `"bullish"` returned in a single token.
- grid-hermes restarted (`sudo systemctl restart grid-hermes` — passwordless sudo works for `grid` on grid-svr) to pick up the new `news_scraper` code.

---

## Production State At Handoff

| Node | Tailscale | Role | State |
|---|---|---|---|
| redbox | 100.126.129.45 | Qwen3-14B Q5_K_M, port 8080 (--reasoning off) | active, systemd-managed |
| grid-svr | 100.75.185.36 | Nemotron-120B on :8081, Gemma-e2b classifier on :8082, all grid-* services | active |
| gridz4 | 100.68.9.27 | Qwen3.5-9B-Claude-Opus-Reasoning-v2 Q4_K_M, port 8080 | active, NOT wired to GRID |

Latest main: `3d7c652 add redbox quick-tier llama.cpp endpoint (#43)`.

PR #43: https://github.com/3pacs/GRID/pull/43 (merged, branch deleted).

---

## Known Gaps / Next Steps

1. **gridz4 is not wired.** It runs llama.cpp with Qwen3.5-9B-Claude-Opus-Reasoning-v2 Q4_K_M on :8080 but has no entry in `config.py` / `.env` / `llm/router.py`. The clean path is a sibling of `llamacpp_quick`: add `LLAMACPP_GRIDZ4_*` (or rename the tier so naming isn't redbox-specific — see #2). User explicitly said not to wire it this session.

2. **Naming is redbox-specific.** `llamacpp_quick` was chosen as the QUICK-tier label, but the *concept* is "medium-sized remote GPU llama.cpp endpoint." If more nodes get added, consider renaming to something like `llamacpp_remote_1`, `llamacpp_remote_2`, or adopting a pool/list config, instead of one field per node.

3. **Only two callsites opted in.** `news_scraper.py` and `ux_auditor.py` are the only two files routed to redbox. Other QUICK-style work still goes through `Tier.LOCAL` → gemma. Good candidates that do extraction/classification/summarization and could be migrated if redbox proves stable: `intelligence/llm_red_team.py`, `scripts/hermes_health.py`, `api/routers/watchlist_overview.py`, `api/routers/system.py`, `agents/config.py`. All already call `get_llm(Tier.LOCAL)` — one-line change each.

4. **SSH key to gridz4 missing.** From redbox, `ssh grid@gridz4` asks for password. If someone wants to script against gridz4 from another node, add redbox's pubkey (in `/home/grid/.ssh/id_ed25519.pub` on redbox, starts `ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAINhHwnfjWfZJKa822kKf4hr4raqW/g4BddeDqTtaGusy grid@redbox`) to `/home/grid/.ssh/authorized_keys` on gridz4.

5. **Test collection errors on grid-svr (unrelated).** `tests/test_circuit_breaker.py`, `tests/test_failure_analysis.py`, `tests/test_trade_logger.py` all fail collection with `ModuleNotFoundError` on missing `inference.failure_analysis`, `inference.trade_logger`, etc. Pre-existing, not caused by this session. Flag for cleanup.

6. **Env-var name drift.** The .env on grid-svr was initially added with `LLAMACPP_QUICK_MODEL` but the config.py field is `LLAMACPP_QUICK_CHAT_MODEL`. Fixed live in this session — `.env` now uses `LLAMACPP_QUICK_CHAT_MODEL=qwen3-14b`. If someone regenerates `.env` from an older template, re-check this.

7. **Nouveau i2c log spam on GPU 06:00.3.** The GTX 1660 SUPER's USB-C VirtualLink controller is unused by llama.cpp but the kernel retries it endlessly under nouveau. Proprietary driver mostly silences it, but occasional lines still show up in `dmesg`. Cosmetic — no action needed.

---

## Credentials / Config Nuances To Carry Forward

- redbox sudo requires password (no nopasswd). grid-svr has nopasswd sudo for `grid`.
- Tailscale MagicDNS works from redbox (`ssh grid@grid-svr`, `ssh grid@gridz4` resolve). On the Windows dev box it doesn't — use IPs.
- `huggingface-cli` is deprecated; use `hf` (installed under `/home/grid/hf-venv/bin/hf` on redbox). xet chunked-download puts the final file in `/data/models/` only at end of transfer — during download `ls /data/models` shows empty; progress is in `/data/models/.cache/huggingface/download/*.incomplete`.
- Secure Boot is OFF on redbox; MOK enrollment prompts during driver install can be bypassed with any password — re-enable SB requires re-running the MOK flow.

---

## Files Touched In This Session (for reference)

Repo changes (merged in PR #43):
- `config.py`
- `llm/router.py`
- `tests/test_llamacpp_quick.py` (new)
- `ingestion/altdata/news_scraper.py`
- `scripts/ux_auditor.py`

Out-of-tree (grid-svr live state, not in repo):
- `/data/grid_v4/astrogrid_dedup/.env` — added LLAMACPP_QUICK_* vars. Backup `.env.bak-before-redbox`.
- `/data/grid_v4/astrogrid_dedup/config.py.bak-before-quick-tier` — pre-deploy snapshot.
- `/data/grid_v4/astrogrid_dedup/llm/router.py.bak-before-quick-tier` — pre-deploy snapshot.

On redbox:
- `/etc/systemd/system/llama-server.service`
- `/data/models/Qwen_Qwen3-14B-Q5_K_M.gguf`
- `/home/grid/llama.cpp/` (built from source with `-DGGML_CUDA=ON`)
- `/home/grid/hf-venv/` (python venv for `hf` CLI)
- `/home/grid/.ssh/id_ed25519{,.pub}` (key added to grid-svr's authorized_keys)
