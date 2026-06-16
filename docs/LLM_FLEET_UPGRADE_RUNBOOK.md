# LLM Fleet Upgrade & tok/sec Runbook

Goal: bring every general-reasoning LLM on the fleet **up to the Qwen 3.6+ quality bar** and tune each host for the highest tok/sec it can sustain **without dropping below the quality floor**. A low-quality LLM does more harm than good — speed never buys its way past correctness.

This runbook is executed **on the fleet** (grid-svr + nodes). The companion harness (`scripts/run_llm_autoresearch.py`, `llm/autoresearch/`) measures quality + tok/sec so you can verify each change.

---

## 0. The two Qwen 3.6 options

The Qwen 3.6 open-weight lineup is just two models — pick per host:

| Model | Type | Active params | Best for | GGUF source |
|---|---|---|---|---|
| **Qwen3.6-27B** | dense | 27B | Max quality where VRAM allows (≥24 GB) | `unsloth/Qwen3.6-27B-GGUF` |
| **Qwen3.6-35B-A3B** | MoE | **3B active** / 35B total | Speed + fitting constrained VRAM (expert offload) | `unsloth/Qwen3.6-35B-A3B-GGUF` |

Both support **native MTP (Multi-Token Prediction)** — the tok/sec lever. Enable with `--spec-type draft-mtp` (merged to llama.cpp master). Expect **1.4–2.2×** with **no accuracy change**.

> **Do not bother with a separate classic draft model for these.** Public benchmarks show classic draft (`-md`) yields ~no net speedup on Ampere + the A3B MoE. MTP is integrated into the weights and is the correct path.

---

## 1. Detect the real hardware first — do NOT trust hardcoded specs

Card configs go stale the moment hardware is swapped. **Resolve each host's
GPUs at runtime**, then let the planner derive recommendations from the
*actual* VRAM/arch.

> **Profiles are now self-updating from the live fleet dashboard.** The
> dashboard (`network.stepdad.finance/api/snapshot`, override via
> `GRID_FLEET_SNAPSHOT_URL`) SSH-polls every host on a fixed cycle and
> publishes per-GPU name/VRAM/architecture. `--plan` / `--audit` pull it live
> each run, so you usually **don't hand-edit `host_profiles.json` at all** —
> just run `--plan`. To cache a copy (offline fallback): `--refresh-profiles`.
> Heterogeneous boxes (the norm on this fleet — grid-svr A2000+Blackwell=28GB,
> koala 2070S+2060 Turing=20GB, panda 3×P100 Pascal=48GB) are summed and given
> the conservative capability intersection automatically. The hand-edit path
> below is only for hosts the dashboard can't reach.

Per-host manual detection (shells out to `nvidia-smi`):

```bash
# On each box (grid-svr, panda, ocr-node, koala, z400, ...):
python -m scripts.run_llm_autoresearch --detect "$(hostname)"
```

Paste each snippet into `llm/autoresearch/host_profiles.json` (real values as
detected 2026-05-26 — note these are mixed-card boxes, so set `arch`/
`flash_attn`/`fp8` explicitly; they can't be inferred from a single name):

```json
{
  "grid-svr": {"vram_gb": 14, "gpus": 2, "gpu_name": "RTX A2000 12GB, RTX PRO 2000 Blackwell", "arch": "mixed:ampere+blackwell", "flash_attn": true, "fp8": false},
  "panda":    {"vram_gb": 16, "gpus": 3, "gpu_name": "3x Tesla P100-PCIE-16GB", "arch": "pascal", "flash_attn": false, "fp8": false},
  "koala":    {"vram_gb": 10, "gpus": 2, "gpu_name": "RTX 2070 SUPER, RTX 2060", "arch": "turing", "flash_attn": true, "fp8": false}
}
```

For single-name single-arch hosts, `arch`/`flash_attn`/`fp8` are inferred from
`gpu_name`; for mixed boxes pin them as above (`vram_gb * gpus` = total VRAM).
Then print the plan derived from the resolved profiles:

```bash
python -m scripts.run_llm_autoresearch --plan
```

If a row shows `SRC = fallback` or you see the STALE warning, the override
file is missing/incomplete — fix it before planning. Never plan off the
baked-in fallback table; those numbers are wrong by design.

---

## 2. VRAM-tier decision tree (what `--plan` applies)

Recommendations key on **resolved total VRAM** + arch, so they stay correct
no matter which cards are in a box. Estimated need = weights + ~2.5 GB.

| Resolved VRAM | Model | Quant | Extra flags | Why |
|---|---|---|---|---|
| **≥ 40 GB** | Qwen3.6-27B dense | Q6_K | — | room for high quant + big ctx |
| **24–40 GB** | Qwen3.6-27B dense | Q5_K_M | — | dense at near-lossless quant |
| **16–24 GB** | Qwen3.6-35B-A3B MoE | Q4_K_M | `-ot ".ffn_.*_exps.=CPU"` | MoE (3B active) + expert-offload fits |
| **12–16 GB, multi-GPU ≥ 20 total** | Qwen3.6-27B dense | Q4_K_M | `-ts 1,1 -sm layer` | tensor-split dense across cards |
| **12–16 GB, single card** | Qwen3.6-35B-A3B MoE | IQ4_XS | `-ot ...` (heavy) | only on-bar option; expect lower tok/s |
| **< 12 GB** | — | — | — | **can't meet the Qwen 3.6 floor in VRAM — repurpose as embeddings/draft/utility node** |

Arch overrides applied automatically: **Maxwell/Pascal drop `-fa`** (no
Flash-Attention kernels); **Ada/Blackwell/Hopper** add FP8 KV cache.

---

## 3. `llama-server` launch lines by VRAM tier

Substitute the model/quant/flags `--plan` printed for the host. Templates:

### ≥ 24 GB — dense 27B (Blackwell/Ampere, Flash-Attention on)
```bash
llama-server -m /models/Qwen3.6-27B-Q6_K.gguf \
  --spec-type draft-mtp -fa -ngl 99 \
  --cache-type-k q8_0 --cache-type-v q8_0 \
  -c 16384 -b 2048 -ub 512 \
  --host 0.0.0.0 --port 8081 --metrics
```
(Use Q5_K_M in the 24–40 GB tier. Staying on Ollama instead: `ollama pull qwen3.6:27b-q5_K_M` — MTP is automatic.)

### 12–16 GB single card — 35B-A3B MoE with expert-offload
The `-ot` regex keeps attention on the GPU and pushes the big expert FFN
tensors to CPU RAM — the trick that fits a 35B MoE on a small card:
```bash
llama-server -m /models/Qwen3.6-35B-A3B-Q4_K_M.gguf \
  --spec-type draft-mtp -fa -ngl 99 \
  -ot ".ffn_.*_exps.=CPU" \
  -c 8192 -b 1024 -ub 256 \
  --host 0.0.0.0 --port 8081 --metrics
```
Tighter than ~14 GB: use `IQ4_XS` and lower `-ngl` to offload some attention layers too.

### multi-GPU (e.g. 2 cards) — tensor-split dense 27B; drop `-fa` on Maxwell
```bash
llama-server -m /models/Qwen3.6-27B-Q4_K_M.gguf \
  --spec-type draft-mtp -ngl 99 -ts 1,1 -sm layer \
  -c 8192 --host 0.0.0.0 --port 11434 --metrics
```
**No `-fa`** on Maxwell/Pascal — the kernels don't exist; it will fail or silently fall back.

> Wire whichever flags win into the host's systemd unit / `/etc/default/*-llama` so they survive restarts.

---

## 4. Speculative-decoding / KV tuning quick reference

| Flag | Effect | When |
|---|---|---|
| `--spec-type draft-mtp` | Native multi-token prediction | **Always** for Qwen 3.6 |
| `-fa` | Flash Attention (faster, less KV VRAM) | Ampere/Blackwell; **not** Maxwell |
| `--cache-type-k/v q8_0` | Quantize KV cache → more ctx / less VRAM | Any; q8_0 is near-lossless |
| `-ot ".ffn_.*_exps.=CPU"` | Offload MoE experts to CPU | MoE on tight VRAM (12–16 GB) |
| `-ts a,b -sm layer` | Tensor-split across GPUs | any multi-GPU host |
| `-ngl N` | Layers on GPU (99 = all) | Lower to trade speed for fit |
| `-b / -ub` | Batch / micro-batch | Lower on tight VRAM |

---

## 5. Verify every change with the harness (before / after)

```bash
# 1. See which endpoints are below the Qwen-3.6 bar right now:
python -m scripts.run_llm_autoresearch --audit

# 2. Baseline quality + tok/sec for everything (includes below-bar):
python -m scripts.run_llm_autoresearch --baseline --include-below-bar
#    -> note the JSONL journal path it prints

# 3. Apply a host change (deploy model + flags above, restart server).

# 4. Re-measure ONLY the eligible (now on-bar) endpoints:
python -m scripts.run_llm_autoresearch --baseline --quality-floor 0.7

# 5. Compare: quality must be >= floor AND tok/sec should rise vs. the
#    baseline journal. If quality dropped below floor, the change is
#    rejected — revert (a weak LLM does more harm than good).
```

**Acceptance criteria per host:**
- Model clears the Qwen 3.6+ bar (`--audit` shows PASS).
- Quality score ≥ your floor (default 0.6; the runbook examples use 0.7).
- tok/sec ≥ the pre-change baseline (MTP should give 1.4–2.2×).

Keep the winning launch line in the systemd unit and record the journal path in your ops notes.

---

## 6. Sources
- Qwen3.6 lineup & MTP: <https://github.com/QwenLM/Qwen3.6>, <https://unsloth.ai/docs/models/qwen3.6>
- GGUFs: `unsloth/Qwen3.6-27B-GGUF`, `unsloth/Qwen3.6-35B-A3B-GGUF`
- MTP merged / `--spec-type draft-mtp`; classic-draft "no net speedup on Ampere + A3B MoE": <https://github.com/thc1006/qwen3.6-speculative-decoding-rtx3090>
- 35B-A3B MoE on 12 GB via offload: <https://carteakey.dev/blog/running-qwen3-6-mtp-locally/>
