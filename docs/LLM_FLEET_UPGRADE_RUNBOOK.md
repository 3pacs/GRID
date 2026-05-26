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

## 1. Per-host plan

VRAM/arch from `llm/autoresearch/hosts.py`. Estimated VRAM = weights + ~2.5 GB overhead.

| Host | HW | Today (below bar) | Deploy | Quant | tok/sec levers |
|---|---|---|---|---|---|
| **grid-svr** | Blackwell ~48 GB, FP8+FA | `Qwen3-32B` (3.0<3.6) | **Qwen3.6-27B** (dense) | **Q6_K** (~22 GB) | `-fa`, MTP, FP8 KV cache, big ctx |
| **panda** | 24 GB Ampere | `qwen3.6:27b-q4` ✅ on bar | keep 27B, **bump quant** | **Q5_K_M** (~19 GB) | `-fa`, MTP |
| **ocr-node** | 16 GB Ampere | `gemma3:12b` | **Qwen3.6-35B-A3B** (MoE) | **Q4_K_M** + expert offload | `-fa`, MTP, `-ot` expert→CPU |
| **koala** | 2× 12 GB **Maxwell** | `gemma3:12b` | **Qwen3.6-27B** tensor-split | **Q4_K_M** (~17 GB / 24 GB) | MTP, `-ts 1,1`; **no `-fa`** (Maxwell) |
| **z400** | single 12 GB | `qwen2.5:7b` | **Qwen3.6-35B-A3B** (MoE) | **IQ4_XS** + heavy offload | MTP, `-ot` expert→CPU |

**Honest constraints:**
- **z400 (12 GB) cannot hold a Qwen 3.6 model fully in VRAM** — the smallest on-bar model is 27B dense (~17 GB at Q4) or the 35B MoE (~21 GB at Q4). Only the **MoE with expert-offload** is viable, and it will be slow. Consider repurposing z400 as an embeddings / draft / utility node instead of a reasoning endpoint.
- **koala is Maxwell (sm_52)** — no Flash Attention, no FP8, slow compute. Tensor-split 27B across both cards works but expect modest tok/sec. It is the weakest reasoning box; weigh effort vs. retiring it.
- **ocr-node at 16 GB** fits the 35B-A3B MoE only with partial expert-offload to CPU; the MoE tolerates this far better than a dense model because only 3B params are active per token.

---

## 2. Exact `llama-server` launch lines

### grid-svr — Qwen3.6-27B dense, max quality + MTP (Blackwell)
```bash
llama-server \
  -m /models/Qwen3.6-27B-Q6_K.gguf \
  --spec-type draft-mtp \
  -fa \
  -ngl 99 \
  --cache-type-k q8_0 --cache-type-v q8_0 \
  -c 16384 -b 2048 -ub 512 \
  --host 0.0.0.0 --port 8081 --metrics
```

### panda — Qwen3.6-27B dense, quality bump + MTP (Ampere, Ollama today)
If staying on Ollama: pull the higher quant tag (`ollama pull qwen3.6:27b-q5_K_M`) — MTP is automatic for Qwen3.6 in current Ollama. If moving to llama-server:
```bash
llama-server -m /models/Qwen3.6-27B-Q5_K_M.gguf \
  --spec-type draft-mtp -fa -ngl 99 \
  --cache-type-k q8_0 --cache-type-v q8_0 \
  -c 12288 --host 0.0.0.0 --port 11434 --metrics
```

### ocr-node / z400 — Qwen3.6-35B-A3B MoE with expert offload (fit 12–16 GB)
The `-ot` regex keeps attention on the GPU and pushes the big expert FFN tensors to CPU RAM — the trick that makes a 35B MoE fit a small card:
```bash
llama-server \
  -m /models/Qwen3.6-35B-A3B-Q4_K_M.gguf \
  --spec-type draft-mtp \
  -fa -ngl 99 \
  -ot ".ffn_.*_exps.=CPU" \
  -c 8192 -b 1024 -ub 256 \
  --host 0.0.0.0 --port 8081 --metrics
```
On z400 (tighter) use `IQ4_XS` and, if still OOM, lower `-ngl` to offload some attention layers too.

### koala — Qwen3.6-27B tensor-split across 2× Maxwell (no Flash Attention)
```bash
llama-server \
  -m /models/Qwen3.6-27B-Q4_K_M.gguf \
  --spec-type draft-mtp \
  -ngl 99 -ts 1,1 -sm layer \
  -c 8192 --host 0.0.0.0 --port 11434 --metrics
```
Note: **no `-fa`** — Maxwell lacks the kernels; adding it will fail or fall back.

> Wire whichever flags win into the host's systemd unit / `/etc/default/*-llama` so they survive restarts.

---

## 3. Speculative-decoding / KV tuning quick reference

| Flag | Effect | When |
|---|---|---|
| `--spec-type draft-mtp` | Native multi-token prediction | **Always** for Qwen 3.6 |
| `-fa` | Flash Attention (faster, less KV VRAM) | Ampere/Blackwell; **not** Maxwell |
| `--cache-type-k/v q8_0` | Quantize KV cache → more ctx / less VRAM | Any; q8_0 is near-lossless |
| `-ot ".ffn_.*_exps.=CPU"` | Offload MoE experts to CPU | MoE on tight VRAM (ocr/z400) |
| `-ts a,b -sm layer` | Tensor-split across GPUs | koala (2 cards) |
| `-ngl N` | Layers on GPU (99 = all) | Lower to trade speed for fit |
| `-b / -ub` | Batch / micro-batch | Lower on tight VRAM |

---

## 4. Verify every change with the harness (before / after)

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

## 5. Sources
- Qwen3.6 lineup & MTP: <https://github.com/QwenLM/Qwen3.6>, <https://unsloth.ai/docs/models/qwen3.6>
- GGUFs: `unsloth/Qwen3.6-27B-GGUF`, `unsloth/Qwen3.6-35B-A3B-GGUF`
- MTP merged / `--spec-type draft-mtp`; classic-draft "no net speedup on Ampere + A3B MoE": <https://github.com/thc1006/qwen3.6-speculative-decoding-rtx3090>
- 35B-A3B MoE on 12 GB via offload: <https://carteakey.dev/blog/running-qwen3-6-mtp-locally/>
