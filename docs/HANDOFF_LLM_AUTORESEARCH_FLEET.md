# Handoff: Execute LLM Autoresearch on the Fleet

**For:** a CLI agent running **on the GRID fleet** (grid-svr or any host with tailnet/SSH, local `nvidia-smi`, and systemd/ollama control).
**From:** a Claude Code *web sandbox* session that built the tooling but **cannot reach the fleet** — its egress is a locked-down allowlist proxy (GitHub/PyPI only; `*.tailscale.com` and `*.stepdad.finance` return 403, no SSH client, no keys, internal hostnames don't resolve). So everything below has to run from inside the network.
**Branch:** `claude/llm-autoresearch` (PR #264). **Pull it before starting.**

---

## 0. Goal

Bring every general-reasoning LLM on the fleet up to the **Qwen 3.6+ quality bar** and tune each host for the **highest tok/sec it can sustain without dropping below the quality floor**. Hard rule: *a low-quality LLM does more harm than good* — speed never buys its way past correctness.

Two things to deliver:
1. A correct, committed `llm/autoresearch/host_profiles.json` (real hardware, detected — the specs in `config.py` are **stale, do not trust them**).
2. Each host upgraded + tuned, with before/after quality+tok/sec proof from the harness.

Optional follow-on: feed detected hardware to `network.stepdad.finance` so its card list stays current (§6).

---

## 1. What's already built (read these first)

| Path | What it is |
|---|---|
| `llm/autoresearch/registry.py` | Endpoint discovery + Qwen-3.6 quality-bar gate (`assess_model`) |
| `llm/autoresearch/bench.py` | tok/sec measurement + quality scoring vs `evals/grid_eval.jsonl` |
| `llm/autoresearch/hosts.py` | runtime GPU detection, `host_profiles.json` loader, VRAM-tier `recommend_for_host()` |
| `llm/autoresearch/loop.py` | multi-objective keep/discard loop (hard quality gate, Pareto, JSONL journal, pluggable `ConfigApplier`) |
| `scripts/run_llm_autoresearch.py` | CLI: `--detect`, `--plan`, `--audit`, `--baseline` |
| `docs/LLM_FLEET_UPGRADE_RUNBOOK.md` | the per-tier upgrade procedure + exact `llama-server` launch lines |

Run the tests once to confirm the env is sane: `python -m pytest tests/test_llm_autoresearch.py -q` (28 should pass).

---

## 2. Hosts in scope

From `config.py` (URLs are probably still valid; **hardware specs are stale**):

| Host | Endpoint | Served today |
|---|---|---|
| grid-svr | llama.cpp `:8081` | `Qwen3-32B` (3.0 < 3.6 → **below**) |
| panda | retired from GRID routing | down for the foreseeable future; do not target `panda:11434` |
| ocr-node | ollama `ocr-node:11434` | `gemma3:12b` (below) |
| koala | ollama `koala:11434` | `gemma3:12b` (below) |
| z400 | ollama `z400:11434` | `qwen2.5:7b` (below) |
| grid-svr | gemma micros `:8082-85` | task-specific fine-tunes — **exempt** from the bar, leave alone |

Discover the live list anytime: `python -m scripts.run_llm_autoresearch --audit`.

---

## 3. Task sequence

### Step 1 — Resolve real hardware (now self-updating)
Profiles auto-resolve from the live fleet dashboard
(`network.stepdad.finance/api/snapshot`, which SSH-polls every host on a fixed
cycle). `--plan` / `--audit` pull it live each run, so in the normal case you
**don't build `host_profiles.json` by hand** — just verify:
```bash
python -m scripts.run_llm_autoresearch --plan   # rows show SRC=snapshot
python -m scripts.run_llm_autoresearch --refresh-profiles   # optional: cache an offline copy
```
Any host the dashboard didn't cover this poll shows `SRC=fallback` / the STALE
warning — for those, SSH in and `--detect "$(hostname)"`, then paste the
emitted snippet (which now includes explicit `arch`/`flash_attn`/`fp8`, needed
for mixed-card boxes) into `host_profiles.json`.

Real fleet as detected 2026-05-26 (all mixed-card): grid-svr A2000+Blackwell
**28 GB / Ampere+Blackwell**, panda **3× P100 = 48 GB / Pascal** (was 4 — one
dropped), ocr-node 3060+3050 **20 GB / Ampere**, koala 2070S+2060 **20 GB /
Turing** (NOT Maxwell), gridz4 Blackwell+A2000 **36 GB**, redbox 1070+1660S
**14 GB**, z400 **GPU driver down**.

### Step 2 — Baseline (the "before")
```bash
python -m scripts.run_llm_autoresearch --baseline --include-below-bar
```
Save the JSONL journal path it prints. This is the pre-upgrade quality + tok/sec for every endpoint.

### Step 3 — Upgrade + tune each host
Follow **`docs/LLM_FLEET_UPGRADE_RUNBOOK.md` §3** for the host's VRAM tier. In short:
- Pick model from `--plan` (Qwen3.6-27B dense if VRAM ≥ 24 GB; Qwen3.6-35B-A3B MoE + `-ot` expert-offload for 12–16 GB; tensor-split on multi-GPU; **< 12 GB cannot meet the bar — repurpose, don't force it**).
- GGUFs: `unsloth/Qwen3.6-27B-GGUF`, `unsloth/Qwen3.6-35B-A3B-GGUF`.
- **Always** enable native MTP speculative decoding: `--spec-type draft-mtp` (do **not** add a classic `-md` draft model — benchmarks show no net gain on Ampere/MoE).
- `-fa` on Ampere/Ada/Blackwell/Hopper; **never on Maxwell/Pascal** (koala = Maxwell).
- For Ollama hosts, the equivalent is `ollama pull qwen3.6:27b-q5_K_M` (MTP automatic).
- Persist the winning flags in the host's systemd unit / `/etc/default/*-llama`, then restart the service.

### Step 4 — Verify (the "after"), enforce the gate
```bash
python -m scripts.run_llm_autoresearch --baseline --quality-floor 0.7
```
Acceptance per host:
- `--audit` shows **PASS** (model clears Qwen 3.6+).
- quality ≥ floor (0.7 used here).
- tok/sec ≥ the Step-2 baseline (MTP should give 1.4–2.2×).

**If quality dropped below the floor, REVERT the change.** A fast but weak model is a regression, not a win. Record the result either way.

### Step 5 — Commit & report
- Commit `host_profiles.json` and any winning launch-config notes.
- Per the CLAUDE.md standing rule, finish with `agent-report <agent> llm-fleet-upgrade <body.md>` summarizing: what changed per host, before/after quality+tok/sec, what's blocked, what's left.

---

## 4. Guardrails (do not violate)

- **Quality bar = Qwen 3.6+.** Below-bar models are rejected; the gate is hard.
- **Don't trust `config.py` / the fallback table for hardware** — always `--detect`.
- **Don't distribute SSH private keys or paste secrets into logs.** Prefer Tailscale SSH (ACL-based) / existing fleet auth.
- Standard GRID rules still apply: parameterized SQL only, no secrets in commits, PIT correctness for any data path you touch.
- Don't touch the gemma micro models (`:8082-85`) — they're narrow task fine-tunes, exempt from the general-reasoning bar.

---

## 5. If you want to extend the loop (optional)

The `ConfigApplier` protocol in `loop.py` lets the loop *search* configs automatically instead of you applying them by hand. Implement one that:
- restarts `llama-server` with candidate flags (or `ollama` model swaps),
- returns `(base_url, model)` once healthy,
and feed `AutoResearchLoop.run([...candidate TrialConfigs...], budget_seconds=...)` a search space (model × quant × `-ngl`/KV/ctx). It will keep the fastest config that clears the quality floor and journal every trial. Realize this against the **real systemd launch command** (which lives on grid-svr, not in the repo).

---

## 6. Optional: keep `network.stepdad.finance` card list current

The detection in `hosts.py` is the data source. Wire a small recurring job (Hermes step) that, on each host, runs `detect_local_profile(hostname)` and POSTs the result to a fleet inventory endpoint the dashboard reads. That makes the site's card list self-updating instead of hand-maintained. (The web sandbox can't build/see that page — it's behind auth + the 403 proxy — so it belongs to a fleet agent.)

---

## 7. Open questions for the human (resolve if blocked)

- Confirm current GPUs per host (Step 1 answers this definitively).
- For hosts < 12 GB that can't hold any Qwen 3.6 model: repurpose as embeddings/draft/utility, or retire? (Default: repurpose, don't force a sub-bar model back on.)
- Where do GGUF model files live on each host / is there shared model storage to populate?
