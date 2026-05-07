---
source: /Users/anikdang/grid_obsidian/Dead-Ends/120B-CPU-Inference.md
promoted_at: 2026-04-13
promoted_via: OBSIDIAN-2 (task #76)
---
---
tags: [dead-end, llm]
---
# Dead End: 120B on CPU

**Date:** 2026-04-04
**Model:** Nemotron-3-Super-120B-A12B Q6_K (106GB)

## What Happened
- Loaded into 503GB RAM via mmap
- 0.18 tok/s — 7.5 minutes for a simple ticker extraction
- Split 10/88 layers GPU: same speed (GPU bottleneck on remaining CPU layers)
- Tried as ORACLE-only tier: request timed out after 15 minutes

## Why It Failed
MoE [[architecture]] needs ALL weights in fast memory. With 88 layers and only 10 on GPU, every token decode crosses the CPU-GPU boundary 78 times. CPU memory bandwidth (~50 GB/s) is the bottleneck, not compute.

## Don't Retry Unless
- Get 2x A6000 (96GB VRAM total) — fits entirely on GPU
- Or quantize to IQ2 (44GB) and split more aggressively

## Related
- [[Decisions/LLM-Model-Selection]]
