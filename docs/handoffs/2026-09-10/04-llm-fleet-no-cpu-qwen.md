# 04 — No CPU-only Qwen: retire :8080, move its callers to a tailnet GPU node or Gemini

Branch: `claude/handoff-04-llm-fleet`. Lane: code + ops-exec (unit changes need sudo on the box).

## Operator directive (2026-09-10, verbatim intent)

"There should be no CPU-only Qwens — use another machine on the tailnet. This now
supersedes OCMRI. Or use one of the frontiers — if you can make Gemini do the
work, that is best."

## Current state (verified 2026-09-10)

- `grid-llamacpp.service` on grid-svr runs `/data/models/Qwen3.6-27B-Q4_K_M.gguf`
  with `LLAMACPP_NGL=0` (CPU only, ~17 GB RAM) on :8080. Its log shows nothing
  but `POST /v1/embeddings … 501` — llama-server is not started with
  `--embeddings`, so every embedding call fails.
- Config still points the disabled `gemma` provider at `http://localhost:8080`
  (`GEMMA_BASE_URL`, `GEMMA_EMBED_MODEL`) and `LLAMACPP_EMBED_MODEL` defaults to
  the chat model; `llamacpp/client.py::LlamaCppClient.embed` posts to
  `/v1/embeddings`. Find the live caller: grep `embed(` across `intelligence/`,
  `api/`, `scripts/`, `store/` and check `llm/router.py::get_llm(...).embed`
  fallbacks; the router's `ollama_koala` / `ollama_z400` providers already carry
  `nomic-embed-text`.
- GPU tiers are fine: grid-svr RTX 3090 Qwen3.8-27B (:8086 behind :8081),
  redbox qwen3.8-27b, gridz4 Qwen3.8-27B, Ollama :11434 qwen3.8:27b.
- Frontier: `GEMINI_API_KEY` exists in config and `/etc/grid/gemini.env` on the
  box; `gemini-task.yml` runs the Gemini CLI on grid-svr. Paid providers are
  gated by `GRID_ALLOW_PAID_LLM` (default False) — Gemini is a paid provider,
  so wiring it as an inference tier means the operator turns that flag on; say
  so in the PR.

## Steps

1. Map every consumer of :8080 and of `provider="llamacpp"` / `gemma` in the
   router chains (`_fallback_chain`). List them in the PR.
2. Embeddings: route through the router's embed path to a tailnet GPU Ollama
   node running `nomic-embed-text` (koala:11434 first, z400:11434 fallback,
   grid-svr :11434 last since it shares the 3090). Add an `EMBED_PROVIDER_CHAIN`
   setting with that order; `embed()` returns `None` when none answers
   (graceful degradation, no exception). Tests with a fake provider set.
3. Chat/REASON fallbacks: remove `llamacpp` (:8080) and `gemma` (:8080) from
   `_fallback_chain` for LOCAL/REASON/ORACLE; keep `llamacpp_z4`, `llamacpp_quick`,
   `llamacpp_oracle`, the Ollama nodes, then `openrouter`/`openai` behind the
   paid gate. Add a `gemini` provider (Google Generative AI REST, model from
   `GEMINI_CHAT_MODEL`, default the Flash model enabled on the account) behind
   the same paid gate, placed before `openrouter`. Mock-tested; never hit the
   network in tests.
4. Server: via ops-exec, `sudo systemctl disable --now grid-llamacpp` and
   update `server_setup/grid-llamacpp.service` in the repo to match (or delete
   it and document in `docs/SERVER-SERVICES.md`). Free the 17 GB. Confirm
   nothing else listens on :8080 afterwards and the health endpoint's
   `llm_available` stays true (it should read the :8081 shim, check
   `api/routers/system.py::health`).
5. OCMRI: `scripts/compute_coordinator.py` enforces an OCMRI priority floor and
   `yield_policy.yield_to must include 'ocmri'` for boogerbots jobs. The
   operator says this directive "supersedes OCMRI". Interpretation to confirm
   with the operator before changing code: GRID's own LLM/compute work no longer
   yields to OCMRI tenants. If confirmed, make the OCMRI yield requirement
   optional (config flag `COMPUTE_YIELD_TO_OCMRI`, default False) and keep the
   existing tests passing under the flag; if not confirmed, leave the
   coordinator alone and note it.
6. Docs: `CLAUDE.md` LLM bullet, `docs/SERVER-SERVICES.md`, `config.py` comments.

## Done when

No process on grid-svr serves a CPU-only Qwen; embedding calls succeed against a
tailnet GPU node (show one `embed()` round trip in the ops-exec log); router
tests pass; the OCMRI decision is recorded either way.
