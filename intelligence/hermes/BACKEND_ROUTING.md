# Hermes backend routing — subscription vs API key

**TL;DR — before wiring the 6:30 batch scorer, split the lanes:**

| Lane | `HERMES_BACKEND` | Auth | Why |
|---|---|---|---|
| **Interactive / on-demand analyst** (you, ad-hoc `analyze`/`cli ask`) | `codex` | ChatGPT/Codex **subscription** (`codex login`) | No per-token cost; draws on the plan's included agentic usage. Great for human-paced use. |
| **Scheduled / batch** (the 6:30 `score_hypothesis` loop, any continuous server-side run) | `openai` | **API key** (`OPENAI_API_KEY` + `GRID_ALLOW_PAID_LLM=true`) | Predictable per-token billing; immune to the subscription's agentic-usage throttle. |
| **Always underneath** | (n/a) | local REASON-tier (llama/Qwen) | Free, on-prem fallback when neither cloud path answers. |

## Why not just run the batch off the subscription too?

Because it's the wrong tool for automation, and OpenAI says so:

- **Apr 2, 2026** — OpenAI re-metered Codex subscription usage to **token-aligned "agentic usage" limits** by plan; when you exceed them you get throttled / must buy credits.
- OpenAI's own guidance steers **automation / CI / headless** workloads onto an **API key** (platform billing), positioning the subscription for interactive/agentic use.

A long-running, scheduled Hermes daemon hammering a personal Codex subscription will burn the plan's agentic budget, then throttle mid-cycle — exactly when the 6:30 run needs to complete. The subscription is perfect for *your* interactive queries; the unattended batch belongs on an API key with a spend cap.

(Anthropic went further last year — weekly caps, pulled Claude Code from Pro — but **reversed** the plan to bill programmatic/Agent-SDK use separately on **2026-06-15**. OpenAI never "closed" the subscription path; it just metered it. Net: don't build the unattended lane on either consumer subscription.)

## Config recipes

**Interactive (default for the office / your laptop / grid-svr ad-hoc):**
```
HERMES_BACKEND=codex
# auth = `codex login` (Sign in with ChatGPT) on the host running grid-hermes
# optional: HERMES_CODEX_MODEL=gpt-5.5
```

**6:30 batch hypothesis scorer (unattended):**
```
HERMES_BACKEND=openai
GRID_ALLOW_PAID_LLM=true            # REQUIRED — the openai backend zeroes its key without this
OPENAI_API_KEY=sk-...               # (or HERMES_API_KEY=)
HERMES_MODEL=gpt-5.4                # or an o* reasoning model
HERMES_DAILY_SPEND_CAP_USD=5.00     # the cap only applies to the openai backend; set it before wiring
```

## Notes

- The **`GRID_ALLOW_PAID_LLM` gate** (in `config.load_hermes_config`) zeroes the openai-backend key unless explicitly opted in — so a stray `OPENAI_API_KEY` in the env can't silently start billing through the bridge. The `codex` backend is subscription-based and unaffected by this gate. The spend cap (`HERMES_DAILY_SPEND_CAP_USD`) and `SpendLedger` likewise apply only to the `openai` backend.
- `HermesAgent` always falls back to the local REASON-tier analyst on `None` from either cloud backend, so a throttle or outage degrades to local rather than failing the run.
- Provider/policy facts above were current as of **2026-06-21**; re-check the Codex rate card before relying on subscription headroom for anything unattended.
