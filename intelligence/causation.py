"""
GRID Intelligence — Causal Connection Engine.

Connects actor trades to the public events that preceded them. Since slice
N2 (2026-09-27) a single-hop "cause" is only an event that was public
before the trade day (a reported earnings release, or a contract award GRID
had already seen), stamped with known_at — a time-ordered co-occurrence,
not proof of cause. See intelligence.causal_links for the rules and the
scheduled writer.

Key entry points:
  find_causes              — public events knowable before a single action
  batch_find_causes        — the same for all recent trades (bounded batches)
  get_suspicious_trades    — trades where the cause is likely non-public info
  generate_causal_narrative — LLM or rule-based "why is everyone trading X?"

This file is a backward-compatible facade. All implementation lives in:
  - intelligence.causation_core     — data classes, schema, constants, helpers
  - intelligence.causation_scoring  — single-hop cause checks, suspicious trades, narratives
  - intelligence.causation_graph    — multi-hop causal chains, chain detection
"""

# Re-export everything for backward compatibility
from intelligence.causation_core import *       # noqa: F401,F403
from intelligence.causation_scoring import *    # noqa: F401,F403
from intelligence.causation_graph import *      # noqa: F401,F403
