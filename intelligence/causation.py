"""
GRID Intelligence — Causal Connection Engine.

Connects actor actions to the events, policies, and contracts that likely
drove them.  For every trade in signal_sources, this module searches for
the probable CAUSE — a government contract, legislation, earnings event,
committee hearing, macro release, or cluster signal — and scores its
likelihood.

Key entry points:
  find_causes              — all probable causes for a single action
  batch_find_causes        — run find_causes for all recent signal_sources
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
