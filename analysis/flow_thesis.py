"""
GRID — Flow Thesis Knowledge Base.

Maintains the system's unified understanding of how capital flows drive
markets. Each thesis is a named mental model backed by data: Fed liquidity,
dealer gamma, vanna/charm, institutional rotation, congressional signals,
insider clusters, cross-reference divergences, supply chain leading,
prediction markets, and trust convergence.

Key entry points:
  update_current_states(engine) — fill live data into each thesis
  generate_unified_thesis(engine) — combine all theses into one market view

This file is a backward-compatible facade. All implementation lives in:
  - analysis.flow_thesis_data      — FLOW_KNOWLEDGE, constants, state updaters
  - analysis.flow_thesis_scoring   — scoring, narrative, unified thesis generation
"""

# Re-export everything for backward compatibility
from analysis.flow_thesis_data import *      # noqa: F401,F403
from analysis.flow_thesis_scoring import *   # noqa: F401,F403
