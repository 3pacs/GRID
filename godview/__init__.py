"""godview — writers for the God View pillars (materialize.py orchestrator lands in G7).

Package rules (see docs at C:/Users/owner/Documents/Codex/2026-09-14/wha/outputs/
GRID-GODVIEW-MATERIALIZATION-PLAN-20260926.md, finding 6):

* Never import ``ingestion.god_view_materializer``, any ``ingestion.altdata.*_materializer``
  module, ``derivatives.dealer_gex_engine``, or ``api.routers.god_view`` — those are the
  untracked incident modules (still present in the deployed trees, not in this repo's
  history) that wrote hard-coded fallback constants instead of failing closed.
* A tracked module here is never named ``god_view*`` — only ``godview`` — so it cannot
  collide with the untracked incident files on a `git checkout` in a deployed tree.
"""

from __future__ import annotations
