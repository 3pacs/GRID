"""trade_edge tracker v2: large insider open-market buys, forward paper log.

Implements ``docs/paper_log/trade-edge-v2-preregistration.md``. That file is
the source of truth; where this package and it disagree, the code has a bug.

Entry point: ``python -m paper_log.trade_edge {run|status|verify} --log-dir <dir>``

Research only: read-only DB, no orders, no broker calls, no paid APIs. Every
output carries "UNPROVEN — not investment advice, research paper log" until
the pre-registered label rule (§10) says otherwise.
"""

from __future__ import annotations
