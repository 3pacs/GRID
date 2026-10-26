"""SPY GEX structural-levels forward paper log v1.

Implements ``docs/paper_log/gex-levels-v1-preregistration.md`` exactly.
That file is the source of truth; this package must never diverge from it.
If the pre-registration and this code ever disagree, the pre-registration
wins and the code has a bug.

Entry point: ``python -m paper_log.gex_levels {preopen|postclose|status|evaluate} --log-dir <dir>``
"""

from __future__ import annotations
