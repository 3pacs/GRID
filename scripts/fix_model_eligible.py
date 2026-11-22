"""RETIRED: Legacy one-off model_eligible fixer (IDs 1-25 / 102-136).

Retired 2026-11-21: Hardcoding ``WHERE id BETWEEN 1 AND 25`` disabled
``vix_spot`` (id 11) and ``yld_curve_2s10s`` (id 1), blinding ``auto_regime.py``
during live volatility shocks.

Use ``normalization.eligibility_audit`` (a dry-run report: observation count, freshness,
no future dates, non-zero variance) to see what should change, and ship any change as a
reviewed Alembic migration that records the pre-image of every row it touches.
"""

from __future__ import annotations

import sys


def fix_model_eligible() -> None:
    raise RuntimeError(
        "scripts/fix_model_eligible.py is retired because hardcoded ID ranges "
        "disabled vix_spot and yld_curve_2s10s. Run "
        "`python -m normalization.eligibility_audit` instead."
    )


if __name__ == "__main__":
    print(
        "RETIRED: use `python -m normalization.eligibility_audit` instead.",
        file=sys.stderr,
    )
    sys.exit(2)
