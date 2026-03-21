"""Fix model_eligible flags: IDs 1-25 FALSE (no data), IDs 102-136 TRUE (have data).

Idempotent — safe to run multiple times.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db import get_connection
from loguru import logger as log


def fix_model_eligible() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE feature_registry SET model_eligible = FALSE WHERE id BETWEEN 1 AND 25"
            )
            log.info("Set model_eligible=FALSE for {} rows (IDs 1-25)", cur.rowcount)

            cur.execute(
                "UPDATE feature_registry SET model_eligible = TRUE WHERE id BETWEEN 102 AND 136"
            )
            log.info("Set model_eligible=TRUE for {} rows (IDs 102-136)", cur.rowcount)

            # Verify
            cur.execute(
                "SELECT id, name, model_eligible FROM feature_registry ORDER BY id"
            )
            log.info("--- Verification ---")
            for row in cur.fetchall():
                log.info("  ID {:>4}: {:<35} eligible={}", row[0], row[1], row[2])


if __name__ == "__main__":
    fix_model_eligible()
