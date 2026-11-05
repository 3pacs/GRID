"""FinBERT's auto-created source_catalog row must satisfy source_catalog_latency_class_check.

It inserted latency_class 'BATCH', which the live CHECK rejects, so once the model could load (Python 3.14 venv,
2026-11-04) every FinBERT scoring run failed with CheckViolation for all five sources.
"""

from __future__ import annotations

import re
from pathlib import Path

from ingestion.base import VALID_LATENCY_CLASSES

SCORER = Path(__file__).resolve().parents[1] / "ingestion" / "ml" / "finbert_scorer.py"


def test_finbert_auto_create_uses_allowed_latency_class():
    src = SCORER.read_text(encoding="utf-8")
    m = re.search(r"VALUES \('FinBERT', '[^']*', '[A-Z]+', '(?P<latency>[A-Z]+)'", src)
    assert m, "FinBERT source_catalog INSERT not found"
    assert m.group("latency") in VALID_LATENCY_CLASSES
