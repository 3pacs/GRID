"""Retired Finviz import compatibility; use the existing SEC EDGAR source.

The owner approved replacement with free SEC filings. Construction refuses
before database registration or any browser/provider access. The actual
Finviz catalog deactivation remains a separately verified root operation.
"""

from typing import Any


class FinvizScraperPuller:
    """Prevent stale callers from scraping or resurrecting the retired feed."""

    def __init__(self, db_engine: Any) -> None:
        raise RuntimeError("Finviz retired; use SECEdgarCompanyPuller")
