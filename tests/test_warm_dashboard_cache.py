"""Regression guard for scripts/warm_dashboard_cache.py's endpoint list.

/api/v1/flows/sectors was missing from ENDPOINTS when PR #441 landed the
stale-while-revalidate cache for it — the cron-driven warmer never touched
that endpoint, so the sector cache had no external re-warm path other than
the in-process background loop. This locks the endpoint in.
"""

from __future__ import annotations

import os

os.environ.setdefault("GRID_JWT_SECRET", "test-secret-key-for-testing-only")

from scripts.warm_dashboard_cache import ENDPOINTS  # noqa: E402


def test_flows_sectors_is_warmed():
    assert "/api/v1/flows/sectors" in ENDPOINTS


def test_endpoints_have_no_duplicates():
    assert len(ENDPOINTS) == len(set(ENDPOINTS))
