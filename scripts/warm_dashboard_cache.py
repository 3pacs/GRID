#!/usr/bin/env python3
"""Warm the dashboard cache by hitting slow endpoints.

Intended to run via cron at 6am, noon, 6pm, midnight so users
never hit a cold cache.
"""
import sys
import time

sys.path.insert(0, ".")

from api.auth import create_token
from loguru import logger as log

BASE = "http://localhost:8000"
TOKEN = create_token(role="admin")
HEADERS = {"Authorization": f"Bearer {TOKEN}"}

ENDPOINTS = [
    "/api/v1/intelligence/dashboard",
    "/api/v1/intelligence/thesis",
    "/api/v1/flows/aggregated",
    "/api/v1/regime/current",
    "/api/v1/system/status",
]


def warm():
    import requests

    for ep in ENDPOINTS:
        url = f"{BASE}{ep}"
        t0 = time.time()
        try:
            resp = requests.get(url, headers=HEADERS, timeout=120)
            elapsed = time.time() - t0
            log.info(
                "Warmed {ep} — {code} in {t:.1f}s",
                ep=ep,
                code=resp.status_code,
                t=elapsed,
            )
        except Exception as exc:
            log.error("Failed to warm {ep}: {e}", ep=ep, e=str(exc))


if __name__ == "__main__":
    log.info("Dashboard cache warm starting")
    warm()
    log.info("Dashboard cache warm complete")
