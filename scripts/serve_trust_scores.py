"""
Long-lived Prefect server() process for refresh_trust_scores flow.

Registers a deployment with cron 30 2 * * * (UTC) and serves it
without requiring a separate work pool/worker. flow.serve() is the
single-process pattern recommended by Prefect 3.x for simple flows.

Run as systemd service grid-prefect-trust-scores.service.
"""
from __future__ import annotations

import os
os.environ.setdefault("PREFECT_API_URL", "http://localhost:4200/api")

from flows.refresh_trust_scores import refresh_trust_scores_flow


if __name__ == "__main__":
    refresh_trust_scores_flow.serve(
        name="refresh-trust-scores-nightly",
        cron="30 2 * * *",
        description=(
            "Nightly recompute of source_trust_scores_cached (2,771 sources). "
            "Eliminates ~12s of dashboard cold-load cost. "
            "Cache fallback: trust_scorer.load_trust_scores_cached() falls "
            "back to live update_trust_scores() if cache is missing or >24h."
        ),
        tags=["dashboard", "trust-scores", "nightly"],
    )
