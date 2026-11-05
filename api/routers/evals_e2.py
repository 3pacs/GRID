"""Read-only E2 forward-scoreboard endpoints.

``GET /api/v1/evals/e2/scoreboard`` serves the latest snapshot from the E2
ledger after verifying its hash chain and anchors; ``GET .../scoreboard.md``
serves the same snapshot as markdown. Neither writes, creates or locks
anything: ``evals.e2.report.load_board`` opens files read-only, a missing
board directory is reported as ``not_initialized`` and is not created, and a
broken chain is reported as ``chain_broken`` with no scores. The only writer
of the ledger is ``python -m evals.e2 run`` (the daily job).
"""

from __future__ import annotations

import os
from pathlib import Path

from fastapi import APIRouter, Depends, Query
from fastapi.responses import PlainTextResponse

from api.auth import require_auth

router = APIRouter(prefix="/api/v1/evals/e2", tags=["evals"])

DEFAULT_BOARD_DIR = "/data/grid/evals/e2_scoreboard"


def board_dir() -> Path:
    return Path(os.environ.get("GRID_E2_BOARD_DIR", DEFAULT_BOARD_DIR))


def _board() -> dict:
    from evals.e2 import VERSION
    from evals.e2.report import load_board

    return load_board(board_dir(), VERSION)


@router.get("/scoreboard")
def get_scoreboard(
    _token: str = Depends(require_auth),
    window: str = Query("all", pattern="^(all|last_20|last_60)$", description="aggregate window to return"),
    bucket: str = Query("official", pattern="^(official|pre_registration|any)$"),
) -> dict:
    """Latest E2 snapshot (verified ledger), filtered to one window and bucket."""
    board = _board()
    snapshot = board.get("snapshot")
    if snapshot:
        rows = [r for r in snapshot["aggregates"] if r["window"] == window and (bucket == "any" or r["bucket"] == bucket)]
        snapshot = {**{k: v for k, v in snapshot.items() if k != "aggregates"}, "aggregates": rows,
                    "aggregates_filter": {"window": window, "bucket": bucket}}
    return {"status": board["status"], "version": board["version"], "chain": board.get("chain"),
            "detail": board.get("detail"), "snapshot": snapshot}


@router.get("/scoreboard.md", response_class=PlainTextResponse)
def get_scoreboard_markdown(_token: str = Depends(require_auth)) -> str:
    from evals.e2.report import render_markdown

    board = _board()
    if board["status"] != "ok":
        return f"# E2 forward scoreboard\n\nStatus: {board['status']}. {board.get('detail') or ''}\n"
    return render_markdown(board["snapshot"])
