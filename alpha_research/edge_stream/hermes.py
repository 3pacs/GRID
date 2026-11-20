"""Hermes hook: start one edge-stream cycle a day, detached, never waited on.

Called from ``scripts/hermes_operator.py::run_cycle``. Without
``EDGE_STREAM_LEDGER_PATH`` / ``EDGE_STREAM_ANCHOR_PATH`` / ``EDGE_STREAM_DATA_DIR``
in grid-hermes' environment it does nothing. With them, once per UTC date at or after
:data:`SPAWN_HOUR_UTC` (Polygon has the previous session by then) it starts
``python -m alpha_research.edge_stream cycle`` in its own session with its own log and
a wall-clock limit, so a slow fetch can never hold a Hermes cycle or leave an orphan
thread on the ledger lock. The child takes the cycle lock itself; a second spawn the
same day (after a Hermes restart) finds the marker and is skipped.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ENV_KEYS = ("EDGE_STREAM_LEDGER_PATH", "EDGE_STREAM_ANCHOR_PATH", "EDGE_STREAM_DATA_DIR")
SPAWN_HOUR_UTC = 11
CYCLE_WALL_CLOCK_S = 5400
REPO_ROOT = Path(__file__).resolve().parents[2]


def maybe_spawn_cycle(now: datetime | None = None, env: dict | None = None, popen=subprocess.Popen) -> dict | None:
    env = dict(os.environ if env is None else env)
    if not all(env.get(k) for k in ENV_KEYS):
        return None
    now = now or datetime.now(timezone.utc)
    if now.hour < SPAWN_HOUR_UTC:
        return None
    data_dir = Path(env["EDGE_STREAM_DATA_DIR"])
    data_dir.mkdir(parents=True, exist_ok=True)
    marker = data_dir / "last_spawn_utc_date"
    today = now.date().isoformat()
    if marker.exists() and marker.read_text().strip() == today:
        return None
    cmd = [sys.executable, "-m", "alpha_research.edge_stream", "cycle"]
    timeout = shutil.which("timeout")
    if timeout:
        cmd = [timeout, str(CYCLE_WALL_CLOCK_S)] + cmd
    child_env = {**env, "OMP_NUM_THREADS": "2", "OPENBLAS_NUM_THREADS": "2", "MKL_NUM_THREADS": "2"}
    with open(data_dir / "cycle.log", "ab") as out:
        proc = popen(cmd, cwd=str(REPO_ROOT), env=child_env, stdout=out, stderr=subprocess.STDOUT,
                     stdin=subprocess.DEVNULL, start_new_session=True)
    marker.write_text(today + "\n")
    return {"spawned": True, "pid": getattr(proc, "pid", None), "date": today}
