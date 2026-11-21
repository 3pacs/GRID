"""Server-side run of AstroGrid's twelve lens engines.

``astrogrid.engine_run`` had no producer (issue #500, ``docs/astrogrid-schema.md``):
rows came only from ``engine_outputs`` in a ``POST /astrogrid/interpret``
request, the scheduled celestial cycle sends none, so the table stayed empty.

The engines exist in one place, ``astrogrid_shared/prophecy.js``: the browser
runs them, and so does ``scripts/astrogrid_celestial_features.mjs`` for the
celestial-equity research. This module runs the same file under Node through
``scripts/astrogrid_engine_outputs.mjs`` rather than a Python port, so the rows
the server stores are the outputs of the code the site ships and cannot drift
from it. Each output carries the sha256 of the JS it came from.

Point-in-time: the engines read only the snapshot they are handed. They open
no database connection and no network; the knowledge time of a row is its
``created_at``. The caller pins the snapshot's ``timestamp`` to the day's
stable 12:00Z label (the same one ``sky_snapshot.snapshot_ts`` uses), because
the engines seed their phrasing from it: unpinned, two runs over one sky an
hour apart would disagree.

Graceful degradation: Node missing, a timeout, a non-zero exit or malformed
output returns no outputs and an ``error`` string. Nothing raises.
"""

from __future__ import annotations

import json
import math
import subprocess
from collections.abc import Sequence
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any

from loguru import logger as log

__all__ = [
    "ENGINE_PRODUCER",
    "RUNNER",
    "compute_engine_outputs",
    "pinned_engine_snapshot",
]

# Stamped into every scheduled row's raw_output (store.save_engine_runs).
ENGINE_PRODUCER = "astrogrid_cycle"

RUNNER = Path(__file__).resolve().parent.parent / "scripts" / "astrogrid_engine_outputs.mjs"

# prophecy.js and snapshot.js are ES modules in a directory with no
# package.json. The research runner uses the same flag; Node builds that
# dropped it detect ESM syntax on their own, so a "bad option" exit retries
# without it.
_ESM_FLAG = "--experimental-default-type=module"

_DEFAULT_TIMEOUT_SECONDS = 30.0
_STDERR_LIMIT = 400


def _snapshot_date(snapshot: dict[str, Any]) -> date:
    raw = str(snapshot.get("date") or "")[:10]
    return date.fromisoformat(raw)


def pinned_engine_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of ``snapshot`` with ``timestamp`` set to the day's 12:00Z.

    Matches ``store.astrogrid._stable_snapshot_ts`` and the research runner's
    labelling, so every run over one day's sky gives byte-identical output.
    """
    pinned = dict(snapshot)
    stable = datetime.combine(_snapshot_date(snapshot), time(12, 0, tzinfo=UTC))
    pinned["timestamp"] = stable.isoformat()
    return pinned


def _invalid_output(item: Any) -> str | None:
    if not isinstance(item, dict):
        return "engine output is not an object"
    if not str(item.get("engine_id") or "").strip():
        return "engine output has no engine_id"
    confidence = item.get("confidence")
    if (
        isinstance(confidence, bool)
        or not isinstance(confidence, (int, float))
        or not math.isfinite(float(confidence))
        or not 0.0 <= float(confidence) <= 1.0
    ):
        return f"engine {item.get('engine_id')!r} confidence {confidence!r} is not in [0, 1]"
    return None


def _run_node(node_bin: str, payload: str, timeout: float, with_flag: bool) -> subprocess.CompletedProcess[str]:
    argv = [node_bin, _ESM_FLAG, str(RUNNER)] if with_flag else [node_bin, str(RUNNER)]
    return subprocess.run(  # fixed argv, no shell
        argv,
        input=payload,
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=str(RUNNER.parent.parent),
        check=False,
    )


def compute_engine_outputs(
    snapshot: dict[str, Any],
    *,
    lens_ids: Sequence[str] = (),
    mode: str = "chorus",
    node_bin: str | None = None,
    timeout: float = _DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Run the shipped lens engines over one sky snapshot.

    Args:
        snapshot: A ``build_snapshot`` payload. Must carry ``date``.
        lens_ids: Lenses to run; empty runs all twelve (as ``chorus`` does).
        mode: Lens mode passed to the engines.
        node_bin: Node executable. Defaults to ``settings.ASTROGRID_NODE_BIN``.
        timeout: Seconds before the Node process is abandoned.

    Returns:
        ``{"engine_outputs": [...], "code_sha256": {...}, "error": str | None}``.
        On any failure ``engine_outputs`` is empty and ``error`` says why. Each
        output gains ``code_sha256`` and ``snapshot_built_at`` (the unpinned
        build time) so a stored row names the code and the build it came from.
    """
    result: dict[str, Any] = {"engine_outputs": [], "code_sha256": {}, "error": None}

    try:
        pinned = pinned_engine_snapshot(snapshot)
    except (TypeError, ValueError) as exc:
        result["error"] = f"snapshot has no usable date: {exc}"
        return result

    if node_bin is None:
        from config import settings

        node_bin = settings.ASTROGRID_NODE_BIN

    payload = json.dumps(
        {"snapshot": pinned, "lens_ids": list(lens_ids), "mode": mode},
        default=str,
    )

    try:
        proc = _run_node(node_bin, payload, timeout, with_flag=True)
        if proc.returncode != 0 and "bad option" in (proc.stderr or ""):
            proc = _run_node(node_bin, payload, timeout, with_flag=False)
    except FileNotFoundError:
        result["error"] = f"node executable not found: {node_bin}"
    except subprocess.TimeoutExpired:
        result["error"] = f"engine runner timed out after {timeout:g}s"
    except OSError as exc:
        result["error"] = f"engine runner could not start: {exc}"
    else:
        if proc.returncode != 0:
            stderr = (proc.stderr or "").strip()[-_STDERR_LIMIT:]
            result["error"] = f"engine runner exited {proc.returncode}: {stderr}"
        else:
            result.update(_parse_runner_output(proc.stdout, snapshot.get("timestamp")))

    if result["error"]:
        log.warning("AstroGrid lens engines unavailable: {e}", e=result["error"])
    return result


def _parse_runner_output(stdout: str, built_at: Any) -> dict[str, Any]:
    try:
        parsed = json.loads(stdout)
    except (TypeError, ValueError) as exc:
        return {"engine_outputs": [], "error": f"engine runner output is not JSON: {exc}"}

    outputs = parsed.get("engine_outputs") if isinstance(parsed, dict) else None
    if not isinstance(outputs, list) or not outputs:
        return {"engine_outputs": [], "error": "engine runner returned no engine outputs"}

    # Fail closed: one malformed engine means the set is not the shipped
    # twelve, and a partial set would read as a complete one later.
    for item in outputs:
        problem = _invalid_output(item)
        if problem:
            return {"engine_outputs": [], "error": problem}

    code_sha256 = parsed.get("code_sha256") if isinstance(parsed.get("code_sha256"), dict) else {}
    stamped = [
        {**item, "code_sha256": code_sha256, "snapshot_built_at": built_at}
        for item in outputs
    ]
    return {"engine_outputs": stamped, "code_sha256": code_sha256, "error": None}
