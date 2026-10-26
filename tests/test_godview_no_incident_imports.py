"""Guard: god view code never imports the untracked incident modules (plan finding 6, slice G7).

The deployed trees (``/data/grid_v4/grid_release``, ``/data/grid_v4/grid_repo``)
still carry untracked incident files -- ``api/routers/god_view.py``,
``ingestion/god_view_materializer.py``, ``ingestion/altdata/*_materializer.py``,
``derivatives/dealer_gex_engine.py`` -- which wrote hard-coded fallback
constants instead of failing closed. The systemd units run
``scripts/run_godview_writers.py`` from such a tree, so an import of any of
them would silently execute incident code. This test fails on:

* a static import of an incident module anywhere in ``godview/`` or the runner;
* an incident module actually loaded after importing the runner and every
  writer (catches indirect imports through other packages);
* a tracked file at an incident path or a tracked ``god_view*`` module name
  (it would collide with the untracked copy on ``git checkout``).
"""

from __future__ import annotations

import ast
import json
import os
import pathlib
import subprocess
import sys

REPO = pathlib.Path(__file__).resolve().parent.parent

INCIDENT_MODULES: tuple[str, ...] = (
    "api.routers.god_view",
    "ingestion.god_view_materializer",
    "ingestion.altdata.cftc_materializer",
    "ingestion.altdata.commodity_warehouse_materializer",
    "ingestion.altdata.fed_liquidity_materializer",
    "ingestion.altdata.short_volume_ftd_materializer",
    "derivatives.dealer_gex_engine",
)
INCIDENT_PATHS: tuple[str, ...] = (
    "api/routers/god_view.py",
    "ingestion/god_view_materializer.py",
    "ingestion/altdata/cftc_materializer.py",
    "ingestion/altdata/commodity_warehouse_materializer.py",
    "ingestion/altdata/fed_liquidity_materializer.py",
    "ingestion/altdata/short_volume_ftd_materializer.py",
    "derivatives/dealer_gex_engine.py",
)


def _is_incident(name: str) -> bool:
    if name in INCIDENT_MODULES:
        return True
    return name.startswith("ingestion.altdata.") and name.endswith("_materializer")


def _scanned_files() -> list[pathlib.Path]:
    files = sorted((REPO / "godview").rglob("*.py"))
    files.append(REPO / "scripts" / "run_godview_writers.py")
    files.append(REPO / "api" / "routers" / "godview.py")  # G8 API
    return files


def _imported_names(path: pathlib.Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.extend(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            names.append(node.module)
            names.extend(f"{node.module}.{a.name}" for a in node.names)
        elif (
            isinstance(node, ast.Call)
            and getattr(node.func, "attr", None) == "import_module"
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            names.append(node.args[0].value)
    return names


def test_no_static_import_of_incident_modules():
    offenders = [
        f"{p.relative_to(REPO).as_posix()}: {name}"
        for p in _scanned_files()
        for name in _imported_names(p)
        if _is_incident(name)
    ]
    assert not offenders, "god view code imports an incident module:\n" + "\n".join(offenders)


def test_incident_module_names_do_not_appear_in_godview_sources():
    offenders = []
    for p in _scanned_files():
        text = p.read_text(encoding="utf-8")
        for mod in INCIDENT_MODULES:
            if f"import {mod}" in text or f"from {mod} " in text:
                offenders.append(f"{p.relative_to(REPO).as_posix()}: {mod}")
    assert not offenders, offenders


def test_runner_and_writers_load_no_incident_module_at_runtime():
    code = (
        "import sys, json\n"
        "import scripts.run_godview_writers as r\n"
        "r._writer('fed'); r._writer('cftc')\n"
        "import godview.fed_liquidity, godview.cftc_positioning\n"
        "print(json.dumps(sorted(sys.modules)))\n"
    )
    out = subprocess.run(
        [sys.executable, "-c", code],
        cwd=REPO,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
        env={**os.environ, "DB_PASSWORD": os.environ.get("DB_PASSWORD", "testpass")},
    )
    assert out.returncode == 0, out.stderr
    loaded = json.loads(out.stdout.strip().splitlines()[-1])
    bad = [m for m in loaded if _is_incident(m)]
    assert not bad, f"incident modules loaded at runtime: {bad}"


def test_no_tracked_file_at_an_incident_path_or_named_god_view():
    tracked = subprocess.run(
        ["git", "ls-files"], cwd=REPO, capture_output=True, text=True, check=True, timeout=60
    ).stdout.splitlines()
    at_incident = sorted(set(tracked) & set(INCIDENT_PATHS))
    assert not at_incident, f"tracked file collides with an untracked incident file: {at_incident}"
    named = [
        t for t in tracked
        if t.startswith(("godview/", "scripts/run_godview", "api/routers/godview")) and "god_view" in t
    ]
    assert not named, named
