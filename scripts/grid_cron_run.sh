#!/usr/bin/env bash
# ============================================================
# Run one GRID Python job from the tree this script lives in (in production
# the release tree, /data/grid_v4/grid_release), with the GRID env file loaded.
# This is the crontab entry point for jobs that used to run as
# "cd /home/grid/grid_v4/grid_repo && /usr/bin/python3 ..." (OPS-W1).
#
# Usage:
#   grid_cron_run.sh -m scripts.auto_improve_from_postmortems
#   grid_cron_run.sh scripts/warm_dashboard_cache.py
#   grid_cron_run.sh --check TARGET [TARGET...]
#
# --check is a preflight. It loads the env file, checks the interpreter, then
# for each TARGET (a dotted module or a .py path under the tree) resolves it,
# parses it, and imports the modules it imports at top level. It never runs
# the target itself, so it writes nothing. Exit 0 means every target passed.
#
# Environment:
#   GRID_ENV_FILE  env file to load (default: see scripts/grid_cron_env.sh)
#   GRID_PYTHON    interpreter (default /usr/bin/python3, as the release units)
#   GRID_ROOT      tree to run from (default: this script's tree, symlinks
#                  resolved so one run never mixes two releases)
# ============================================================
set -euo pipefail

GRID_ROOT="${GRID_ROOT:-$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)}"
export GRID_ROOT

# shellcheck source=grid_cron_env.sh
source "$GRID_ROOT/scripts/grid_cron_env.sh"

if [[ $# -eq 0 ]]; then
    echo "usage: grid_cron_run.sh [--check TARGET...] | <python args...>" >&2
    exit 64
fi

grid_cron_load_env || exit $?

PY="$(grid_cron_python)"
if [[ ! -x "$PY" ]]; then
    echo "[$(date -Is)] ERROR: interpreter not executable: $PY (set GRID_PYTHON)" >&2
    exit 78
fi

cd "$GRID_ROOT"

if [[ "$1" == "--check" ]]; then
    shift
    if [[ $# -eq 0 ]]; then
        echo "usage: grid_cron_run.sh --check TARGET [TARGET...]" >&2
        exit 64
    fi
    echo "[$(date -Is)] CHECK root=$GRID_ROOT python=$PY env_file=$GRID_ENV_FILE"
    exec "$PY" - "$@" <<'PY'
import ast
import importlib
import importlib.util
import os
import sys

root = os.environ["GRID_ROOT"]
sys.path.insert(0, root)
print(f"python {sys.version.split()[0]} at {sys.executable}")
failures = 0


def top_level_imports(tree):
    names = []
    for node in tree.body:
        if isinstance(node, ast.Import):
            names.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            names.append(node.module)
    return list(dict.fromkeys(names))


for target in sys.argv[1:]:
    try:
        if target.endswith(".py"):
            path = os.path.join(root, target)
            if not os.path.isfile(path):
                raise FileNotFoundError(path)
        else:
            spec = importlib.util.find_spec(target)
            if spec is None or not spec.origin:
                raise ModuleNotFoundError(target)
            path = spec.origin
        real = os.path.realpath(path)
        if not real.startswith(os.path.realpath(root) + os.sep):
            raise RuntimeError(f"{target} resolves outside {root}: {real}")
        with open(real, encoding="utf-8") as fh:
            tree = ast.parse(fh.read(), filename=real)
        imported = top_level_imports(tree)
        for name in imported:
            importlib.import_module(name)
        print(f"OK   {target} ({len(imported)} top-level imports)")
    except Exception as exc:  # report every target, then fail
        failures += 1
        print(f"FAIL {target}: {type(exc).__name__}: {exc}")

sys.exit(1 if failures else 0)
PY
fi

exec "$PY" "$@"
