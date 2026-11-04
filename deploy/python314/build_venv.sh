#!/usr/bin/env bash
# Build GRID's Python 3.14 virtualenv on grid-svr (step 2b of the 3.14 move, Anik 2026-11-04), as user grid, from the
# runtime build_runtime.sh made. It changes nothing that runs: every GRID unit keeps /usr/bin/python3 (3.10) and the
# /home/grid/.local site-packages until the separate cutover. A failed build removes the half-built venv (nothing
# uses it); an existing venv is kept and only re-checked.
#
#   tar -C deploy/python314 -cf - . | ssh grid-svr 'd=/data/grid_v4/python314-src && mkdir -p $d && tar -C $d -xf - && bash $d/build_venv.sh'
#
# Installs: requirements.txt + deploy/python314/requirements-runtime.txt under deploy/python314/constraints-3.14.txt (the
# exact lock CI's "Python 3.14" job tests), plus torch==2.11.0+cu128 (production's version) from the PyTorch cu128 index.
# The lock files are read from this script's own directory (the copied deploy/python314/). Checks: pip check; every third-party top-level module GRID imports;
# every GRID .py file compiles under 3.14; torch CUDA visibility is printed (informational).
set -euo pipefail
umask 022

PY=/data/grid_v4/grid_python.runtimes-314/cpython-3.14.9/bin/python3.14
VENV=/data/grid_v4/grid_venv-314
RELEASE=/data/grid_v4/grid_release
TORCH='torch==2.11.0+cu128'
TORCH_INDEX=https://download.pytorch.org/whl/cu128

[ "$(id -un)" = grid ] || { echo "ABORT: run as grid (ssh grid-svr)" >&2; exit 2; }
[ -x "$PY" ] || { echo "ABORT: $PY missing; run build_runtime.sh first" >&2; exit 2; }
[ -f "$RELEASE/requirements.txt" ] || { echo "ABORT: no $RELEASE/requirements.txt" >&2; exit 2; }

check() {   # $1 = venv dir
  local v=$1
  "$v/bin/python" -V
  "$v/bin/pip" check
  "$v/bin/python" - "$RELEASE" <<'EOF'
import ast, importlib, os, sys, sysconfig
root = sys.argv[1]
stdlib = set(sys.stdlib_module_names)
local = {n.split(".")[0] for n in os.listdir(root)}
mods, bad_syntax, n = set(), [], 0
for d, dirs, files in os.walk(root):
    dirs[:] = [x for x in dirs if x not in (".git", "node_modules", "pwa", "venv", ".venv", "__pycache__")]
    for f in files:
        if not f.endswith(".py"):
            continue
        p = os.path.join(d, f); n += 1
        try:
            t = ast.parse(open(p, encoding="utf-8").read(), p)
            compile(t, p, "exec")
        except (SyntaxError, ValueError) as e:
            bad_syntax.append(f"{p}: {e}"); continue
        if "/tests/" in p:
            continue
        for x in ast.walk(t):
            if isinstance(x, ast.Import):
                mods |= {a.name.split(".")[0] for a in x.names}
            elif isinstance(x, ast.ImportFrom) and x.module and x.level == 0:
                mods.add(x.module.split(".")[0])
third = sorted(m for m in mods if m not in stdlib and m not in local and not m.startswith("_"))
missing = []
import signal, subprocess
for m in third:   # each import in its own process: a crash (e.g. SIGILL on this CPU) names the module, never hides the rest
    r = subprocess.run([sys.executable, "-c", f"import {m}"], capture_output=True, text=True, timeout=300)
    if r.returncode < 0:
        missing.append(f"{m} (killed by {signal.Signals(-r.returncode).name})")
    elif r.returncode:
        last = (r.stderr.strip().splitlines() or ["?"])[-1]
        missing.append(f"{m} ({last[:100]})")
print(f"compiled {n} GRID files under {sysconfig.get_python_version()}; syntax errors: {len(bad_syntax)}")
for b in bad_syntax[:20]:
    print("  SYNTAX", b)
print(f"third-party modules imported by GRID code: {len(third)}; not importable here: {len(missing)}")
for m in missing:
    print("  MISSING", m)
crashed = [m for m in missing if "killed by" in m]
if crashed:
    sys.exit(f"CRASHED on this CPU: {', '.join(crashed)}")
EOF
  "$v/bin/python" -c "import torch; print('torch', torch.__version__, '| cuda available:', torch.cuda.is_available())" || true
  "$v/bin/python" -c "import polars._plr as p; print('polars runtime:', getattr(p, '__file__', '?').split('/site-packages/')[-1].split('/')[0])" || true
}

src="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$src/constraints-3.14.txt" ] && [ -f "$src/requirements-runtime.txt" ] || { echo "ABORT: lock files not next to the script" >&2; exit 2; }

if [ -x "$VENV/bin/python" ]; then
  # update in place to the (possibly extended) lock; pip only adds or changes what differs. Nothing runs from it yet,
  # but a failed update keeps the venv for inspection instead of removing it.
  echo "venv exists: $VENV (updating to the lock, then re-checking)"
else
  trap 'rc=$?; [ $rc -ne 0 ] && { echo "FAILED (exit $rc): removing half-built $VENV" >&2; rm -rf "$VENV"; }' EXIT
  "$PY" -m venv "$VENV"
fi
"$VENV/bin/python" -m pip install -q --upgrade pip
nice -n 19 "$VENV/bin/pip" install -q -r "$RELEASE/requirements.txt" -r "$src/requirements-runtime.txt" -c "$src/constraints-3.14.txt"
nice -n 19 "$VENV/bin/pip" install -q "$TORCH" --index-url "$TORCH_INDEX"
"$VENV/bin/pip" freeze > "$VENV/FREEZE-3.14.txt"
check "$VENV"
echo "DONE: $VENV ($(wc -l < "$VENV/FREEZE-3.14.txt") packages). Nothing running uses it yet."
