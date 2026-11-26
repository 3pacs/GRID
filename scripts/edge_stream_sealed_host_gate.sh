#!/bin/bash
# Exact-host gate for the edge stream's sealed trees (alpha_research/edge_stream/sealed).
#
# A registration's code hash carries numpy's major.minor and Python's ast.dump, so a sealed tree
# reproduces its hashes only under grid-hermes' own interpreter. This runs
# tests/test_edge_stream_sealed.py at a pinned commit under that interpreter on the host and then
# prints, for each tree, its byte check and which sealed registration hashes it reproduces.
#
# It never touches grid_release, the live ledger or the live data dir. The commit is exported with
# `git archive` (only the paths below) into a new stage directory under STAGE_ROOT, and nothing is
# written outside that directory (no bytecode, no pytest cache).
#
# Usage: scripts/edge_stream_sealed_host_gate.sh <commit> [host] [stage-root]
# Exit 0 only if every test passed with none skipped and every sealed hash reproduced.
set -euo pipefail
commit=$(git rev-parse --verify "${1:?usage: $0 <commit> [host] [stage-root]}^{commit}")
host=${2:-grid-svr}
root=${3:-/data/agent-home/anikdang}
py=/data/grid_v4/grid_venv-314/bin/python
stage="$root/edge-sealed-gate-${commit:0:12}-$(date -u +%Y%m%dT%H%M%SZ)"
paths=(alpha_research analysis pytest.ini tests/conftest.py tests/test_edge_stream_sealed.py)

ssh -o BatchMode=yes "$host" "mkdir -- '$stage'"  # refuses an existing directory
git archive "$commit" "${paths[@]}" | ssh -o BatchMode=yes "$host" "tar -x -C '$stage'"
echo "commit $commit -> $host:$stage"

# shellcheck disable=SC2029,SC2087  # expanded locally on purpose: stage path and interpreter only
ssh -o BatchMode=yes "$host" "cd '$stage' && env -u PYTHONPATH -u PYTHONHOME PYTHONDONTWRITEBYTECODE=1 \
  OMP_NUM_THREADS=2 OPENBLAS_NUM_THREADS=2 MKL_NUM_THREADS=2 bash -s" <<EOF
set -uo pipefail
$py -B -c 'import sys, numpy; print("python", sys.version.split()[0], "numpy", numpy.__version__)'
$py -B -m pytest -q -p no:cacheprovider -rs tests/test_edge_stream_sealed.py | tee pytest.out
rc=\${PIPESTATUS[0]}
grep -qE '[0-9]+ skipped' pytest.out && { echo "GATE FAIL: tests were skipped on this host"; exit 1; }
[ "\$rc" -eq 0 ] || { echo "GATE FAIL: pytest exit \$rc"; exit 1; }
$py -B - <<'PY'
import sys
from alpha_research.edge_stream import sealed as SE
bad = 0
for name, tree in SE.TREES.items():
    ok = SE.verified(name)
    print(f"tree {name}: bytes {'OK' if ok else 'DIFFER'} (git tree {tree['git_tree'][:12]})")
    bad += not ok
    for pid, code in tree["seals"].items():
        ev = SE.evaluator({"family": pid.split("-")[0], "code_sha256": code})
        hit = ev is not None and ev.__name__ == f"{SE.__name__}.{name}.runner"
        print(f"  {pid} {code[:16]}: {'REPRODUCED' if hit else 'NOT REPRODUCED'}")
        bad += not hit
print("GATE PASS" if not bad else f"GATE FAIL: {bad} problem(s)")
sys.exit(1 if bad else 0)
PY
EOF
