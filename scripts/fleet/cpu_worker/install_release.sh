#!/usr/bin/env bash
# Install one GRID release tree plus a venv on a CPU compute-worker node (slice S4, 2026-11-06).
#
# Run as root on the worker node. It never touches a running unit: it unpacks the release, builds or re-checks the
# venv, smoke-imports the BACKTEST path, and only then flips the `release` symlink.
#
#   git archive --format=tar.gz -o grid-$SHA.tar.gz $SHA
#   git show $SHA:deploy/python314/constraints-3.14.txt > constraints.txt
#   scp grid-$SHA.tar.gz constraints.txt install_release.sh node:/tmp/
#   ssh node "sudo GRID_SHA=$SHA GRID_TARBALL=/tmp/grid-$SHA.tar.gz GRID_CONSTRAINTS=/tmp/constraints.txt \
#             GRID_USER=gridworker bash /tmp/install_release.sh"
#
# Layout (GRID_ROOT, default /opt/grid):
#   releases/<sha>/   root-owned, read-only code (the worker writes no files into the tree)
#   release           symlink -> releases/<sha>  (units use this path)
#   venv-<pyver>/     root-owned venv: requirements.txt under the CI 3.14 lock; no torch, no LLM extras
#   home/             the service user's writable HOME (caches)
#
# Rollback: ln -sfn releases/<previous-sha> $GRID_ROOT/release && systemctl restart 'grid-worker*'
set -euo pipefail
umask 022

GRID_ROOT=${GRID_ROOT:-/opt/grid}
GRID_PY=${GRID_PY:-/usr/bin/python3.12}
: "${GRID_SHA:?set GRID_SHA}" "${GRID_TARBALL:?set GRID_TARBALL}" "${GRID_CONSTRAINTS:?set GRID_CONSTRAINTS}"
: "${GRID_USER:?set GRID_USER (service user; created as a system user if missing)}"

[ "$(id -u)" = 0 ] || { echo "ABORT: run as root" >&2; exit 2; }
[ -x "$GRID_PY" ] || { echo "ABORT: $GRID_PY missing" >&2; exit 2; }
[[ "$GRID_SHA" =~ ^[0-9a-f]{40}$ ]] || { echo "ABORT: GRID_SHA must be a full 40-hex commit" >&2; exit 2; }

PYVER=$("$GRID_PY" -c 'import sys; print(f"{sys.version_info[0]}{sys.version_info[1]}")')
VENV="$GRID_ROOT/venv-$PYVER"
REL="$GRID_ROOT/releases/$GRID_SHA"

if ! id "$GRID_USER" >/dev/null 2>&1; then
  useradd --system --home-dir "$GRID_ROOT/home" --no-create-home --shell /usr/sbin/nologin "$GRID_USER"
  echo "created system user $GRID_USER"
fi
install -d -o root -g root -m 0755 "$GRID_ROOT" "$GRID_ROOT/releases"
install -d -o "$GRID_USER" -g "$GRID_USER" -m 0700 "$GRID_ROOT/home"

if [ -f "$REL/.release-sha" ]; then
  echo "release $GRID_SHA already unpacked"
else
  rm -rf "$REL.tmp"
  install -d -o root -g root -m 0755 "$REL.tmp"
  tar -xzf "$GRID_TARBALL" -C "$REL.tmp" --no-same-owner
  echo "$GRID_SHA" > "$REL.tmp/.release-sha"
  mv -T "$REL.tmp" "$REL"
  echo "unpacked $GRID_SHA -> $REL"
fi

if [ ! -x "$VENV/bin/python" ]; then
  "$GRID_PY" -m venv "$VENV"
fi
"$VENV/bin/pip" install --quiet --root-user-action=ignore --upgrade pip
"$VENV/bin/pip" install --quiet --root-user-action=ignore -r "$REL/requirements.txt" -c "$GRID_CONSTRAINTS"
"$VENV/bin/pip" check
# Byte-compile once as root so the read-only tree imports fast for the service user.
"$VENV/bin/python" -m compileall -q -j 4 -x '/(tests|pwa|node_modules|outputs?)/' "$REL" >/dev/null || true

# Smoke: the BACKTEST import path, as the service user, with a dummy password (config rejects an empty one).
( cd "$REL" && runuser -u "$GRID_USER" -- env -i HOME="$GRID_ROOT/home" PATH=/usr/bin:/bin PYTHONPATH="$REL" \
    DB_PASSWORD=smoke-not-a-secret "$VENV/bin/python" -c '
import config, db, store.pit, scripts.worker
import alpha_research.strategies.rotation_variant_backtest, sklearn, pandas, numpy
print("smoke ok:", __import__("sys").version.split()[0], "pandas", pandas.__version__, "numpy", numpy.__version__)
' )

ln -sfn "releases/$GRID_SHA" "$GRID_ROOT/release.tmp"
mv -Tf "$GRID_ROOT/release.tmp" "$GRID_ROOT/release"
echo "release -> $(readlink "$GRID_ROOT/release"); venv $VENV"
