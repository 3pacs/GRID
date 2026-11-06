#!/usr/bin/env bash
# Install one GRID release tree plus a venv on a CPU compute-worker node (slice S4, 2026-11-06).
#
# Run as root on the worker node. It never touches a running unit: it unpacks the release, builds or re-checks the
# venv, smoke-imports the BACKTEST path, and only then flips the `release` symlink.
#
# On the trusted build host (hashes are computed there and passed on the command line, never read from the node):
#   git archive --format=tar.gz -o grid-$SHA.tar.gz $SHA
#   git show $SHA:deploy/python314/constraints-3.14.txt > constraints.txt
#   TAR_SHA256=$(sha256sum grid-$SHA.tar.gz | cut -d' ' -f1); CON_SHA256=$(sha256sum constraints.txt | cut -d' ' -f1)
# Copy to the node's login home, then move into a private root-owned staging dir and run from there (never /tmp):
#   scp grid-$SHA.tar.gz constraints.txt install_release.sh node:s4-upload/
#   ssh node "sudo install -d -m 0700 -o root -g root /var/lib/grid-staging && \
#             sudo install -m 0600 -o root -g root s4-upload/* /var/lib/grid-staging/ && rm -rf s4-upload && \
#             sudo GRID_SHA=$SHA GRID_USER=gridworker \
#                  GRID_TARBALL=/var/lib/grid-staging/grid-$SHA.tar.gz GRID_TARBALL_SHA256=$TAR_SHA256 \
#                  GRID_CONSTRAINTS=/var/lib/grid-staging/constraints.txt GRID_CONSTRAINTS_SHA256=$CON_SHA256 \
#                  bash /var/lib/grid-staging/install_release.sh"
# The script copies both inputs into a fresh mktemp dir under /var/lib/grid-staging, verifies the sha256 of the copies
# (and `git get-tar-commit-id` == GRID_SHA when git is installed), and aborts on any mismatch before extracting or
# installing anything.
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
: "${GRID_TARBALL_SHA256:?set GRID_TARBALL_SHA256 (computed on the build host)}"
: "${GRID_CONSTRAINTS_SHA256:?set GRID_CONSTRAINTS_SHA256 (computed on the build host)}"
: "${GRID_USER:?set GRID_USER (service user; created as a system user if missing)}"
STAGING_ROOT=${GRID_STAGING_ROOT:-/var/lib/grid-staging}

abort() { echo "ABORT: $*" >&2; exit 2; }
[ "$(id -u)" = 0 ] || abort "run as root"
[ -x "$GRID_PY" ] || abort "$GRID_PY missing"
[[ "$GRID_SHA" =~ ^[0-9a-f]{40}$ ]] || abort "GRID_SHA must be a full 40-hex commit"
[[ "$GRID_TARBALL_SHA256" =~ ^[0-9a-f]{64}$ ]] || abort "GRID_TARBALL_SHA256 must be 64 hex"
[[ "$GRID_CONSTRAINTS_SHA256" =~ ^[0-9a-f]{64}$ ]] || abort "GRID_CONSTRAINTS_SHA256 must be 64 hex"

# Every directory from / down to $1 must be root-owned and not group/other-writable (no /tmp, no login homes).
assert_private_path() {
  local d
  d=$(readlink -f -- "$1")
  while :; do
    [ "$(stat -c %u -- "$d")" = 0 ] || abort "$d is not root-owned (run and stage from $STAGING_ROOT)"
    [ $(( 0$(stat -c %a -- "$d") & 022 )) = 0 ] || abort "$d is group/other-writable (run and stage from $STAGING_ROOT)"
    [ "$d" = / ] && break
    d=$(dirname -- "$d")
  done
}
assert_private_path "$(dirname -- "${BASH_SOURCE[0]}")"
install -d -m 0700 -o root -g root "$STAGING_ROOT"
assert_private_path "$STAGING_ROOT"

# Copy first, then hash the copies: nothing that could change after the check is ever extracted or installed.
STAGE=$(mktemp -d -p "$STAGING_ROOT" install.XXXXXXXX)
trap 'rm -rf -- "$STAGE"' EXIT
install -m 0600 -o root -g root -- "$GRID_TARBALL" "$STAGE/release.tar.gz"
install -m 0600 -o root -g root -- "$GRID_CONSTRAINTS" "$STAGE/constraints.txt"
echo "$GRID_TARBALL_SHA256  $STAGE/release.tar.gz" | sha256sum -c --quiet - || abort "tarball sha256 mismatch"
echo "$GRID_CONSTRAINTS_SHA256  $STAGE/constraints.txt" | sha256sum -c --quiet - || abort "constraints sha256 mismatch"
if command -v git >/dev/null 2>&1; then
  tar_commit=$(gzip -dc "$STAGE/release.tar.gz" | git get-tar-commit-id || true)
  [ "$tar_commit" = "$GRID_SHA" ] || abort "tarball commit id '${tar_commit:-none}' != GRID_SHA"
else
  echo "WARN: git not installed; skipped get-tar-commit-id (sha256 already verified)" >&2
fi
GRID_TARBALL="$STAGE/release.tar.gz"
GRID_CONSTRAINTS="$STAGE/constraints.txt"

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
  tar -xzf "$GRID_TARBALL" -C "$REL.tmp" --no-same-owner --no-same-permissions
  chmod -R go-w "$REL.tmp"   # git archive modes are 0664/0775; root extracts them verbatim otherwise
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
