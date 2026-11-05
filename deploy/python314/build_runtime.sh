#!/usr/bin/env bash
# Build the CPython 3.14 runtime GRID will move to (Anik 2026-11-04), on grid-svr, as user grid, beside the existing
# runtimes (/data/grid_v4/grid_python.runtimes-r2 holds the 3.11.17 build). It changes nothing that runs:
# /usr/bin/python3 (3.10), grid-api, grid-hermes, their units and the release tree are untouched. Re-running after a
# successful build only re-verifies it.
#
#   ssh grid-svr 'bash -s' < deploy/python314/build_runtime.sh
#
# Source: python.org Python-3.14.9.tgz, the version GitHub's setup-python gives the "Python 3.14" CI job. Its sha256
# below was pinned on 2026-11-04 after its Sigstore bundle verified against the 3.14 release manager's identity
# (hugo@python.org, issuer https://github.com/login/oauth). Plain -O2 build (no PGO/LTO: shorter, gentler on the live
# host); nice 19 so grid-api / grid-hermes keep priority.
set -euo pipefail
umask 022

VER=3.14.9
SHA256=8b47e85ee67e93945e93620a051a8f44bfc1e9735ed67075d166f4a74b7a060b
BASE=/data/grid_v4/grid_python.runtimes-314
PREFIX="$BASE/cpython-$VER"
PY="$PREFIX/bin/python3.14"
MODULES='import ssl, sqlite3, ctypes, lzma, bz2, zlib, readline, uuid, curses, decimal, hashlib, dbm.gnu, tkinter, venv, ensurepip'

[ "$(id -un)" = grid ] || { echo "ABORT: run as grid (ssh grid-svr)" >&2; exit 2; }

if [ -x "$PY" ]; then
  echo "already built: $("$PY" -V)"
  "$PY" -c "$MODULES; print('modules ok;', ssl.OPENSSL_VERSION, '; sqlite', sqlite3.sqlite_version)"
  exit 0
fi

mkdir -p "$BASE/src"
cd "$BASE/src"
[ -f "Python-$VER.tgz" ] || curl -fsSL --retry 3 -o "Python-$VER.tgz" "https://www.python.org/ftp/python/$VER/Python-$VER.tgz"
echo "$SHA256  Python-$VER.tgz" | sha256sum -c - || { echo "ABORT: source hash mismatch; nothing built" >&2; exit 3; }
rm -rf "Python-$VER"
tar xzf "Python-$VER.tgz"
cd "Python-$VER"

start=$(date -u +%FT%TZ)
nice -n 19 ./configure --prefix="$PREFIX" --with-ensurepip=install > "$BASE/src/configure-$VER.log" 2>&1
nice -n 19 make -j16 > "$BASE/src/make-$VER.log" 2>&1
# modules that failed to build are listed at the end of make; any missing stdlib module fails the check below
grep -A20 -E "Failed to build these modules|could not build|necessary bits to build these optional modules" "$BASE/src/make-$VER.log" || true
nice -n 19 make install > "$BASE/src/install-$VER.log" 2>&1
end=$(date -u +%FT%TZ)

"$PY" -V
"$PY" -c "$MODULES; print('modules ok;', ssl.OPENSSL_VERSION, '; sqlite', sqlite3.sqlite_version)"
"$PY" - "$BASE/BUILD-$VER.json" "$start" "$end" "$SHA256" <<'EOF'
import json, platform, sys, sysconfig
out, start, end, sha = sys.argv[1:5]
json.dump({"version": platform.python_version(), "prefix": sys.prefix, "source_sha256": sha, "started": start,
           "finished": end, "config_args": sysconfig.get_config_var("CONFIG_ARGS")}, open(out, "w"), indent=1)
print("receipt", out)
EOF
echo "DONE: CPython $VER at $PREFIX (nothing running uses it yet)"
