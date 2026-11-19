#!/usr/bin/env bash
# Compare the systemd units INSTALLED on this host with the unit files shipped in
# deploy/systemd/, and (with --sync) reinstall the ones that drifted.
#
# Why: deploys update the templates in the release tree but never touched
# /etc/systemd/system, so an installed unit silently kept yesterday's ExecStart or
# Environment= (2026-11-08: grid-publication-packet.service ran without the public
# tree env for a day). This closes that class without changing the standing rule that
# nothing installs a NEW unit: only units that are already installed are considered.
#
# Only templates that opt in with a line `# deploy-sync: managed` are ever synced; every
# other installed unit is compared and REPORTED only (an operator may have diverged it on
# purpose, e.g. grid-e2-scoreboard's dedicated user). Comparison ignores comment lines
# and blank lines. Drop-ins under <unit>.d/ are left alone. A templated instance unit
# (name@.service) is reported but never synced.
# Long-running services that are active are synced on disk and daemon-reloaded but NOT
# restarted (the next deploy restart or the operator picks up the new file); timers and
# oneshot/inactive services take effect on the next activation.
#
# Usage: check_systemd_unit_drift.sh [--sync] [--strict] [--verbose] [--templates DIR] [--system-dir DIR]
#   --sync        reinstall drifted MANAGED units (install -m 0644) and daemon-reload
#   --strict      exit 2 when drift remains after the run (no --sync: any drift)
#   --verbose     print the differing lines (Environment= values redacted). Off by default and
#                 never passed from deploy.yml: unit contents must not land in CI logs.
#   --templates   directory holding *.service.template / *.timer.template / *.service / *.timer
#                 (default: deploy/systemd next to this script's repo root)
#   --system-dir  where units are installed (default /etc/systemd/system)
# Env: SUDO (default "sudo -n" when not root, "" when root) -- the privilege command for writes;
#      -n so a password prompt is a clear failure, never a hang.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATES="$SCRIPT_DIR/../deploy/systemd"
SYSTEM_DIR=/etc/systemd/system
SYNC=0
STRICT=0
VERBOSE=0
while [ $# -gt 0 ]; do
  case "$1" in
    --sync) SYNC=1 ;;
    --strict) STRICT=1 ;;
    --verbose) VERBOSE=1 ;;
    --templates) TEMPLATES="$2"; shift ;;
    --system-dir) SYSTEM_DIR="$2"; shift ;;
    -h|--help) sed -n '2,24p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 64 ;;
  esac
  shift
done
if [ -z "${SUDO+x}" ]; then
  if [ "$(id -u)" -eq 0 ]; then SUDO=""; else SUDO="sudo -n"; fi
fi
[ -d "$TEMPLATES" ] || { echo "::error::templates dir not found: $TEMPLATES" >&2; exit 64; }
[ -d "$SYSTEM_DIR" ] || { echo "::error::system dir not found: $SYSTEM_DIR" >&2; exit 64; }

normalize() { grep -vE '^[[:space:]]*(#|$)' "$1" || true; }
# Differing-line count only by default; with --verbose the lines, Environment= values redacted.
show_drift() {
  local n
  n="$(diff <(normalize "$1") <(normalize "$2") | grep -cE '^[<>]' || true)"
  echo "         $n differing line(s)"
  if [ "$VERBOSE" -eq 1 ]; then
    diff <(normalize "$1") <(normalize "$2") | sed -E 's/^([<>] *Environment=[^=]+=).*/\1<redacted>/' | sed 's/^/         /' || true
  fi
}
# The marker may carry a trailing explanation: "# deploy-sync: managed  (why ...)".
managed() { grep -qE '^#[[:space:]]*deploy-sync:[[:space:]]*managed([[:space:]]|$)' "$1"; }

checked=0; drifted=0; synced=0; skipped=0; remaining=0; unmanaged=0
reloaded=0
for template in "$TEMPLATES"/*; do
  [ -f "$template" ] || continue
  base="$(basename -- "$template")"
  case "$base" in
    *.service.template) unit="${base%.template}" ;;
    *.timer.template)   unit="${base%.template}" ;;
    *.service|*.timer)  unit="$base" ;;
    *) continue ;;
  esac
  installed="$SYSTEM_DIR/$unit"
  [ -f "$installed" ] || continue          # not installed here: never install new units
  checked=$((checked + 1))
  if diff -q <(normalize "$template") <(normalize "$installed") >/dev/null; then
    echo "ok     $unit"
    continue
  fi
  drifted=$((drifted + 1))
  case "$unit" in
    *@.service|*@.timer)
      echo "DRIFT  $unit (templated instance unit: review by hand)"
      skipped=$((skipped + 1)); remaining=$((remaining + 1)); continue ;;
  esac
  if ! managed "$template"; then
    echo "DRIFT  $unit (unmanaged template: not synced; add '# deploy-sync: managed' to opt in)"
    show_drift "$installed" "$template"
    unmanaged=$((unmanaged + 1)); remaining=$((remaining + 1))
    continue
  fi
  if [ "$SYNC" -ne 1 ]; then
    echo "DRIFT  $unit"
    show_drift "$installed" "$template"
    remaining=$((remaining + 1))
    continue
  fi
  # shellcheck disable=SC2086 # SUDO is intentionally word-split ("" when root)
  if $SUDO install -m 0644 -- "$template" "$installed"; then
    echo "SYNCED $unit"
    synced=$((synced + 1))
    reloaded=1
  else
    echo "::error::could not install $unit from $template" >&2
    remaining=$((remaining + 1))
  fi
done
if [ "$reloaded" -eq 1 ]; then
  # shellcheck disable=SC2086
  if $SUDO systemctl daemon-reload; then
    echo "daemon-reload done"
  else
    echo "::error::daemon-reload failed" >&2
    remaining=$((remaining + 1))
  fi
fi
echo "units checked=$checked drifted=$drifted synced=$synced skipped=$skipped unmanaged=$unmanaged remaining=$remaining"
if [ "$STRICT" -eq 1 ] && [ "$remaining" -gt 0 ]; then
  exit 2
fi
exit 0
