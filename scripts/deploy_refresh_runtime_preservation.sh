#!/usr/bin/env bash
# Refresh the release root's .runtime-preservation record after an activate_scheduler restart.
#
# deploy_release_swap.sh refuses to build (exit 5) unless the running grid-scheduler's PID cwd and effective
# WorkingDirectory equal the record's scheduler= directory. Activation moves the scheduler to a new release, so
# before this script every activation left the record stale and blocked the next deploy until someone rewrote it
# by hand (docs/runtime-preservation-bootstrap.md; it happened on 2026-11-04).
#
# Run by deploy.yml only after the activation's own verify steps pass. Under the same flock the swap uses, it:
#   - requires a valid six-line record and keeps its recovery= fields unchanged (re-verifying that checkout);
#   - requires grid-scheduler's PID cwd == effective WorkingDirectory == the resolved live release, a direct
#     child of the release root whose clean Git HEAD is <expected_sha>;
#   - backs up the old record, then writes the new one via temp file + rename.
# Any mismatch exits non-zero before writing, leaving the old record, so later deploys still fail closed.
#
# Usage: deploy_refresh_runtime_preservation.sh <live_path> <expected_sha>
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: $0 <live_path> <expected_sha>" >&2
  exit 2
fi
LIVE_PATH="$1"
EXPECTED_SHA="$2"
LOCK_WAIT_SECS="${LOCK_WAIT_SECS:-600}"
RELEASES_DIR="${LIVE_PATH}.releases"
RECORD="${RELEASES_DIR}/.runtime-preservation"

fail() { echo "runtime preservation refresh: $*" >&2; exit 5; }

[[ "$EXPECTED_SHA" =~ ^[0-9a-f]{40}$ ]] || fail "expected sha is not a 40-character Git SHA"
releases_root="$(realpath -e -- "$RELEASES_DIR")" || fail "release root missing: $RELEASES_DIR"
[ "$releases_root" = "$RELEASES_DIR" ] || fail "release root must be an absolute canonical directory: $RELEASES_DIR"

exec {lock_fd}>"${RELEASES_DIR}/.lock"
flock -w "$LOCK_WAIT_SECS" "$lock_fd" || { echo "could not acquire deploy lock within ${LOCK_WAIT_SECS}s" >&2; exit 3; }

[ -f "$RECORD" ] && [ ! -L "$RECORD" ] || fail "record missing or linked: $RECORD (bootstrap it first)"
mapfile -t lines < "$RECORD"
if [ "${#lines[@]}" -ne 6 ] || [[ "${lines[0]}" != scheduler=* ]] || [[ "${lines[1]}" != recovery=* ]] ||
   [[ "${lines[2]}" != scheduler_sha=* ]] || [[ "${lines[3]}" != scheduler_tree=* ]] ||
   [[ "${lines[4]}" != recovery_sha=* ]] || [[ "${lines[5]}" != recovery_tree=* ]]; then
  fail "existing record is not the six-line format; refusing to rewrite it"
fi
recovery_dir="${lines[1]#recovery=}"
recovery_sha="${lines[4]#recovery_sha=}"
recovery_tree="${lines[5]#recovery_tree=}"

verify_checkout() {  # name dir sha [tree]: canonical direct child, clean, HEAD (and tree) as given
  local name="$1" dir="$2" sha="$3" tree="${4:-}"
  [ -d "$dir" ] && [ ! -L "$dir" ] && [ "$(realpath -e -- "$dir")" = "$dir" ] &&
    [ "$(dirname -- "$dir")" = "$releases_root" ] || fail "$name directory missing, linked, outside or noncanonical: $dir"
  [ "$(git -C "$dir" rev-parse --show-toplevel 2>/dev/null || true)" = "$dir" ] &&
    [ "$(git -C "$dir" rev-parse HEAD 2>/dev/null || true)" = "$sha" ] || fail "$name HEAD is not $sha"
  if [ -n "$tree" ]; then
    [ "$(git -C "$dir" rev-parse 'HEAD^{tree}' 2>/dev/null || true)" = "$tree" ] || fail "$name tree is not $tree"
  fi
  git -C "$dir" diff --quiet HEAD -- || fail "$name has tracked-file changes"
}
verify_checkout recovery "$recovery_dir" "$recovery_sha" "$recovery_tree"

scheduler_dir="$(readlink -f -- "$LIVE_PATH")"
verify_checkout scheduler "$scheduler_dir" "$EXPECTED_SHA"
scheduler_tree="$(git -C "$scheduler_dir" rev-parse 'HEAD^{tree}')"

pid="$(systemctl show -p MainPID --value grid-scheduler 2>/dev/null || true)"
workdir="$(systemctl show -p WorkingDirectory --value grid-scheduler 2>/dev/null || true)"
[[ "$pid" =~ ^[1-9][0-9]*$ ]] || fail "grid-scheduler has no running MainPID"
[ "$(readlink -f -- "/proc/${pid}/cwd" 2>/dev/null || sudo -n readlink -f -- "/proc/${pid}/cwd" 2>/dev/null || true)" = "$scheduler_dir" ] ||
  fail "grid-scheduler PID $pid cwd is not $scheduler_dir"
[ "$workdir" = "$scheduler_dir" ] || fail "grid-scheduler WorkingDirectory is $workdir, not $scheduler_dir"

backup="${RECORD}.bak-$(date -u +%Y%m%dT%H%M%SZ)"
cp -p -- "$RECORD" "$backup"
tmp="$(mktemp "${RECORD}.tmp.XXXXXX")"
printf 'scheduler=%s\nrecovery=%s\nscheduler_sha=%s\nscheduler_tree=%s\nrecovery_sha=%s\nrecovery_tree=%s\n' \
  "$scheduler_dir" "$recovery_dir" "$EXPECTED_SHA" "$scheduler_tree" "$recovery_sha" "$recovery_tree" > "$tmp"
chmod 0644 -- "$tmp"
mv -f -- "$tmp" "$RECORD"
echo "runtime preservation record refreshed: scheduler=$scheduler_dir (recovery unchanged: $recovery_dir); previous record kept at $backup"
