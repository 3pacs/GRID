#!/usr/bin/env bash
# Linux-only integration test for scripts/deploy_refresh_runtime_preservation.sh. Real Git checkouts, a real
# process cwd and a fake systemctl, in a mktemp sandbox; the live release root is never touched.
set -euo pipefail
repo="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
script="$repo/scripts/deploy_refresh_runtime_preservation.sh"
box="$(mktemp -d)"
trap 'kill "${proc_pid:-}" 2>/dev/null || true; rm -rf "$box"' EXIT
live="$box/grid_release"
root="${live}.releases"
mkdir -p "$root" "$box/bin"
pass=0 failn=0
ok() { echo "PASS: $1"; pass=$((pass + 1)); }
bad() { echo "FAIL: $1"; failn=$((failn + 1)); }

mkrel() {  # name -> git checkout under root, prints its HEAD
  mkdir -p "$root/$1"; printf '%s\n' "$1" > "$root/$1/marker.txt"
  git -C "$root/$1" init -q; git -C "$root/$1" add marker.txt
  git -C "$root/$1" -c user.name=Test -c user.email=test@example.invalid commit -qm "$1"
  git -C "$root/$1" rev-parse HEAD
}
old_sha="$(mkrel old)"; rec_sha="$(mkrel recovery)"; new_sha="$(mkrel new)"
tree() { git -C "$root/$1" rev-parse 'HEAD^{tree}'; }
write_record() {
  printf 'scheduler=%s\nrecovery=%s\nscheduler_sha=%s\nscheduler_tree=%s\nrecovery_sha=%s\nrecovery_tree=%s\n' \
    "$root/old" "$root/recovery" "$old_sha" "$(tree old)" "$rec_sha" "$(tree recovery)" > "$root/.runtime-preservation"
}
cat > "$box/bin/systemctl" <<'SH'
#!/usr/bin/env bash
case "$*" in
  'show -p MainPID --value grid-scheduler') printf '%s\n' "$TEST_PID" ;;
  'show -p WorkingDirectory --value grid-scheduler') printf '%s\n' "$TEST_WD" ;;
  *) exit 1 ;;
esac
SH
chmod +x "$box/bin/systemctl"
export PATH="$box/bin:$PATH"

start_scheduler_in() { kill "${proc_pid:-}" 2>/dev/null || true; (cd "$1" && exec sleep 300) & proc_pid=$!; sleep 0.2; export TEST_PID=$proc_pid; }
run() { bash "$script" "$live" "$1" > "$box/out" 2>&1; }

# 1. activation moved the scheduler to "new": the record is refreshed, recovery kept, old record backed up
write_record; ln -sfn "$root/new" "$live"; start_scheduler_in "$root/new"; export TEST_WD="$root/new"
if run "$new_sha"; then
  mapfile -t got < "$root/.runtime-preservation"
  if [ "${got[0]}" = "scheduler=$root/new" ] && [ "${got[2]}" = "scheduler_sha=$new_sha" ] &&
     [ "${got[3]}" = "scheduler_tree=$(tree new)" ] && [ "${got[1]}" = "recovery=$root/recovery" ] &&
     [ "${got[4]}" = "recovery_sha=$rec_sha" ] && [ "${got[5]}" = "recovery_tree=$(tree recovery)" ] &&
     ls "$root"/.runtime-preservation.bak-* >/dev/null 2>&1 && ! ls "$root"/.runtime-preservation.tmp.* >/dev/null 2>&1; then
    ok "refreshes scheduler fields, keeps recovery, backs up the old record"
  else bad "record content after refresh: $(cat "$root/.runtime-preservation")"; fi
else bad "refresh failed: $(cat "$box/out")"; fi

# The refreshed record satisfies the swap's own gate expression for the scheduler.
[ "$(readlink -f "/proc/$TEST_PID/cwd")" = "$root/new" ] && [ "$TEST_WD" = "$root/new" ] && ok "running scheduler matches refreshed record" || bad "gate identity"

expect_refusal() {  # label: run must fail with exit 5 and leave the record byte-identical
  local label="$1" sha="$2" before rc=0; before="$(cat "$root/.runtime-preservation")"
  run "$sha" || rc=$?
  if [ "$rc" -eq 5 ] && [ "$(cat "$root/.runtime-preservation")" = "$before" ]; then
    ok "$label (exit 5, record unchanged)"
  else bad "$label: rc=$rc, record $( [ "$(cat "$root/.runtime-preservation")" = "$before" ] && echo unchanged || echo CHANGED)"; fi
}

# 2. scheduler process still running the old release (restart did not happen)
write_record; start_scheduler_in "$root/old"; export TEST_WD="$root/new"
expect_refusal "scheduler cwd not the new release" "$new_sha"

# 3. effective WorkingDirectory is the mutable link, not the immutable release
start_scheduler_in "$root/new"; export TEST_WD="$live"
expect_refusal "WorkingDirectory is the mutable link" "$new_sha"

# 4. live release HEAD is not the deployed sha
export TEST_WD="$root/new"
expect_refusal "live release HEAD differs from expected sha" "$old_sha"

# 5. tracked-file change in the new release
echo dirty >> "$root/new/marker.txt"
expect_refusal "new release has tracked-file changes" "$new_sha"
git -C "$root/new" checkout -q -- marker.txt

# 6. recovery checkout changed since it was approved
echo dirty >> "$root/recovery/marker.txt"
expect_refusal "recovery checkout no longer matches its record" "$new_sha"
git -C "$root/recovery" checkout -q -- marker.txt

# 7. malformed record is never rewritten
printf 'scheduler=%s\n' "$root/old" > "$root/.runtime-preservation"
expect_refusal "malformed record" "$new_sha"

# 8. no running scheduler
write_record; export TEST_PID=0
expect_refusal "no running scheduler" "$new_sha"

echo "=== $pass passed, $failn failed ==="
[ "$failn" -eq 0 ]
