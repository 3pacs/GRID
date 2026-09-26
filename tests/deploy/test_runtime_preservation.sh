#!/usr/bin/env bash
# Linux-only integration test. Uses a real process cwd and a fake systemctl.
set -euo pipefail
repo="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
swap="$repo/scripts/deploy_release_swap.sh"
box="$(mktemp -d)"
trap 'kill "${scheduler_pid:-}" "${activated_pid:-}" "${realtime_pid:-}" "${deleted_pid:-}" 2>/dev/null || true; rm -rf "$box"' EXIT
live="$box/grid_release"
root="${live}.releases"
mkdir -p "$root/scheduler-old" "$root/recovery-old" "$root/current" "$box/bin" \
  "$root/realtime-old/subdir" "$root/realtime-restart"
printf 'retained\n' > "$root/realtime-old/subdir/module.txt"
ln -s "$root/realtime-restart" "$box/realtime-config-link"
ln -s "$root/realtime-restart" "$root/realtime-alias"
for dir in scheduler-old recovery-old current; do
  printf '%s\n' "$dir" > "$root/$dir/marker.txt"
done
for dir in scheduler-old recovery-old; do
  git -C "$root/$dir" init -q
  git -C "$root/$dir" add marker.txt
  git -C "$root/$dir" -c user.name=Test -c user.email=test@example.invalid commit -qm initial
done
scheduler_sha="$(git -C "$root/scheduler-old" rev-parse HEAD)"
scheduler_tree="$(git -C "$root/scheduler-old" rev-parse 'HEAD^{tree}')"
recovery_sha="$(git -C "$root/recovery-old" rev-parse HEAD)"
recovery_tree="$(git -C "$root/recovery-old" rev-parse 'HEAD^{tree}')"
ln -s "$root/current" "$live"
cat > "$box/bin/systemctl" <<'SH'
#!/usr/bin/env bash
case "$*" in
  'list-unit-files --no-legend --no-pager grid-*.service'|'list-units --all --plain --no-legend --no-pager grid-*.service')
    [ "${TEST_INVENTORY_FAIL:-0}" != 1 ] || exit 1
    printf '%s\n' grid-scheduler.service grid-realtime.service grid-worker@.service ;;
  'show --property=LoadState,MainPID,WorkingDirectory -- grid-scheduler.service')
    printf 'LoadState=loaded\nMainPID=%s\nWorkingDirectory=%s\n' "$TEST_SCHEDULER_PID" "$TEST_SCHEDULER_WORKDIR" ;;
  'show --property=LoadState,MainPID,WorkingDirectory -- grid-realtime.service')
    [ "${TEST_SHOW_FAIL:-0}" != 1 ] || exit 1
    printf 'LoadState=loaded\nMainPID=%s\nWorkingDirectory=%s\n' "$TEST_REALTIME_PID" "$TEST_REALTIME_WORKDIR" ;;
  'show -p MainPID --value grid-scheduler') printf '%s\n' "$TEST_SCHEDULER_PID" ;;
  'show -p WorkingDirectory --value grid-scheduler') printf '%s\n' "$TEST_SCHEDULER_WORKDIR" ;;
  *) exit 1 ;;
esac
SH
chmod +x "$box/bin/systemctl"
cat > "$box/build" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$2" > "$1/marker.txt"
SH
chmod +x "$box/build"
( cd "$root/scheduler-old" && exec sleep 300 ) &
scheduler_pid=$!
( cd "$root/realtime-old/subdir" && exec sleep 300 ) &
realtime_pid=$!
export TEST_SCHEDULER_PID="$scheduler_pid" TEST_SCHEDULER_WORKDIR="$root/scheduler-old"
export TEST_REALTIME_PID="$realtime_pid" TEST_REALTIME_WORKDIR="$box/realtime-config-link"
export PATH="$box/bin:$PATH"
record="$root/.runtime-preservation"
printf 'scheduler=%s\nrecovery=%s\nscheduler_sha=%s\nscheduler_tree=%s\nrecovery_sha=%s\nrecovery_tree=%s\n' \
  "$root/scheduler-old" "$root/recovery-old" "$scheduler_sha" "$scheduler_tree" \
  "$recovery_sha" "$recovery_tree" > "$record"

fail_without_swap() {
  local expected="$1" label="$2"
  set +e
  bash "$swap" "$live" "$label" "$box/build" "$label" > "$box/log" 2>&1
  local rc=$?
  set -e
  test "$rc" -eq 5 || { cat "$box/log"; echo "expected preservation exit 5, got $rc" >&2; exit 1; }
  test "$(readlink -f "$live")" = "$expected"
  test ! -e "$root/$label"
}

# Missing record, then invalid/outside/missing targets all fail before build.
mv "$record" "$box/saved-record"
fail_without_swap "$root/current" missing-record
mv "$box/saved-record" "$record"
sed -i 's/^recovery=.*/recovery=relative/' "$record"
fail_without_swap "$root/current" relative-target
sed -i "s|^recovery=.*|recovery=$box/outside|" "$record"
mkdir -p "$box/outside"
fail_without_swap "$root/current" outside-target
sed -i "s|^recovery=.*|recovery=$root/gone|" "$record"
fail_without_swap "$root/current" missing-target
sed -i "s|^recovery=.*|recovery=$root/recovery-old|" "$record"
sed -i 's/^recovery_sha=.*/recovery_sha=0000000000000000000000000000000000000000/' "$record"
fail_without_swap "$root/current" wrong-recovery-sha
sed -i "s|^recovery_sha=.*|recovery_sha=$recovery_sha|" "$record"
sed -i 's/^recovery_tree=.*/recovery_tree=0000000000000000000000000000000000000000/' "$record"
fail_without_swap "$root/current" wrong-recovery-tree
sed -i "s|^recovery_tree=.*|recovery_tree=$recovery_tree|" "$record"
sed -i '/^recovery_sha=/d' "$record"
fail_without_swap "$root/current" missing-recovery-sha
sed -i "/^recovery_tree=/i recovery_sha=$recovery_sha" "$record"
printf '%s\n' altered > "$root/recovery-old/marker.txt"
fail_without_swap "$root/current" dirty-recovery-tree
git -C "$root/recovery-old" restore marker.txt
TEST_SCHEDULER_WORKDIR="$live" fail_without_swap "$root/current" mutable-unit
TEST_INVENTORY_FAIL=1 fail_without_swap "$root/current" failed-inventory
TEST_SHOW_FAIL=1 fail_without_swap "$root/current" failed-show

# An existing candidate that is a retained runtime must never be erased.
set +e
bash "$swap" "$live" realtime-old "$box/build" bad > "$box/collision.log" 2>&1
collision_rc=$?
set -e
test "$collision_rc" -eq 5
test "$(cat "$root/realtime-old/subdir/module.txt")" = retained
test "$(readlink -f "$live")" = "$root/current"
set +e
TEST_REALTIME_WORKDIR="$root/realtime-alias" bash "$swap" "$live" realtime-alias "$box/build" bad > "$box/alias-collision.log" 2>&1
collision_rc=$?
set -e
test "$collision_rc" -eq 5
test -L "$root/realtime-alias"

# Deleted cwd fails before any build, even if the configured restart path exists.
mkdir "$box/deleted-cwd"
( cd "$box/deleted-cwd" && exec sleep 300 ) &
deleted_pid=$!
for _ in {1..50}; do
  [ "$(readlink "/proc/$deleted_pid/cwd")" = "$box/deleted-cwd" ] && break
  sleep 0.02
done
rmdir "$box/deleted-cwd"
TEST_REALTIME_PID="$deleted_pid" fail_without_swap "$root/current" deleted-runtime
kill "$deleted_pid"
wait "$deleted_pid" 2>/dev/null || true
deleted_pid=

# Old scheduler dir sorts last by mtime; both protected identities survive
# two swaps and the ordinary retention budget still prunes stale current.
touch -d '2020-01-01 UTC' "$root/scheduler-old" "$root/recovery-old"
touch -d '2019-01-01 UTC' "$root/realtime-old" "$root/realtime-restart"
bash "$swap" "$live" next-1 "$box/build" next-1 > "$box/first.log" 2>&1
test -d "$root/scheduler-old" && test -d "$root/recovery-old"
touch -d '2021-01-01 UTC' "$root/current"
bash "$swap" "$live" next-2 "$box/build" next-2 > "$box/second.log" 2>&1
test -d "$root/scheduler-old" && test -d "$root/recovery-old"
test "$(readlink -f "$live")" = "$root/next-2"
test ! -d "$root/current"
test "$(cat "/proc/$realtime_pid/cwd/module.txt")" = retained
test -d "$root/realtime-restart"

# A third swap retains actual cwd even while configured restart points elsewhere.
bash "$swap" "$live" next-3 "$box/build" next-3 > "$box/third.log" 2>&1
test "$(cat "/proc/$realtime_pid/cwd/module.txt")" = retained
test -d "$root/realtime-restart"
test ! -d "$root/next-1"

# Inactive service has no process, but its symlink-resolved restart tree survives.
kill "$realtime_pid"
wait "$realtime_pid" 2>/dev/null || true
realtime_pid=
export TEST_REALTIME_PID=0
bash "$swap" "$live" next-4 "$box/build" next-4 > "$box/inactive.log" 2>&1
test -d "$root/realtime-restart"

# Retrying the live label is a no-op, including when it is a protected path.
bash "$swap" "$live" next-4 "$box/build" overwritten > "$box/same.log" 2>&1
test "$(cat "$live/marker.txt")" = next-4

# After an acknowledged scheduler activation, the old record must stop the
# next deployment until the controller updates both approved identities.
( cd "$root/next-4" && exec sleep 300 ) &
activated_pid=$!
TEST_SCHEDULER_PID="$activated_pid" TEST_SCHEDULER_WORKDIR="$root/next-4" \
  fail_without_swap "$root/next-4" stale-after-activation
echo 'PASS: runtime preservation, realtime multi-swap/collision/inactive/symlink/deleted-cwd, query failures, invalid identities and stale activation record'
