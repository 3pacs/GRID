#!/usr/bin/env bash
# Linux-only integration test. Uses a real process cwd and a fake systemctl.
set -euo pipefail
repo="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
source_swap="$repo/scripts/deploy_release_swap.sh"
box="$(mktemp -d)"
trap 'kill "${scheduler_pid:-}" "${activated_pid:-}" "${realtime_pid:-}" "${deleted_pid:-}" 2>/dev/null || true; rm -rf "$box"' EXIT
live="$box/grid_release"
root="${live}.releases"
mkdir -p "$box/cgroup/scheduler" "$box/cgroup/realtime/workers" "$box/proc"
touch "$box/cgroup/cgroup.controllers" "$box/cgroup/realtime/cgroup.procs"
# Only cgroup membership roots are substituted in this test copy. Process cwd
# and start-time identity inspection still use real /proc.
swap="$box/swap.sh"
sed -e "s|^CGROUP_ROOT=/sys/fs/cgroup$|CGROUP_ROOT=$box/cgroup|" \
    -e "s|^PROC_CGROUP_ROOT=/proc$|PROC_CGROUP_ROOT=$box/proc|" "$source_swap" > "$swap"
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
    printf '%s\n' grid-scheduler.service grid-realtime.service grid-db.service grid-worker@.service ;;
  'show --property=LoadState,MainPID,WorkingDirectory,ActiveState,ControlGroup,SubState,Type,RemainAfterExit,ExecMainPID -- grid-scheduler.service')
    printf 'LoadState=loaded\nMainPID=%s\nWorkingDirectory=%s\nActiveState=active\nControlGroup=/scheduler\nSubState=running\nType=simple\nRemainAfterExit=no\nExecMainPID=%s\n' "$TEST_SCHEDULER_PID" "$TEST_SCHEDULER_WORKDIR" "$TEST_SCHEDULER_PID" ;;
  'show --property=LoadState,MainPID,WorkingDirectory,ActiveState,ControlGroup,SubState,Type,RemainAfterExit,ExecMainPID -- grid-realtime.service')
    [ "${TEST_SHOW_FAIL:-0}" != 1 ] || exit 1
    printf 'LoadState=loaded\nMainPID=%s\nWorkingDirectory=%s\nActiveState=%s\nControlGroup=%s\nSubState=running\nType=simple\nRemainAfterExit=no\nExecMainPID=%s\n' "$TEST_REALTIME_PID" "$TEST_REALTIME_WORKDIR" "$TEST_REALTIME_STATE" "$TEST_REALTIME_CGROUP" "${TEST_REALTIME_EXEC_PID:-$TEST_REALTIME_PID}" ;;
  'show --property=LoadState,MainPID,WorkingDirectory,ActiveState,ControlGroup,SubState,Type,RemainAfterExit,ExecMainPID -- grid-db.service')
    printf 'LoadState=loaded\nMainPID=0\nWorkingDirectory=\nActiveState=active\nControlGroup=\nSubState=%s\nType=%s\nRemainAfterExit=%s\nExecMainPID=0\n' "${TEST_DB_SUBSTATE-exited}" "${TEST_DB_TYPE-oneshot}" "${TEST_DB_REMAIN-yes}" ;;
  'show -p MainPID --value grid-scheduler') printf '%s\n' "$TEST_SCHEDULER_PID" ;;
  'show -p WorkingDirectory --value grid-scheduler') printf '%s\n' "$TEST_SCHEDULER_WORKDIR" ;;
  *) exit 1 ;;
esac
SH
chmod +x "$box/bin/systemctl"
export TEST_REAL_FIND="$(command -v find)" TEST_RELEASE_ROOT="$root"
cat > "$box/bin/find" <<'SH'
#!/usr/bin/env bash
if [ "${TEST_FIND_PARTIAL_FAIL:-0}" = 1 ] && [ "$1" = "$TEST_RELEASE_ROOT" ]; then
  "$TEST_REAL_FIND" "$@"
  exit 42
fi
exec "$TEST_REAL_FIND" "$@"
SH
chmod +x "$box/bin/find"
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
mkdir -p "$box/proc/$scheduler_pid" "$box/proc/$realtime_pid"
printf '0::/scheduler\n' > "$box/proc/$scheduler_pid/cgroup"
printf '0::/realtime/workers\n' > "$box/proc/$realtime_pid/cgroup"
printf '%s\n' "$scheduler_pid" > "$box/cgroup/scheduler/cgroup.procs"
printf '%s\n' "$realtime_pid" > "$box/cgroup/realtime/workers/cgroup.procs"
export TEST_SCHEDULER_PID="$scheduler_pid" TEST_SCHEDULER_WORKDIR="$root/scheduler-old"
export TEST_REALTIME_PID="$realtime_pid" TEST_REALTIME_WORKDIR="$box/realtime-config-link"
export TEST_REALTIME_STATE=active TEST_REALTIME_CGROUP=/realtime
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
TEST_REALTIME_PID=0 TEST_REALTIME_CGROUP= fail_without_swap "$root/current" no-active-cgroup
TEST_DB_TYPE=simple fail_without_swap "$root/current" non-oneshot-active-empty
TEST_DB_REMAIN=no fail_without_swap "$root/current" non-remaining-oneshot
TEST_DB_SUBSTATE=running fail_without_swap "$root/current" running-oneshot-empty
TEST_DB_TYPE= fail_without_swap "$root/current" missing-oneshot-type
TEST_REALTIME_EXEC_PID=999999 fail_without_swap "$root/current" mismatched-exec-main-pid
printf '1:cpu:/realtime/workers\n' > "$box/proc/$realtime_pid/cgroup"
fail_without_swap "$root/current" unsupported-actual-cgroup
printf '0::/realtime/workers\n' > "$box/proc/$realtime_pid/cgroup"
TEST_REALTIME_CGROUP=/realtime/../scheduler fail_without_swap "$root/current" traversed-cgroup
ln -s "$box/cgroup/realtime" "$box/cgroup/linked"
TEST_REALTIME_CGROUP=/linked fail_without_swap "$root/current" linked-cgroup
mkdir "$box/cgroup/empty"
touch "$box/cgroup/empty/cgroup.procs"
TEST_REALTIME_PID=0 TEST_REALTIME_CGROUP=/empty fail_without_swap "$root/current" empty-active-cgroup

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
mkdir "$box/proc/$deleted_pid"
printf '0::/realtime/workers\n' > "$box/proc/$deleted_pid/cgroup"
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

# A third swap retains a living descendant even with MainPID=0 and a distinct
# configured restart directory (the independent review's child-cwd regression).
TEST_REALTIME_PID=0 bash "$swap" "$live" next-3 "$box/build" next-3 > "$box/third.log" 2>&1
test "$(cat "/proc/$realtime_pid/cwd/module.txt")" = retained
test -d "$root/realtime-restart"
test ! -d "$root/next-1"

# Production-shaped escaped MainPID: declared system cgroup is empty, actual
# unified user scope contains the verified process. Preserve it without a
# service-name exemption, while still inspecting the declared group.
mkdir "$box/cgroup/escaped-user-scope"
printf '%s\n' "$realtime_pid" > "$box/cgroup/escaped-user-scope/cgroup.procs"
: > "$box/cgroup/realtime/workers/cgroup.procs"
printf '0::/escaped-user-scope\n' > "$box/proc/$realtime_pid/cgroup"
bash "$swap" "$live" escaped-main "$box/build" escaped-main > "$box/escaped.log" 2>&1
test "$(cat "/proc/$realtime_pid/cwd/module.txt")" = retained
test -d "$root/realtime-restart"

# Inactive service has no process, but its symlink-resolved restart tree survives.
kill "$realtime_pid"
wait "$realtime_pid" 2>/dev/null || true
realtime_pid=
export TEST_REALTIME_PID=0
export TEST_REALTIME_STATE=inactive TEST_REALTIME_CGROUP=
: > "$box/cgroup/realtime/workers/cgroup.procs"
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

# A failed find must not yield a partially usable prune list after the swap.
mkdir "$root/unused-sentinel"
touch -d '2018-01-01 UTC' "$root/unused-sentinel"
set +e
TEST_FIND_PARTIAL_FAIL=1 bash "$swap" "$live" partial-inventory "$box/build" partial > "$box/partial.log" 2>&1
partial_rc=$?
set -e
test "$partial_rc" -eq 5
test -d "$root/unused-sentinel"
test "$(readlink -f "$live")" = "$root/partial-inventory"
grep -q 'cannot completely inventory releases' "$box/partial.log"
echo 'PASS: runtime preservation, realtime descendants/multi-swap/collision/inactive/symlink/deleted-cwd, failed inventory, invalid identities and stale activation record'
