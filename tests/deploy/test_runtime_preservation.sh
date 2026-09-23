#!/usr/bin/env bash
# Linux-only integration test. Uses a real process cwd and a fake systemctl.
set -euo pipefail
repo="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
swap="$repo/scripts/deploy_release_swap.sh"
box="$(mktemp -d)"
trap 'kill "${scheduler_pid:-}" "${activated_pid:-}" 2>/dev/null || true; rm -rf "$box"' EXIT
live="$box/grid_release"
root="${live}.releases"
mkdir -p "$root/scheduler-old" "$root/recovery-old" "$root/current" "$box/bin"
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
( cd "$root/scheduler-old" && exec sleep 120 ) &
scheduler_pid=$!
export TEST_SCHEDULER_PID="$scheduler_pid" TEST_SCHEDULER_WORKDIR="$root/scheduler-old"
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

# Old scheduler dir sorts last by mtime; both protected identities survive
# two swaps and the ordinary retention budget still prunes stale current.
touch -d '2020-01-01 UTC' "$root/scheduler-old" "$root/recovery-old"
bash "$swap" "$live" next-1 "$box/build" next-1 > "$box/first.log" 2>&1
test -d "$root/scheduler-old" && test -d "$root/recovery-old"
touch -d '2021-01-01 UTC' "$root/current"
bash "$swap" "$live" next-2 "$box/build" next-2 > "$box/second.log" 2>&1
test -d "$root/scheduler-old" && test -d "$root/recovery-old"
test "$(readlink -f "$live")" = "$root/next-2"
test ! -d "$root/current"

# Retrying the live label is a no-op, including when it is a protected path.
bash "$swap" "$live" next-2 "$box/build" overwritten > "$box/same.log" 2>&1
test "$(cat "$live/marker.txt")" = next-2

# After an acknowledged scheduler activation, the old record must stop the
# next deployment until the controller updates both approved identities.
( cd "$root/next-2" && exec sleep 120 ) &
activated_pid=$!
TEST_SCHEDULER_PID="$activated_pid" TEST_SCHEDULER_WORKDIR="$root/next-2" \
  fail_without_swap "$root/next-2" stale-after-activation
echo 'PASS: runtime preservation, two swaps, invalid identities, same release, and stale activation record'
