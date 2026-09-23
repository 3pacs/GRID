#!/usr/bin/env bash
# Linux-only integration test. Uses a real process cwd and a fake systemctl.
set -euo pipefail
repo="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
swap="$repo/scripts/deploy_release_swap.sh"
box="$(mktemp -d)"
trap 'kill "${scheduler_pid:-}" 2>/dev/null || true; rm -rf "$box"' EXIT
live="$box/grid_release"
root="${live}.releases"
mkdir -p "$root/scheduler-old" "$root/recovery-old" "$root/current" "$box/bin"
for dir in scheduler-old recovery-old current; do
  printf '%s\n' "$dir" > "$root/$dir/marker.txt"
done
git -C "$root/scheduler-old" init -q
git -C "$root/scheduler-old" -c user.name=Test -c user.email=test@example.invalid add marker.txt
git -C "$root/scheduler-old" -c user.name=Test -c user.email=test@example.invalid commit -qm initial
sha="$(git -C "$root/scheduler-old" rev-parse HEAD)"
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
printf 'scheduler=%s\nrecovery=%s\nscheduler_sha=%s\n' \
  "$root/scheduler-old" "$root/recovery-old" "$sha" > "$record"

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
echo 'PASS: runtime preservation, two swaps, invalid targets, and same release'
