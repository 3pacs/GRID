#!/usr/bin/env bash
# Failure-injection test for scripts/deploy_pin_candidate_sha.sh, against the
# REAL script (not a re-implementation) -- same philosophy as
# tests/deploy/test_deploy_release_swap.sh and
# tests/deploy/test_deploy_verify_release_tree.sh in this directory.
#
# Proves the specific property this script exists for: `main` advancing
# during a build cannot change the commit the candidate ends up at, or --
# via the real deploy_release_swap.sh -- the commit that ultimately goes
# live. Not "unlikely to" -- cannot, because the fetch names the exact
# commit rather than a moving branch ref.
#
#   1. Baseline: origin's `main` IS at the expected commit -- the candidate
#      lands there. Sanity check before the harder cases.
#   2. The core proof: origin's `main` has ALREADY ADVANCED to a real,
#      later commit (a genuine descendant of the expected one) BEFORE this
#      script ever runs -- reproducing "a push landed on main between this
#      deploy's checkout and its own fetch" without needing to win an
#      actual race. The candidate still ends up EXACTLY at the originally
#      expected commit, not the advanced one -- isolated to just this
#      script, no build_hook/pip/npm/alembic involved.
#   3. The same proof again, but plugged into the REAL
#      deploy_release_swap.sh as its build_hook: after `main` has advanced,
#      the release that actually goes LIVE (the symlink target real callers
#      would restart onto) is also exactly the originally expected commit,
#      not the advanced one -- "built" and "activated" both covered, not
#      just the raw candidate directory in isolation.
#   4. A candidate that already has unrelated content (matching how
#      deploy_release_swap.sh seeds a real candidate via `cp -a` from the
#      previous release) is still pinned exactly, proving this isn't
#      relying on starting from an empty directory.
#   5. An expected SHA that does not exist on the source at all fails
#      loudly (fetch error), not silently on some other commit.
#   6. Usage error: exits 2.
#
# Runs entirely inside a temp sandbox with real local git repositories used
# as fetch sources -- never touches any real GRID path, any real database,
# or any production host, and never touches the network (all "origin"
# repos are local paths). Real Linux only: relies on real `git fetch
# <path> <sha>` semantics against a local repo, verified on gridz4 before
# pushing (matching the existing caveat for this test directory on
# git-bash-on-Windows).
#
# Usage: bash tests/deploy/test_deploy_pin_candidate_sha.sh

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
PIN_SCRIPT="${REPO_ROOT}/scripts/deploy_pin_candidate_sha.sh"
SWAP_SCRIPT="${REPO_ROOT}/scripts/deploy_release_swap.sh"

SANDBOX="$(mktemp -d)"
trap 'rm -rf "$SANDBOX"' EXIT

pass_count=0
fail_count=0

assert_eq() {
  local desc="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    echo "PASS: $desc"
    pass_count=$((pass_count + 1))
  else
    echo "FAIL: $desc -- expected [$expected], got [$actual]"
    fail_count=$((fail_count + 1))
  fi
}

assert_true() {
  local desc="$1" cond="$2"
  if [ "$cond" = "true" ]; then
    echo "PASS: $desc"
    pass_count=$((pass_count + 1))
  else
    echo "FAIL: $desc"
    fail_count=$((fail_count + 1))
  fi
}

git_commit() {
  local dir="$1" msg="$2"
  git -C "$dir" -c user.name="test" -c user.email="test@example.invalid" commit -q -m "$msg"
}

# ── Shared fixture: one real "origin" git repo, used as the fetch source
#    throughout -- never the network, always a local path.

origin="${SANDBOX}/origin"
mkdir -p "$origin"
git -C "$origin" init -q -b main
echo "v1" > "$origin/marker.txt"
git -C "$origin" add marker.txt
git_commit "$origin" "commit A (what this deploy was supposed to ship)"
sha_a="$(git -C "$origin" rev-parse HEAD)"

# ── Test 1: origin's main IS at the expected commit -- baseline sanity ─────

t1_candidate="${SANDBOX}/t1/candidate"
mkdir -p "$t1_candidate"
git -C "$t1_candidate" init -q

set +e
out1="$(bash "$PIN_SCRIPT" "$t1_candidate" "$origin" "$sha_a" 2>&1)"
exit1=$?
set -e
if [ "$exit1" -ne 0 ]; then echo "--- test1 output ---"; echo "$out1"; echo "--- end ---"; fi
assert_eq "baseline (main == expected): exits 0" "0" "$exit1"
assert_eq "baseline: candidate HEAD is exactly the expected commit" \
  "$sha_a" "$(git -C "$t1_candidate" rev-parse HEAD)"

# ── Test 2: origin's main has ALREADY ADVANCED past the expected commit
#    before this script ever runs -- the core race-closure proof ──────────

echo "v2" > "$origin/marker.txt"
git -C "$origin" add marker.txt
git_commit "$origin" "commit B (a later, unrelated push that landed on main mid-build)"
sha_b="$(git -C "$origin" rev-parse HEAD)"
# Confirm the fixture is real: B is a genuine descendant of A, not just a
# same-named commit -- if this ever fails, the test below would be proving
# nothing.
assert_true "fixture check: commit B is a real descendant of commit A" \
  "$(git -C "$origin" merge-base --is-ancestor "$sha_a" "$sha_b" && echo true || echo false)"

t2_candidate="${SANDBOX}/t2/candidate"
mkdir -p "$t2_candidate"
git -C "$t2_candidate" init -q

set +e
out2="$(bash "$PIN_SCRIPT" "$t2_candidate" "$origin" "$sha_a" 2>&1)"
exit2=$?
set -e
if [ "$exit2" -ne 0 ]; then echo "--- test2 output ---"; echo "$out2"; echo "--- end ---"; fi
assert_eq "main advanced past expected: pin script still exits 0" "0" "$exit2"
assert_eq "main advanced past expected: candidate HEAD is EXACTLY commit A, not the advanced B" \
  "$sha_a" "$(git -C "$t2_candidate" rev-parse HEAD)"
assert_eq "main advanced past expected: candidate content matches A (v1), not B (v2)" \
  "v1" "$(cat "$t2_candidate/marker.txt")"

# ── Test 3: the same scenario, plugged into the REAL deploy_release_swap.sh
#    -- proves the ACTIVATED (live-swapped) release is also unaffected by
#    main having advanced, not just the raw candidate directory ──────────

t3_live="${SANDBOX}/t3/grid_release"
mkdir -p "$(dirname "$t3_live")"
# A real production release directory always already has git metadata
# (deploy_build_hook.sh's fetch+reset approach is incremental) -- not a bare
# marker file. Seed it as a real checkout so deploy_release_swap.sh's
# copy-forward (`cp -a`) during the plain-directory-to-symlink migration
# produces something `git fetch` can actually run against, matching
# production, not an artificial gap this test would otherwise introduce.
git clone -q "$origin" "$t3_live" >/dev/null 2>&1
git -C "$t3_live" checkout -q "$sha_a"

# A minimal build_hook that does ONLY the pinning step (mirrors how
# deploy_build_hook.sh delegates to this script, without needing a working
# pip/npm/alembic toolchain just to prove this specific property).
t3_hook="${SANDBOX}/t3/pin_only_hook.sh"
cat > "$t3_hook" << HOOK
#!/usr/bin/env bash
set -euo pipefail
"$PIN_SCRIPT" "\$1" "$origin" "\$3"
HOOK
chmod +x "$t3_hook"

set +e
bash "$SWAP_SCRIPT" "$t3_live" "$sha_a" "$t3_hook" "$origin" "$sha_a" \
  > "${SANDBOX}/t3/swap.log" 2>&1
t3_exit=$?
set -e
if [ "$t3_exit" -ne 0 ]; then echo "--- t3 swap.log ---"; cat "${SANDBOX}/t3/swap.log"; echo "--- end ---"; fi

assert_eq "activated via real swap script (main already advanced): swap exits 0" "0" "$t3_exit"
t3_live_target="$(readlink -f "$t3_live")"
assert_eq "activated: the LIVE (swapped) target's HEAD is exactly commit A, not advanced B" \
  "$sha_a" "$(git -C "$t3_live_target" rev-parse HEAD)"
assert_eq "activated: the LIVE content matches A (v1), not B (v2) -- what a restart would actually serve" \
  "v1" "$(cat "$t3_live/marker.txt")"

# ── Test 4: candidate pre-seeded with unrelated existing content, matching
#    how deploy_release_swap.sh really seeds one (`cp -a` from the previous
#    release) -- not relying on starting from an empty directory ─────────

t4_candidate="${SANDBOX}/t4/candidate"
mkdir -p "$t4_candidate"
git -C "$t4_candidate" init -q
echo "leftover-from-a-previous-release" > "$t4_candidate/stale.txt"
git -C "$t4_candidate" add stale.txt
git_commit "$t4_candidate" "unrelated prior content"

set +e
out4="$(bash "$PIN_SCRIPT" "$t4_candidate" "$origin" "$sha_a" 2>&1)"
exit4=$?
set -e
assert_eq "pre-seeded candidate: pin script still exits 0" "0" "$exit4"
assert_eq "pre-seeded candidate: ends up exactly at commit A regardless of prior content" \
  "$sha_a" "$(git -C "$t4_candidate" rev-parse HEAD)"
assert_true "pre-seeded candidate: the unrelated prior file is gone (real git reset --hard, not a merge)" \
  "$([ ! -f "$t4_candidate/stale.txt" ] && echo true || echo false)"

# ── Test 5: expected SHA does not exist on the source at all -- fails
#    loudly, not silently on whatever main happens to be ─────────────────

t5_candidate="${SANDBOX}/t5/candidate"
mkdir -p "$t5_candidate"
git -C "$t5_candidate" init -q

bogus_sha="0000000000000000000000000000000000dead"
set +e
out5="$(bash "$PIN_SCRIPT" "$t5_candidate" "$origin" "$bogus_sha" 2>&1)"
exit5=$?
set -e
assert_true "nonexistent expected sha: exits non-zero" "$([ "$exit5" -ne 0 ] && echo true || echo false)"

# ── Usage error: exits 2 ────────────────────────────────────────────────────

set +e
bash "$PIN_SCRIPT" "$t1_candidate" "$origin" > "${SANDBOX}/usage.log" 2>&1
exit_usage=$?
set -e
assert_eq "missing argument: exits with the usage-error code" "2" "$exit_usage"

echo
echo "=== ${pass_count} passed, ${fail_count} failed ==="
if [ "$fail_count" -gt 0 ]; then
  exit 1
fi
