#!/usr/bin/env bash
# Commit and push the E2 scoreboard's off-host anchor lines from its dedicated vault clone.
#
#   usage: e2_witness_sync.sh [WITNESS_CLONE]
#
# Run by grid-e2-scoreboard.service (ExecStopPost, so it also runs after a
# failed scoreboard run; anchor lines already exported must still reach GitHub).
#
# WITNESS_CLONE defaults to /home/grid/dev/obsidian-vault-e2witness, a clone of
# 3pacs/obsidian-vault that nothing else writes. It must never be the GEX
# paper-log mirror's clone: that mirror refuses to run while its clone has
# changes outside its own folder.
#
# The witness file must stay append-only on main (evals.e2.witness.check_offhost
# rejects any committed version that is not a strict line-prefix extension of
# the one before), so each run:
#   1. refuses an unsafe clone: the GEX mirror's clone (by real path), not on
#      main, a git operation in progress, or any change outside 05-GRID/Paper-Log/e2/;
#   2. takes the scoreboard's own lock, so no scoreboard run is appending while
#      the folder is checked and committed;
#   3. refuses (commits nothing, pushes nothing) unless every change in the folder
#      is an append: no deletion or rename, only *.anchors.jsonl and .gitattributes,
#      every *.jsonl ends with a newline, and every tracked one still starts with
#      its committed bytes;
#   4. under the clone's sync lock, keeps 05-GRID/Paper-Log/e2/.gitattributes at
#      "*.jsonl -text" and commits ONLY that folder (git commit -- <folder>);
#   5. releases the locks and runs obsidian-vault-sync.sh on the clone (integrate
#      origin/main, push), with any GH_TOKEN/GITHUB_TOKEN from the unit's
#      environment removed so the stored gh login is used;
#   6. fails (and alerts the agent hub, if available) when HEAD is not on
#      origin/main afterwards: a push that never lands must not be silent.
#
# Environment overrides (tests): E2_VAULT_SYNC (sync script), E2_SCOREBOARD_LOCK,
# E2_PAPERLOG_CLONE, E2_WITNESS_LOCK_WAIT_S.
# Exit codes: 0 ok / nothing to do; 2 unsafe clone or missing tool; 3 changes
# outside the folder; 4 the folder is not a pure append; 5 not pushed.
set -euo pipefail

CLONE="${1:-/home/grid/dev/obsidian-vault-e2witness}"
FOLDER="05-GRID/Paper-Log/e2"
SYNC="${E2_VAULT_SYNC:-/home/grid/bin/obsidian-vault-sync.sh}"
SCOREBOARD_LOCK="${E2_SCOREBOARD_LOCK:-/tmp/grid-e2-scoreboard.lock}"
PAPERLOG="${E2_PAPERLOG_CLONE:-/home/grid/dev/obsidian-vault-paperlog}"
LOCK_WAIT_S="${E2_WITNESS_LOCK_WAIT_S:-30}"

say() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] e2-witness: $*"; }
alert() {
  say "$1"
  if command -v agent-report >/dev/null 2>&1; then
    local body; body="$(mktemp)"
    printf 'E2 scoreboard witness on %s: %s\n\nclone: %s\n' "$(hostname -s)" "$1" "$CLONE" > "$body"
    agent-report e2-witness "e2-witness-$2" "$body" >/dev/null 2>&1 || true
    rm -f "$body"
  fi
}

for tool in git flock cmp; do
  command -v "$tool" >/dev/null 2>&1 || { say "refusing: $tool is not installed"; exit 2; }
done

[ -d "$CLONE/.git" ] || { alert "refusing: $CLONE is not a git clone" unsafe; exit 2; }
if [ "$(realpath "$CLONE")" = "$(realpath -m "$PAPERLOG")" ]; then
  alert "refusing: $CLONE is the GEX paper-log mirror's clone" unsafe; exit 2
fi
cd "$CLONE"

branch="$(git symbolic-ref --short -q HEAD || true)"
[ "$branch" = "main" ] || { alert "refusing: clone not on main (branch=${branch:-detached})" unsafe; exit 2; }
for marker in rebase-merge rebase-apply MERGE_HEAD CHERRY_PICK_HEAD REVERT_HEAD index.lock; do
  [ ! -e ".git/$marker" ] || { alert "refusing: a git operation is in progress ($marker)" unsafe; exit 2; }
done

# Hold the scoreboard's lock: a run still appending anchor lines must not be committed half-way.
exec 8>"$SCOREBOARD_LOCK"
flock -w "$LOCK_WAIT_S" 8 || { say "a scoreboard run holds $SCOREBOARD_LOCK; leaving the commit to the next run"; exit 0; }

# Every change, NUL-separated, renames reported as delete + add (never hidden behind an old path).
status_file="$(mktemp)"
trap 'rm -f "$status_file"' EXIT
git status --porcelain=v1 -z --no-renames --untracked-files=all > "$status_file"
stray=""; problems=""
while IFS= read -r -d '' entry; do
  code="${entry:0:2}"; path="${entry:3}"
  case "$path" in
    "$FOLDER"/*) ;;
    *) stray="$stray $path"; continue ;;
  esac
  name="${path#"$FOLDER"/}"
  case "$code" in *D*) problems="$problems deleted:$name"; continue ;; esac
  case "$name" in
    .gitattributes) ;;
    *.anchors.jsonl)
      if [ -s "$path" ] && [ "$(tail -c 1 "$path" | od -An -tx1 | tr -d ' \n')" != "0a" ]; then
        problems="$problems partial-last-line:$name"
      elif git cat-file -e "HEAD:$path" 2>/dev/null; then
        committed="$(git cat-file -s "HEAD:$path")"
        if ! git show "HEAD:$path" | cmp -s -n "$committed" - "$path"; then
          problems="$problems not-an-append:$name"
        fi
      fi ;;
    *) problems="$problems unexpected-file:$name" ;;
  esac
done < "$status_file"

if [ -n "$stray" ]; then
  alert "refusing: the clone has changes outside $FOLDER:$(echo "$stray" | cut -c1-300)" stray; exit 3
fi
if [ -n "$problems" ]; then
  alert "refusing to commit $FOLDER, it is not a pure append:$problems" not-append; exit 4
fi

(
  flock -w "$LOCK_WAIT_S" 9 || { say "sync lock busy for ${LOCK_WAIT_S}s; leaving the commit to the next run"; exit 0; }
  if [ -d "$FOLDER" ]; then
    if [ "$(cat "$FOLDER/.gitattributes" 2>/dev/null || true)" != "*.jsonl -text" ]; then
      printf '%s\n' "*.jsonl -text" > "$FOLDER/.gitattributes"
    fi
    git add -- "$FOLDER"
    if ! git diff --cached --quiet -- "$FOLDER"; then
      git commit -q -m "e2 scoreboard: witness anchors $(date -u +%Y-%m-%dT%H:%M:%SZ)" -- "$FOLDER"
      say "committed $(git rev-parse --short HEAD) ($FOLDER only)"
    else
      say "no new anchor lines"
    fi
  else
    say "no $FOLDER yet (the scoreboard has not exported anchors)"
  fi
) 9>".git/.vault-sync.lock"
exec 8>&-  # release the scoreboard lock before the (slow) push

# Push (and integrate origin) with the shared sync script, outside our lock: it takes the same lock.
env -u GH_TOKEN -u GITHUB_TOKEN "$SYNC" "$CLONE" || say "the sync script exited $?"
if git merge-base --is-ancestor HEAD origin/main 2>/dev/null; then
  say "pushed: HEAD $(git rev-parse --short HEAD) is on origin/main"
  exit 0
fi
alert "HEAD $(git rev-parse --short HEAD) is not on origin/main after the sync (push failed?); the next run retries" unpushed
exit 5
