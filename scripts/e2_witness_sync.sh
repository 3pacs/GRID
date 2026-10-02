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
# paper-log mirror's clone (/home/grid/dev/obsidian-vault-paperlog): that mirror
# refuses to run while its clone has changes outside its own folder.
#
# Each run:
#   1. refuses an unsafe clone: not on main, a git operation in progress, or
#      any change outside 05-GRID/Paper-Log/e2/ (nothing is committed then);
#   2. under the clone's sync lock, makes sure 05-GRID/Paper-Log/e2/.gitattributes
#      keeps the anchor files byte-exact (*.jsonl -text), then commits ONLY that
#      folder (git commit -- <folder>);
#   3. releases the lock and runs obsidian-vault-sync.sh on the clone, which
#      integrates origin/main and pushes (it adds nothing else: the clone is
#      clean outside the folder, checked in step 1).
#
# E2_VAULT_SYNC overrides the sync script (tests use a stub). Exit status is
# non-zero only when the clone is unsafe or the commit is refused.
set -euo pipefail

CLONE="${1:-/home/grid/dev/obsidian-vault-e2witness}"
FOLDER="05-GRID/Paper-Log/e2"
SYNC="${E2_VAULT_SYNC:-/home/grid/bin/obsidian-vault-sync.sh}"
LOCK_WAIT_S="${E2_WITNESS_LOCK_WAIT_S:-60}"

say() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] e2-witness: $*"; }

case "$(basename "$CLONE")" in
  obsidian-vault-paperlog)
    say "refusing: $CLONE is the GEX paper-log mirror's clone"; exit 2 ;;
esac

[ -d "$CLONE/.git" ] || { say "refusing: $CLONE is not a git clone"; exit 2; }
cd "$CLONE"

branch="$(git symbolic-ref --short -q HEAD || true)"
[ "$branch" = "main" ] || { say "refusing: clone not on main (branch=${branch:-detached})"; exit 2; }
for marker in rebase-merge rebase-apply MERGE_HEAD CHERRY_PICK_HEAD REVERT_HEAD; do
  [ ! -e ".git/$marker" ] || { say "refusing: a git operation is in progress ($marker)"; exit 2; }
done

stray="$(git status --porcelain=v1 --untracked-files=all | cut -c4- | grep -v -e "^$FOLDER/" -e "^\"$FOLDER/" || true)"
if [ -n "$stray" ]; then
  say "refusing: the clone has changes outside $FOLDER: $(echo "$stray" | head -5 | tr '\n' ' ')"
  exit 3
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

# Push (and integrate origin) with the shared sync script, outside our lock: it takes the same lock.
"$SYNC" "$CLONE"
if git merge-base --is-ancestor HEAD origin/main 2>/dev/null; then
  say "pushed: HEAD $(git rev-parse --short HEAD) is on origin/main"
else
  say "not on origin/main yet; the next run retries the push"
fi
exit 0
