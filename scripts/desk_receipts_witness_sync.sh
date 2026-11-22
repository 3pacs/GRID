#!/usr/bin/env bash
# Export, commit and push the Research Desk receipt log's anchor lines from a dedicated vault clone.
#
#   usage: desk_receipts_witness_sync.sh [WITNESS_CLONE]
#
# Run by grid-desk-receipts.service (ExecStopPost, so it also runs after a failed receipt run:
# anchor lines already written must still reach GitHub). Built 2026-11-21 from
# scripts/e2_witness_sync.sh, which does the same for the E2 scoreboard.
#
# Why: the factory pre-screen look (docs/reference/FACTORY_PRESCREEN.md) is verified with
# `verify --external-anchors` against the pushed copy of this log's anchors. export-anchors
# (intelligence/desk_source_receipts.py) writes only the working-tree file; until now nothing
# committed or pushed it (DESK_SOURCE_RECEIPTS.md, "Not done").
#
# WITNESS_CLONE defaults to /home/grid/dev/obsidian-vault-deskwitness, a clone of
# 3pacs/obsidian-vault that nothing else writes. Never the GEX paper-log mirror's clone, never the
# E2 witness clone. Each run:
#   1. refuses an unsafe clone: either of those two (by real path), not on main, a git operation in
#      progress, or any change outside 05-GRID/Paper-Log/desk-receipts/ (checked before exporting);
#   2. takes the receipt job's own lock, so no receipt run is appending while it exports;
#   3. runs export-anchors into the clone (it refuses a log that fails verify, and a vault file that
#      witnesses another log);
#   4. refuses (commits nothing, pushes nothing) unless every change in the folder is an append:
#      no deletion or rename, only the log's anchor file and .gitattributes, the file ends with a
#      newline, still starts with its committed bytes, and is a byte-prefix of the receipt log's own
#      anchor file;
#   5. under the clone's sync lock, keeps the folder's .gitattributes at "*.jsonl -text" and commits
#      ONLY that folder;
#   6. releases the locks and runs obsidian-vault-sync.sh on the clone (integrate origin/main, push),
#      with GH_TOKEN/GITHUB_TOKEN removed so the stored gh login is used;
#   7. fails (and alerts the agent hub, if available) when HEAD is not on origin/main afterwards.
# The anchor lines carry only hashes, counts and times: no ticker, rule text or PHI.
#
# Environment overrides (tests): DESK_WITNESS_VAULT_SYNC (sync script), DESK_WITNESS_LOCK,
# DESK_WITNESS_PAPERLOG_CLONE, DESK_WITNESS_E2CLONE, DESK_RECEIPTS_DIR, DESK_WITNESS_EXPORT (export
# command, given the clone path), DESK_RECEIPTS_PYTHON, DESK_WITNESS_LOCK_WAIT_S.
# Exit codes: 0 ok / nothing to do; 2 unsafe clone or missing tool; 3 changes outside the folder;
# 4 the folder is not a pure append; 5 not pushed; 6 export-anchors refused.
set -euo pipefail

CLONE="${1:-/home/grid/dev/obsidian-vault-deskwitness}"
FOLDER="05-GRID/Paper-Log/desk-receipts"
ANCHOR_NAME="grid-desk-source-receipts.anchors.jsonl"
CODE="$(cd "$(dirname "$0")/.." && pwd)"
SYNC="${DESK_WITNESS_VAULT_SYNC:-/home/grid/bin/obsidian-vault-sync.sh}"
RECEIPTS_LOCK="${DESK_WITNESS_LOCK:-/tmp/grid-desk-receipts.lock}"
PAPERLOG="${DESK_WITNESS_PAPERLOG_CLONE:-/home/grid/dev/obsidian-vault-paperlog}"
E2CLONE="${DESK_WITNESS_E2CLONE:-/home/grid/dev/obsidian-vault-e2witness}"
BOARD_DIR="${DESK_RECEIPTS_DIR:-/data/grid_ledgers/desk-receipts}"
PY="${DESK_RECEIPTS_PYTHON:-python3}"
LOCK_WAIT_S="${DESK_WITNESS_LOCK_WAIT_S:-30}"

export_anchors() {
  if [ -n "${DESK_WITNESS_EXPORT:-}" ]; then "$DESK_WITNESS_EXPORT" "$CLONE"; return; fi
  (cd "$CODE" && "$PY" -m intelligence.desk_source_receipts export-anchors --dir "$BOARD_DIR" --vault-worktree "$CLONE")
}

say() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] desk-witness: $*"; }
alert() {
  say "$1"
  if command -v agent-report >/dev/null 2>&1; then
    local body; body="$(mktemp)"
    printf 'Desk receipts witness on %s: %s\n\nclone: %s\n' "$(hostname -s)" "$1" "$CLONE" > "$body"
    agent-report desk-witness "desk-witness-$2" "$body" >/dev/null 2>&1 || true
    rm -f "$body"
  fi
}

for tool in git flock cmp stat; do
  command -v "$tool" >/dev/null 2>&1 || { say "refusing: $tool is not installed"; exit 2; }
done

[ -d "$CLONE/.git" ] || { alert "refusing: $CLONE is not a git clone" unsafe; exit 2; }
if [ "$(realpath "$CLONE")" = "$(realpath -m "$PAPERLOG")" ]; then
  alert "refusing: $CLONE is the GEX paper-log mirror's clone" unsafe; exit 2
fi
if [ "$(realpath "$CLONE")" = "$(realpath -m "$E2CLONE")" ]; then
  alert "refusing: $CLONE is the E2 scoreboard's witness clone" unsafe; exit 2
fi
cd "$CLONE"

branch="$(git symbolic-ref --short -q HEAD || true)"
[ "$branch" = "main" ] || { alert "refusing: clone not on main (branch=${branch:-detached})" unsafe; exit 2; }
for marker in rebase-merge rebase-apply MERGE_HEAD CHERRY_PICK_HEAD REVERT_HEAD index.lock; do
  [ ! -e ".git/$marker" ] || { alert "refusing: a git operation is in progress ($marker)" unsafe; exit 2; }
done

# Nothing outside the folder may be pending before we write into the clone.
pre="$(git status --porcelain=v1 -z --no-renames --untracked-files=all | tr '\0' '\n' | cut -c4- | grep -v "^$FOLDER/" | grep -v '^$' || true)"
if [ -n "$pre" ]; then
  alert "refusing: the clone has changes outside $FOLDER: $(echo "$pre" | tr '\n' ' ' | cut -c1-300)" stray; exit 3
fi

# Hold the receipt job's lock: a run still appending anchor lines must not be exported half-way.
exec 8>"$RECEIPTS_LOCK"
flock -w "$LOCK_WAIT_S" 8 || { say "a receipt run holds $RECEIPTS_LOCK; leaving the export to the next run"; exit 0; }

if ! out="$(export_anchors 2>&1)"; then
  alert "refusing: export-anchors failed: $(echo "$out" | tail -3 | tr '\n' ' ' | cut -c1-300)" export; exit 6
fi
say "export-anchors: $(echo "$out" | tail -1 | cut -c1-120)"

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
    "$ANCHOR_NAME")
      if [ -s "$path" ] && [ "$(tail -c 1 "$path" | od -An -tx1 | tr -d ' \n')" != "0a" ]; then
        problems="$problems partial-last-line:$name"
      elif [ ! -f "$BOARD_DIR/$name" ] || ! cmp -s -n "$(stat -c%s "$path")" "$path" "$BOARD_DIR/$name"; then
        problems="$problems not-in-the-ledger-anchors:$name"
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
    git -c gc.auto=0 add -- "$FOLDER"
    if ! git diff --cached --quiet -- "$FOLDER"; then
      git -c gc.auto=0 commit -q -m "desk receipts: witness anchors $(date -u +%Y-%m-%dT%H:%M:%SZ)" -- "$FOLDER"
      say "committed $(git rev-parse --short HEAD) ($FOLDER only)"
    else
      say "no new anchor lines"
    fi
  else
    say "no $FOLDER yet (the receipt log has no anchors to export)"
  fi
) 9>".git/.vault-sync.lock"
exec 8>&-  # release the receipt lock before the (slow) push

# Push (and integrate origin) with the shared sync script, outside our lock: it takes the same lock.
env -u GH_TOKEN -u GITHUB_TOKEN "$SYNC" "$CLONE" || say "the sync script exited $?"
if git merge-base --is-ancestor HEAD origin/main 2>/dev/null; then
  say "pushed: HEAD $(git rev-parse --short HEAD) is on origin/main"
  exit 0
fi
alert "HEAD $(git rev-parse --short HEAD) is not on origin/main after the sync (push failed?); the next run retries" unpushed
exit 5
