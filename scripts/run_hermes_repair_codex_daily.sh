#!/usr/bin/env bash
# Run the daily GPT-5.5 Codex automation for Hermes repair work.

set -euo pipefail

export CODEX_HOME="${CODEX_HOME:-$HOME/.codex}"

AUTOMATION_ID="back-test-post-mortem-daily-check"
AUTOMATION_DIR="$CODEX_HOME/automations/$AUTOMATION_ID"
LOG_DIR="$AUTOMATION_DIR/logs"
REPO_DIR="${GRID_REPO:-/Users/anikdang/dev/GRID}"
CODEX_BIN="${CODEX_BIN:-/opt/homebrew/bin/codex}"
MODEL="${CODEX_MODEL:-gpt-5.5}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"

mkdir -p "$LOG_DIR"

if [[ ! -x "$CODEX_BIN" ]]; then
  echo "Codex binary not executable: $CODEX_BIN" >&2
  exit 2
fi

if [[ ! -d "$REPO_DIR" ]]; then
  echo "GRID repo not found: $REPO_DIR" >&2
  exit 2
fi

PROMPT_FILE="$LOG_DIR/hermes-repair-prompt-$STAMP.md"
OUT_FILE="$LOG_DIR/hermes-repair-codex-$STAMP.out.log"
ERR_FILE="$LOG_DIR/hermes-repair-codex-$STAMP.err.log"
LAST_FILE="$LOG_DIR/hermes-repair-codex-$STAMP.final.md"

cat > "$PROMPT_FILE" <<'PROMPT'
Automation: back test / post mortem / daily check
Automation ID: back-test-post-mortem-daily-check
Automation memory: $CODEX_HOME/automations/back-test-post-mortem-daily-check/memory.md

Daily Hermes repair pass:
- Read automation memory first.
- Check Hermes logs, operator_issues, and the local/server-facing runtime surfaces available from this host.
- Run a repair-focused Hermes dry run or the narrowest safe smoke that exposes current errors.
- Fix severe Hermes/runtime mistakes directly.
- If Hermes lacks a bounded skill needed to repair a repeated class of issue, add the skill with tests.
- Defer minor issues into /Users/anikdang/dev/obsidian-vault/Inbox/Agent-TODO.md.
- Leave a durable report with ~/scripts/agent_hub/report_to_hub.sh.
- Update the automation memory with timestamp, decisions, fixes, verification, and unresolved blockers.
PROMPT

"$CODEX_BIN" exec \
  --model "$MODEL" \
  --cd "$REPO_DIR" \
  --sandbox danger-full-access \
  --ask-for-approval never \
  -c 'model_reasoning_effort="xhigh"' \
  --output-last-message "$LAST_FILE" \
  - < "$PROMPT_FILE" > "$OUT_FILE" 2> "$ERR_FILE"
