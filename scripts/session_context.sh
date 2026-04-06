#!/usr/bin/env bash
# GRID Session Context Generator
# Called by SessionStart hook to inject live state into Claude's context.
# Must output JSON with hookSpecificOutput.additionalContext.
# Designed to complete in <8 seconds.

set -euo pipefail

GRID_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SERVER="grid-svr"
DB_CMD="PGPASSWORD=gridmaster2026 psql -U grid -d griddb -h localhost -t -A"

# --- Static index (always available) ---
STATIC_INDEX=""
if [ -f "$GRID_DIR/.claude/CODEBASE_INDEX.md" ]; then
    STATIC_INDEX=$(cat "$GRID_DIR/.claude/CODEBASE_INDEX.md")
fi

# --- Live state from server (SSH with ConnectTimeout) ---
LIVE_STATE=""
LIVE_STATE=$(ssh -o ConnectTimeout=5 -o BatchMode=yes -o ServerAliveInterval=2 "$SERVER" "
    echo '## Server Services'
    systemctl is-active grid-api grid-realtime grid-scheduler grid-hermes 2>/dev/null | paste - - - - | awk '{printf \"grid-api=%s grid-realtime=%s grid-scheduler=%s grid-hermes=%s\n\", \$1, \$2, \$3, \$4}'

    echo ''
    echo '## DB Table Counts'
    $DB_CMD -c \"
        SELECT string_agg(tbl || '=' || cnt::text, ' ')
        FROM (
            SELECT 'actors' as tbl, count(*) as cnt FROM actors
            UNION ALL SELECT 'signal_data', count(*) FROM signal_data
            UNION ALL SELECT 'discovered_hypotheses', count(*) FROM discovered_hypotheses
            UNION ALL SELECT 'company_profiles', count(*) FROM company_profiles
            UNION ALL SELECT 'realtime_candles', count(*) FROM realtime_candles
            UNION ALL SELECT 'hypothesis_postmortems', count(*) FROM hypothesis_postmortems
            UNION ALL SELECT 'oracle_predictions', count(*) FROM oracle_predictions
            UNION ALL SELECT 'decision_journal', count(*) FROM decision_journal
        ) t;
    \" 2>/dev/null || echo 'DB_QUERY_FAILED'

    echo ''
    echo '## Latest Data'
    $DB_CMD -c \"
        SELECT string_agg(tbl || '=' || latest, ' ')
        FROM (
            SELECT 'signals' as tbl, max(created_at)::date::text as latest FROM signal_data
            UNION ALL SELECT 'candles', max(ts)::date::text FROM realtime_candles
            UNION ALL SELECT 'hypotheses', max(created_at)::date::text FROM discovered_hypotheses
        ) t;
    \" 2>/dev/null || echo 'FRESHNESS_QUERY_FAILED'
" 2>/dev/null) || LIVE_STATE="SERVER_UNREACHABLE"

# --- Local git state ---
GIT_STATE=$(cd "$GRID_DIR" && git log --oneline -3 2>/dev/null || echo "no git")

# --- Assemble context ---
CONTEXT="# GRID Session Context (auto-generated)

## Git (last 3 commits)
$GIT_STATE

## Live Server State
$LIVE_STATE

$STATIC_INDEX"

# Output JSON for hook
python3 -c "
import json, sys
ctx = sys.stdin.read()
print(json.dumps({
    'hookSpecificOutput': {
        'hookEventName': 'SessionStart',
        'additionalContext': ctx
    }
}))
" <<< "$CONTEXT"
