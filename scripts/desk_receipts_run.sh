#!/usr/bin/env bash
# Record the latest closed session's Desk source receipt, verify the log, and
# post the new head to the agent hub.
#
#   usage: desk_receipts_run.sh
#
# Run by grid-desk-receipts.service from the root of a GRID checkout, with the
# database environment already loaded (EnvironmentFile).
#
# The hub post is the log's off-host witness for now: the hub runs on another
# host and is mirrored to the vault on GitHub, so the full head hash of each
# night's log is held, with a timestamp, somewhere this host cannot rewrite.
# It is weaker than a committed anchor file (docs/reference/DESK_SOURCE_RECEIPTS.md):
# it is checked by a person reading it, not by `verify --external-anchors`.
#
# A run that finds the session already recorded posts nothing and exits 0.
# A source recorded as unknown is not a failure of this script: the receipt
# was written and says so, and the hub post names the unknown sources.
#
# Environment:
#   DESK_RECEIPTS_DIR      the log folder (required)
#   DESK_FACTORY_LEDGER    module-factory ledger file (required)
#   DESK_FACTORY_ANCHOR    its anchor file (required)
#   DESK_RECEIPTS_PYTHON   interpreter (default: python3)
#   DESK_RECEIPTS_REPORT   hub reporter (default: agent-report; empty disables)
# Exit codes: 0 recorded or already recorded; 2 refused (session not closed or
# superseded); 3 the recorder crashed; 4 the log failed verification.
set -uo pipefail

: "${DESK_RECEIPTS_DIR:?set DESK_RECEIPTS_DIR}"
: "${DESK_FACTORY_LEDGER:?set DESK_FACTORY_LEDGER}"
: "${DESK_FACTORY_ANCHOR:?set DESK_FACTORY_ANCHOR}"
PY="${DESK_RECEIPTS_PYTHON:-python3}"
REPORT="${DESK_RECEIPTS_REPORT-agent-report}"

say() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] desk-receipts: $*"; }
field() { "$PY" -c 'import json,sys; v=json.load(sys.stdin).get(sys.argv[1]); print(",".join(v) if isinstance(v,list) else ("" if v is None else v))' "$1"; }
post() {  # post <slug> <body-file>; best effort, never changes the exit code
  if [ -z "$REPORT" ] || ! command -v "$REPORT" >/dev/null 2>&1; then
    say "no hub reporter; not posted"
    return 0
  fi
  if "$REPORT" desk-receipts "$1" "$2" >/dev/null 2>&1; then
    say "posted $1 to the hub"
  else
    say "hub post $1 FAILED"
  fi
}

body="$(mktemp)"; trap 'rm -f "$body"' EXIT
host="$(hostname -s 2>/dev/null || hostname)"

out="$("$PY" -m intelligence.desk_source_receipts record --dir "$DESK_RECEIPTS_DIR" \
        --factory-ledger "$DESK_FACTORY_LEDGER" --factory-anchor "$DESK_FACTORY_ANCHOR")"
rc=$?
echo "$out"
if [ "$rc" -eq 2 ]; then
  say "refused: $(echo "$out" | field refused)"
  exit 2
fi
if [ "$rc" -ne 0 ]; then
  printf '# Desk source receipts: the recorder crashed on %s\n\nExit code %s. No receipt was written by this run. See `journalctl --user -u grid-desk-receipts` on %s.\n\nA session that is not recorded before the next close stays unknown.\n' "$host" "$rc" "$host" > "$body"
  post "desk-receipts-crash-$(date -u +%Y%m%d)" "$body"
  exit 3
fi

check="$("$PY" -m intelligence.desk_source_receipts verify --dir "$DESK_RECEIPTS_DIR")"
vrc=$?
if [ "$vrc" -ne 0 ]; then
  echo "$check"
  printf '# Desk source receipts: the log FAILED verification on %s\n\n%s\n\nNothing more will be appended until a person repairs it.\n' "$host" "$(echo "$check" | field detail)" > "$body"
  post "desk-receipts-broken-$(date -u +%Y%m%d)" "$body"
  exit 4
fi

session="$(echo "$out" | field session)"
if [ "$(echo "$out" | field written)" != "True" ]; then
  say "session $session already recorded; nothing written"
  exit 0
fi
unknown="$(echo "$out" | field unknown_sources)"
{
  printf '# Desk source receipt, session %s\n\n' "$session"
  printf -- '- Host: %s, log `%s`\n' "$host" "$DESK_RECEIPTS_DIR"
  printf -- '- Evidence cutoff: %s\n' "$(echo "$out" | field evidence_cutoff)"
  printf -- '- Log head sha256: `%s`\n' "$(echo "$check" | field head_sha256)"
  printf -- '- Records in the log: %s (receipts: %s)\n' "$(echo "$check" | field records)" "$(echo "$check" | field receipts)"
  printf -- '- Sources recorded as unknown: %s\n' "${unknown:-none}"
  printf '\nThis post is the off-host copy of the head for this session. Compare it with `verify` on the host.\n'
} > "$body"
post "desk-receipt-$session" "$body"
say "recorded $session; unknown: ${unknown:-none}"
exit 0
