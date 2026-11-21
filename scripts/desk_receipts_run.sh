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
# receipt is held, with the hub's own arrival time, somewhere this host cannot
# rewrite. It is weaker than a committed anchor file
# (docs/reference/DESK_SOURCE_RECEIPTS.md): it is checked by a person reading
# it, not by `verify --external-anchors`.
#
# Delivery is separate from recording and can be retried. Every run posts each
# receipt whose (session, head) has no delivery note, oldest first, and writes
# the note only after the reporter exits 0 and prints its "sent" line.
# agent-report exits 0 when it merely spooled a post the hub refused; that is
# not a delivery here. A post that fails leaves the
# receipt as it is, leaves the head pending, and makes this run exit 5, so the
# next run (19:20, or the evening check) posts the same session and the same
# head again. A run that finds nothing new to record still delivers what is
# pending. The post for one (session, head) is the same text every time.
#
# A source recorded as unknown is not a failure of this script: the receipt
# was written and says so, and the hub post names the unknown sources.
#
# Environment:
#   DESK_RECEIPTS_DIR      the log folder (required)
#   DESK_FACTORY_LEDGER    module-factory ledger file (required)
#   DESK_FACTORY_ANCHOR    its anchor file (required)
#   DESK_PANEL_TRIALS_DB   gex-signals signals.db, read-only (optional). Unset:
#                          the live trial ledger is recorded as unknown.
#   DESK_RECEIPTS_PYTHON   interpreter (default: python3)
#   DESK_RECEIPTS_REPORT   hub reporter (default: agent-report). Empty turns
#                          delivery off: heads stay pending and the run says so.
# Exit codes: 0 recorded or already recorded, and nothing pending that this
# run was meant to deliver; 2 refused (session not closed or superseded);
# 3 the recorder crashed; 4 the log failed verification; 5 the receipt is
# safe but a head could not be handed to the witness (retry delivers it).
set -uo pipefail

: "${DESK_RECEIPTS_DIR:?set DESK_RECEIPTS_DIR}"
: "${DESK_FACTORY_LEDGER:?set DESK_FACTORY_LEDGER}"
: "${DESK_FACTORY_ANCHOR:?set DESK_FACTORY_ANCHOR}"
PY="${DESK_RECEIPTS_PYTHON:-python3}"
REPORT="${DESK_RECEIPTS_REPORT-agent-report}"

say() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] desk-receipts: $*"; }
field() { "$PY" -c 'import json,sys; v=json.load(sys.stdin).get(sys.argv[1]); print(",".join(v) if isinstance(v,list) else ("" if v is None else v))' "$1"; }
dsr() { "$PY" -m intelligence.desk_source_receipts "$@"; }
post() {  # post <slug> <body-file>; 0 only when the reporter says the hub took it
  # agent-report is fail-soft: it exits 0 when the hub refused the post and it
  # only spooled it for later. So the exit code is not enough. A delivery
  # counts when the reporter exits 0 AND prints its "sent" line for this slug.
  local out
  if ! command -v "$REPORT" >/dev/null 2>&1; then
    say "hub reporter '$REPORT' not found"
    return 1
  fi
  out="$("$REPORT" desk-receipts "$1" "$2" 2>&1)" || { say "hub reporter exited nonzero for $1"; return 1; }
  case "$out" in
    *"agent-report sent: desk-receipts/$1 "*) return 0 ;;
  esac
  say "hub reporter did not confirm $1 (spooled or unanswered)"
  return 1
}
alert() {  # alert <slug> <body-file>; a failure notice, best effort
  if [ -n "$REPORT" ] && post "$1" "$2"; then
    say "posted $1 to the hub"
  else
    say "hub post $1 NOT delivered"
  fi
}

body="$(mktemp)"; trap 'rm -f "$body"' EXIT
host="$(hostname -s 2>/dev/null || hostname)"

panel=(); [ -n "${DESK_PANEL_TRIALS_DB:-}" ] && panel=(--panel-trials-db "$DESK_PANEL_TRIALS_DB")
out="$("$PY" -m intelligence.desk_source_receipts record --dir "$DESK_RECEIPTS_DIR" \
        --factory-ledger "$DESK_FACTORY_LEDGER" --factory-anchor "$DESK_FACTORY_ANCHOR" "${panel[@]}")"
rc=$?
echo "$out"
if [ "$rc" -eq 2 ]; then
  say "refused: $(echo "$out" | field refused)"
  exit 2
fi
if [ "$rc" -ne 0 ]; then
  printf '# Desk source receipts: the recorder crashed on %s\n\nExit code %s. No receipt was written by this run. See `journalctl --user -u grid-desk-receipts` on %s.\n\nA session that is not recorded before the next close stays unknown.\n' "$host" "$rc" "$host" > "$body"
  alert "desk-receipts-crash-$(date -u +%Y%m%d)" "$body"
  exit 3
fi

check="$("$PY" -m intelligence.desk_source_receipts verify --dir "$DESK_RECEIPTS_DIR")"
vrc=$?
if [ "$vrc" -ne 0 ]; then
  echo "$check"
  printf '# Desk source receipts: the log FAILED verification on %s\n\n%s\n\nNothing more will be appended until a person repairs it.\n' "$host" "$(echo "$check" | field detail)" > "$body"
  alert "desk-receipts-broken-$(date -u +%Y%m%d)" "$body"
  exit 4
fi

session="$(echo "$out" | field session)"
unknown="$(echo "$out" | field unknown_sources)"
if [ "$(echo "$out" | field written)" = "True" ]; then
  say "recorded $session; unknown: ${unknown:-none}"
else
  say "session $session already recorded; nothing written"
fi
if [ "$(echo "$out" | field recovered_anchor)" = "True" ]; then
  say "finished the anchor an interrupted run left out"
fi

# Hand every pending head to the witness. The receipt above is safe whatever
# happens here.
pending="$(dsr witness-status --dir "$DESK_RECEIPTS_DIR" --pending --lines)"
prc=$?
if [ "$prc" -ne 0 ]; then
  echo "$pending"
  say "witness status could not be read (exit $prc); delivery not attempted"
  exit 5
fi
if [ -z "$pending" ]; then
  say "witness: nothing pending"
  exit 0
fi
if [ -z "$REPORT" ]; then
  say "witness: delivery is off (DESK_RECEIPTS_REPORT is empty); pending: $(echo "$pending" | cut -f1 | paste -sd, -)"
  exit 0
fi
failed=0
while IFS=$'\t' read -r w_session w_head w_records w_cutoff w_unknown; do
  [ -n "$w_session" ] || continue
  [ "$w_unknown" = "-" ] && w_unknown="none"
  {
    printf '# Desk source receipt, session %s\n\n' "$w_session"
    printf -- '- Host: %s, log `%s`\n' "$host" "$DESK_RECEIPTS_DIR"
    printf -- '- Session close (evidence cutoff): %s\n' "$w_cutoff"
    printf -- '- Receipt head sha256: `%s`\n' "$w_head"
    printf -- '- Records in the log up to this receipt: %s\n' "$w_records"
    printf -- '- Sources recorded as unknown: %s\n' "$w_unknown"
    printf '\nThis post is the off-host copy of this receipt'"'"'s head. The time the hub received it is the witness time; the sources were read after the close, at the clocks in the receipt. Compare the head with `witness-status` on the host.\n'
  } > "$body"
  if ! post "desk-receipt-$w_session" "$body"; then
    say "witness: hub post for $w_session FAILED; head $w_head stays pending"
    failed=1
    continue
  fi
  if dsr witness-mark --dir "$DESK_RECEIPTS_DIR" --session "$w_session" --head "$w_head" --via hub >/dev/null; then
    say "witness: posted $w_session head $w_head to the hub"
  else
    say "witness: posted $w_session but the delivery note was refused; it will be posted again"
    failed=1
  fi
done <<< "$pending"
[ "$failed" -eq 0 ] || exit 5
exit 0
