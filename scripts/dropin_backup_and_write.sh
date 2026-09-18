#!/usr/bin/env bash
# Writes new content (from stdin) to a systemd drop-in file, backing up
# whatever is already there first rather than assuming deletion is a safe
# rollback -- true only if this is genuinely the first activation. Prints
# the exact rollback command either way, so a human never has to guess it.
#
# Extracted so this logic can actually be tested (tests/test_dropin_backup_and_write.py)
# instead of only existing as inline bash inside deploy.yml, where the
# closest thing to coverage was reading it.
#
# Usage: dropin_backup_and_write.sh <dropin_path> <service_name> < new_content
#   Prints backup/rollback info to stdout, then writes stdin to <dropin_path>.
#   Caller is responsible for privilege (sudo) and for daemon-reload/restart
#   afterward -- this script only ever touches the one file it's given.
set -euo pipefail

DROPIN="${1:?usage: dropin_backup_and_write.sh <dropin_path> <service_name> < new_content}"
SERVICE="${2:?usage: dropin_backup_and_write.sh <dropin_path> <service_name> < new_content}"

if [ -f "$DROPIN" ]; then
  BACKUP="${DROPIN}.bak-$(date -u +%Y%m%dT%H%M%SZ)"
  cp -a "$DROPIN" "$BACKUP"
  echo "Existing drop-in backed up to $BACKUP"
  echo "Exact rollback: cp -a $BACKUP $DROPIN && systemctl daemon-reload && systemctl restart $SERVICE"
else
  echo "No pre-existing drop-in at $DROPIN (first activation)."
  echo "Exact rollback: rm $DROPIN && systemctl daemon-reload && systemctl restart $SERVICE"
fi

cat > "$DROPIN"
