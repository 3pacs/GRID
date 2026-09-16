#!/usr/bin/env bash
# Writes a systemd drop-in pinning WorkingDirectory to the release path,
# backing up any pre-existing drop-in first so a human can actually restore
# it -- not just be told the shape of the command that would.
#
# Split out of deploy.yml's inline restart step (rather than left inline)
# specifically so this logic is unit-testable against a temp directory
# instead of only exercisable by actually restarting grid-realtime on
# grid-svr. See tests/test_realtime_dropin_backup.py, which round-trips a
# real backup-then-restore and asserts the file byte-for-byte matches what
# was there before -- "verified rollback", not just a printed command.
#
# Usage: realtime_dropin_backup.sh <dropin_dir> <dropin_file> <working_directory>
#   Prints BACKED_UP:<path> or NO_PRIOR_DROPIN, then ROLLBACK:<exact command>,
#   then WROTE:<path>, in that order, one per line.
set -euo pipefail

dropin_dir="$1"
dropin_file="$2"
working_directory="$3"
service_name="${4:-grid-realtime}"

mkdir -p "$dropin_dir"

if [ -f "$dropin_file" ]; then
  backup="${dropin_file}.bak-$(date -u +%Y%m%dT%H%M%SZ)"
  cp -a "$dropin_file" "$backup"
  echo "BACKED_UP:$backup"
  echo "ROLLBACK:cp -a $backup $dropin_file && systemctl daemon-reload && systemctl restart $service_name"
else
  echo "NO_PRIOR_DROPIN"
  echo "ROLLBACK:rm $dropin_file && systemctl daemon-reload && systemctl restart $service_name"
fi

printf '%s\n' '[Service]' "WorkingDirectory=$working_directory" > "$dropin_file"
echo "WROTE:$dropin_file"
