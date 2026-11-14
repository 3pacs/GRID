#!/bin/bash
# One-shot runner for retract_vix_spot_20260409.py (operator approved 2026-11-13).
# Dry run first; apply only if the dry run exits 0. Outputs never overwrite (label is unique per run).
set -u
label="$1"
P=/data/agent-home/anikdang/grid-repair-plans
cd "$P" || exit 1
set -a; . /home/grid/grid_v4/grid_repo/.env >/dev/null 2>&1; set +a
export PYTHONPATH=/data/grid_v4/grid_release
/usr/bin/python3 retract_vix_spot_20260409.py --run-tag vix-spot-yf-20260409 --expect-rows 9134 \
  --out "$P/vix-0409-retract-$label-dryrun.json" 2>"$P/vix-0409-retract-$label-dryrun.stderr.log" >/dev/null
rc=$?
echo "dryrun rc=$rc" >"$P/vix-0409-retract-$label.status"
[ "$rc" -eq 0 ] || exit "$rc"
/usr/bin/python3 retract_vix_spot_20260409.py --apply --run-tag vix-spot-yf-20260409 --expect-rows 9134 \
  --out "$P/vix-0409-retract-$label-apply.json" 2>"$P/vix-0409-retract-$label-apply.stderr.log" >/dev/null
rc=$?
echo "apply rc=$rc" >>"$P/vix-0409-retract-$label.status"
exit "$rc"
