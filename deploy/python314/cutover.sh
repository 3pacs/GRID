#!/usr/bin/env bash
# Move GRID's systemd services from /usr/bin/python3 (3.10 + /home/grid/.local) to /data/grid_v4/grid_venv-314
# (Anik 2026-11-04: "lets get it to 3.14"). One drop-in per unit, /etc/systemd/system/<unit>.d/py314.conf, that resets
# ExecStart and repeats the unit's own command with the interpreter swapped. Nothing else in the unit changes.
#
#   bash cutover.sh plan        read-only: each unit's current ExecStart and the 3.14 one (default)
#   bash cutover.sh apply       write the drop-ins, daemon-reload, restart the units that are active now
#   bash cutover.sh rollback    remove every py314.conf drop-in, daemon-reload, restart the units that are active now
#   bash cutover.sh status      which units carry the drop-in, their state and restart counts
#
# Run on grid-svr as grid (apply/rollback use the grid user's sudo for the drop-ins and systemctl). The unit list is
# explicit (UNITS below): GRID services whose ExecStart runs /usr/bin/python3. Not touched: gex-*, muse-*,
# astrogrid-api, boogerbots-web, ua-*/ubuntu-advantage, grid-media-backup (stdlib libexec script), and the units already
# on their own venvs (grid-gem-outcomes, grid-goal-worker@, grid-options-puller, grid-predict-crypto, grid-td-*).
# Timer-started oneshots pick the drop-in up at their next run. The grid user's crontab is out of scope: on 2026-11-04
# none of its active lines ran /usr/bin/python3 (they use grid_v4/venv or /data/grid_v4/venv; paper_log jobs run frozen,
# commit-pinned code and stay on their interpreter).
set -euo pipefail

VENV_PY=/data/grid_v4/grid_venv-314/bin/python
DROPIN=py314.conf
UNITS=(
  grid-api grid-hermes grid-coordinator grid-worker grid-intelligence grid-scheduler grid-realtime
  grid-backlinker grid-breaking-news grid-extractor grid-prefect-trust-scores grid-surfacer-backfill
  grid-analytics-snapshots grid-causal-links grid-compute-producer grid-compute-reaper grid-confidence-calibration
  grid-dollar-flows grid-e2-scoreboard grid-fleet-heartbeat grid-flow-materializer grid-gem-watchlist-coverage
  grid-godview-cftc grid-godview-fed grid-hypothesis-forward-log grid-market-diary grid-regime-state-vectors
  grid-spider grid-walk-forward-daily grid-walk-forward-weekly
  grid-dealer-gex-daily grid-market-state
)

# Never restarted by this script: deploy.yml requires an explicit human acknowledgement before interrupting the
# sequential ingestion loop (some pullers are not audited for re-run dedup). Its drop-in is installed like the rest and
# takes effect when you restart it yourself, once `journalctl -u grid-scheduler` shows it idle between passes.
NO_AUTO_RESTART=(grid-scheduler)
held() { [[ " ${NO_AUTO_RESTART[*]} " == *" $1 "* ]]; }

mode=${1:-plan}
[ "$(id -un)" = grid ] || { echo "ABORT: run as grid on grid-svr" >&2; exit 2; }
[ -x "$VENV_PY" ] || { echo "ABORT: $VENV_PY missing (build_venv.sh first)" >&2; exit 2; }

# the unit's own effective ExecStart line(s) from its fragment + drop-ins, excluding our own drop-in
own_execstart() {
  local u=$1
  # backslash-continued directives are joined first (several units spread ExecStart over lines)
  systemctl cat "$u.service" 2>/dev/null | sed -e ':a' -e '/\\$/N; s/\\\n[[:space:]]*/ /; ta' | awk -v skip="/$u.service.d/$DROPIN" '
    /^# \// { infile=$2; next }
    infile ~ skip { next }
    /^ExecStart=/ { v=substr($0, 11); if (v == "") { n=0 } else { lines[++n]=v } }
    END { for (i=1; i<=n; i++) print lines[i] }'
}

swap() {   # /usr/bin/python3 or /usr/bin/python3.10 as a whole word -> the venv interpreter
  sed -E "s#(^|[[:space:]])/usr/bin/python3(\.10)?([[:space:]]|\$)#\1${VENV_PY}\3#g"
}

status_report() {   # per unit: drop-in present, state, restart count, pid (a new pid after apply = restarted on 3.14)
  for u in "${UNITS[@]}"; do
    d=-; [ -f "/etc/systemd/system/$u.service.d/$DROPIN" ] && d=3.14
    printf '%-34s %-5s %-9s restarts=%s pid=%s\n' "$u" "$d" "$(systemctl is-active "$u.service" 2>/dev/null || true)" \
      "$(systemctl show -p NRestarts --value "$u.service" 2>/dev/null)" "$(systemctl show -p MainPID --value "$u.service" 2>/dev/null)"
  done
  code=$(curl -s -m 5 -o /dev/null -w '%{http_code}' http://127.0.0.1:8000/api/v1/system/health || true)
  echo "grid-api health: HTTP $code"
}

case "$mode" in
  plan|apply)
    for u in "${UNITS[@]}"; do
      old=$(own_execstart "$u")
      [ -n "$old" ] || { echo "SKIP $u: no unit / no ExecStart"; continue; }
      new=$(printf '%s\n' "$old" | swap)
      if [ "$new" = "$old" ]; then echo "SKIP $u: ExecStart has no /usr/bin/python3"; continue; fi
      state=$(systemctl is-active "$u.service" 2>/dev/null || true)
      echo "== $u ($state)"; echo "   now: $old"; echo "   3.14: $new"
      if [ "$mode" = apply ]; then
        { echo "# GRID Python 3.14 cutover (deploy/python314/cutover.sh); remove with: cutover.sh rollback"
          echo "[Service]"; echo "ExecStart="; printf 'ExecStart=%s\n' "$new"; echo "Environment=GRID_PYTHON_RUNTIME=3.14"; } |
          sudo -n install -D -m 0644 /dev/stdin "/etc/systemd/system/$u.service.d/$DROPIN"
      fi
    done
    if [ "$mode" = apply ]; then
      sudo -n systemctl daemon-reload
      for u in "${UNITS[@]}"; do
        [ -f "/etc/systemd/system/$u.service.d/$DROPIN" ] || continue
        if [ "$(systemctl is-active "$u.service" 2>/dev/null || true)" = active ]; then
          if held "$u"; then echo "HELD $u: restart it yourself when idle: sudo systemctl restart $u.service"; continue; fi
          echo "restart $u"; sudo -n systemctl restart "$u.service"
        fi
      done
      sleep 20
      status_report
    fi
    ;;
  rollback)
    removed=()
    for u in "${UNITS[@]}"; do
      f="/etc/systemd/system/$u.service.d/$DROPIN"
      if [ -f "$f" ]; then sudo -n rm -f "$f"; echo "removed $f"; removed+=("$u"); fi
    done
    sudo -n systemctl daemon-reload
    for u in "${removed[@]}"; do
      if [ "$(systemctl is-active "$u.service" 2>/dev/null || true)" = active ]; then
        if held "$u"; then echo "HELD $u: restart it yourself when idle: sudo systemctl restart $u.service"; continue; fi
        echo "restart $u"; sudo -n systemctl restart "$u.service"
      fi
    done
    sleep 20
    status_report
    ;;
  status)
    status_report
    ;;
  *) echo "usage: cutover.sh plan|apply|rollback|status" >&2; exit 2 ;;
esac
