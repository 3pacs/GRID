#!/usr/bin/env bash
# Daily freshness sweep (task #153), run by grid-data-freshness-check.timer (05:00 UTC).
#
# 1. Ticker level: rebuilds data_freshness_audit and writes one alert file per
#    DEAD / STALE_30+ ticker in signal_registry.
# 2. Source level: per-PULLER_REGISTRY-source freshness from pull_log
#    (ingestion/source_freshness.py). Written as JSON plus a summary line;
#    one alert file per STALE / NEVER source. PARTIAL and UNKNOWN are reported
#    in the JSON but not alerted: their data may still be landing (see the
#    module docstring).
#
# Installed copy: /usr/local/bin/grid-data-freshness-check.sh (root-owned).
# Install from the deployed release tree:
#   sudo install -m 0755 -o root -g root \
#     /data/grid_v4/grid_release/server_setup/grid-data-freshness-check.sh \
#     /usr/local/bin/grid-data-freshness-check.sh
#
# Idempotent and read-only except for data_freshness_audit/_universe (step 1)
# and the alert files under ~/.grid/alerts.

set -euo pipefail

ALERT_DIR="${HOME}/.grid/alerts"
mkdir -p "$ALERT_DIR"

RELEASE_DIR="/data/grid_v4/grid_release"

# The reviewed SQL (transactional DELETE+INSERT, bounded lock waits) lives in
# the release tree. The host copy under astrogrid_dedup predates the
# 2026-09-17 fix (DROP TABLE + autocommit, which blocked for hours behind
# pg_dump), so it is only a last-resort fallback.
SQL_PATH="${RELEASE_DIR}/scripts/freshness_audit_universe.sql"
if [ ! -f "$SQL_PATH" ]; then
  SQL_PATH="/data/grid_v4/astrogrid_dedup/scripts/freshness_audit_universe.sql"
fi

PSQL_OPTS="-d griddb -v ON_ERROR_STOP=1"
sudo -u postgres psql $PSQL_OPTS -f "$SQL_PATH" > /tmp/freshness_audit_last.log 2>&1

TODAY="$(date -u +%Y-%m-%d)"

find "$ALERT_DIR" -maxdepth 1 -name 'data-stale-*.txt' -mtime +2 -delete 2>/dev/null || true

sudo -u postgres psql $PSQL_OPTS -t -A -F'|' -c "
  SELECT ticker, bucket, COALESCE(last_obs::text,'NEVER'), COALESCE(age_days::text,'inf')
  FROM data_freshness_audit
  WHERE audited_at = (SELECT MAX(audited_at) FROM data_freshness_audit)
    AND bucket IN ('DEAD','STALE_30+')
  ORDER BY ticker
" 2>/dev/null | while IFS='|' read -r tic bkt last age; do
  [ -z "$tic" ] && continue
  cat > "$ALERT_DIR/data-stale-${tic}.txt" <<EOF
ticker:      $tic
bucket:      $bkt
last_obs:    $last
age_days:    $age
detected_at: $TODAY
audit_source: data_freshness_audit (grid-svr griddb)
note:        Stale price/series — cross-class technical confirmation
             cannot fire for this ticker; trade-ticket extraction
             will lack spot price.
EOF
done

sudo -u postgres psql $PSQL_OPTS -c "
  SELECT bucket, COUNT(*) AS n
  FROM data_freshness_audit
  WHERE audited_at = (SELECT MAX(audited_at) FROM data_freshness_audit)
  GROUP BY bucket
  ORDER BY n DESC
" > "$ALERT_DIR/freshness-summary.txt" 2>&1

# ── Source level ─────────────────────────────────────────────────────────
# Runs as this unit's user (grid) with the app's own DB settings. A failure
# here is logged and leaves the ticker-level results above intact.
find "$ALERT_DIR" -maxdepth 1 -name 'source-stale-*.txt' -mtime +2 -delete 2>/dev/null || true
SRC_JSON="$ALERT_DIR/source-freshness.json"
if (
  cd "$RELEASE_DIR" &&
  set -a && . /home/grid/grid_v4/grid_repo/.env >/dev/null 2>&1 && set +a &&
  timeout 120 /usr/bin/python3 scripts/source_freshness_check.py --json --all > "$SRC_JSON.tmp"
); then
  mv "$SRC_JSON.tmp" "$SRC_JSON"
  /usr/bin/python3 - "$SRC_JSON" "$ALERT_DIR" "$TODAY" <<'PY'
import json, sys
path, alert_dir, today = sys.argv[1:4]
data = json.load(open(path))
print("source freshness:", " ".join(f"{k}={v}" for k, v in sorted(data["counts"].items())))
for s in data["sources"]:
    if s["verdict"] not in ("STALE", "NEVER"):
        continue
    with open(f"{alert_dir}/source-stale-{s['name']}.txt", "w") as f:
        f.write(
            f"source:       {s['name']}\nverdict:      {s['verdict']}\n"
            f"cadence_h:    {s['freq_h']}\nlast_success: {s['last_success'] or 'NEVER'}\n"
            f"last_attempt: {s['last_attempt'] or '-'} ({s['last_status'] or '-'})\n"
            f"attempts_since_success: {s['attempts_since_success']}\n"
            f"last_error:   {(s['last_error'] or '')[:300]}\n"
            f"detected_at:  {today}\naudit_source: pull_log (ingestion/source_freshness.py)\n"
        )
PY
else
  rm -f "$SRC_JSON.tmp"
  echo "$(date -u +'%Y-%m-%dT%H:%M:%SZ') source freshness check FAILED (ticker audit unaffected)"
fi

echo "$(date -u +'%Y-%m-%dT%H:%M:%SZ') freshness audit complete" >> "$ALERT_DIR/freshness-check.log"
