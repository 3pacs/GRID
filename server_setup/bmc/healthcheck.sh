#!/usr/bin/env bash
#
# healthcheck.sh — cron-friendly probe of the BMC.
#
# Pings the BMC, reports power state via ipmitool, logs to journald.
# Exits 0 on success, 1 on any failure (so cron can email on failure).
#
# Sample crontab (every 15 min):
#   */15 * * * * /opt/grid/server_setup/bmc/healthcheck.sh
#
# Or as a systemd timer — add a .timer unit later if cron is not preferred.
#
# Reads BMC_IP / BMC_USER / BMC_PASS from /etc/grid/bmc-env (or env).

set -uo pipefail

BMC_ENV="${BMC_ENV:-/etc/grid/bmc-env}"
[[ -r "$BMC_ENV" ]] && { set -a; . "$BMC_ENV"; set +a; }

TAG="grid-bmc-healthcheck"
log_info()  { logger -t "$TAG" -p user.info  "$*"; }
log_warn()  { logger -t "$TAG" -p user.warning "$*"; }

if [[ -z "${BMC_IP:-}" || -z "${BMC_USER:-}" || -z "${BMC_PASS:-}" ]]; then
  log_warn "BMC_IP/USER/PASS unset — is $BMC_ENV populated?"
  exit 1
fi

if ! ping -c 2 -W 3 "$BMC_IP" >/dev/null 2>&1; then
  log_warn "BMC unreachable via ICMP at $BMC_IP — Tailscale route down? gateway box offline?"
  exit 1
fi

power=$(ipmitool -I lanplus -H "$BMC_IP" -U "$BMC_USER" -P "$BMC_PASS" \
    chassis power status 2>/dev/null) || {
  log_warn "ipmitool chassis power status FAILED at $BMC_IP — creds rotated? BMC firmware locked?"
  exit 1
}

log_info "BMC OK at $BMC_IP — $power"
exit 0
