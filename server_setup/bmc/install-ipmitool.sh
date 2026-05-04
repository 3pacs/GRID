#!/usr/bin/env bash
#
# install-ipmitool.sh — install BMC client tools and optionally probe the BMC.
#
# Usage:
#   sudo ./install-ipmitool.sh           # install only
#   ./install-ipmitool.sh --check        # probe the BMC at $BMC_IP
#   sudo ./install-ipmitool.sh --check   # install AND probe
#
# Reads BMC_IP / BMC_USER / BMC_PASS from environment. Source bmc-env first:
#   set -a; . ./bmc-env; set +a
#
# Idempotent — re-running is safe.

set -euo pipefail

# ---- defaults / placeholders (override via env or sourcing bmc-env) ----
# BMC_IP=192.168.1.XXX        # CHANGE ME
# BMC_USER=ADMIN              # CHANGE ME if not Supermicro
# BMC_PASS=changeme           # CHANGE ME
BMC_IP="${BMC_IP:-}"
BMC_USER="${BMC_USER:-}"
BMC_PASS="${BMC_PASS:-}"

DO_INSTALL=1
DO_CHECK=0
for arg in "$@"; do
  case "$arg" in
    --check)        DO_CHECK=1 ;;
    --check-only)   DO_CHECK=1; DO_INSTALL=0 ;;
    -h|--help)
      sed -n '2,15p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown arg: $arg" >&2
      exit 2
      ;;
  esac
done

log() { printf '[install-ipmitool] %s\n' "$*"; }
err() { printf '[install-ipmitool] ERROR: %s\n' "$*" >&2; }

install_packages() {
  if command -v ipmitool >/dev/null 2>&1 && command -v ipmi-sensors >/dev/null 2>&1; then
    log "ipmitool and freeipmi-tools already installed — skipping apt"
    return 0
  fi

  if [[ $EUID -ne 0 ]]; then
    err "install requires root. Re-run with sudo, or pass --check-only."
    exit 1
  fi

  log "installing ipmitool + freeipmi-tools via apt"
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -y
  apt-get install -y --no-install-recommends ipmitool freeipmi-tools
  log "install complete"
}

check_bmc() {
  if ! command -v ipmitool >/dev/null 2>&1; then
    err "ipmitool not on PATH; run without --check-only first."
    exit 1
  fi

  if [[ -z "$BMC_IP" || -z "$BMC_USER" || -z "$BMC_PASS" ]]; then
    err "BMC_IP / BMC_USER / BMC_PASS not set."
    err "Source the bmc-env file first:  set -a; . ./bmc-env; set +a"
    exit 1
  fi

  if [[ "$BMC_IP" == *XXX* || "$BMC_PASS" == "changeme" ]]; then
    err "BMC_IP or BMC_PASS still has placeholder value — fill in bmc-env."
    exit 1
  fi

  log "probing BMC at $BMC_IP as $BMC_USER"
  if ipmitool -I lanplus -H "$BMC_IP" -U "$BMC_USER" -P "$BMC_PASS" \
      chassis status; then
    log "BMC reachable. Power state above."
  else
    err "ipmitool chassis status failed."
    err "  - is the Tailscale subnet route approved?  https://login.tailscale.com/admin/machines"
    err "  - can you ping \$BMC_IP from this host? ($BMC_IP)"
    err "  - are credentials correct?"
    exit 1
  fi
}

if [[ $DO_INSTALL -eq 1 ]]; then
  install_packages
fi

if [[ $DO_CHECK -eq 1 ]]; then
  check_bmc
fi
