#!/bin/bash
# GRID — Install and enable all systemd services on grid-svr
# Usage: sudo bash scripts/install_services.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
GRID_DIR="$(dirname "$SCRIPT_DIR")"
SETUP_DIR="${GRID_DIR}/server_setup"
LOG_DIR="/data/grid/logs"

echo "=== GRID Service Installer ==="
echo "Grid dir: ${GRID_DIR}"
echo ""

# Ensure log directory exists
mkdir -p "$LOG_DIR"
chown grid:grid "$LOG_DIR"

# Copy service files
echo "Installing service files..."
for svc in grid-db grid-llamacpp grid-crucix grid-api grid-hermes; do
    if [[ -f "${SETUP_DIR}/${svc}.service" ]]; then
        cp "${SETUP_DIR}/${svc}.service" /etc/systemd/system/
        echo "  Installed ${svc}.service"
    else
        echo "  WARNING: ${svc}.service not found in ${SETUP_DIR}"
    fi
done

# Reload systemd
echo ""
echo "Reloading systemd..."
systemctl daemon-reload

# Enable services
echo "Enabling services..."
systemctl enable grid-db grid-llamacpp grid-crucix grid-api grid-hermes

echo ""
echo "=== Services installed and enabled ==="
echo ""
echo "Start all:  sudo systemctl start grid-db grid-llamacpp grid-crucix grid-api grid-hermes"
echo "Stop all:   sudo systemctl stop grid-hermes grid-api grid-crucix grid-llamacpp grid-db"
echo "Status:     sudo systemctl status grid-db grid-llamacpp grid-crucix grid-api grid-hermes"
echo ""
echo "Boot order: grid-db → grid-llamacpp + grid-crucix → grid-api → grid-hermes"
