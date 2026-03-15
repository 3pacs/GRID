#!/bin/bash
# ============================================================
# GRID x Hyperspace — Install Script
# Installs the Hyperspace CLI, checks hardware, and pulls
# the best model for the available GPU.
# ============================================================
set -e

echo "=== GRID x Hyperspace Install ==="

# Check if already installed
if command -v hyperspace &> /dev/null; then
    echo "Hyperspace already installed: $(hyperspace --version)"
else
    echo "Installing Hyperspace CLI..."
    curl -fsSL https://agents.hyper.space/cli | bash
    export PATH="$HOME/.local/bin:$PATH"
    echo "Installed: $(hyperspace --version)"
fi

# Check GPU
echo ""
echo "=== Hardware Detection ==="
hyperspace system-info

# Pull best model
echo ""
echo "=== Pulling Best Model ==="
hyperspace models pull --auto

echo ""
echo "=== Install Complete ==="
echo "Next: run ./hyperspace_setup/start_node.sh"
