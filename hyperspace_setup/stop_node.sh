#!/bin/bash
# ============================================================
# GRID x Hyperspace — Stop Node
# Gracefully shuts down the Hyperspace node.
# ============================================================

echo "Stopping Hyperspace node..."
pkill -f "hyperspace start" 2>/dev/null && echo "Stopped" || echo "Not running"
