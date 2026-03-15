#!/bin/bash
# ============================================================
# GRID x Hyperspace — Start Node
# Starts the Hyperspace node with API and agent enabled,
# then waits for the API to become ready.
# ============================================================

echo "=== Starting Hyperspace Node ==="

# Kill any existing instance
pkill -f "hyperspace start" 2>/dev/null || true
sleep 2

# Ensure config directory exists
mkdir -p ~/.hyperspace

# Start node
nohup hyperspace start --api --agent --profile full \
    > ~/.hyperspace/node.log 2>&1 &

NODE_PID=$!
echo "Node PID: $NODE_PID"

# Wait for API to be ready (up to 60 seconds)
echo "Waiting for API at localhost:8080..."
for i in $(seq 1 60); do
    if curl -s http://localhost:8080/v1/models > /dev/null 2>&1; then
        echo "API ready after ${i}s"
        break
    fi
    sleep 1
    if [ "$i" -eq 60 ]; then
        echo "API did not come up after 60s. Check ~/.hyperspace/node.log"
        exit 1
    fi
done

echo ""
hyperspace hive whoami 2>/dev/null || echo "(whoami not yet available)"
echo ""
echo "=== Node is running. GRID can now use Hyperspace. ==="
