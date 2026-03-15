#!/bin/bash
# ============================================================
# GRID x Hyperspace — Status Check
# Shows process status, API availability, and node identity.
# ============================================================

echo "=== Hyperspace Node Status ==="
echo ""

if pgrep -f "hyperspace start" > /dev/null; then
    echo "Process: RUNNING (PID: $(pgrep -f 'hyperspace start'))"
else
    echo "Process: NOT RUNNING"
fi

echo ""
if curl -s http://localhost:8080/v1/models > /dev/null 2>&1; then
    echo "API:     AVAILABLE at localhost:8080"
    echo "Models:  $(curl -s http://localhost:8080/v1/models | python3 -c 'import sys,json; d=json.load(sys.stdin); print(", ".join(m["id"] for m in d.get("data",[])))' 2>/dev/null || echo 'parse error')"
else
    echo "API:     NOT AVAILABLE"
fi

echo ""
echo "=== Points & Identity ==="
hyperspace hive whoami 2>/dev/null || echo "Node offline"
