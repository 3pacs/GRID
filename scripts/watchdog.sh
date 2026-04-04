#!/bin/bash
# GRID Server Watchdog — prevents OOM, restarts crashed services, logs issues

LOG="/data/grid/logs/watchdog.log"

# 1. Check memory — if >90% used, kill non-essential processes
MEM_PCT=$(free | awk '/Mem:/{printf "%d", $3/$2*100}')
if [ "$MEM_PCT" -gt 90 ]; then
    echo "$(date) WARN: Memory at ${MEM_PCT}% — killing tao-miner to free RAM" >> $LOG
    sudo systemctl stop grid-tao-miner
fi
if [ "$MEM_PCT" -gt 95 ]; then
    echo "$(date) CRITICAL: Memory at ${MEM_PCT}% — killing crucix" >> $LOG
    sudo systemctl stop grid-crucix
fi

# 2. Check if services are running — restart if crashed
for svc in grid-api grid-hermes grid-llamacpp; do
    if ! systemctl is-active --quiet $svc; then
        echo "$(date) WARN: $svc is down — restarting" >> $LOG
        sudo systemctl restart $svc
        sleep 5
    fi
done

# 3. Check disk space
DISK_PCT=$(df / | awk 'NR==2{print $5}' | tr -d '%')
if [ "$DISK_PCT" -gt 90 ]; then
    echo "$(date) WARN: Disk at ${DISK_PCT}% — cleaning logs" >> $LOG
    find /data/grid/logs -name "*.log" -mtime +7 -delete
    find /data/grid_v4/grid_repo/outputs/llm_insights -mtime +30 -delete
fi

# 4. Check if API is responding
if ! curl -sf http://localhost:8000/api/v1/system/health > /dev/null 2>&1; then
    echo "$(date) WARN: API not responding — restarting" >> $LOG
    sudo systemctl restart grid-api
fi

# 5. Check if LLM is responding
if ! curl -sf "${LLAMACPP_BASE_URL:-http://localhost:8080}/health" > /dev/null 2>&1; then
    echo "$(date) WARN: LLM not responding — restarting" >> $LOG
    sudo systemctl restart grid-llamacpp
fi

# 6. Log memory state
echo "$(date) INFO: mem=${MEM_PCT}% disk=${DISK_PCT}% api=$(systemctl is-active grid-api) hermes=$(systemctl is-active grid-hermes) llm=$(systemctl is-active grid-llamacpp)" >> $LOG
