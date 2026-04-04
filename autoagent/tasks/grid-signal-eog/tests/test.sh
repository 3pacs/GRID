#!/bin/bash
set -euo pipefail

# ============================================================
# GRID Signal Hypothesis Scorer
# Runs pytest against agent output, writes reward to Harbor path
# ============================================================

echo "=== GRID AutoAgent Verifier ==="
echo "Task: EOG Signal Hypothesis Optimization"
echo "Timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)"

# Ensure output directories exist
mkdir -p /logs/verifier

# Install test dependencies (uv should be available in autoagent-base)
if command -v uv &>/dev/null; then
    echo "[+] Installing test deps via uv..."
    uv pip install --system \
        pytest==8.4.1 \
        pytest-json-ctrf==0.3.5 \
        2>/dev/null || true
elif command -v pip &>/dev/null; then
    echo "[+] Installing test deps via pip..."
    pip install --quiet \
        pytest==8.4.1 \
        pytest-json-ctrf==0.3.5 \
        2>/dev/null || true
fi

# Verify the agent produced output
if [ ! -f /app/predictions.csv ]; then
    echo "[FAIL] Agent did not produce /app/predictions.csv"
    echo "0.0" > /logs/verifier/reward.txt
    exit 0  # Exit 0 so Harbor records the score, not a crash
fi

echo "[+] predictions.csv found ($(wc -l < /app/predictions.csv) lines)"

# Run pytest with CTRF output for Harbor
echo "[+] Running scoring suite..."
python -m pytest \
    --ctrf /logs/verifier/ctrf.json \
    /tests/test_state.py \
    -rA \
    --tb=short \
    -v \
    2>&1 | tee /logs/verifier/pytest_output.txt

TEST_EXIT=$?

# If pytest wrote the reward file (via test_score_and_write_reward), use that
if [ -f /logs/verifier/reward.txt ]; then
    SCORE=$(cat /logs/verifier/reward.txt | tr -d '[:space:]')
    echo "[+] Composite score: ${SCORE}"
else
    # Fallback: binary pass/fail based on pytest exit code
    if [ $TEST_EXIT -eq 0 ]; then
        echo "0.5" > /logs/verifier/reward.txt
        echo "[+] Tests passed but no composite score written. Default: 0.5"
    else
        echo "0.0" > /logs/verifier/reward.txt
        echo "[-] Tests failed. Score: 0.0"
    fi
fi

# Log results summary if detail file exists
if [ -f /logs/verifier/reward_detail.json ]; then
    echo ""
    echo "=== Score Breakdown ==="
    python3 -c "
import json
d = json.load(open('/logs/verifier/reward_detail.json'))
print(f'Composite: {d[\"composite_score\"]:.4f}')
for k, v in d['components'].items():
    print(f'  {k}: {v[\"score\"]:.3f} (value={v.get(\"value\", v.get(\"n_features\", \"?\"))}, weight={v[\"weight\"]})')
print(f'Stats: {d[\"stats\"][\"n_buy_signals\"]} BUY / {d[\"stats\"][\"total_predictions\"]} total')
" 2>/dev/null || true
fi

echo ""
echo "=== Verifier complete ==="
exit 0
