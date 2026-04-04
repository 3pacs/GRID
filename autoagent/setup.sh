#!/bin/bash
set -euo pipefail

# ============================================================
# GRID AutoAgent Setup
# Clones AutoAgent framework and wires in GRID task structure
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
GRID_REPO="$(dirname "$SCRIPT_DIR")"

echo "=== GRID AutoAgent Setup ==="
echo "AutoAgent dir: $SCRIPT_DIR"
echo "GRID repo: $GRID_REPO"
echo ""

# --- 1. Clone AutoAgent framework if not present ---
if [ ! -d "$SCRIPT_DIR/autoagent-framework" ]; then
    echo "[1/5] Cloning AutoAgent framework..."
    git clone https://github.com/kevinrgu/autoagent.git "$SCRIPT_DIR/autoagent-framework"
else
    echo "[1/5] AutoAgent framework already cloned"
fi

# --- 2. Install dependencies ---
echo "[2/5] Installing dependencies..."
cd "$SCRIPT_DIR/autoagent-framework"
if command -v uv &>/dev/null; then
    uv sync 2>/dev/null || echo "  uv sync skipped (no pyproject.toml?)"
else
    echo "  WARN: uv not found. Install with: curl -LsSf https://astral.sh/uv/install.sh | sh"
fi

# --- 3. Build base Docker image ---
echo "[3/5] Building autoagent-base Docker image..."
if [ -f Dockerfile.base ]; then
    docker build -f Dockerfile.base -t autoagent-base . 2>&1 | tail -5
else
    echo "  WARN: Dockerfile.base not found in AutoAgent repo"
fi

# --- 4. Symlink GRID tasks into AutoAgent ---
echo "[4/5] Linking GRID tasks..."
TASKS_TARGET="$SCRIPT_DIR/autoagent-framework/tasks"
mkdir -p "$TASKS_TARGET"

for task_dir in "$SCRIPT_DIR/tasks"/*/; do
    task_name=$(basename "$task_dir")
    if [ ! -e "$TASKS_TARGET/$task_name" ]; then
        ln -sf "$task_dir" "$TASKS_TARGET/$task_name"
        echo "  Linked: $task_name"
    else
        echo "  Already linked: $task_name"
    fi
done

# --- 5. Copy agent.py and program.md ---
echo "[5/5] Copying agent and program files..."
cp -f "$SCRIPT_DIR/agent.py" "$SCRIPT_DIR/autoagent-framework/agent.py"
cp -f "$SCRIPT_DIR/program.md" "$SCRIPT_DIR/autoagent-framework/program.md"

# --- Verify ---
echo ""
echo "=== Setup complete ==="
echo ""
echo "Directory structure:"
find "$SCRIPT_DIR/tasks" -type f | sort | sed "s|$SCRIPT_DIR/||"
echo ""
echo "To run manually:"
echo "  cd $SCRIPT_DIR/autoagent-framework"
echo "  uv run harbor run -p tasks/ --task-name harbor/grid-signal-eog --agent-import-path agent:AutoAgent -o jobs"
echo ""
echo "To run the GRID integration loop:"
echo "  cd $SCRIPT_DIR"
echo "  python grid_autoagent_runner.py --task grid-signal-eog --hours 24"
echo ""
echo "To test the baseline locally (without Docker):"
echo "  cd $GRID_REPO"
echo "  PYTHONPATH=$SCRIPT_DIR/tasks/grid-signal-eog/files python $SCRIPT_DIR/baseline_signal_generator.py"
