#!/bin/bash
# Set up the prediction-market-backtesting framework (NautilusTrader fork).
# Source: https://github.com/evan-kolberg/prediction-market-backtesting
set -e

INSTALL_DIR="/home/user/GRID/vendor/prediction-market-backtesting"
REPO_URL="https://github.com/evan-kolberg/prediction-market-backtesting.git"

echo "=== Setting up prediction-market-backtesting ==="

# Check for uv
if ! command -v uv &> /dev/null; then
    echo "Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.local/bin:$PATH"
fi

# Clone if not present
if [ -d "$INSTALL_DIR" ]; then
    echo "Repo already cloned at $INSTALL_DIR"
    cd "$INSTALL_DIR"
    git pull origin main 2>/dev/null || true
else
    echo "Cloning repo..."
    mkdir -p "$(dirname "$INSTALL_DIR")"
    git clone "$REPO_URL" "$INSTALL_DIR"
    cd "$INSTALL_DIR"
fi

# Install dependencies
echo "Installing dependencies..."
unset CONDA_PREFIX
uv venv --python 3.13 2>/dev/null || uv venv --python 3.11
uv pip install -e nautilus_pm/ bokeh plotly numpy py-clob-client duckdb textual

echo ""
echo "=== Setup complete ==="
echo "Location: $INSTALL_DIR"
echo ""
echo "Usage:"
echo "  cd $INSTALL_DIR"
echo "  uv run python main.py              # Run backtest"
echo "  make backtest                       # Same via make"
echo ""
echo "To use with GRID data, configure backtests/ runner files"
echo "to point at GRID's prediction_market_trades data."
