#!/bin/bash
# Download Jon Becker's prediction market analysis dataset (36GB compressed).
# Contains 7.68M markets and 72.1M trades from Polymarket + Kalshi.
# Source: https://github.com/Jon-Becker/prediction-market-analysis
set -e

URL="https://s3.jbecker.dev/data.tar.zst"
DATA_DIR="/home/user/GRID/data/prediction_markets"
OUTPUT_FILE="data.tar.zst"
DATA_PATH="${DATA_DIR}/${OUTPUT_FILE}"
SENTINEL="${DATA_DIR}/.download_complete"

# Skip if already downloaded
if [ -f "$SENTINEL" ]; then
    echo "Prediction market data already downloaded, skipping."
    echo "Data directory: ${DATA_DIR}"
    du -sh "${DATA_DIR}"
    exit 0
fi

mkdir -p "$DATA_DIR"

echo "=== Downloading prediction market dataset (36GB compressed) ==="
echo "Source: ${URL}"
echo "Destination: ${DATA_DIR}"
echo ""

# Download with best available tool
if command -v aria2c &> /dev/null; then
    echo "Using aria2c (multi-connection)..."
    aria2c -x 16 -s 16 -d "$DATA_DIR" -o "$OUTPUT_FILE" "$URL"
elif command -v curl &> /dev/null; then
    echo "Using curl..."
    curl -L --progress-bar --create-dirs -o "$DATA_PATH" "$URL"
elif command -v wget &> /dev/null; then
    echo "Using wget..."
    wget --progress=bar:force -O "$DATA_PATH" "$URL"
else
    echo "Error: No download tool available (aria2c, curl, or wget required)."
    exit 1
fi

# Extract
if ! command -v zstd &> /dev/null; then
    echo "Error: zstd not installed. Run: sudo apt-get install -y zstd"
    exit 1
fi

echo ""
echo "=== Extracting archive ==="
cd "$DATA_DIR"
zstd -d "$OUTPUT_FILE" --stdout | tar -xf -
echo "Extraction complete."

# Move extracted data/* contents up if nested
if [ -d "$DATA_DIR/data" ]; then
    mv "$DATA_DIR/data"/* "$DATA_DIR/" 2>/dev/null || true
    rmdir "$DATA_DIR/data" 2>/dev/null || true
fi

# Cleanup compressed archive
rm -f "$DATA_PATH"

touch "$SENTINEL"
echo ""
echo "=== Data directory ready ==="
du -sh "$DATA_DIR"
echo ""
echo "Expected structure:"
echo "  ${DATA_DIR}/kalshi/markets/   - Kalshi market metadata (Parquet)"
echo "  ${DATA_DIR}/kalshi/trades/    - Kalshi trade history (Parquet)"
echo "  ${DATA_DIR}/polymarket/markets/ - Polymarket market metadata (Parquet)"
echo "  ${DATA_DIR}/polymarket/trades/  - Polymarket trade history (Parquet)"
echo "  ${DATA_DIR}/polymarket/blocks/  - Polygon block timestamps (Parquet)"
