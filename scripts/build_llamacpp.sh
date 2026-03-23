#!/usr/bin/env bash
# ============================================================
# Build llama.cpp from source with CUDA support.
#
# Targets Tesla GPUs (P40/P100 sm_60/61, V100 sm_70, T4 sm_75),
# Ampere (A100/A10 sm_80/86), Ada (L40 sm_89), and Blackwell
# (B-series sm_100, RTX PRO sm_120).
#
# Usage:
#   bash scripts/build_llamacpp.sh [--clean]
#
# Prerequisites:
#   - CUDA toolkit (nvcc in PATH)
#   - cmake >= 3.14
#   - git
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
GRID_ROOT="$(dirname "$SCRIPT_DIR")"
LLAMA_DIR="${GRID_ROOT}/vendor/llama.cpp"
MODELS_DIR="${GRID_ROOT}/models"

# CUDA compute capabilities for Tesla GPU range + newer
# 60/61=P40/P100, 70=V100, 75=T4, 80/86=A100/A10, 89=L40/Ada, 100=B100/B200, 120=RTX PRO Blackwell
CUDA_ARCHS="60;61;70;75;80;86;89;100;120"

# ── Clean build ─────────────────────────────────────────────
if [[ "${1:-}" == "--clean" ]] && [[ -d "$LLAMA_DIR" ]]; then
    echo "Cleaning existing build..."
    rm -rf "$LLAMA_DIR"
fi

# ── Clone or update ─────────────────────────────────────────
if [[ ! -d "$LLAMA_DIR" ]]; then
    echo "Cloning llama.cpp..."
    git clone --depth 1 https://github.com/ggerganov/llama.cpp.git "$LLAMA_DIR"
else
    echo "Updating llama.cpp..."
    cd "$LLAMA_DIR"
    git pull --ff-only || echo "Warning: git pull failed, using existing checkout"
fi

cd "$LLAMA_DIR"

# ── Verify CUDA ─────────────────────────────────────────────
if ! command -v nvcc &>/dev/null; then
    echo "ERROR: nvcc not found. Install CUDA toolkit and add to PATH."
    echo "  Ubuntu: sudo apt install nvidia-cuda-toolkit"
    echo "  Or set: export PATH=/usr/local/cuda/bin:\$PATH"
    exit 1
fi

CUDA_VERSION=$(nvcc --version | grep -oP 'release \K[0-9]+\.[0-9]+')
echo "CUDA version: ${CUDA_VERSION}"
echo "Target architectures: ${CUDA_ARCHS}"

# ── Detect GPU ──────────────────────────────────────────────
if command -v nvidia-smi &>/dev/null; then
    echo ""
    echo "Detected GPUs:"
    nvidia-smi --query-gpu=name,memory.total,compute_cap --format=csv,noheader 2>/dev/null || true
    echo ""
fi

# ── Build ───────────────────────────────────────────────────
echo "Building llama.cpp with CUDA..."

mkdir -p build && cd build

cmake .. \
    -DGGML_CUDA=ON \
    -DCMAKE_CUDA_ARCHITECTURES="${CUDA_ARCHS}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DLLAMA_CURL=OFF \
    -DBUILD_SHARED_LIBS=OFF

cmake --build . --config Release -j "$(nproc)"

echo ""
echo "Build complete."

# ── Verify binaries ─────────────────────────────────────────
LLAMA_SERVER="${LLAMA_DIR}/build/bin/llama-server"
LLAMA_CLI="${LLAMA_DIR}/build/bin/llama-cli"

if [[ -x "$LLAMA_SERVER" ]]; then
    echo "llama-server: ${LLAMA_SERVER}"
else
    # Older builds put it directly in build/
    LLAMA_SERVER="${LLAMA_DIR}/build/llama-server"
    if [[ -x "$LLAMA_SERVER" ]]; then
        echo "llama-server: ${LLAMA_SERVER}"
    else
        echo "WARNING: llama-server binary not found. Check build output."
    fi
fi

# ── Create models directory ─────────────────────────────────
mkdir -p "$MODELS_DIR"
echo ""
echo "Models directory: ${MODELS_DIR}"
echo ""
echo "Next steps:"
echo "  1. Download a Hermes GGUF model:"
echo "     huggingface-cli download NousResearch/Hermes-3-Llama-3.1-8B-GGUF \\"
echo "       Hermes-3-Llama-3.1-8B.Q4_K_M.gguf --local-dir ${MODELS_DIR}"
echo ""
echo "  2. Start the server:"
echo "     bash scripts/start_llamacpp.sh"
