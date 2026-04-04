#!/usr/bin/env bash
# ============================================================
# Start llama-server with Hermes model.
#
# Serves an OpenAI-compatible API at localhost:8080.
# All GRID components (llamacpp client, agents, reasoner)
# point here.
#
# Usage:
#   bash scripts/start_llamacpp.sh                  # default model
#   bash scripts/start_llamacpp.sh --model /path/to/model.gguf
#   bash scripts/start_llamacpp.sh --port 8081
#   bash scripts/start_llamacpp.sh --ngl 0           # CPU only
#
# Environment overrides:
#   LLAMACPP_MODEL    — path to GGUF file
#   LLAMACPP_PORT     — server port (default 8080)
#   LLAMACPP_HOST     — bind address (default 0.0.0.0)
#   LLAMACPP_NGL      — GPU layers (default 99 = all on GPU)
#   LLAMACPP_CTX      — context size (default 8192)
#   LLAMACPP_THREADS  — CPU threads (default: nproc)
#   LLAMACPP_PARALLEL — concurrent request slots (default 4)
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
GRID_ROOT="$(dirname "$SCRIPT_DIR")"
LLAMA_DIR="${GRID_ROOT}/vendor/llama.cpp"
MODELS_DIR="${GRID_ROOT}/models"

# ── Ensure shared libs are findable ─────────────────────────
export LD_LIBRARY_PATH="${LLAMA_DIR}/build/bin:${LD_LIBRARY_PATH:-}"

# ── Find the server binary ──────────────────────────────────
LLAMA_SERVER=""
for candidate in \
    "${LLAMA_DIR}/build/bin/llama-server" \
    "${LLAMA_DIR}/build/llama-server" \
    "$(command -v llama-server 2>/dev/null || true)"; do
    if [[ -x "${candidate:-}" ]]; then
        LLAMA_SERVER="$candidate"
        break
    fi
done

if [[ -z "$LLAMA_SERVER" ]]; then
    echo "ERROR: llama-server not found. Run: bash scripts/build_llamacpp.sh"
    exit 1
fi

echo "Using: ${LLAMA_SERVER}"

# ── Parse args (override env vars) ──────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --model)  LLAMACPP_MODEL="$2"; shift 2 ;;
        --port)   LLAMACPP_PORT="$2"; shift 2 ;;
        --host)   LLAMACPP_HOST="$2"; shift 2 ;;
        --ngl)    LLAMACPP_NGL="$2"; shift 2 ;;
        --ctx)    LLAMACPP_CTX="$2"; shift 2 ;;
        *)        echo "Unknown arg: $1"; exit 1 ;;
    esac
done

# ── Defaults ────────────────────────────────────────────────
PORT="${LLAMACPP_PORT:-8080}"
HOST="${LLAMACPP_HOST:-0.0.0.0}"
NGL="${LLAMACPP_NGL:-99}"           # 99 = offload all layers to GPU
CTX="${LLAMACPP_CTX:-8192}"
THREADS="${LLAMACPP_THREADS:-$(nproc)}"
PARALLEL="${LLAMACPP_PARALLEL:-4}"   # concurrent request slots

# ── Find model ──────────────────────────────────────────────
if [[ -z "${LLAMACPP_MODEL:-}" ]]; then
    # Auto-detect: prefer Hermes GGUF in models/
    LLAMACPP_MODEL=$(find "$MODELS_DIR" -name "*.gguf" -print -quit 2>/dev/null || true)
fi

if [[ -z "${LLAMACPP_MODEL:-}" ]] || [[ ! -f "${LLAMACPP_MODEL}" ]]; then
    echo "ERROR: No GGUF model found."
    echo ""
    echo "Download one:"
    echo "  pip install huggingface-hub"
    echo "  huggingface-cli download NousResearch/Hermes-3-Llama-3.1-8B-GGUF \\"
    echo "    Hermes-3-Llama-3.1-8B.Q4_K_M.gguf --local-dir ${MODELS_DIR}"
    echo ""
    echo "Or specify: LLAMACPP_MODEL=/path/to/model.gguf bash $0"
    exit 1
fi

MODEL_NAME=$(basename "$LLAMACPP_MODEL" .gguf)
DEVICE="${LLAMACPP_DEVICE:-}"   # e.g. "none" for CPU-only

echo "Model:    ${LLAMACPP_MODEL}"
echo "Port:     ${PORT}"
echo "GPU layers: ${NGL}"
echo "Device:   ${DEVICE:-auto}"
echo "Context:  ${CTX}"
echo "Threads:  ${THREADS}"
echo "Parallel: ${PARALLEL}"
echo ""

# ── Check for existing server ───────────────────────────────
if curl -sf "http://localhost:${PORT}/health" >/dev/null 2>&1; then
    echo "WARNING: llama-server already running on port ${PORT}"
    echo "Kill it first: pkill -f llama-server"
    exit 1
fi

# ── SSD cache for KV (if /fast is mounted) ─────────────────
SLOT_SAVE=""
if [[ -d "/fast/llm_cache" ]]; then
    SLOT_SAVE="--slot-save-path /fast/llm_cache"
    echo "KV cache: /fast/llm_cache (SSD-backed)"
fi

# ── Build device flag ───────────────────────────────────────
DEVICE_FLAG=""
if [[ -n "$DEVICE" ]]; then
    DEVICE_FLAG="--device $DEVICE"
fi

# ── Launch ──────────────────────────────────────────────────
echo "Starting llama-server..."
exec "$LLAMA_SERVER" \
    --model "$LLAMACPP_MODEL" \
    --host "$HOST" \
    --port "$PORT" \
    --n-gpu-layers "$NGL" \
    --ctx-size "$CTX" \
    --threads "$THREADS" \
    --parallel "$PARALLEL" \
    --metrics \
    --alias "$MODEL_NAME" \
    $DEVICE_FLAG \
    $SLOT_SAVE
