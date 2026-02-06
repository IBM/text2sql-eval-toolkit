#!/bin/bash

# Script to start a local vLLM server for testing with a small model
# This script is designed for local development and testing purposes
# Automatically uses GPU if available, otherwise falls back to CPU

set -e

# Configuration
MODEL_NAME="${VLLM_MODEL:-Qwen/Qwen2.5-0.5B-Instruct}"
HOST="${VLLM_HOST:-127.0.0.1}"
PORT="${VLLM_PORT:-8000}"
MAX_MODEL_LEN="${VLLM_MAX_MODEL_LEN:-2048}"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Detect if GPU is available
USE_GPU=false
if command -v nvidia-smi &> /dev/null; then
    if nvidia-smi &> /dev/null; then
        USE_GPU=true
    fi
fi

# Allow override via environment variable
if [ ! -z "${VLLM_USE_GPU}" ]; then
    if [ "${VLLM_USE_GPU}" = "true" ] || [ "${VLLM_USE_GPU}" = "1" ]; then
        USE_GPU=true
    else
        USE_GPU=false
    fi
fi

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Starting vLLM Local Server${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "${YELLOW}Configuration:${NC}"
echo -e "  Model: ${MODEL_NAME}"
echo -e "  Host: ${HOST}"
echo -e "  Port: ${PORT}"
echo -e "  Max Model Length: ${MAX_MODEL_LEN}"
if [ "$USE_GPU" = true ]; then
    echo -e "  Device: ${BLUE}GPU${NC}"
else
    echo -e "  Device: ${BLUE}CPU${NC}"
fi
echo ""

# Detect Python command (prefer python3, fall back to python)
PYTHON_CMD="python3"
if ! command -v python3 &> /dev/null; then
    PYTHON_CMD="python"
fi

# Check if vllm is installed
if ! $PYTHON_CMD -c "import vllm" 2>/dev/null; then
    echo -e "${RED}Error: vLLM is not installed.${NC}"
    echo ""
    echo "To install vLLM, run:"
    echo -e "${YELLOW}  pip install vllm${NC}"
    echo ""
    echo "If using uv, run:"
    echo -e "${YELLOW}  uv pip install vllm${NC}"
    echo ""
    echo "Note: vLLM will automatically use CPU if no GPU is available."
    echo ""
    exit 1
fi

# Check if the port is already in use
if lsof -Pi :${PORT} -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo -e "${RED}Error: Port ${PORT} is already in use.${NC}"
    echo ""
    echo "To use a different port, set the VLLM_PORT environment variable:"
    echo -e "${YELLOW}  export VLLM_PORT=8001${NC}"
    echo -e "${YELLOW}  ./scripts/inference/vllm/start_vllm_local.sh${NC}"
    echo ""
    exit 1
fi

echo -e "${GREEN}Starting vLLM server...${NC}"
echo -e "${YELLOW}Note: First run will download the model (may take a few minutes)${NC}"
echo ""

# Build command based on device availability
# Note: vLLM 0.11+ automatically detects platform (CPU/GPU)
# The --device flag has been removed in newer versions
CMD="$PYTHON_CMD -m vllm.entrypoints.openai.api_server \
    --model ${MODEL_NAME} \
    --host ${HOST} \
    --port ${PORT} \
    --max-model-len ${MAX_MODEL_LEN}"

if [ "$USE_GPU" = false ]; then
    echo -e "${YELLOW}Running on CPU (slower but works without GPU)${NC}"
    # For CPU, we can still set dtype and enforce eager mode
    CMD="${CMD} --dtype float32 --enforce-eager"
else
    echo -e "${BLUE}Running on GPU${NC}"
fi

echo ""

# Start vLLM server
eval $CMD

# Note: The script will keep running until interrupted with Ctrl+C