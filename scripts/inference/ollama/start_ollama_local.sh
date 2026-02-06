#!/bin/bash

# Script to start a local Ollama server for testing
# Ollama works great on Apple Silicon (M1/M2/M3/M4) and provides OpenAI-compatible API

set -e

# Configuration
MODEL_NAME="${OLLAMA_MODEL:-qwen2.5:0.5b}"
HOST="${OLLAMA_HOST:-127.0.0.1}"
PORT="${OLLAMA_PORT:-11434}"
CONTEXT_LENGTH="${OLLAMA_NUM_CTX:-32768}"

# Set context length environment variable for Ollama
export OLLAMA_NUM_CTX="${CONTEXT_LENGTH}"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Starting Ollama Local Server${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Check if Ollama is installed
if ! command -v ollama &> /dev/null; then
    echo -e "${RED}Error: Ollama is not installed.${NC}"
    echo ""
    echo "To install Ollama on macOS:"
    echo -e "${YELLOW}  brew install ollama${NC}"
    echo ""
    echo "Or download from:"
    echo -e "${YELLOW}  https://ollama.ai/download${NC}"
    echo ""
    exit 1
fi

echo -e "${YELLOW}Configuration:${NC}"
echo -e "  Model: ${MODEL_NAME}"
echo -e "  Host: ${HOST}"
echo -e "  Port: ${PORT}"
echo -e "  Context Length: ${CONTEXT_LENGTH}"
echo ""

# Check if Ollama service is running
if ! pgrep -x "ollama" > /dev/null; then
    echo -e "${YELLOW}Starting Ollama service...${NC}"
    ollama serve > /dev/null 2>&1 &
    sleep 3
fi

# Check if model is available, if not pull it
echo -e "${GREEN}Checking if model ${MODEL_NAME} is available...${NC}"
if ! ollama list | grep -q "${MODEL_NAME}"; then
    echo -e "${YELLOW}Model not found. Pulling ${MODEL_NAME}...${NC}"
    echo -e "${YELLOW}This may take a few minutes on first run.${NC}"
    ollama pull "${MODEL_NAME}"
fi

echo ""
echo -e "${GREEN}✓ Ollama server is running${NC}"
echo -e "${GREEN}✓ Model ${MODEL_NAME} is ready${NC}"
echo ""
echo -e "${BLUE}API Endpoints:${NC}"
echo -e "  OpenAI-compatible: http://${HOST}:${PORT}/v1"
echo -e "  Ollama native: http://${HOST}:${PORT}/api"
echo ""
echo -e "${YELLOW}Test the server:${NC}"
echo -e "  curl http://${HOST}:${PORT}/api/tags"
echo ""
echo -e "${YELLOW}Generate text:${NC}"
echo -e "  ollama run ${MODEL_NAME}"
echo ""
echo -e "${GREEN}Server is ready for use!${NC}"
echo -e "${YELLOW}Press Ctrl+C to stop (will keep Ollama service running)${NC}"
echo ""

# Keep script running
trap 'echo ""; echo -e "${GREEN}Ollama server is still running in background${NC}"; exit 0' INT
while true; do
    sleep 1
done