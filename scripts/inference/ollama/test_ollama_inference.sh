#!/bin/bash

# Quick test script to run inference with Ollama
# This tests the toolkit with your local Ollama server

set -e

echo "========================================="
echo "Testing Ollama with text2sql-eval-toolkit"
echo "========================================="
echo ""

# Check if Ollama server is running
if ! curl -s http://127.0.0.1:11434/api/tags > /dev/null; then
    echo "Error: Ollama server is not running!"
    echo "Start it with: python scripts/inference/ollama/start_ollama_local.py"
    exit 1
fi

echo "✓ Ollama server is running"
echo ""

# Get the model name from Ollama
MODEL=$(curl -s http://127.0.0.1:11434/api/tags | python3 -c "import sys, json; data=json.load(sys.stdin); print(data['models'][0]['name'] if data.get('models') else 'qwen2.5:0.5b')" 2>/dev/null || echo "qwen2.5:0.5b")

echo "Using model: $MODEL"
echo ""

# Run a small test inference
echo "Running inference on bird_sqlite_test_benchmark..."
echo ""

uv run python scripts/inference/run_inference.py \
    bird_sqlite_test_benchmark \
    --model_names "ollama:${MODEL}" \
    --max_new_tokens 256 \
    --decoding_method greedy

echo ""
echo "========================================="
echo "Test completed!"
echo "========================================="
echo ""
echo "Check the results in: data/results/"