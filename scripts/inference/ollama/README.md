# Local Ollama Server for Testing

This directory contains scripts to easily start a local Ollama server for testing text-to-SQL models. **Ollama works great on Apple Silicon** and provides an OpenAI-compatible API.

## Why Ollama?

- ✅ **Works perfectly on new MacBooks** and other Apple Silicon
- ✅ **Easy to install and use** - no complex setup
- ✅ **OpenAI-compatible API** - works with existing code
- ✅ **Fast on Apple Silicon** - optimized for Metal
- ✅ **Small models available** - from 397MB to a few GB
- ✅ **No Python dependencies** - standalone application

## Quick Start

### Option 1: Using the Python Script (Recommended)

```bash
python scripts/inference/ollama/start_ollama_local.py
```

### Option 2: Using the Bash Script

```bash
./scripts/inference/ollama/start_ollama_local.sh
```

## Installation

Install Ollama on macOS:

```bash
brew install ollama
```

Or download from: https://ollama.ai/download

## Usage Examples

### Basic Usage

Start with default settings (qwen2.5:0.5b model, 32K context length):

```bash
python scripts/inference/ollama/start_ollama_local.py
```

### Use a Different Model

```bash
python scripts/inference/ollama/start_ollama_local.py --model qwen2.5:1.5b
```

### Use a Larger Model

```bash
python scripts/inference/ollama/start_ollama_local.py --model llama3.2:3b
```

### Custom Context Length

By default, the server starts with a 32K (32768 tokens) context length. You can customize this:

```bash
# Python script
python scripts/inference/ollama/start_ollama_local.py --context-length 16384

# Bash script
OLLAMA_NUM_CTX=16384 ./scripts/inference/ollama/start_ollama_local.sh

# Or set as environment variable
export OLLAMA_NUM_CTX=65536
python scripts/inference/ollama/start_ollama_local.py
```

## Recommended Models for Testing

Small models optimized for quick testing:

1. **qwen2.5:0.5b** (default)
   - Size: 397MB
   - Fastest option
   - Good for quick testing

2. **qwen2.5:1.5b**
   - Size: 987MB
   - Better quality
   - Still very fast

3. **qwen2.5:3b**
   - Size: 1.9GB
   - Best quality among small models
   - Good balance

4. **llama3.2:1b**
   - Size: 1GB
   - Meta's small model

5. **llama3.2:3b**
   - Size: 2GB
   - Better quality

6. **phi3:mini**
   - Size: 2.3GB
   - Microsoft's efficient model

See all available models: https://ollama.ai/library

## Configuration Options

The scripts support the following configuration options:

| Option | Environment Variable | Default | Description |
|--------|---------------------|---------|-------------|
| Model | `OLLAMA_MODEL` | `qwen2.5:0.5b` | Model to use |
| Host | `OLLAMA_HOST` | `127.0.0.1` | Host to bind to |
| Port | `OLLAMA_PORT` | `11434` | Port to bind to |
| Context Length | `OLLAMA_NUM_CTX` | `32768` | Maximum context length (tokens) |

### Python Script Arguments

```bash
python scripts/inference/ollama/start_ollama_local.py \
  --model qwen2.5:1.5b \
  --host 127.0.0.1 \
  --port 11434 \
  --context-length 32768
```

### Bash Script Environment Variables

```bash
OLLAMA_MODEL=qwen2.5:1.5b \
OLLAMA_HOST=127.0.0.1 \
OLLAMA_PORT=11434 \
OLLAMA_NUM_CTX=32768 \
./scripts/inference/ollama/start_ollama_local.sh
```

## Using with the Toolkit

Once the server is running, you can use it with the text2sql-eval-toolkit:

### Update your `.env` file:

```bash
# For OpenAI-compatible API
OLLAMA_API_BASE=http://localhost:11434/v1
OLLAMA_API_KEY=ollama  # Can be any value

# Or use Ollama's native API
OLLAMA_HOST=http://localhost:11434
```

### Run inference:

```bash
# Using OpenAI-compatible endpoint
python scripts/inference/run_inference.py \
  --benchmark data/test-benchmarks.json \
  --model "ollama:qwen2.5:0.5b" \
  --output data/results/test-predictions.json
```

## Testing the Server

Once the server is running, test it:

### List available models:

```bash
curl http://localhost:11434/api/tags
```

### Generate text (native API):

```bash
curl http://localhost:11434/api/generate -d '{
  "model": "qwen2.5:0.5b",
  "prompt": "SELECT * FROM users WHERE",
  "stream": false
}'
```

### Chat completion (OpenAI-compatible):

```bash
curl http://localhost:11434/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "qwen2.5:0.5b",
    "messages": [
      {"role": "user", "content": "Write a SQL query to get all users"}
    ]
  }'
```

### Interactive mode:

```bash
ollama run qwen2.5:0.5b
```

## Managing Models

### List installed models:

```bash
ollama list
```

### Pull a new model:

```bash
ollama pull qwen2.5:1.5b
```

### Remove a model:

```bash
ollama rm qwen2.5:0.5b
```

### Show model info:

```bash
ollama show qwen2.5:0.5b
```

## Troubleshooting

### Ollama not found

If you get "ollama: command not found":
1. Install Ollama: `brew install ollama`
2. Or download from https://ollama.ai/download

### Port already in use

Ollama uses port 11434 by default. If it's in use:
```bash
# Stop Ollama
pkill ollama

# Or use a different port (set OLLAMA_HOST environment variable)
export OLLAMA_HOST=127.0.0.1:11435
```

### Model download is slow

First download may take time depending on your internet speed. The model is cached locally for future use.

### Out of memory

If you run out of memory:
1. Use a smaller model (qwen2.5:0.5b)
2. Close other applications
3. Restart Ollama: `pkill ollama && ollama serve`

## Stopping the Server

The scripts keep Ollama running in the background. To stop:

```bash
pkill ollama
```

Or use Activity Monitor to quit the Ollama application.

## Performance on Apple Silicon

Ollama is optimized for Apple Silicon and uses Metal for acceleration:
- **M4 MacBook**: Excellent performance, even with 3B models
- **M3 MacBook**: Great performance
- **M2 MacBook**: Good performance
- **M1 MacBook**: Good performance with smaller models

## Notes

- Ollama runs as a background service and persists after the script exits
- Models are cached in `~/.ollama/models/`
- The OpenAI-compatible API endpoint is at `/v1`
- The native Ollama API endpoint is at `/api`
- Ollama automatically manages memory and model loading
- **Default context length is 32K (32768 tokens)** - suitable for most text-to-SQL tasks
- Larger context lengths require more memory but allow for longer prompts and schemas