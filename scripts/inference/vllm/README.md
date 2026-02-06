# Local vLLM Server for Testing

This directory contains scripts to easily start a local vLLM server for testing text-to-SQL models. The scripts automatically detect and use GPU if available, otherwise fall back to CPU.

## Quick Start

### Option 1: Using the Python Script (Recommended, works with uv)

```bash
# With uv
uv run python scripts/inference/vllm/start_vllm_local.py

# Or with regular Python
python scripts/inference/vllm/start_vllm_local.py
```

### Option 2: Using the Bash Script (macOS/Linux, requires system Python)

```bash
# Only works if vLLM is installed in system Python
./scripts/inference/vllm/start_vllm_local.sh
```

## Installation

Install vLLM using pip:

```bash
pip install vllm
```

Or if using `uv`:

```bash
uv pip install vllm
```

**⚠️ Important Note about vLLM 0.11.0 on CPU:**

There is a known compatibility issue with vLLM 0.11.0 running on CPU with certain models (tokenizer compatibility). The scripts are provided for reference, but **CPU inference with vLLM may not work reliably**.

**Recommended alternatives for CPU inference:**
1. **Use GPU** - vLLM is designed primarily for GPU inference and works best with CUDA
2. **Use the toolkit's built-in inference** with API providers (OpenAI, Anthropic, WatsonX)
3. **Use llama.cpp** for local CPU inference
4. **Use Ollama** for easy local model serving on CPU

**GPU Detection:**
- If NVIDIA GPU is detected (via `nvidia-smi`), it will use GPU mode (recommended)
- If no GPU is available, it will fall back to CPU mode (may have compatibility issues)
- You can force CPU or GPU mode using command-line flags or environment variables

**Important for `uv` users:**

The Python script works seamlessly with `uv run`:
```bash
uv run python scripts/inference/vllm/start_vllm_local.py
```

The bash script requires vLLM to be installed in your system Python (not the uv-managed environment), so it's recommended to use the Python script when working with uv.

## Usage Examples

### Basic Usage

Start with default settings (auto-detects GPU/CPU, Qwen2.5-0.5B model on port 8000):

```bash
# With uv
uv run python scripts/inference/vllm/start_vllm_local.py

# Or with regular Python
python scripts/inference/vllm/start_vllm_local.py
```

### Force CPU Mode

Force CPU mode even if GPU is available:

```bash
uv run python scripts/inference/vllm/start_vllm_local.py --cpu
```

### Force GPU Mode

Force GPU mode (will fail if no GPU available):

```bash
uv run python scripts/inference/vllm/start_vllm_local.py --gpu
```

### Custom Model

Use a different model:

```bash
uv run python scripts/inference/vllm/start_vllm_local.py --model Qwen/Qwen2.5-1.5B-Instruct
```

### Custom Port

Use a different port:

```bash
VLLM_PORT=8001 uv run python scripts/inference/vllm/start_vllm_local.py
# or
uv run python scripts/inference/vllm/start_vllm_local.py --port 8001
```

### Combined Options

```bash
uv run python scripts/inference/vllm/start_vllm_local.py \
  --model Qwen/Qwen2.5-1.5B-Instruct \
  --port 8001 \
  --max-model-len 4096 \
  --cpu
```

## Environment Variables

You can also configure the server using environment variables:

```bash
export VLLM_MODEL="Qwen/Qwen2.5-1.5B-Instruct"
export VLLM_PORT=8001
export VLLM_MAX_MODEL_LEN=4096
export VLLM_USE_GPU=false  # Force CPU mode
uv run python scripts/inference/vllm/start_vllm_local.py
```

Available environment variables:
- `VLLM_MODEL`: Model to use (default: Qwen/Qwen2.5-0.5B-Instruct)
- `VLLM_HOST`: Host to bind to (default: 127.0.0.1)
- `VLLM_PORT`: Port to bind to (default: 8000)
- `VLLM_MAX_MODEL_LEN`: Maximum model length (default: 2048)
- `VLLM_USE_GPU`: Force GPU usage (true/false, overrides auto-detection)

## Recommended Models for CPU Testing

The following small models work well on CPU for local testing:

1. **Qwen/Qwen2.5-0.5B-Instruct** (default)
   - Smallest and fastest
   - Good for quick testing
   - ~500M parameters

2. **Qwen/Qwen2.5-1.5B-Instruct**
   - Better quality than 0.5B
   - Still fast on CPU
   - ~1.5B parameters

3. **Qwen/Qwen2.5-3B-Instruct**
   - Best quality among small models
   - Slower on CPU but manageable
   - ~3B parameters

4. **microsoft/phi-2**
   - Alternative option
   - ~2.7B parameters

## Using with the Toolkit

Once the server is running, update your `.env` file:

```bash
VLLM_API_BASE=http://localhost:8000/v1
VLLM_API_KEY=optional_api_key_here
```

Then use the `vllm:` prefix in your model configuration:

```bash
python scripts/inference/run_inference.py \
  --benchmark data/test-benchmarks.json \
  --model "vllm:Qwen/Qwen2.5-0.5B-Instruct" \
  --output data/results/test-predictions.json
```

## Testing the Server

Once the server is running, you can test it with curl:

```bash
# Health check
curl http://localhost:8000/health

# List models
curl http://localhost:8000/v1/models

# Test completion
curl http://localhost:8000/v1/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "Qwen/Qwen2.5-0.5B-Instruct",
    "prompt": "SELECT * FROM",
    "max_tokens": 50
  }'

# Test chat completion
curl http://localhost:8000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "Qwen/Qwen2.5-0.5B-Instruct",
    "messages": [
      {"role": "user", "content": "Write a SQL query to get all users"}
    ],
    "max_tokens": 100
  }'
```

## Troubleshooting

### Port Already in Use

If you get a "port already in use" error, either:
1. Stop the existing server
2. Use a different port with `--port 8001`

### Out of Memory

If you run out of memory:
1. Use a smaller model (e.g., Qwen2.5-0.5B)
2. Reduce `--max-model-len` (e.g., `--max-model-len 1024`)
3. Close other applications

### Slow Performance

CPU inference is slower than GPU. To improve performance:
1. Use the smallest model that meets your needs
2. Reduce `--max-model-len`
3. Consider using GPU mode if available

### Model Download Issues

On first run, the model will be downloaded from Hugging Face. If you have issues:
1. Check your internet connection
2. Ensure you have enough disk space
3. Set `HF_HOME` environment variable to control cache location:
   ```bash
   export HF_HOME=/path/to/cache
   ```

## Stopping the Server

Press `Ctrl+C` in the terminal where the server is running.

## Notes

- The server runs in the foreground. Use a terminal multiplexer (tmux/screen) or run in a separate terminal for background operation.
- First run will download the model, which may take several minutes depending on your internet speed.
- CPU inference is significantly slower than GPU but sufficient for testing and development.
- The server implements the OpenAI-compatible API, so it works with any client that supports OpenAI's API format.