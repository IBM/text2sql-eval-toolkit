#!/usr/bin/env python3
"""
Script to start a local vLLM server for testing with a small model.
This script is designed for local development and testing purposes.
Automatically uses GPU if available, otherwise falls back to CPU.

Usage:
    python scripts/inference/vllm/start_vllm_local.py
    
    # Or with custom settings:
    python scripts/inference/vllm/start_vllm_local.py --model Qwen/Qwen2.5-1.5B-Instruct --port 8001
    
    # Force CPU mode:
    python scripts/inference/vllm/start_vllm_local.py --cpu

Environment Variables:
    VLLM_MODEL: Model to use (default: Qwen/Qwen2.5-0.5B-Instruct)
    VLLM_HOST: Host to bind to (default: 127.0.0.1)
    VLLM_PORT: Port to bind to (default: 8000)
    VLLM_MAX_MODEL_LEN: Maximum model length (default: 2048)
    VLLM_USE_GPU: Force GPU usage (true/false, overrides auto-detection)
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path


def check_vllm_installed():
    """Check if vLLM is installed."""
    try:
        import vllm
        return True
    except ImportError:
        return False


def check_gpu_available():
    """Check if NVIDIA GPU is available."""
    try:
        result = subprocess.run(
            ['nvidia-smi'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=5
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def check_port_available(port):
    """Check if a port is available."""
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(('127.0.0.1', port))
            return True
        except OSError:
            return False


def main():
    parser = argparse.ArgumentParser(
        description="Start a local vLLM server for testing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start with default settings (auto-detects GPU/CPU)
  python scripts/inference/vllm/start_vllm_local.py
  
  # Force CPU mode
  python scripts/inference/vllm/start_vllm_local.py --cpu
  
  # Use a different model
  python scripts/inference/vllm/start_vllm_local.py --model Qwen/Qwen2.5-1.5B-Instruct
  
  # Use a different port
  python scripts/inference/vllm/start_vllm_local.py --port 8001
  
  # Combine options
  python scripts/inference/vllm/start_vllm_local.py --model Qwen/Qwen2.5-1.5B-Instruct --port 8001 --max-model-len 4096

Recommended small models for CPU testing:
  - Qwen/Qwen2.5-0.5B-Instruct (smallest, fastest)
  - Qwen/Qwen2.5-1.5B-Instruct (better quality)
  - Qwen/Qwen2.5-3B-Instruct (best quality, slower)
  - microsoft/phi-2 (2.7B parameters)
        """
    )
    
    parser.add_argument(
        '--model',
        type=str,
        default=os.getenv('VLLM_MODEL', 'Qwen/Qwen2.5-0.5B-Instruct'),
        help='Model to use (default: Qwen/Qwen2.5-0.5B-Instruct)'
    )
    
    parser.add_argument(
        '--host',
        type=str,
        default=os.getenv('VLLM_HOST', '127.0.0.1'),
        help='Host to bind to (default: 127.0.0.1)'
    )
    
    parser.add_argument(
        '--port',
        type=int,
        default=int(os.getenv('VLLM_PORT', '8000')),
        help='Port to bind to (default: 8000)'
    )
    
    parser.add_argument(
        '--max-model-len',
        type=int,
        default=int(os.getenv('VLLM_MAX_MODEL_LEN', '2048')),
        help='Maximum model length (default: 2048)'
    )
    
    parser.add_argument(
        '--cpu',
        action='store_true',
        help='Force CPU mode (default: auto-detect GPU availability)'
    )
    
    parser.add_argument(
        '--gpu',
        action='store_true',
        help='Force GPU mode (default: auto-detect GPU availability)'
    )
    
    args = parser.parse_args()
    
    # Check if vLLM is installed
    if not check_vllm_installed():
        print("\033[0;31mError: vLLM is not installed.\033[0m")
        print()
        print("To install vLLM, run:")
        print("\033[1;33m  pip install vllm\033[0m")
        print()
        print("Note: vLLM will automatically use CPU if no GPU is available.")
        print("This script is configured to explicitly use CPU mode with --device cpu.")
        print()
        sys.exit(1)
    
    # Check if port is available
    if not check_port_available(args.port):
        print(f"\033[0;31mError: Port {args.port} is already in use.\033[0m")
        print()
        print("To use a different port, use the --port option:")
        print(f"\033[1;33m  python scripts/inference/vllm/start_vllm_local.py --port 8001\033[0m")
        print()
        sys.exit(1)
    
    # Determine device to use
    use_gpu = False
    
    # Check environment variable first
    env_gpu = os.getenv('VLLM_USE_GPU', '').lower()
    if env_gpu in ('true', '1'):
        use_gpu = True
    elif env_gpu in ('false', '0'):
        use_gpu = False
    # Then check command line args
    elif args.gpu:
        use_gpu = True
    elif args.cpu:
        use_gpu = False
    # Finally auto-detect
    else:
        use_gpu = check_gpu_available()
    
    # Print configuration
    print("\033[0;32m========================================\033[0m")
    print("\033[0;32mStarting vLLM Local Server\033[0m")
    print("\033[0;32m========================================\033[0m")
    print()
    print("\033[1;33mConfiguration:\033[0m")
    print(f"  Model: {args.model}")
    print(f"  Host: {args.host}")
    print(f"  Port: {args.port}")
    print(f"  Max Model Length: {args.max_model_len}")
    if use_gpu:
        print(f"  Device: \033[0;34mGPU\033[0m")
    else:
        print(f"  Device: \033[0;34mCPU\033[0m")
    print()
    print(f"\033[0;32mAPI will be available at: http://{args.host}:{args.port}/v1\033[0m")
    print()
    print("\033[1;33mNote: First run will download the model (may take a few minutes)\033[0m")
    print("\033[1;33mPress Ctrl+C to stop the server\033[0m")
    print()
    
    if not use_gpu:
        print("\033[1;33mRunning on CPU (slower but works without GPU)\033[0m")
        print()
    
    # Build command
    cmd = [
        sys.executable, '-m', 'vllm.entrypoints.openai.api_server',
        '--model', args.model,
        '--host', args.host,
        '--port', str(args.port),
        '--max-model-len', str(args.max_model_len),
    ]
    
    # Note: vLLM 0.11+ automatically detects platform (CPU/GPU)
    # The --device flag has been removed in newer versions
    # vLLM will use GPU if available, otherwise CPU
    
    if not use_gpu:
        # For CPU, we can still set dtype and enforce eager mode
        cmd.extend([
            '--dtype', 'float32',
            '--enforce-eager'
        ])
    
    # Start vLLM server
    try:
        subprocess.run(cmd, check=True)
    except KeyboardInterrupt:
        print()
        print("\033[0;32mServer stopped.\033[0m")
        sys.exit(0)
    except subprocess.CalledProcessError as e:
        print(f"\033[0;31mError starting vLLM server: {e}\033[0m")
        sys.exit(1)


if __name__ == '__main__':
    main()