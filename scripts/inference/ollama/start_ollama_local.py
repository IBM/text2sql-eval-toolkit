#!/usr/bin/env python3
"""
Script to start and configure a local Ollama server for testing.
Ollama works great on Apple Silicon (M1/M2/M3/M4) and provides OpenAI-compatible API.

Usage:
    python scripts/inference/ollama/start_ollama_local.py
    
    # Or with custom settings:
    python scripts/inference/ollama/start_ollama_local.py --model qwen2.5:1.5b

Environment Variables:
    OLLAMA_MODEL: Model to use (default: qwen2.5:0.5b)
    OLLAMA_HOST: Host to bind to (default: 127.0.0.1)
    OLLAMA_PORT: Port to bind to (default: 11434)
"""

import argparse
import os
import subprocess
import sys
import time


def check_ollama_installed():
    """Check if Ollama is installed."""
    try:
        subprocess.run(['ollama', '--version'], 
                      stdout=subprocess.PIPE, 
                      stderr=subprocess.PIPE,
                      check=True)
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False


def is_ollama_running():
    """Check if Ollama service is running."""
    try:
        result = subprocess.run(['pgrep', '-x', 'ollama'],
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)
        return result.returncode == 0
    except FileNotFoundError:
        # pgrep not available, try alternative
        try:
            result = subprocess.run(['ps', 'aux'],
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE,
                                  text=True)
            return 'ollama' in result.stdout
        except:
            return False


def start_ollama_service(context_length=32768):
    """Start Ollama service in background with specified context length."""
    try:
        # Set environment variable for context length
        env = os.environ.copy()
        env['OLLAMA_NUM_CTX'] = str(context_length)
        
        subprocess.Popen(['ollama', 'serve'],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        env=env)
        time.sleep(3)
        return True
    except Exception as e:
        print(f"Error starting Ollama service: {e}")
        return False


def check_model_available(model):
    """Check if model is available locally."""
    try:
        result = subprocess.run(['ollama', 'list'],
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE,
                              text=True)
        return model in result.stdout
    except:
        return False


def pull_model(model):
    """Pull model from Ollama registry."""
    try:
        print(f"\033[1;33mPulling model {model}...\033[0m")
        print("\033[1;33mThis may take a few minutes on first run.\033[0m")
        subprocess.run(['ollama', 'pull', model], check=True)
        return True
    except subprocess.CalledProcessError:
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Start and configure local Ollama server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start with default settings (qwen2.5:0.5b)
  python scripts/inference/ollama/start_ollama_local.py
  
  # Use a different model
  python scripts/inference/ollama/start_ollama_local.py --model qwen2.5:1.5b
  
  # Use a larger model
  python scripts/inference/ollama/start_ollama_local.py --model llama3.2:3b

Recommended small models for testing:
  - qwen2.5:0.5b (smallest, fastest - 397MB)
  - qwen2.5:1.5b (better quality - 987MB)
  - qwen2.5:3b (best quality - 1.9GB)
  - llama3.2:1b (1GB)
  - llama3.2:3b (2GB)
  - phi3:mini (2.3GB)

See all available models: https://ollama.ai/library
        """
    )
    
    parser.add_argument(
        '--model',
        type=str,
        default=os.getenv('OLLAMA_MODEL', 'qwen2.5:0.5b'),
        help='Model to use (default: qwen2.5:0.5b)'
    )
    
    parser.add_argument(
        '--host',
        type=str,
        default=os.getenv('OLLAMA_HOST', '127.0.0.1'),
        help='Host to bind to (default: 127.0.0.1)'
    )
    
    parser.add_argument(
        '--port',
        type=int,
        default=int(os.getenv('OLLAMA_PORT', '11434')),
        help='Port to bind to (default: 11434)'
    )
    
    parser.add_argument(
        '--context-length',
        type=int,
        default=int(os.getenv('OLLAMA_NUM_CTX', '32768')),
        help='Context length for the model (default: 32768)'
    )
    
    args = parser.parse_args()
    
    # Set context length environment variable
    os.environ['OLLAMA_NUM_CTX'] = str(args.context_length)
    
    # Check if Ollama is installed
    if not check_ollama_installed():
        print("\033[0;31mError: Ollama is not installed.\033[0m")
        print()
        print("To install Ollama on macOS:")
        print("\033[1;33m  brew install ollama\033[0m")
        print()
        print("Or download from:")
        print("\033[1;33m  https://ollama.ai/download\033[0m")
        print()
        sys.exit(1)
    
    # Print configuration
    print("\033[0;32m========================================\033[0m")
    print("\033[0;32mStarting Ollama Local Server\033[0m")
    print("\033[0;32m========================================\033[0m")
    print()
    print("\033[1;33mConfiguration:\033[0m")
    print(f"  Model: {args.model}")
    print(f"  Host: {args.host}")
    print(f"  Port: {args.port}")
    print(f"  Context Length: {args.context_length}")
    print()
    
    # Check if Ollama service is running
    if not is_ollama_running():
        print("\033[1;33mStarting Ollama service...\033[0m")
        if not start_ollama_service(args.context_length):
            print("\033[0;31mFailed to start Ollama service\033[0m")
            sys.exit(1)
    
    # Check if model is available
    print(f"\033[0;32mChecking if model {args.model} is available...\033[0m")
    if not check_model_available(args.model):
        if not pull_model(args.model):
            print(f"\033[0;31mFailed to pull model {args.model}\033[0m")
            sys.exit(1)
    
    print()
    print("\033[0;32m✓ Ollama server is running\033[0m")
    print(f"\033[0;32m✓ Model {args.model} is ready\033[0m")
    print()
    print("\033[0;34mAPI Endpoints:\033[0m")
    print(f"  OpenAI-compatible: http://{args.host}:{args.port}/v1")
    print(f"  Ollama native: http://{args.host}:{args.port}/api")
    print()
    print("\033[1;33mTest the server:\033[0m")
    print(f"  curl http://{args.host}:{args.port}/api/tags")
    print()
    print("\033[1;33mGenerate text:\033[0m")
    print(f"  ollama run {args.model}")
    print()
    print("\033[0;32mServer is ready for use!\033[0m")
    print("\033[1;33mPress Ctrl+C to exit (Ollama service will keep running)\033[0m")
    print()
    
    # Keep script running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print()
        print("\033[0;32mOllama server is still running in background\033[0m")
        sys.exit(0)


if __name__ == '__main__':
    main()