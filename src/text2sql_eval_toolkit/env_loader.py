#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Environment variable loader for text2sql-eval-toolkit.

Automatically loads environment variables from .env file in project root.
"""

import os
from pathlib import Path
from dotenv import load_dotenv


def load_env():
    """
    Load environment variables from .env file in the project root.
    
    This function looks for .env starting from the current working directory
    and searches upwards through parent directories. This allows it to work
    correctly when the toolkit is used as a dependency in other projects.
    
    Returns:
        bool: True if .env file was found and loaded, False otherwise
    """
    # Start from current working directory and search upwards
    current_dir = Path.cwd()
    
    # Search upwards for .env file (max 10 levels)
    for _ in range(10):
        env_path = current_dir / ".env"
        if env_path.exists():
            load_dotenv(env_path, override=False)  # Don't override existing env vars
            return True
        
        # Move to parent directory
        parent = current_dir.parent
        if parent == current_dir:  # Reached root
            break
        current_dir = parent
    
    # Try to load from ~/.env as fallback
    home_env = Path.home() / ".env"
    if home_env.exists():
        load_dotenv(home_env, override=False)
        return True
    
    return False


# Auto-load on import
load_env()

