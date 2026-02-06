#!/usr/bin/env python3
#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Script to add unique 'id' fields to JSON objects in an array.
Takes a JSON file path as input, adds hash-based IDs to each object,
and writes back to the same file with 'id' as the first field.
"""

import json
import hashlib
import sys
from collections import OrderedDict
from pathlib import Path


def generate_id(obj):
    """Generate a unique ID based on the hash of the object content."""
    # Create a deterministic string representation of the object
    obj_str = json.dumps(obj, sort_keys=True, separators=(",", ":"))
    # Generate SHA256 hash and take first 12 characters for a shorter ID
    return hashlib.sha256(obj_str.encode("utf-8")).hexdigest()[:12]


def add_ids_to_objects(data):
    """Add unique 'id' field to each object in the array."""
    for obj in data:
        if isinstance(obj, dict):
            # Generate ID based on current object content
            obj_id = generate_id(obj)

            # Create new ordered dict with 'id' first
            new_obj = OrderedDict()
            new_obj["id"] = obj_id

            # Add all other fields
            for key, value in obj.items():
                new_obj[key] = value

            # Update the original object
            obj.clear()
            obj.update(new_obj)

    return data


def main():
    if len(sys.argv) != 2:
        print("Usage: python add_ids_to_json.py <path_to_json_file>")
        sys.exit(1)

    json_path = Path(sys.argv[1])

    # Check if file exists
    if not json_path.exists():
        print(f"Error: File '{json_path}' does not exist.")
        sys.exit(1)

    try:
        # Read the JSON file
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Verify it's an array
        if not isinstance(data, list):
            print("Error: JSON file must contain an array at the root level.")
            sys.exit(1)

        print(f"Processing {len(data)} objects...")

        # Add IDs to each object
        updated_data = add_ids_to_objects(data)

        # Write back to the same file
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(updated_data, f, indent=2, ensure_ascii=False)

        print(f"Successfully added IDs to {len(updated_data)} objects in '{json_path}'")
        print("Sample ID format:", updated_data[0]["id"] if updated_data else "N/A")

    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in file '{json_path}': {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error processing file: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
