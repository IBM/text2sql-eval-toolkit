#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import argparse
import json
from pathlib import Path
import sys


def main():
    parser = argparse.ArgumentParser(
        description="Remove a field from every object in a JSON array."
    )
    parser.add_argument(
        "json_file", help="Path to the input JSON file containing an array of objects."
    )
    parser.add_argument(
        "--drop", required=True, help="Field name to remove from each object."
    )
    parser.add_argument(
        "--inplace",
        action="store_true",
        help="Modify the file in place. Otherwise, prints to stdout.",
    )
    args = parser.parse_args()

    # Read and parse JSON
    try:
        json_path = Path(args.json_file)
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        sys.exit(f"Error: File '{args.json_file}' not found.")
    except json.JSONDecodeError as e:
        sys.exit(f"Error: Failed to parse JSON file: {e}")

    if not isinstance(data, list):
        sys.exit("Error: JSON root element must be an array.")

    # Remove the specified field from each object
    for obj in data:
        if isinstance(obj, dict):
            obj.pop(args.drop, None)

    # Output result
    if args.inplace:
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    else:
        json.dump(data, sys.stdout, indent=2, ensure_ascii=False)
        sys.stdout.write("\n")


if __name__ == "__main__":
    main()
