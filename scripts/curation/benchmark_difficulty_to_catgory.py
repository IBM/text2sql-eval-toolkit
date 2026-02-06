#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import os
import json
import argparse


def process_files(folder_path, suffix):
    print(f"Processing path: {folder_path}")
    for filename in os.listdir(folder_path):
        if filename.endswith(suffix):
            file_path = os.path.join(folder_path, filename)
            print(f"Processing file: {filename}")

            with open(file_path, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError:
                    print(f"Skipping {filename}: invalid JSON format.")
                    continue

            modified = False
            for obj in data:
                if "difficulty" in obj:
                    difficulty = "difficulty_" + obj["difficulty"]
                    if "meta" not in obj:
                        obj["meta"] = {}
                    if "categories" not in obj["meta"]:
                        obj["meta"]["categories"] = []
                    if difficulty not in obj["meta"]["categories"]:
                        obj["meta"]["categories"].append(difficulty)
                        modified = True

            if modified:
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)
                print(f"Updated: {filename}")
            else:
                print(f"No changes needed: {filename}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Update JSON files by adding difficulty to categories."
    )
    parser.add_argument(
        "--folder",
        type=str,
        default="data/benchmarks",
        help="Folder containing JSON files",
    )
    parser.add_argument(
        "--suffix", type=str, default=".json", help="Suffix of files to process"
    )
    args = parser.parse_args()

    process_files(args.folder, args.suffix)
