#!/usr/bin/env python3
#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

"""
Spider Data Converter

Converts Spider benchmark data format to the toolkit's expected format.
Reads spider-dev.json and spider-dev_gold.sql to create the final benchmark files.

Usage:
    python spider_data_converter.py --input data/benchmarks/spider-dev.json \
                                     --gold data/benchmarks/spider-dev_gold.sql \
                                     --output data/benchmarks/spider-dev-converted.json \
                                     --sample 50 --random-seed 42
"""

import json
import argparse
import random
from typing import List, Dict, Any


def load_gold_sql(gold_file: str) -> List[str]:
    """Load gold SQL queries from the tab-separated file."""
    with open(gold_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Each line is: SQL_QUERY\tDB_ID
    gold_queries = []
    for line in lines:
        line = line.strip()
        if line:
            # Split by tab and take the SQL part
            parts = line.split('\t')
            if len(parts) >= 1:
                gold_queries.append(parts[0])
            else:
                gold_queries.append(line)
    
    return gold_queries


def convert_spider_to_toolkit_format(
    spider_data: List[Dict[str, Any]],
    gold_queries: List[str],
    sample_size: int | None = None,
    random_seed: int | None = None
) -> List[Dict[str, Any]]:
    """
    Convert Spider format to toolkit format.
    
    Spider format has:
    - db_id: database identifier
    - query: SQL query (but we'll use gold_queries as authoritative)
    - question: natural language question
    - question_toks: tokenized question
    - sql: parsed SQL structure (we'll ignore this)
    
    Toolkit format needs:
    - question_id: unique identifier
    - db_id: database identifier
    - question: natural language question
    - SQL: the gold SQL query
    - evidence: additional context (empty for Spider)
    - difficulty: difficulty level (empty for Spider)
    - meta: metadata with categories and features (empty for Spider)
    """
    
    if len(spider_data) != len(gold_queries):
        print(f"⚠️  Warning: spider_data has {len(spider_data)} entries but gold_queries has {len(gold_queries)} entries")
        # Use the minimum length to avoid index errors
        min_len = min(len(spider_data), len(gold_queries))
        spider_data = spider_data[:min_len]
        gold_queries = gold_queries[:min_len]
    
    # Create indices for sampling
    indices = list(range(len(spider_data)))
    
    # If sample_size is specified, randomly sample indices
    if sample_size and sample_size < len(indices):
        if random_seed is not None:
            random.seed(random_seed)
        indices = random.sample(indices, sample_size)
        indices.sort()  # Sort to maintain some order
        print(f"✂️  Randomly sampled {sample_size} entries from {len(spider_data)} total (seed: {random_seed})")
    
    converted_data = []
    
    for new_idx, original_idx in enumerate(indices):
        spider_entry = spider_data[original_idx]
        gold_sql = gold_queries[original_idx]
        
        toolkit_entry = {
            "question_id": new_idx,
            "db_id": spider_entry["db_id"],
            "question": spider_entry["question"],
            "SQL": gold_sql,
            "evidence": "",
            "difficulty": "",
            "meta": {
                "categories": [],
                "features": {}
            },
            "sql": [gold_sql]  # Add sql array for compatibility
        }
        
        converted_data.append(toolkit_entry)
    
    return converted_data


def main():
    parser = argparse.ArgumentParser(
        description="Convert Spider benchmark data to toolkit format"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Input Spider JSON file (e.g., spider-dev.json)"
    )
    parser.add_argument(
        "--gold",
        required=True,
        help="Gold SQL file (e.g., spider-dev_gold.sql)"
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output JSON file path"
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=None,
        help="Create a sample with N entries (optional)"
    )
    parser.add_argument(
        "--random-seed",
        type=int,
        default=42,
        help="Random seed for sampling (default: 42)"
    )
    
    args = parser.parse_args()
    
    print(f"📖 Loading Spider data from {args.input}")
    with open(args.input, 'r', encoding='utf-8') as f:
        spider_data = json.load(f)
    print(f"   Loaded {len(spider_data)} entries")
    
    print(f"📖 Loading gold SQL from {args.gold}")
    gold_queries = load_gold_sql(args.gold)
    print(f"   Loaded {len(gold_queries)} gold queries")
    
    print(f"🔄 Converting to toolkit format...")
    converted_data = convert_spider_to_toolkit_format(
        spider_data,
        gold_queries,
        sample_size=args.sample,
        random_seed=args.random_seed if args.sample else None
    )
    
    print(f"💾 Writing to {args.output}")
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(converted_data, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Conversion complete! Created {len(converted_data)} entries")
    
    # Print some statistics
    db_counts = {}
    for entry in converted_data:
        db_id = entry["db_id"]
        db_counts[db_id] = db_counts.get(db_id, 0) + 1
    
    print(f"\n📊 Statistics:")
    print(f"   Total entries: {len(converted_data)}")
    print(f"   Unique databases: {len(db_counts)}")
    print(f"   Top 5 databases by question count:")
    for db_id, count in sorted(db_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
        print(f"      {db_id}: {count} questions")


if __name__ == "__main__":
    main()