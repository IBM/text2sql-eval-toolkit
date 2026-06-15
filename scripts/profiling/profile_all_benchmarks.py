#
# Copyright IBM Corp. 2025 - 2026
# SPDX-License-Identifier: Apache-2.0
#

import argparse
import time
from text2sql_eval_toolkit.utils import get_benchmarks_info
from text2sql_eval_toolkit.profiling.profiling_tools import profile_benchmark_id
from text2sql_eval_toolkit.logging import get_logger


logger = get_logger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Profile all benchmarks or test benchmarks (SQLite-backed)"
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Profile test benchmarks instead of production benchmarks",
    )
    args = parser.parse_args()

    benchmarks = get_benchmarks_info(is_test=args.test)

    benchmark_type = "test benchmarks" if args.test else "benchmarks"
    logger.info(f"Starting profiling for all {benchmark_type}")

    start_time = time.time()
    for benchmark_id, benchmark_info in benchmarks.items():
        try:
            logger.info(f"🚀 Running profiling for benchmark: {benchmark_id}")
            db_engine = benchmark_info["db_engine"]
            if db_engine["db_type"] not in [
                "postgres",
                "sqlite",
                "db2",
                "mysql",
            ]:
                raise NotImplementedError(
                    f"Unsupported DB type '{db_engine['db_type']}'."
                )
            profile_benchmark_id(
                benchmark_id,
                profile_gold=True,
                profile_predictions=True,
                profile_eval=True,
            )
            logger.info(f"✅ Done profiling for benchmark: {benchmark_id}")
        except Exception as e:
            logger.info(
                f"‼ ERROR: Profiling for benchmark {benchmark_id} failed : {repr(e)}"
            )
            raise e

    logger.info(
        f"Total running time for profiling all {benchmark_type}: {time.time() - start_time:.2f} seconds"
    )


if __name__ == "__main__":
    main()
