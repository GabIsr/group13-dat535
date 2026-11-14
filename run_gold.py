"""
DAT535 Medallion Pipeline: Gold Layer Orchestrator Runner.
Builds the Daily Service Demand Mart and Agency Resolution Performance Mart.
Author: Group 13
"""

import argparse
import logging
import sys
import time
from pathlib import Path

# Add project root to path for module resolution
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.gold.build_gold import run_gold_pipeline
from src.utils.benchmark import get_profiler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("run_gold")


def main():
    parser = argparse.ArgumentParser(
        description="DAT535 Medallion Gold Pipeline: Build Business Analytical Data Marts."
    )
    parser.add_argument(
        "--env",
        choices=["dev", "prod"],
        default="dev",
        help="Target execution environment (default: dev)",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Optional ISO start date for partition-scoped execution (e.g. 2026-08-01)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="Optional ISO end date for partition-scoped execution (e.g. 2026-08-31)",
    )

    args = parser.parse_args()
    logger.info(
        f"Starting Gold Pipeline Orchestrator | Env: {args.env} | "
        f"Window: [{args.start_date or 'ALL'} -> {args.end_date or 'ALL'}]"
    )
    run_gold_pipeline(start_date=args.start_date, end_date=args.end_date)
    profiler = get_profiler()
    profiler.save_json()
    profiler.print_summary_table()


if __name__ == "__main__":
    main()
