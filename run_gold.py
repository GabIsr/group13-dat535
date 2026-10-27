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

    args = parser.parse_args()
    logger.info(f"Starting Gold Pipeline Orchestrator | Env: {args.env}")
    run_gold_pipeline()


if __name__ == "__main__":
    main()
