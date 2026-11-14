"""
DAT535 Medallion Pipeline: Silver Layer Orchestrator.
Runs data cleaning, spatial imputation, physical validation, broadcast fact join,
and emits the Before-and-After Data Quality Ledger.
Author: Group 13
"""

import argparse
import logging
import sys
import time
from pathlib import Path

# Add project root to path for module resolution
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.silver.clean_311 import clean_and_validate_311
from src.silver.clean_weather import clean_and_validate_weather
from src.silver.join_silver import join_silver_datasets
from src.silver.quality_report import generate_quality_reconciliation_report
from src.utils.benchmark import get_profiler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("run_silver")


def main():
    parser = argparse.ArgumentParser(
        description="DAT535 Medallion Silver Pipeline: Clean, Standardize, Impute, and Join."
    )
    parser.add_argument(
        "--stage",
        choices=["all", "311", "weather", "join"],
        default="all",
        help="Pipeline stage to execute (default: all)",
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
        f"Starting Silver Pipeline Orchestrator | Stage: {args.stage} | Env: {args.env} | "
        f"Window: [{args.start_date or 'ALL'} -> {args.end_date or 'ALL'}]"
    )
    overall_start = time.time()

    m_weather = {}
    m_311 = {}
    m_join = {}

    # Stage 1: Clean Weather
    if args.stage in ["all", "weather"]:
        logger.info("Executing Silver Stage: Clean Weather Reanalysis...")
        m_weather = clean_and_validate_weather(start_date=args.start_date, end_date=args.end_date)

    # Stage 2: Clean NYC 311
    if args.stage in ["all", "311"]:
        logger.info("Executing Silver Stage: Clean NYC 311 Service Requests...")
        m_311 = clean_and_validate_311(start_date=args.start_date, end_date=args.end_date)

    # Stage 3: Unified Fact Join
    if args.stage in ["all", "join"]:
        if args.stage == "all" and (m_311.get("status") == "skipped" or m_weather.get("status") == "skipped"):
            logger.warning(
                f"Skipping Unified Analytical Fact Join because upstream Silver stage was skipped "
                f"(311 status: {m_311.get('status', 'unknown')}, weather status: {m_weather.get('status', 'unknown')})."
            )
        else:
            logger.info("Executing Silver Stage: Unified Analytical Fact Join...")
            m_join = join_silver_datasets(start_date=args.start_date, end_date=args.end_date)

    # If full pipeline ran, compile Quality Reconciliation Ledger
    if args.stage == "all" and m_311 and m_weather and m_join:
        generate_quality_reconciliation_report(m_311, m_weather, m_join)

    elapsed = time.time() - overall_start
    logger.info(f"Silver Pipeline completed successfully in {elapsed:.2f} seconds.")

    # Save benchmark JSON and print terminal summary table
    profiler = get_profiler()
    profiler.save_json()
    profiler.print_summary_table()


if __name__ == "__main__":
    main()
