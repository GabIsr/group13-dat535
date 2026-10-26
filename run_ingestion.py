"""
DAT535 Medallion Pipeline Ingestion Runner.
Coordinates landing and Bronze layer ingestion for NYC 311 and Open-Meteo ERA5-Land weather.
Supports incremental CDC updates and backfill operations.
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

from src.ingestion.ingest_311 import run_311_ingestion
from src.ingestion.ingest_weather import run_weather_ingestion
from src.utils.config import MANIFESTS_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("run_ingestion")


def print_summary_table(manifests: list):
    """Print an ASCII summary table of the executed ingestion batches."""
    if not manifests:
        print("\n[INFO] No new ingestion batches were executed.\n")
        return

    print("\n" + "=" * 105)
    print("DAT535 INGESTION BATCH EXECUTION SUMMARY (MEDALLION BRONZE & LANDING)")
    print("=" * 105)
    header = (
        f"{'Dataset':<14} | {'Batch ID':<18} | {'Mode':<11} | {'Interval':<23} | "
        f"{'Rows':<9} | {'Files':<5} | {'Sec':<5} | {'Status'}"
    )
    print(header)
    print("-" * 105)

    for m in manifests:
        start_short = str(m.get("source_data_start", "N/A"))[:10]
        end_short = str(m.get("source_data_end", "N/A"))[:10]
        interval = f"{start_short} -> {end_short}"
        row = (
            f"{m.get('dataset_name', ''):<14} | "
            f"{m.get('batch_id', ''):<18} | "
            f"{m.get('extraction_mode', ''):<11} | "
            f"{interval:<23} | "
            f"{m.get('row_count', 0):<9,d} | "
            f"{len(m.get('files', [])):<5d} | "
            f"{m.get('duration_seconds', 0.0):<5.1f} | "
            f"{m.get('status', 'unknown')}"
        )
        print(row)

    print("=" * 105)
    print("Tier 1 Manifest Documents saved to: data/metadata/manifests/")
    print("=" * 105 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="DAT535 Medallion Bronze Ingestion Pipeline (NYC 311 + ERA5-Land Weather)"
    )
    parser.add_argument(
        "--dataset",
        choices=["all", "311", "weather"],
        default="all",
        help="Target dataset to ingest (default: all)",
    )
    parser.add_argument(
        "--mode",
        choices=["incremental", "backfill", "bootstrap"],
        default="incremental",
        help="Acquisition mode: incremental (CDC via watermark), backfill, or bootstrap (default: incremental)",
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
        help="Start date filter (e.g. 2026-07-15 or 2026-10-14T00:00:00.000)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date filter (e.g. 2026-10-22 or 2026-10-22T23:59:59.000)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50000,
        help="Page limit per Socrata network request (default: 50,000)",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        help="Optional maximum records limit for testing / quick validation runs",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=14,
        help="Lookback buffer in days for CDC status updates on existing tickets (default: 14)",
    )
    parser.add_argument(
        "--app-token",
        type=str,
        help="Optional Socrata App Token to avoid public rate throttling",
    )

    args = parser.parse_args()
    logger.info(
        f"Starting Pipeline Orchestrator | Dataset: {args.dataset} | Mode: {args.mode} | Env: {args.env}"
    )

    manifests = []
    overall_start = time.time()

    # 1. NYC 311 Ingestion
    if args.dataset in ["all", "311"]:
        try:
            m_311 = run_311_ingestion(
                mode=args.mode,
                start_date=args.start_date,
                end_date=args.end_date,
                batch_size=args.batch_size,
                max_records=args.max_records,
                lookback_days=args.lookback_days,
                app_token=args.app_token,
                env=args.env,
                manifests_dir=MANIFESTS_DIR,
            )
            if m_311:
                manifests.append(m_311)
        except Exception as e:
            logger.error(f"Failed to execute NYC 311 ingestion: {e}", exc_info=True)
            if args.dataset == "311":
                sys.exit(1)

    # 2. Open-Meteo ERA5-Land Weather Ingestion
    if args.dataset in ["all", "weather"]:
        try:
            # Weather mode does not use "bootstrap" directly; maps to "incremental" or "backfill"
            weather_mode = "backfill" if args.mode == "backfill" else "incremental"
            m_wth = run_weather_ingestion(
                mode=weather_mode,
                start_date=args.start_date,
                end_date=args.end_date,
                env=args.env,
                manifests_dir=MANIFESTS_DIR,
            )
            if m_wth:
                manifests.append(m_wth)
        except Exception as e:
            logger.error(f"Failed to execute weather ingestion: {e}", exc_info=True)
            if args.dataset == "weather":
                sys.exit(1)

    print_summary_table(manifests)
    total_elapsed = time.time() - overall_start
    logger.info(f"Pipeline run completed in {total_elapsed:.2f} seconds.")


if __name__ == "__main__":
    main()
