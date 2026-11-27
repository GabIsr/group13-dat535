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
from src.silver.config import BRONZE_DIR, METADATA_DIR, SILVER_DIR
from src.silver.join_silver import join_silver_datasets
from src.silver.quality_report import generate_quality_reconciliation_report
from src.utils.benchmark import get_profiler
from src.utils.partitioning import get_monthly_execution_windows

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
        "--mode",
        choices=["all", "incremental", "backfill"],
        default="all",
        help="Pipeline execution mode (default: all)",
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

    bronze_311_dir = BRONZE_DIR / "nyc311"
    manifests_dir = METADATA_DIR / "manifests"
    windows = get_monthly_execution_windows(
        start_date=args.start_date,
        end_date=args.end_date,
        bronze_dir=bronze_311_dir,
        mode=args.mode,
        manifests_dir=manifests_dir,
    )

    if not windows:
        logger.warning("No execution windows found matching the specified date bounds. Exiting.")
        return

    logger.info(
        f"Partition-Batching Plan: {len(windows)} sequential monthly slice(s): "
        f"{', '.join([f'[{w[0]} -> {w[1]}]' for w in windows])}"
    )

    profiler = get_profiler()
    m_weather = {}
    m_311 = {}
    m_join = {}

    for idx, (w_start, w_end) in enumerate(windows, 1):
        partition_label = w_start[:7]
        profiler.set_active_partition(partition_label)
        logger.info(
            f"=== Processing Partition Slice {idx}/{len(windows)}: [{w_start} -> {w_end}] "
            f"(Partition: {partition_label}) ==="
        )

        # Stage 1: Clean Weather
        if args.stage in ["all", "weather"]:
            logger.info(f"[{w_start} -> {w_end}] Executing Silver Stage: Clean Weather Reanalysis...")
            curr_weather = clean_and_validate_weather(start_date=w_start, end_date=w_end)
            if not m_weather:
                m_weather = curr_weather.copy()
            else:
                for k in ["raw_rows", "clean_rows", "anomalies_quarantined"]:
                    if k in curr_weather and k in m_weather:
                        m_weather[k] += curr_weather[k]

        # Stage 2: Clean NYC 311
        if args.stage in ["all", "311"]:
            logger.info(f"[{w_start} -> {w_end}] Executing Silver Stage: Clean NYC 311 Service Requests...")
            curr_311 = clean_and_validate_311(start_date=w_start, end_date=w_end)
            if not m_311:
                m_311 = curr_311.copy()
            else:
                for k in [
                    "input_rows",
                    "duplicate_rows_removed",
                    "valid_rows_accepted",
                    "closed_tickets_count",
                    "open_tickets_count",
                    "status_conflict_count",
                    "instant_closure_count",
                    "quarantined_rows",
                ]:
                    if k in curr_311 and k in m_311:
                        m_311[k] += curr_311[k]

        # Stage 3: Unified Fact Join
        if args.stage in ["all", "join"]:
            if args.stage == "all" and (
                m_311.get("status") == "skipped"
                or m_weather.get("status") == "skipped"
                or m_311.get("valid_rows_accepted", 0) == 0
                or m_weather.get("clean_rows", 0) == 0
            ):
                logger.warning(
                    f"Skipping Unified Analytical Fact Join because upstream Silver stage was skipped or produced 0 rows "
                    f"(311 valid: {m_311.get('valid_rows_accepted', 0)}, weather clean: {m_weather.get('clean_rows', 0)})."
                )
            else:
                logger.info(f"[{w_start} -> {w_end}] Executing Silver Stage: Unified Analytical Fact Join...")
                curr_join = join_silver_datasets(start_date=w_start, end_date=w_end)
                if not m_join:
                    m_join = curr_join.copy()
                else:
                    for k in ["total_311_input", "matched_weather_rows", "unmatched_historical_rows"]:
                        if k in curr_join and k in m_join:
                            m_join[k] += curr_join[k]

    profiler.set_active_partition(None)

    # If full pipeline ran, compile Quality Reconciliation Ledger
    if args.stage == "all" and m_311 and m_weather and m_join:
        try:
            from src.utils.spark import get_spark_session
            audit_spark = get_spark_session("DAT535-Silver-Audit")
            total_lakehouse_count = audit_spark.read.parquet(str(SILVER_DIR / "nyc311")).count()
            m_311["total_lakehouse_records"] = total_lakehouse_count
        except Exception as e:
            logger.debug(f"Could not compute total on-disk Lakehouse records: {e}")
        generate_quality_reconciliation_report(m_311, m_weather, m_join)

    elapsed = time.time() - overall_start
    logger.info(f"Silver Pipeline completed successfully in {elapsed:.2f} seconds.")

    # Save benchmark JSON and print terminal summary table
    profiler = get_profiler()
    profiler.save_json()
    profiler.print_summary_table()


if __name__ == "__main__":
    main()
