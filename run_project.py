"""
DAT535 Course Project End-to-End Orchestrator (Medallion Pipeline).
Coordinates Bronze Ingestion, Silver Standardization/Joining, and Test Suites.
Author: Group 13
"""

import argparse
import logging
import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("run_project")


def run_tests():
    """Run all automated unit and integration tests."""
    logger.info("=" * 70)
    logger.info("RUNNING AUTOMATED TEST SUITE (INGESTION & SILVER)")
    logger.info("=" * 70)
    start = time.time()
    res = subprocess.run([sys.executable, "-m", "unittest", "discover", "-s", "tests"])
    elapsed = time.time() - start
    if res.returncode == 0:
        logger.info(f"All unit tests passed successfully in {elapsed:.2f}s.")
        return True
    else:
        logger.error(f"Test suite failed with returncode {res.returncode}.")
        return False


def run_bronze(env: str = "dev"):
    """Execute Bronze Landing and Ingestion."""
    logger.info("=" * 70)
    logger.info("EXECUTING BRONZE LAYER (LANDING & PRESERVATION)")
    logger.info("=" * 70)
    from run_ingestion import main as ingestion_main
    # Call ingestion
    cmd = [sys.executable, "run_ingestion.py", "--dataset", "all", "--mode", "incremental", "--env", env]
    res = subprocess.run(cmd)
    return res.returncode == 0


def run_silver(env: str = "dev"):
    """Execute Silver Cleaning, Validation, Imputation, and Broadcast Join."""
    logger.info("=" * 70)
    logger.info("EXECUTING SILVER LAYER (CLEANING, IMPUTATION & FACT JOIN)")
    logger.info("=" * 70)
    cmd = [sys.executable, "run_silver.py", "--stage", "all", "--env", env]
    res = subprocess.run(cmd)
    return res.returncode == 0


def run_gold(env: str = "dev"):
    """Execute Gold Business Marts Generation."""
    logger.info("=" * 70)
    logger.info("EXECUTING GOLD LAYER (BUSINESS ANALYTICAL MARTS)")
    logger.info("=" * 70)
    cmd = [sys.executable, "run_gold.py", "--env", env]
    res = subprocess.run(cmd)
    return res.returncode == 0


def run_compaction(target_dir: str = "data/bronze", target_mb: int = 64):
    """Execute partition compaction to resolve the Small Files Problem."""
    logger.info("=" * 70)
    logger.info(f"EXECUTING PARTITION COMPACTION ({target_dir} -> {target_mb}MB target)")
    logger.info("=" * 70)
    from src.utils.compaction import compact_partitions
    comp_path = PROJECT_ROOT / target_dir
    res = compact_partitions(comp_path, target_size_mb=target_mb)
    return res["status"] if "status" in res else True


def main():
    parser = argparse.ArgumentParser(
        description="DAT535 Medallion Course Project Orchestrator (Group 13)"
    )
    parser.add_argument(
        "action",
        choices=["all", "test", "bronze", "silver", "gold", "compact"],
        default="all",
        nargs="?",
        help="Pipeline layer or action to run (default: all)",
    )
    parser.add_argument(
        "--env",
        choices=["dev", "prod"],
        default="dev",
        help="Target execution environment (default: dev)",
    )
    parser.add_argument(
        "--compact",
        action="store_true",
        help="Run partition compaction on Bronze after ingestion",
    )

    args = parser.parse_args()
    total_start = time.time()

    if args.action == "test":
        success = run_tests()
        sys.exit(0 if success else 1)

    if args.action == "compact":
        run_compaction()
        sys.exit(0)

    if args.action in ["all", "bronze"]:
        if not run_bronze(env=args.env):
            logger.error("Bronze layer failed. Halting pipeline.")
            sys.exit(1)

    if args.compact or (args.action == "all" and args.compact):
        run_compaction()

    if args.action in ["all", "silver"]:
        if not run_silver(env=args.env):
            logger.error("Silver layer failed. Halting pipeline.")
            sys.exit(1)

    if args.action in ["all", "gold"]:
        if not run_gold(env=args.env):
            logger.error("Gold layer failed. Halting pipeline.")
            sys.exit(1)

    if args.action == "all":
        logger.info("Running post-execution test verification...")
        if not run_tests():
            sys.exit(1)

    elapsed = time.time() - total_start
    logger.info(f"Medallion project run completed successfully in {elapsed:.2f} seconds.")


if __name__ == "__main__":
    main()
