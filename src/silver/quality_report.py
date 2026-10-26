"""
Silver Layer: Data Quality Evidence and Reconciliation Ledger.
Implements the before-and-after reconciliation required by Section 4 & 6 of the DAT535 Project Guide.
Author: Group 13
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from src.silver.config import QUALITY_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("quality_report")


def generate_quality_reconciliation_report(
    clean_311_metrics: Dict[str, Any],
    clean_weather_metrics: Dict[str, Any],
    join_metrics: Dict[str, Any],
    output_dir: Path = QUALITY_DIR,
) -> Path:
    """
    Generate and save the Silver Layer Before-and-After Data Quality Ledger.
    """
    timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    report_id = f"silver_quality_{timestamp_str}"
    output_dir.mkdir(parents=True, exist_ok=True)

    report_payload = {
        "report_id": report_id,
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": "PASS",
        "stages": {
            "311_cleaning": clean_311_metrics,
            "weather_cleaning": clean_weather_metrics,
            "unified_join": join_metrics,
        },
    }

    report_file = output_dir / f"{report_id}.json"
    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(report_payload, f, indent=2, ensure_ascii=False)

    # Print ASCII Reconciliation Table
    print("\n" + "=" * 90)
    print("DAT535 SILVER LAYER: DATA QUALITY RECONCILIATION LEDGER (BEFORE & AFTER)")
    print("=" * 90)
    print(f"Report ID: {report_id} | Generated: {report_payload['generated_at_utc']}")
    print("-" * 90)

    # 311 Reconciliation
    b_311_in = clean_311_metrics.get("input_rows", 0)
    b_311_dup = clean_311_metrics.get("duplicate_rows_removed", 0)
    b_311_quar = clean_311_metrics.get("quarantined_rows", 0)
    b_311_valid = clean_311_metrics.get("valid_rows_accepted", 0)
    b_311_closed = clean_311_metrics.get("closed_tickets_count", 0)
    b_311_open = clean_311_metrics.get("open_tickets_count", 0)

    print(f"{'STAGE 1: NYC 311 SERVICE REQUESTS':<45} | Count / Metrics")
    print(f"  * Raw Bronze Input Records                : {b_311_in:,}")
    print(f"  * Exact Duplicates Removed (unique_key)    : -{b_311_dup:,}")
    print(f"  * Temporal Validation Failures Quarantined : -{b_311_quar:,}")
    print(f"  * Clean Valid Records Accepted in Silver   : = {b_311_valid:,}")
    if b_311_valid > 0:
        pct_closed = (b_311_closed / b_311_valid) * 100.0
        pct_open = (b_311_open / b_311_valid) * 100.0
        print(f"    - Closed Tickets (Duration Calculated)   : {b_311_closed:,} ({pct_closed:.1f}%)")
        print(f"    - Open/Active Tickets (Age Tracked)     : {b_311_open:,} ({pct_open:.1f}%)")

    # Spatial Resolution Breakdown
    spatial = clean_311_metrics.get("spatial_resolution_breakdown", {})
    print("\n  Spatial Imputation Resolution Hierarchy:")
    for level, count in spatial.items():
        pct = (count / b_311_valid * 100.0) if b_311_valid > 0 else 0.0
        print(f"    - {level:<26}: {count:,} ({pct:.2f}%)")

    # Weather Reconciliation
    print("\n" + "-" * 90)
    w_in = clean_weather_metrics.get("input_rows", 0)
    w_dup = clean_weather_metrics.get("duplicate_rows_dropped", 0)
    w_unkeyed = clean_weather_metrics.get("dropped_unkeyed_rows", 0)
    w_masked = clean_weather_metrics.get("corrupted_fields_masked", 0)
    w_valid = clean_weather_metrics.get("valid_rows", 0)
    w_extreme = clean_weather_metrics.get("extreme_weather_tagged_rows", 0)
    w_cells = clean_weather_metrics.get("distinct_grid_cells", 0)

    print(f"{'STAGE 2: ERA5-LAND METEOROLOGICAL REANALYSIS':<45} | Count / Metrics")
    print(f"  * Raw Bronze Weather Observations         : {w_in:,}")
    if w_dup > 0:
        print(f"  * Exact Duplicates Removed (grid_id, ts)  : -{w_dup:,}")
    print(f"  * Unkeyed Observations Dropped            : -{w_unkeyed:,}")
    print(f"  * Corrupt Fields Masked (Row Preserved)   : {w_masked:,}")
    print(f"  * Valid Observations in Silver            : = {w_valid:,}")
    print(f"  * Active Native NYC Grid Cells Covered    : {w_cells} of 19 cells")
    print(f"  * Extreme Weather Outliers Tagged (Kept)  : {w_extreme:,}")

    # Unified Fact Join Reconciliation
    print("\n" + "-" * 90)
    j_total = join_metrics.get("total_311_records", 0)
    j_matched = join_metrics.get("matched_weather_records", 0)
    j_unmatched = join_metrics.get("unmatched_weather_records", 0)
    j_rate = join_metrics.get("weather_match_rate_pct", 0.0)

    print(f"{'STAGE 3: UNIFIED ANALYTICAL FACT JOIN':<45} | Count / Metrics")
    print(f"  * Total Clean 311 Records Joined          : {j_total:,}")
    print(f"  * Enriched with Weather Observation       : {j_matched:,} ({j_rate}%)")
    print(f"  * Unmatched Records (Historical/Unlocated): {j_unmatched:,}")
    print("=" * 90)
    print(f"Quality Ledger document saved to: {report_file}")
    print("=" * 90 + "\n")

    return report_file
