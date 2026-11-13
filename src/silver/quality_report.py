"""
Silver Layer: Data Quality Evidence, Observability Tags, and Sensor Anomaly Reconciliation Ledger.
Implements the before-and-after reconciliation required by Section 4 & 6 of the DAT535 Project Guide.
Author: Group 13
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from src.silver.config import (
    QUALITY_DIR,
    SENSOR_ANOMALY_LEDGER_PATH,
    UNCLASSIFIED_RESOLUTIONS_LEDGER_PATH,
)

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
        "artifacts": {
            "sensor_anomaly_ledger": str(SENSOR_ANOMALY_LEDGER_PATH),
            "unclassified_resolutions_ledger": str(UNCLASSIFIED_RESOLUTIONS_LEDGER_PATH),
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
    b_311_conflicts = clean_311_metrics.get("status_conflict_count", 0)
    b_311_instant = clean_311_metrics.get("instant_closure_count", 0)

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
        print(f"    - Status Conflicts Tagged (Open w/ Date): {b_311_conflicts:,}")
        print(f"    - Instant Closures Tagged (0.0s SLA)    : {b_311_instant:,}")

    # Spatial Resolution Breakdown
    spatial = clean_311_metrics.get("spatial_resolution_breakdown", {})
    print("\n  Spatial Imputation Resolution Hierarchy:")
    for level, count in spatial.items():
        pct = (count / b_311_valid * 100.0) if b_311_valid > 0 else 0.0
        print(f"    - {level:<26}: {count:,} ({pct:.2f}%)")
    avg_dist = clean_311_metrics.get("avg_grid_distance_km")
    p95_dist = clean_311_metrics.get("p95_grid_distance_km")
    if avg_dist is not None:
        print(f"    - Grid Center Uncertainty Distance: Mean = {avg_dist:.2f} km | P95 = {p95_dist:.2f} km")

    # Incident Clustering & Burst Deduplication Breakdown
    clustering = clean_311_metrics.get("incident_clustering", {})
    if clustering:
        print("\n  STAGE 1B: CITIZEN CALL BURST DEDUPLICATION & INCIDENT CLUSTERING:")
        loc_recs = clustering.get("total_locatable_records", 0)
        unloc_recs = clustering.get("unlocatable_records", 0)
        dup_calls = clustering.get("duplicate_burst_calls_collapsed", 0)
        distinct_inc = clustering.get("distinct_physical_incidents", 0)
        burst_rate = clustering.get("burst_collapse_rate_pct", 0.0)
        print(f"    - Locatable Records Routed            : {loc_recs:,}")
        print(f"    - Unlocatable Records (Isolated)      : {unloc_recs:,}")
        print(f"    - Duplicate Burst Calls Collapsed     : -{dup_calls:,} ({burst_rate:.2f}%)")
        print(f"    - Distinct Physical Incidents Derived : = {distinct_inc:,}")
        routes = clustering.get("route_breakdown", {})
        if routes:
            print("    Route Distribution:")
            for r_name, r_cnt in sorted(routes.items()):
                print(f"      * {r_name:<34}: {r_cnt:,}")

    # Resolution Text Mining & Phantom Closure Auditing Breakdown
    res_mining = clean_311_metrics.get("resolution_text_mining", {})
    if res_mining:
        print("\n  STAGE 1C: UNSTRUCTURED RESOLUTION TEXT MINING & PHANTOM CLOSURES:")
        ver_cnt = res_mining.get("field_verified_remediations", 0)
        ver_rate = res_mining.get("field_verified_rate_pct", (ver_cnt / b_311_closed * 100.0) if b_311_closed > 0 else 0.0)
        phan_cnt = res_mining.get("phantom_closures", 0)
        phan_rate = res_mining.get("phantom_closure_rate_pct", (phan_cnt / b_311_closed * 100.0) if b_311_closed > 0 else 0.0)
        unclass_cnt = res_mining.get("unclassified_resolutions", 0)
        unclass_rate = res_mining.get("unclassified_rate_pct", (unclass_cnt / b_311_closed * 100.0) if b_311_closed > 0 else 0.0)

        print(f"    - True Field Remediations (Verified)  : {ver_cnt:,} ({ver_rate:.2f}% of closed)")
        print(f"    - Administrative Phantom Closures     : {phan_cnt:,} ({phan_rate:.2f}% of closed)")
        print(f"    - Unclassified Vocabulary (Cataloged) : {unclass_cnt:,} ({unclass_rate:.2f}% of closed)")
        breakdown = res_mining.get("outcome_breakdown", {})
        if breakdown:
            print("    Resolution Outcome Distribution:")
            for cat_name, cat_cnt in sorted(breakdown.items(), key=lambda x: x[1], reverse=True):
                if cat_name == "OPEN_TICKET":
                    continue
                cat_pct = (cat_cnt / b_311_closed * 100.0) if b_311_closed > 0 else 0.0
                print(f"      * {cat_name:<34}: {cat_cnt:,} ({cat_pct:.2f}%)")

    # Categorical Schema Drift & Semantic Conformance Breakdown
    # [TEMPORARILY COMMENTED OUT FOR ISOLATED VERIFICATION OF UNSTRUCTURED TEXT MINING]
    # sem_conf = clean_311_metrics.get("semantic_conformance", {})
    # if sem_conf:
    #     print("\n  STAGE 1D: CATEGORICAL SCHEMA DRIFT & CONFORMED TAXONOMY:")
    #     conf_cnt = sem_conf.get("conformed_records", 0)
    #     unmap_cnt = sem_conf.get("unmapped_records", 0)
    #     unmap_rate = sem_conf.get("unmapped_rate_pct", 0.0)
    #     legacy_cnt = sem_conf.get("legacy_mapped_records", 0)
    #     conf_rate = (conf_cnt / b_311_valid * 100.0) if b_311_valid > 0 else 0.0
    #
    #     print(f"    - Conformed to Canonical Domains      : {conf_cnt:,} ({conf_rate:.2f}%)")
    #     print(f"    - Historical Legacy Aliases Conformed : {legacy_cnt:,}")
    #     print(f"    - Unmapped Category Volume (Ledgered) : {unmap_cnt:,} ({unmap_rate:.2f}%)")
    #     if unmap_rate > MAX_UNMAPPED_TAXONOMY_RATE_PCT:
    #         print(f"    - [OBSERVABILITY ALERT] Unmapped rate ({unmap_rate}%) exceeds threshold ({MAX_UNMAPPED_TAXONOMY_RATE_PCT}%)!")
    #     else:
    #         print(f"    - [OBSERVABILITY GATE] Conformance within acceptable threshold (< {MAX_UNMAPPED_TAXONOMY_RATE_PCT}%)")
    #
    #     d_breakdown = sem_conf.get("domain_breakdown", {})
    #     if d_breakdown:
    #         print("    Canonical Service Domain Distribution:")
    #         for d_name, d_cnt in sorted(d_breakdown.items(), key=lambda x: x[1], reverse=True):
    #             d_pct = (d_cnt / b_311_valid * 100.0) if b_311_valid > 0 else 0.0
    #             print(f"      * {d_name:<34}: {d_cnt:,} ({d_pct:.2f}%)") # 

    # Weather Reconciliation
    print("\n" + "-" * 90)
    w_in = clean_weather_metrics.get("input_rows", 0)
    w_dup = clean_weather_metrics.get("duplicate_rows_dropped", 0)
    w_unkeyed = clean_weather_metrics.get("dropped_unkeyed_rows", 0)
    w_masked = clean_weather_metrics.get("corrupted_fields_masked", 0)
    w_anomalies = clean_weather_metrics.get("sensor_anomalies_logged", 0)
    w_valid = clean_weather_metrics.get("valid_rows", 0)
    w_extreme = clean_weather_metrics.get("extreme_weather_tagged_rows", 0)
    w_cells = clean_weather_metrics.get("distinct_grid_cells", 0)

    print(f"{'STAGE 2: ERA5-LAND METEOROLOGICAL REANALYSIS':<45} | Count / Metrics")
    print(f"  * Raw Bronze Weather Observations         : {w_in:,}")
    if w_dup > 0:
        print(f"  * Exact Duplicates Removed (grid_id, ts)  : -{w_dup:,}")
    print(f"  * Unkeyed Observations Dropped            : -{w_unkeyed:,}")
    print(f"  * Corrupt Fields Masked (Row Preserved)   : {w_masked:,}")
    print(f"  * Forensic Sensor Anomalies Ledgered      : {w_anomalies:,} (saved to {SENSOR_ANOMALY_LEDGER_PATH.name})")
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
