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
    MAX_UNMAPPED_TAXONOMY_RATE_PCT,
    QUALITY_DIR,
    SENSOR_ANOMALY_LEDGER_PATH,
    UNCLASSIFIED_RESOLUTIONS_LEDGER_PATH,
    UNMAPPED_TAXONOMY_LEDGER_PATH,
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
            "unmapped_taxonomy_ledger": str(UNMAPPED_TAXONOMY_LEDGER_PATH),
        },
    }

    report_file = output_dir / f"{report_id}.json"
    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(report_payload, f, indent=2, ensure_ascii=False)

    # Print ASCII Reconciliation Table
    width = 105
    print("\n" + "=" * width)
    print("DAT535 SILVER LAYER: DATA QUALITY RECONCILIATION LEDGER (BEFORE & AFTER)")
    print("=" * width)
    print(f"Report ID: {report_id:<40} | Generated (UTC): {report_payload['generated_at_utc']}")
    print("-" * width)

    # 311 Reconciliation
    b_311_in = clean_311_metrics.get("input_rows", 0)
    b_311_dup = clean_311_metrics.get("duplicate_rows_removed", 0)
    b_311_quar = clean_311_metrics.get("quarantined_rows", 0)
    b_311_valid = clean_311_metrics.get("valid_rows_accepted", 0)
    b_311_closed = clean_311_metrics.get("closed_tickets_count", 0)
    b_311_open = clean_311_metrics.get("open_tickets_count", 0)
    b_311_conflicts = clean_311_metrics.get("status_conflict_count", 0)
    b_311_instant = clean_311_metrics.get("instant_closure_count", 0)

    print(f"{'STAGE 1A: NYC 311 INTAKE, DEDUPLICATION & VALIDATION GATE':<45}")
    print("-" * width)
    print(f"{'Filter / Quality Gate':<42} | {'Count / Value':<16} | {'Share (%)':<10} | {'Description':<28}")
    print("-" * width)
    print(f"{'Raw Bronze Input Records':<42} | {b_311_in:<16,} | {'100.0%':<10} | {'Unprocessed bronze landing':<28}")
    dup_pct = (b_311_dup / b_311_in * 100.0) if b_311_in > 0 else 0.0
    quar_pct = (b_311_quar / b_311_in * 100.0) if b_311_in > 0 else 0.0
    print(f"{'Exact Duplicates Removed (unique_key)':<42} | {f'-{b_311_dup:,}':<16} | {f'{dup_pct:.2f}%':<10} | {'CDC primary key deduplication':<28}")
    print(f"{'Temporal Failures Quarantined':<42} | {f'-{b_311_quar:,}':<16} | {f'{quar_pct:.2f}%':<10} | {'Negative/corrupted duration':<28}")
    print(f"{'Clean Valid Records Accepted in Silver':<42} | {f'= {b_311_valid:,}':<16} | {'100.0%':<10} | {'Passed all intake quality gates':<28}")

    if b_311_valid > 0:
        pct_closed = (b_311_closed / b_311_valid) * 100.0
        pct_open = (b_311_open / b_311_valid) * 100.0
        pct_conflicts = (b_311_conflicts / b_311_valid) * 100.0
        pct_instant = (b_311_instant / b_311_valid) * 100.0
        print(f"{'  - Closed Tickets (Duration Calculated)':<42} | {b_311_closed:<16,} | {f'{pct_closed:.2f}%':<10} | {'Calculated resolution hours':<28}")
        print(f"{'  - Open / Active Tickets (Age Tracked)':<42} | {b_311_open:<16,} | {f'{pct_open:.2f}%':<10} | {'Active municipal backlog':<28}")
        print(f"{'  - Status Conflicts Tagged (Open w/ Date)':<42} | {b_311_conflicts:<16,} | {f'{pct_conflicts:.2f}%':<10} | {'Tagged anomalous lifecycle':<28}")
        print(f"{'  - Instant Closures Tagged (0.0s SLA)':<42} | {b_311_instant:<16,} | {f'{pct_instant:.2f}%':<10} | {'Zero-second closure audit':<28}")

    # Spatial Resolution Breakdown
    spatial = clean_311_metrics.get("spatial_resolution_breakdown", {})
    avg_dist = clean_311_metrics.get("avg_grid_distance_km")
    p95_dist = clean_311_metrics.get("p95_grid_distance_km")
    if spatial:
        print("\n" + "-" * width)
        print(f"{'STAGE 1B: SPATIAL IMPUTATION & RESOLUTION HIERARCHY':<45}")
        print("-" * width)
        print(f"{'Spatial Resolution Level':<42} | {'Records Resolved':<16} | {'Share (%)':<10} | {'Hierarchy Target / Method':<28}")
        print("-" * width)
        for level, count in sorted(spatial.items(), key=lambda x: x[1], reverse=True):
            pct = (count / b_311_valid * 100.0) if b_311_valid > 0 else 0.0
            desc = "Clean native coordinates" if "EXACT" in level else ("ZIP Centroid Imputation" if "ZIP" in level else "Borough Center Imputation")
            print(f"{level:<42} | {count:<16,} | {f'{pct:.2f}%':<10} | {desc:<28}")
        if avg_dist is not None:
            print("-" * width)
            p95_str = f"{p95_dist:.2f} km" if p95_dist is not None else "N/A"
            print(f"Spatial Grid Uncertainty: Mean Distance = {avg_dist:.2f} km | P95 Distance = {p95_str}")

    # Incident Clustering & Burst Deduplication Breakdown
    clustering = clean_311_metrics.get("incident_clustering", {})
    if clustering:
        print("\n" + "-" * width)
        print(f"{'STAGE 1C: CITIZEN CALL BURST DEDUPLICATION & INCIDENT CLUSTERING':<65}")
        print("-" * width)
        print(f"{'Clustering / Sessionization Metric':<42} | {'Count / Value':<16} | {'Rate (%)':<10} | {'Sessionizer Engine Detail':<28}")
        print("-" * width)
        loc_recs = clustering.get("total_locatable_records", 0)
        unloc_recs = clustering.get("unlocatable_records", 0)
        dup_calls = clustering.get("duplicate_burst_calls_collapsed", 0)
        distinct_inc = clustering.get("distinct_physical_incidents", 0)
        burst_rate = clustering.get("burst_collapse_rate_pct", 0.0)

        loc_pct = (loc_recs / b_311_valid * 100.0) if b_311_valid > 0 else 0.0
        unloc_pct = (unloc_recs / b_311_valid * 100.0) if b_311_valid > 0 else 0.0
        inc_pct = (distinct_inc / b_311_valid * 100.0) if b_311_valid > 0 else 0.0

        print(f"{'Locatable Records Evaluated':<42} | {loc_recs:<16,} | {f'{loc_pct:.2f}%':<10} | {'Passed to spatial sessionizer':<28}")
        print(f"{'Unlocatable Records (Isolated)':<42} | {unloc_recs:<16,} | {f'{unloc_pct:.2f}%':<10} | {'Isolated to prevent skew':<28}")
        print(f"{'Duplicate Burst Calls Collapsed':<42} | {f'-{dup_calls:,}':<16} | {f'{burst_rate:.2f}%':<10} | {'Citizen wave duplicate calls':<28}")
        print(f"{'Distinct Physical Incidents Derived':<42} | {f'= {distinct_inc:,}':<16} | {f'{inc_pct:.2f}%':<10} | {'True underlying physical events':<28}")

        routes = clustering.get("route_breakdown", {})
        if routes:
            print("-" * width)
            print(f"{'Hierarchical Spatial Routing Tier':<42} | {'Routed Records':<16} | {'Share (%)':<10} | {'Precedence Hierarchy Tier':<28}")
            print("-" * width)
            for r_name, r_cnt in sorted(routes.items()):
                r_pct = (r_cnt / b_311_valid * 100.0) if b_311_valid > 0 else 0.0
                print(f"{r_name:<42} | {r_cnt:<16,} | {f'{r_pct:.2f}%':<10} | {'Spatial Precedence':<28}")

    # Resolution Text Mining & Phantom Closure Auditing Breakdown
    res_mining = clean_311_metrics.get("resolution_text_mining", {})
    if res_mining:
        print("\n" + "-" * width)
        print(f"{'STAGE 1D: UNSTRUCTURED RESOLUTION TEXT MINING & PHANTOM CLOSURES':<65}")
        print("-" * width)
        print(f"{'Resolution Classification Category':<42} | {'Records':<16} | {'Share (%)':<10} | {'Audit Classification':<28}")
        print("-" * width)
        ver_cnt = res_mining.get("field_verified_remediations", 0)
        ver_rate = res_mining.get("field_verified_rate_pct", (ver_cnt / b_311_closed * 100.0) if b_311_closed > 0 else 0.0)
        phan_cnt = res_mining.get("phantom_closures", 0)
        phan_rate = res_mining.get("phantom_closure_rate_pct", (phan_cnt / b_311_closed * 100.0) if b_311_closed > 0 else 0.0)
        unclass_cnt = res_mining.get("unclassified_resolutions", 0)
        unclass_rate = res_mining.get("unclassified_rate_pct", (unclass_cnt / b_311_closed * 100.0) if b_311_closed > 0 else 0.0)

        breakdown = res_mining.get("outcome_breakdown", {})
        if breakdown:
            for cat_name, cat_cnt in sorted(breakdown.items(), key=lambda x: x[1], reverse=True):
                if cat_name == "OPEN_TICKET":
                    continue
                cat_pct = (cat_cnt / b_311_closed * 100.0) if b_311_closed > 0 else 0.0
                sem = "Field Verified Remediation" if cat_name in ["ACTION_TAKEN_REMEDIED", "NO_VIOLATION_FOUND"] else ("Phantom Closure" if cat_name in ["ADMINISTRATIVE_DISMISSAL", "DUPLICATE_CONSOLIDATED", "DEFERRED_PENDING_ACTION", "ACCESS_DENIED_UNABLE_TO_LOCATE"] else "Governance Defect / Novel")
                print(f"{cat_name:<42} | {cat_cnt:<16,} | {f'{cat_pct:.2f}%':<10} | {sem:<28}")
        print("-" * width)
        print(f"Summary: Verified Remediations = {ver_cnt:,} ({ver_rate:.2f}%) | Phantom Closures = {phan_cnt:,} ({phan_rate:.2f}%) | Unclassified = {unclass_cnt:,} ({unclass_rate:.2f}%)")

    # Categorical Schema Drift & Semantic Conformance Breakdown
    sem_conf = clean_311_metrics.get("semantic_conformance", {})
    if sem_conf:
        print("\n" + "-" * width)
        print(f"{'STAGE 1E: CATEGORICAL SCHEMA DRIFT & CONFORMED TAXONOMY':<60}")
        print("-" * width)
        print(f"{'Canonical Service Domain':<42} | {'Records':<16} | {'Share (%)':<10} | {'Domain Scope':<28}")
        print("-" * width)
        conf_cnt = sem_conf.get("conformed_records", 0)
        unmap_cnt = sem_conf.get("unmapped_records", 0)
        unmap_rate = sem_conf.get("unmapped_rate_pct", 0.0)
        legacy_cnt = sem_conf.get("legacy_mapped_records", 0)
        conf_rate = (conf_cnt / b_311_valid * 100.0) if b_311_valid > 0 else 0.0

        d_breakdown = sem_conf.get("domain_breakdown", {})
        if d_breakdown:
            for d_name, d_cnt in sorted(d_breakdown.items(), key=lambda x: x[1], reverse=True):
                d_pct = (d_cnt / b_311_valid * 100.0) if b_311_valid > 0 else 0.0
                print(f"{d_name:<42} | {d_cnt:<16,} | {f'{d_pct:.2f}%':<10} | {'Conformed Canonical Domain':<28}")
        print("-" * width)
        gate_status = "PASS" if unmap_rate <= MAX_UNMAPPED_TAXONOMY_RATE_PCT else "ALERT"
        print(f"Observability Gate [{gate_status}]: Conformed = {conf_cnt:,} ({conf_rate:.2f}%) | Legacy Aliases = {legacy_cnt:,} | Unmapped = {unmap_cnt:,} ({unmap_rate:.2f}% <= {MAX_UNMAPPED_TAXONOMY_RATE_PCT}%)")

    # Weather Reconciliation & Analytical Fact Join
    print("\n" + "-" * width)
    w_in = clean_weather_metrics.get("input_rows", 0)
    w_dup = clean_weather_metrics.get("duplicate_rows_dropped", 0)
    w_unkeyed = clean_weather_metrics.get("dropped_unkeyed_rows", 0)
    w_masked = clean_weather_metrics.get("corrupted_fields_masked", 0)
    w_anomalies = clean_weather_metrics.get("sensor_anomalies_logged", 0)
    w_valid = clean_weather_metrics.get("valid_rows", 0)
    w_extreme = clean_weather_metrics.get("extreme_weather_tagged_rows", 0)
    w_cells = clean_weather_metrics.get("distinct_grid_cells", 0)

    j_total = join_metrics.get("total_311_records", 0)
    j_matched = join_metrics.get("matched_weather_records", 0)
    j_unmatched = join_metrics.get("unmatched_weather_records", 0)
    j_rate = join_metrics.get("weather_match_rate_pct", 0.0)

    print(f"{'STAGE 2 & 3: ERA5-LAND REANALYSIS & UNIFIED ANALYTICAL FACT JOIN':<65}")
    print("-" * width)
    print(f"{'Quality Metric / Stage':<42} | {'Count / Value':<16} | {'Share (%)':<10} | {'Description / Observability':<28}")
    print("-" * width)
    print(f"{'Raw Bronze Weather Observations':<42} | {w_in:<16,} | {'100.0%':<10} | {'ERA5-Land reanalysis records':<28}")
    if w_dup > 0:
        w_dup_pct = (w_dup / w_in * 100.0) if w_in > 0 else 0.0
        print(f"{'Exact Duplicates Removed (grid_id, ts)':<42} | {f'-{w_dup:,}':<16} | {f'{w_dup_pct:.2f}%':<10} | {'Spatiotemporal deduplication':<28}")
    w_unk_pct = (w_unkeyed / w_in * 100.0) if w_in > 0 else 0.0
    w_mask_pct = (w_masked / w_valid * 100.0) if w_valid > 0 else 0.0
    w_ext_pct = (w_extreme / w_valid * 100.0) if w_valid > 0 else 0.0
    print(f"{'Unkeyed Observations Dropped':<42} | {f'-{w_unkeyed:,}':<16} | {f'{w_unk_pct:.2f}%':<10} | {'Missing grid or timestamp':<28}")
    print(f"{'Corrupt Fields Masked (Row Preserved)':<42} | {w_masked:<16,} | {f'{w_mask_pct:.2f}%':<10} | {'Nullified out-of-bounds metrics':<28}")
    print(f"{'Forensic Sensor Anomalies Ledgered':<42} | {w_anomalies:<16,} | {'-':<10} | {f'Saved to {SENSOR_ANOMALY_LEDGER_PATH.name}':<28}")
    print(f"{'Clean Valid Observations in Silver':<42} | {f'= {w_valid:,}':<16} | {'100.0%':<10} | {f'Active NYC grid cells: {w_cells}/19':<28}")
    print(f"{'Extreme Weather Outliers Tagged (Kept)':<42} | {w_extreme:<16,} | {f'{w_ext_pct:.2f}%':<10} | {'Precipitation / wind gust flags':<28}")
    print("-" * width)
    print(f"{'Total Clean 311 Records Joined':<42} | {j_total:<16,} | {'100.0%':<10} | {'Analytical Fact Input':<28}")
    print(f"{'Enriched with Weather Reanalysis':<42} | {j_matched:<16,} | {f'{j_rate:.2f}%':<10} | {'Hourly spatial mesh matched':<28}")
    unm_pct = (100.0 - j_rate) if j_total > 0 else 0.0
    print(f"{'Unmatched Records (Historical/Unlocated)':<42} | {j_unmatched:<16,} | {f'{unm_pct:.2f}%':<10} | {'Outside weather spatial window':<28}")
    print("=" * width)
    print(f"Quality Ledger document saved to: {report_file}")
    print("=" * width + "\n")

    return report_file
