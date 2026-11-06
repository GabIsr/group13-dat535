"""
DAT535 CI/CD Observability & GitHub Actions Summary Publisher.
Reads the latest Silver Quality Reconciliation Ledger and Gold Marts,
and generates an executive markdown scorecard for $GITHUB_STEP_SUMMARY.
Author: Group 13
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

# Ensure UTF-8 output across Windows and Linux CI environments
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("publish_ci_summary")


def find_latest_quality_report(quality_dir: Path) -> Optional[Path]:
    """Find the most recently generated silver_quality_*.json report."""
    if not quality_dir.exists():
        return None
    candidates = sorted(quality_dir.glob("silver_quality_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def generate_markdown_summary(quality_json: Dict[str, Any], gold_dir: Optional[Path] = None) -> str:
    """Build a GitHub Flavored Markdown summary dashboard from quality metadata."""
    report_id = quality_json.get("report_id", "N/A")
    generated_at = quality_json.get("generated_at_utc", "N/A")
    status = quality_json.get("status", "PASS")

    stages = quality_json.get("stages", {})
    m_311 = stages.get("311_cleaning", {})
    m_wth = stages.get("weather_cleaning", {})
    m_join = stages.get("unified_join", {})

    md: list = []

    # Title & Executive Header
    md.append("# [DAT535] Medallion Pipeline Execution Summary")
    md.append(f"**Report ID:** `{report_id}` | **Status:** `{status}` | **Timestamp (UTC):** `{generated_at}`")
    md.append("")

    # Section 1: Citizen Call Burst Deduplication & Incident Clustering
    clustering = m_311.get("incident_clustering", {})
    if clustering:
        loc_recs = clustering.get("total_locatable_records", 0)
        unloc_recs = clustering.get("unlocatable_records", 0)
        dup_calls = clustering.get("duplicate_burst_calls_collapsed", 0)
        distinct_inc = clustering.get("distinct_physical_incidents", 0)
        rate = clustering.get("burst_collapse_rate_pct", 0.0)

        md.append("## Citizen Call Burst Deduplication & Incident Clustering")
        md.append("| Metric | Count | Description |")
        md.append("| :--- | :--- | :--- |")
        md.append(f"| **Locatable Records Routed** | `{loc_recs:,}` | Successfully mapped to spatial coordinate hierarchy |")
        md.append(f"| **Unlocatable Records (Isolated)** | `{unloc_recs:,}` | Preserved intact with isolated incident clusters |")
        md.append(f"| **Duplicate Burst Calls Collapsed** | `{dup_calls:,}` | Citizen wave calls clustered into active incident window |")
        md.append(f"| **Burst Call Collapse Rate** | **`{rate:.2f}%`** | Share of raw calls identified as duplicate burst reports |")
        md.append(f"| **Distinct Physical Incidents Derived** | **`{distinct_inc:,}`** | True underlying municipal service events |")
        md.append("")

        routes = clustering.get("route_breakdown", {})
        if routes:
            md.append("### Hierarchical Spatial Routing Distribution")
            md.append("| Spatial Routing Tier | Records Routed |")
            md.append("| :--- | :--- |")
            for r_tier, count in sorted(routes.items()):
                md.append(f"| `{r_tier}` | `{count:,}` |")
            md.append("")

    # Section 2: NYC 311 Bronze -> Silver Data Quality
    in_311 = m_311.get("input_rows", 0)
    dup_311 = m_311.get("duplicate_rows_removed", 0)
    quar_311 = m_311.get("quarantined_rows", 0)
    valid_311 = m_311.get("valid_rows_accepted", 0)
    closed_311 = m_311.get("closed_tickets_count", 0)
    open_311 = m_311.get("open_tickets_count", 0)
    instant_311 = m_311.get("instant_closure_count", 0)
    conflicts_311 = m_311.get("status_conflict_count", 0)

    md.append("## Stage 1: NYC 311 Data Quality & Reconciliation")
    md.append("| Quality Filter / Metric | Value | Description |")
    md.append("| :--- | :--- | :--- |")
    md.append(f"| **Bronze Input Records** | `{in_311:,}` | Raw 311 requests evaluated |")
    md.append(f"| **Exact Duplicates Removed** | `-{dup_311:,}` | Primary key (`unique_key`) deduplication |")
    md.append(f"| **Temporal Failures Quarantined** | `-{quar_311:,}` | Negative durations quarantined to isolated storage |")
    md.append(f"| **Clean Silver Records Accepted** | **`{valid_311:,}`** | Verified records admitted to Silver fact table |")
    md.append(f"| Closed Tickets (Duration Calculated) | `{closed_311:,}` | Resolution duration computed |")
    md.append(f"| Open / Active Tickets (Aging Tracked) | `{open_311:,}` | Open ticket age tracked in days |")
    md.append(f"| Instant Closures (0.0s SLA) | `{instant_311:,}` | Tickets closed at instant of creation |")
    md.append(f"| Status Contradictions Tagged | `{conflicts_311:,}` | Open status with closed date or vice-versa |")
    md.append("")

    # Section 3: Weather Reanalysis Quality
    w_in = m_wth.get("input_rows", 0)
    w_dup = m_wth.get("duplicate_rows_dropped", 0)
    w_anom = m_wth.get("sensor_anomalies_logged", 0)
    w_valid = m_wth.get("valid_rows", 0)
    w_extreme = m_wth.get("extreme_weather_tagged_rows", 0)
    w_cells = m_wth.get("distinct_grid_cells", 0)

    md.append("## Stage 2: ERA5-Land Weather Observations Quality")
    md.append("| Metric | Value | Description |")
    md.append("| :--- | :--- | :--- |")
    md.append(f"| **Bronze Observations Ingested** | `{w_in:,}` | Hourly reanalysis data points across NYC grid |")
    if w_dup > 0:
        md.append(f"| Exact Duplicates Removed | `-{w_dup:,}` | Duplicate (grid_id, timestamp) pairs dropped |")
    md.append(f"| Sensor Anomalies Ledgered | `{w_anom:,}` | Physical violation outliers quarantined to ledger |")
    md.append(f"| **Clean Observations Accepted** | **`{w_valid:,}`** | Admitted Silver weather rows |")
    md.append(f"| Active NYC Grid Cells | `{w_cells} of 19` | Native ERA5-Land 0.1° grid coverage |")
    md.append(f"| Extreme Weather Outliers Tagged | `{w_extreme:,}` | Severe heat/cold/wind preserved intact with audit tag |")
    md.append("")

    # Section 4: Unified Fact Join
    j_total = m_join.get("total_311_records", 0)
    j_matched = m_join.get("matched_weather_records", 0)
    j_rate = m_join.get("weather_match_rate_pct", 0.0)

    md.append("## Stage 3: Unified Analytical Fact Join")
    md.append("| Metric | Value | Description |")
    md.append("| :--- | :--- | :--- |")
    md.append(f"| **Total 311 Records Joined** | `{j_total:,}` | Clean 311 records evaluated |")
    md.append(f"| **Weather Match Rate** | **`{j_rate}%`** | (`{j_matched:,}` enriched with hourly weather observation) |")
    md.append("")

    # Section 5: Gold Analytical Marts Summary
    if gold_dir and gold_dir.exists():
        md.append("## Gold Layer Business Analytical Marts")
        md.append("| Mart Name | Status |")
        md.append("| :--- | :--- |")
        for mart_file in sorted(gold_dir.glob("*.parquet")):
            md.append(f"| `{mart_file.name}` | Completed (Native Spark Parquet) |")
        md.append("")

    return "\n".join(md)


def main():
    parser = argparse.ArgumentParser(description="Publish DAT535 Data Quality Scorecard to GITHUB_STEP_SUMMARY")
    parser.add_argument(
        "--quality-dir",
        type=Path,
        default=PROJECT_ROOT / "data" / "metadata" / "quality",
        help="Path to directory containing silver_quality_*.json reports",
    )
    parser.add_argument(
        "--gold-dir",
        type=Path,
        default=PROJECT_ROOT / "data" / "gold",
        help="Path to directory containing gold marts",
    )
    parser.add_argument(
        "--output-file",
        type=Path,
        help="Optional destination file (defaults to GITHUB_STEP_SUMMARY env var if set)",
    )
    args = parser.parse_args()

    latest_report = find_latest_quality_report(args.quality_dir)
    if not latest_report:
        logger.warning(f"No quality report found in {args.quality_dir}. Skipping summary publishing.")
        return

    logger.info(f"Loading quality ledger: {latest_report}")
    with open(latest_report, "r", encoding="utf-8") as f:
        data = json.load(f)

    md_content = generate_markdown_summary(data, gold_dir=args.gold_dir)

    # Determine destination
    target_summary = args.output_file or os.environ.get("GITHUB_STEP_SUMMARY")
    if target_summary:
        summary_path = Path(target_summary)
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write("\n" + md_content + "\n")
        logger.info(f"Successfully published scorecard to {summary_path}")
    else:
        logger.info("GITHUB_STEP_SUMMARY not set. Printing summary to stdout:\n")
        print(md_content)


if __name__ == "__main__":
    main()
