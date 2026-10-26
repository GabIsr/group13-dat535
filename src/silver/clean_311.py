"""
Silver Layer: NYC 311 Service Requests Cleaning, Deduplication, and Spatial Imputation.
Applies deduplication, UTC timestamp normalization, temporal duration validation,
3-level hierarchical spatial imputation (Coordinates -> ZIP Centroid -> Borough Centroid),
and routes invalid rows to quarantine.
Author: Group 13
"""

import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.silver.config import (
    BOROUGH_CENTROIDS,
    BRONZE_DIR,
    NYC_LAT_MIN,
    NYC_LAT_MAX,
    NYC_LON_MIN,
    NYC_LON_MAX,
    PROJECT_ROOT,
    QUARANTINE_DIR,
    SILVER_DIR,
    ZIP_CENTROIDS_PATH,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("clean_311")


def load_zip_reference(zip_ref_path: Path = ZIP_CENTROIDS_PATH) -> pd.DataFrame:
    """Load the precomputed NYC ZIP centroid reference table."""
    if not zip_ref_path.exists():
        logger.warning(f"ZIP reference table not found at {zip_ref_path}. Creating fallback mapping.")
        return pd.DataFrame(columns=["zip5", "centroid_lat", "centroid_lon", "grid_id", "primary_borough"])
    return pd.read_parquet(zip_ref_path)


def to_utc_datetime(series: pd.Series, local_tz: str = "America/New_York") -> pd.Series:
    """
    Parse NYC 311 local Eastern Time timestamps and convert them accurately to UTC.
    NYC Open Data publishes 311 timestamps in America/New_York local time,
    which varies between Eastern Standard Time (UTC-5) and Eastern Daylight Time (UTC-4).
    Treating them naively as UTC induces a 4 to 5 hour temporal error, breaking joins with
    UTC weather reanalysis.
    """
    dt = pd.to_datetime(series, errors="coerce")
    if dt.dt.tz is None:
        return dt.dt.tz_localize(local_tz, ambiguous="NaT", nonexistent="shift_forward").dt.tz_convert("UTC")
    return dt.dt.tz_convert("UTC")


def clean_and_validate_311(
    bronze_311_dir: Path = BRONZE_DIR / "nyc311",
    silver_311_dir: Path = SILVER_DIR / "nyc311",
    quarantine_dir: Path = QUARANTINE_DIR / "nyc311",
) -> Dict[str, Any]:
    """
    Load Bronze 311 records, deduplicate, validate durations and timestamps,
    apply hierarchical spatial imputation, normalize strings, and save clean & quarantined Parquet tables.
    """
    start_time = time.time()
    processed_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    bronze_files = sorted(bronze_311_dir.glob("**/*.parquet"))
    if not bronze_files:
        logger.warning(f"No Bronze 311 files found in {bronze_311_dir}")
        return {"status": "skipped", "reason": "no_input_files"}

    logger.info(f"Loading {len(bronze_files)} Bronze 311 partition files...")
    df_raw = pd.concat([pd.read_parquet(f) for f in bronze_files], ignore_index=True)
    input_rows = len(df_raw)
    logger.info(f"Loaded {input_rows:,} raw 311 records from Bronze.")

    # 1. Deduplication (Group by unique_key, keep most recently ingested)
    if "_ingested_at_utc" in df_raw.columns:
        df_sorted = df_raw.sort_values("_ingested_at_utc", ascending=False)
    else:
        df_sorted = df_raw

    df_dedup = df_sorted.drop_duplicates(subset=["unique_key"], keep="first").copy()
    duplicate_rows_removed = input_rows - len(df_dedup)
    logger.info(f"Deduplication: removed {duplicate_rows_removed:,} duplicate records on unique_key.")

    # 2. Timestamp Normalization (America/New_York -> UTC) & Temporal Validation
    df_dedup["created_at_utc"] = to_utc_datetime(df_dedup["created_date"])
    df_dedup["closed_at_utc"] = to_utc_datetime(df_dedup.get("closed_date")) if "closed_date" in df_dedup.columns else pd.NaT

    # Open vs. Closed Ticket Semantics
    df_dedup["is_ticket_closed"] = df_dedup["closed_at_utc"].notna()
    df_dedup["ticket_status_category"] = np.where(df_dedup["is_ticket_closed"], "CLOSED", "OPEN")

    # Compute resolution duration in hours (only applicable to closed tickets)
    duration_secs = (df_dedup["closed_at_utc"] - df_dedup["created_at_utc"]).dt.total_seconds()
    df_dedup["resolution_duration_hours"] = np.where(
        df_dedup["is_ticket_closed"],
        (duration_secs / 3600.0).round(2),
        np.nan,
    )

    # For open tickets, compute active queue age in days relative to UTC run time
    now_utc = pd.Timestamp.now(tz=timezone.utc)
    open_age_secs = (now_utc - df_dedup["created_at_utc"]).dt.total_seconds()
    df_dedup["open_ticket_age_days"] = np.where(
        ~df_dedup["is_ticket_closed"],
        (open_age_secs / 86400.0).round(2),
        np.nan,
    )

    # Hourly join key for UTC weather alignment
    df_dedup["weather_join_hour_utc"] = df_dedup["created_at_utc"].dt.floor("h")

    # Calendar row attributes (Derived from local New York time for human behavioral patterns)
    created_local = df_dedup["created_at_utc"].dt.tz_convert("America/New_York")
    df_dedup["hour_of_day"] = df_dedup["created_at_utc"].dt.hour  # UTC hour
    df_dedup["created_hour_local"] = created_local.dt.hour         # Local 0-23 hour
    df_dedup["day_of_week"] = created_local.dt.dayofweek          # 0=Monday, 6=Sunday
    df_dedup["is_weekend"] = df_dedup["day_of_week"].isin([5, 6])
    df_dedup["month_of_year"] = created_local.dt.month

    # Check for invalid records:
    # 1. Missing created_at timestamp
    # 2. Negative duration for closed tickets (closed before created)
    cond_missing_created = df_dedup["created_at_utc"].isna()
    cond_negative_duration = df_dedup["is_ticket_closed"] & (df_dedup["resolution_duration_hours"] < 0.0)

    quarantine_mask = cond_missing_created | cond_negative_duration
    df_quarantined = df_dedup[quarantine_mask].copy()

    if len(df_quarantined) > 0:
        df_quarantined["quarantine_reason"] = np.where(
            df_quarantined["created_at_utc"].isna(),
            "MISSING_CREATED_TIMESTAMP",
            "NEGATIVE_RESOLUTION_DURATION",
        )
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        q_path = quarantine_dir / "quarantined_311.parquet"
        pq.write_table(pa.Table.from_pandas(df_quarantined, preserve_index=False), q_path, compression="snappy")
        logger.warning(f"Quarantined {len(df_quarantined):,} invalid records to {q_path}")

    # Retain only valid records for clean Silver
    df_valid = df_dedup[~quarantine_mask].copy()

    # 3. Hierarchical Spatial Imputation (3 Levels)
    # Parse coordinates to numeric
    raw_lats = pd.to_numeric(df_valid["latitude"], errors="coerce")
    raw_lons = pd.to_numeric(df_valid["longitude"], errors="coerce")

    # Mask for Level 1: Valid Native GPS Coordinates within NYC bounds
    mask_level1 = (
        raw_lats.between(NYC_LAT_MIN, NYC_LAT_MAX) &
        raw_lons.between(NYC_LON_MIN, NYC_LON_MAX)
    )

    # Initialize spatial columns
    df_valid["clean_latitude"] = np.nan
    df_valid["clean_longitude"] = np.nan
    df_valid["grid_id"] = "UNKNOWN"
    df_valid["spatial_resolution_level"] = "UNLOCATED"

    # Apply Level 1: Exact GPS coordinates
    df_valid.loc[mask_level1, "clean_latitude"] = raw_lats[mask_level1].round(6)
    df_valid.loc[mask_level1, "clean_longitude"] = raw_lons[mask_level1].round(6)
    grid_lat_1 = raw_lats[mask_level1].round(1).astype(str)
    grid_lon_1 = raw_lons[mask_level1].round(1).astype(str)
    df_valid.loc[mask_level1, "grid_id"] = "GRID_" + grid_lat_1 + "_" + grid_lon_1
    df_valid.loc[mask_level1, "spatial_resolution_level"] = "EXACT_COORDINATES"

    # Level 2: ZIP Code Centroid Fallback
    mask_need_level2 = ~mask_level1
    zip_ref = load_zip_reference()

    if not zip_ref.empty and "incident_zip" in df_valid.columns:
        # Extract 5-digit ZIP
        zip_series = df_valid.loc[mask_need_level2, "incident_zip"].astype(str).str.extract(r"(\d{5})")[0]
        zip_map_lat = zip_ref.set_index("zip5")["centroid_lat"].to_dict()
        zip_map_lon = zip_ref.set_index("zip5")["centroid_lon"].to_dict()
        zip_map_grid = zip_ref.set_index("zip5")["grid_id"].to_dict()

        level2_lats = zip_series.map(zip_map_lat)
        mask_level2_matched = level2_lats.notna()

        matched_indices = level2_lats[mask_level2_matched].index
        df_valid.loc[matched_indices, "clean_latitude"] = level2_lats[mask_level2_matched]
        df_valid.loc[matched_indices, "clean_longitude"] = zip_series[mask_level2_matched].map(zip_map_lon)
        df_valid.loc[matched_indices, "grid_id"] = zip_series[mask_level2_matched].map(zip_map_grid)
        df_valid.loc[matched_indices, "spatial_resolution_level"] = "ZIP_APPROXIMATION"

    # Level 3: Borough Centroid Fallback
    mask_need_level3 = df_valid["spatial_resolution_level"] == "UNLOCATED"
    borough_series = df_valid.loc[mask_need_level3, "borough"].astype(str).str.strip().str.upper()

    for borough_name, (b_lat, b_lon, b_grid) in BOROUGH_CENTROIDS.items():
        b_mask = borough_series == borough_name
        b_indices = borough_series[b_mask].index
        df_valid.loc[b_indices, "clean_latitude"] = b_lat
        df_valid.loc[b_indices, "clean_longitude"] = b_lon
        df_valid.loc[b_indices, "grid_id"] = b_grid
        df_valid.loc[b_indices, "spatial_resolution_level"] = "BOROUGH_APPROXIMATION"

    # Spatial resolution statistics
    spatial_stats = df_valid["spatial_resolution_level"].value_counts().to_dict()
    logger.info(f"Spatial imputation summary: {spatial_stats}")

    # 4. String Standardizations
    if "borough" in df_valid.columns:
        df_valid["borough"] = df_valid["borough"].astype(str).str.strip().str.upper().fillna("UNSPECIFIED")
    if "complaint_type" in df_valid.columns:
        df_valid["complaint_type"] = df_valid["complaint_type"].astype(str).str.strip().str.title().fillna("Unknown")
    if "descriptor" in df_valid.columns:
        df_valid["descriptor"] = df_valid["descriptor"].astype(str).str.strip().fillna("Not Stated")
    if "status" in df_valid.columns:
        df_valid["status"] = df_valid["status"].astype(str).str.strip().str.upper().fillna("UNKNOWN")
    if "agency" in df_valid.columns:
        df_valid["agency"] = df_valid["agency"].astype(str).str.strip().str.upper().fillna("UNKNOWN")

    # Silver Audit Metadata
    df_valid["_silver_processed_at_utc"] = processed_at_utc

    # 5. Partitioned Parquet Write: year=YYYY / month=MM / clean_311.parquet
    silver_311_dir.mkdir(parents=True, exist_ok=True)
    df_valid["_partition_year"] = df_valid["created_at_utc"].dt.strftime("%Y")
    df_valid["_partition_month"] = df_valid["created_at_utc"].dt.strftime("%m")

    written_files = 0
    for (year, month), sub_df in df_valid.groupby(["_partition_year", "_partition_month"]):
        partition_dir = silver_311_dir / f"year={year}" / f"month={month}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        out_path = partition_dir / "clean_311.parquet"
        save_df = sub_df.drop(columns=["_partition_year", "_partition_month"])

        table = pa.Table.from_pandas(save_df, preserve_index=False)
        pq.write_table(table, out_path, compression="snappy")
        written_files += 1

    elapsed = time.time() - start_time
    logger.info(
        f"Completed Silver 311 processing: {len(df_valid):,} valid records | "
        f"{len(df_quarantined):,} quarantined | {written_files} partition files written | {elapsed:.2f}s"
    )

    closed_count = int(df_valid["is_ticket_closed"].sum()) if "is_ticket_closed" in df_valid.columns else 0
    open_count = len(df_valid) - closed_count

    return {
        "dataset": "silver_311",
        "input_rows": input_rows,
        "duplicate_rows_removed": duplicate_rows_removed,
        "valid_rows_accepted": len(df_valid),
        "closed_tickets_count": closed_count,
        "open_tickets_count": open_count,
        "quarantined_rows": len(df_quarantined),
        "spatial_resolution_breakdown": spatial_stats,
        "partitions_written": written_files,
        "duration_seconds": round(elapsed, 2),
        "status": "success",
    }


if __name__ == "__main__":
    clean_and_validate_311()
