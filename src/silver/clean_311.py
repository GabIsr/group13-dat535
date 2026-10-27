"""
Silver Layer: NYC 311 Service Requests Cleaning, Deduplication, and Spatial Imputation.
Applies deduplication, UTC timestamp normalization, temporal duration validation,
3-level hierarchical spatial imputation (Coordinates -> ZIP Centroid -> Borough Centroid),
comprehensive institutional & spatial consistency checks, and routes invalid rows to quarantine.
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
    KNOWN_311_STATUSES,
    KNOWN_NYC_AGENCIES,
    NYC_311_LAUNCH_DATE,
    NYC_COUNCIL_DISTRICT_MAX,
    NYC_COUNCIL_DISTRICT_MIN,
    NYC_LAT_MAX,
    NYC_LAT_MIN,
    NYC_LON_MAX,
    NYC_LON_MIN,
    NYC_METRO_ZIP_PREFIXES,
    NYC_STATE_PLANE_BOUNDS,
    PROJECT_ROOT,
    QUARANTINE_DIR,
    SILVER_DIR,
    VALID_NYC_BOROUGHS,
    VALID_OPEN_DATA_CHANNELS,
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


def standardize_string_series(
    series: pd.Series,
    default_val: str,
    case: str = "upper",
) -> pd.Series:
    """
    Standardize text columns safely by:
    1. Detecting nulls, empty strings, whitespace, and literal stringified nulls ('nan', 'none', 'null').
    2. Imputing with default_val BEFORE or without converting missing values into literal 'NAN'/'None' strings.
    3. Applying string case formatting ('upper', 'title', 'preserve').
    """
    s = series.astype(object)
    s_str = s.astype(str).str.strip()
    mask_null = s.isna() | s_str.isin(["", "nan", "NaN", "None", "NONE", "NAN", "null", "NULL", "<NA>"])
    clean_s = s_str.where(~mask_null, default_val)
    if case == "upper":
        return clean_s.str.upper()
    elif case == "title":
        return clean_s.str.title()
    return clean_s


def haversine_distance_km(
    lat1: pd.Series,
    lon1: pd.Series,
    lat2: pd.Series,
    lon2: pd.Series,
) -> pd.Series:
    """
    Compute vectorized Great-Circle Haversine distance in kilometers between two coordinate Series.
    Mean Earth radius R = 6,371.0 km.
    Returns NaN for any coordinate pair with missing components.
    """
    valid_mask = lat1.notna() & lon1.notna() & lat2.notna() & lon2.notna()
    dist = pd.Series(np.nan, index=lat1.index, dtype="float64")

    if not valid_mask.any():
        return dist

    r_lat1 = np.radians(lat1[valid_mask].astype(float))
    r_lon1 = np.radians(lon1[valid_mask].astype(float))
    r_lat2 = np.radians(lat2[valid_mask].astype(float))
    r_lon2 = np.radians(lon2[valid_mask].astype(float))

    dlat = r_lat2 - r_lat1
    dlon = r_lon2 - r_lon1

    a = np.sin(dlat / 2.0) ** 2 + np.cos(r_lat1) * np.cos(r_lat2) * np.sin(dlon / 2.0) ** 2
    a = np.clip(a, 0.0, 1.0)
    c = 2.0 * np.arcsin(np.sqrt(a))
    dist.loc[valid_mask] = (6371.0 * c).round(2)
    return dist


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
    if "closed_date" in df_dedup.columns:
        df_dedup["closed_at_utc"] = to_utc_datetime(df_dedup["closed_date"])
    else:
        df_dedup["closed_at_utc"] = pd.Series(pd.NaT, index=df_dedup.index, dtype="datetime64[us, UTC]")

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

    # -------------------------------------------------------------------------
    # Hard Quarantine Boundary Validation
    # 1. Missing created_at timestamp
    # 2. Negative duration for closed tickets (closed before created)
    # 3. Missing or non-positive unique_key primary key (supports 7, 8, 9 digit keys > 0)
    # 4. Pre-311 launch date (earlier than March 2003 indicates epoch/default artifact)
    # 5. Future created or closed timestamp (beyond current UTC time + 2h clock drift allowance)
    # 6. Corrupted ingestion lineage metadata (_batch_id missing or empty)
    # -------------------------------------------------------------------------
    parsed_keys = pd.to_numeric(df_dedup["unique_key"], errors="coerce")
    launch_date_utc = pd.Timestamp(NYC_311_LAUNCH_DATE)
    future_grace_utc = now_utc + pd.Timedelta(hours=2)

    cond_missing_created = df_dedup["created_at_utc"].isna()
    cond_negative_duration = df_dedup["is_ticket_closed"] & (df_dedup["resolution_duration_hours"] < 0.0)
    cond_invalid_key = parsed_keys.isna() | (parsed_keys <= 0)
    cond_pre_epoch = df_dedup["created_at_utc"].notna() & (df_dedup["created_at_utc"] < launch_date_utc)
    cond_future_created = df_dedup["created_at_utc"].notna() & (df_dedup["created_at_utc"] > future_grace_utc)
    cond_future_closed = df_dedup["closed_at_utc"].notna() & (df_dedup["closed_at_utc"] > future_grace_utc)
    cond_corrupt_lineage = (
        (df_dedup["_batch_id"].isna() | df_dedup["_batch_id"].astype(str).str.strip().isin(["", "nan", "None", "<NA>"]))
        if "_batch_id" in df_dedup.columns
        else pd.Series(False, index=df_dedup.index)
    )

    quarantine_mask = (
        cond_missing_created
        | cond_negative_duration
        | cond_invalid_key
        | cond_pre_epoch
        | cond_future_created
        | cond_future_closed
        | cond_corrupt_lineage
    )
    df_quarantined = df_dedup[quarantine_mask].copy()

    if len(df_quarantined) > 0:
        # Build multi-reason quarantine string recording all violated boundary rules per row
        q_idx = df_quarantined.index
        rule_checks = [
            (cond_missing_created.loc[q_idx], "MISSING_CREATED_TIMESTAMP"),
            (cond_invalid_key.loc[q_idx], "INVALID_OR_NON_NUMERIC_UNIQUE_KEY"),
            (cond_pre_epoch.loc[q_idx], "PRE_311_EPOCH_DATE"),
            (cond_future_created.loc[q_idx], "FUTURE_CREATED_DATE"),
            (cond_future_closed.loc[q_idx], "FUTURE_CLOSED_DATE"),
            (cond_negative_duration.loc[q_idx], "NEGATIVE_RESOLUTION_DURATION"),
            (cond_corrupt_lineage.loc[q_idx], "CORRUPTED_INGESTION_METADATA"),
        ]
        df_quarantined["quarantine_reason"] = [
            "; ".join(label for mask, label in rule_checks if mask.at[idx])
            for idx in q_idx
        ]
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        q_path = quarantine_dir / "quarantined_311.parquet"
        pq.write_table(pa.Table.from_pandas(df_quarantined, preserve_index=False), q_path, compression="snappy")
        logger.warning(f"Quarantined {len(df_quarantined):,} invalid records to {q_path}")

    # Retain only valid records for clean Silver and enforce Int64 primary key
    df_valid = df_dedup[~quarantine_mask].copy()
    df_valid["unique_key"] = parsed_keys[~quarantine_mask].astype("Int64")

    # Standardize clean_incident_zip as a persistent 5-digit zero-padded string (never integer!)
    if "incident_zip" in df_valid.columns:
        extracted_zips = df_valid["incident_zip"].astype(str).str.extract(r"(\d{5})")[0]
        df_valid["clean_incident_zip"] = extracted_zips.where(
            extracted_zips.notna() & ~extracted_zips.isin(["nan", "None", "00000_invalid"]),
            None,
        )
    else:
        df_valid["clean_incident_zip"] = None

    # Tag Out-of-State / Non-Metro ZIP Codes
    df_valid["is_out_of_state_zip"] = (
        df_valid["clean_incident_zip"].notna() &
        ~df_valid["clean_incident_zip"].str[:3].isin(NYC_METRO_ZIP_PREFIXES)
    )

    # 3. Hierarchical Spatial Imputation (3 Levels)
    # Parse coordinates to numeric
    raw_lats = pd.to_numeric(df_valid["latitude"], errors="coerce") if "latitude" in df_valid.columns else pd.Series(np.nan, index=df_valid.index)
    raw_lons = pd.to_numeric(df_valid["longitude"], errors="coerce") if "longitude" in df_valid.columns else pd.Series(np.nan, index=df_valid.index)

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

    # Level 2: ZIP Code Centroid Fallback (directly uses standardized clean_incident_zip)
    mask_need_level2 = ~mask_level1
    zip_ref = load_zip_reference()

    if not zip_ref.empty and "clean_incident_zip" in df_valid.columns:
        zip_map_lat = zip_ref.set_index("zip5")["centroid_lat"].to_dict()
        zip_map_lon = zip_ref.set_index("zip5")["centroid_lon"].to_dict()
        zip_map_grid = zip_ref.set_index("zip5")["grid_id"].to_dict()

        level2_zips = df_valid.loc[mask_need_level2, "clean_incident_zip"]
        level2_lats = level2_zips.map(zip_map_lat)
        mask_level2_matched = level2_lats.notna()

        matched_indices = level2_lats[mask_level2_matched].index
        df_valid.loc[matched_indices, "clean_latitude"] = level2_lats[mask_level2_matched]
        df_valid.loc[matched_indices, "clean_longitude"] = level2_zips[mask_level2_matched].map(zip_map_lon)
        df_valid.loc[matched_indices, "grid_id"] = level2_zips[mask_level2_matched].map(zip_map_grid)
        df_valid.loc[matched_indices, "spatial_resolution_level"] = "ZIP_APPROXIMATION"

    # Level 3: Borough Centroid Fallback
    # First, validate and normalize NYC Boroughs (5 Legal Boroughs + UNSPECIFIED)
    if "borough" in df_valid.columns:
        b_clean = standardize_string_series(df_valid["borough"], "UNSPECIFIED", case="upper")
        is_unrec_b = ~b_clean.isin(VALID_NYC_BOROUGHS)
        df_valid["is_valid_borough"] = ~is_unrec_b
        df_valid["unrecognized_borough_val"] = np.where(is_unrec_b, b_clean, "")
        df_valid["borough"] = b_clean.where(~is_unrec_b, "UNSPECIFIED")
    else:
        df_valid["borough"] = "UNSPECIFIED"
        df_valid["is_valid_borough"] = True
        df_valid["unrecognized_borough_val"] = ""

    mask_need_level3 = df_valid["spatial_resolution_level"] == "UNLOCATED"
    borough_series = df_valid.loc[mask_need_level3, "borough"]

    for borough_name, (b_lat, b_lon, b_grid) in BOROUGH_CENTROIDS.items():
        b_mask = borough_series == borough_name
        b_indices = borough_series[b_mask].index
        df_valid.loc[b_indices, "clean_latitude"] = b_lat
        df_valid.loc[b_indices, "clean_longitude"] = b_lon
        df_valid.loc[b_indices, "grid_id"] = b_grid
        df_valid.loc[b_indices, "spatial_resolution_level"] = "BOROUGH_APPROXIMATION"

    # 4. Spatial Uncertainty Quantification (Great-Circle Haversine Distance to Assigned ERA5 Grid Center)
    grid_pattern = r"^GRID_([0-9.-]+)_([0-9.-]+)$"
    extracted_coords = df_valid["grid_id"].str.extract(grid_pattern)
    grid_center_lat = pd.to_numeric(extracted_coords[0], errors="coerce")
    grid_center_lon = pd.to_numeric(extracted_coords[1], errors="coerce")

    df_valid["grid_distance_km"] = haversine_distance_km(
        df_valid["clean_latitude"],
        df_valid["clean_longitude"],
        grid_center_lat,
        grid_center_lon,
    )

    # Spatial resolution statistics
    spatial_stats = df_valid["spatial_resolution_level"].value_counts().to_dict()
    logger.info(f"Spatial imputation summary: {spatial_stats}")

    # 5. Categorical, Syntactic & Institutional Validations
    # A. Complaint Type and Descriptor Standardization
    if "complaint_type" in df_valid.columns:
        df_valid["complaint_type"] = standardize_string_series(df_valid["complaint_type"], "Unknown", case="title")
    if "descriptor" in df_valid.columns:
        df_valid["descriptor"] = standardize_string_series(df_valid["descriptor"], "Not Stated", case="preserve")

    # C. Status Standardization & Validation
    if "status" in df_valid.columns:
        st_clean = standardize_string_series(df_valid["status"], "UNKNOWN", case="upper")
        is_unrec_st = ~st_clean.isin(KNOWN_311_STATUSES)
        df_valid["unrecognized_status_val"] = np.where(is_unrec_st, st_clean, "")
        df_valid["status"] = st_clean
    else:
        df_valid["status"] = "UNKNOWN"
        df_valid["unrecognized_status_val"] = ""

    # D. Agency Acronym Syntactic Validation
    # Enforces syntax: cannot be purely numeric (except "311"/"3-1-1"), length 2-10, valid characters
    if "agency" in df_valid.columns:
        ag_clean = standardize_string_series(df_valid["agency"], "UNKNOWN", case="upper")
        cond_numeric = ag_clean.str.match(r"^\d+$") & ~ag_clean.isin(["311", "3-1-1"])
        cond_bad_len = (ag_clean != "UNKNOWN") & ((ag_clean.str.len() < 2) | (ag_clean.str.len() > 10))
        cond_bad_chars = (ag_clean != "UNKNOWN") & ~ag_clean.str.match(r"^[A-Z0-9\-]+$")
        is_malformed_ag = cond_numeric | cond_bad_len | cond_bad_chars
        df_valid["is_malformed_agency"] = is_malformed_ag
        df_valid["malformed_agency_val"] = np.where(is_malformed_ag, ag_clean, "")
        df_valid["agency"] = ag_clean.where(~is_malformed_ag, "UNKNOWN")
    else:
        df_valid["agency"] = "UNKNOWN"
        df_valid["is_malformed_agency"] = False
        df_valid["malformed_agency_val"] = ""

    # E. Open Data Channel Type Validation
    if "open_data_channel_type" in df_valid.columns:
        ch_clean = standardize_string_series(df_valid["open_data_channel_type"], "UNKNOWN", case="upper")
        is_unrec_ch = ~ch_clean.isin(VALID_OPEN_DATA_CHANNELS)
        df_valid["unrecognized_channel_val"] = np.where(is_unrec_ch, ch_clean, "")
        df_valid["open_data_channel_type"] = ch_clean.where(~is_unrec_ch, "UNKNOWN")
    else:
        df_valid["open_data_channel_type"] = "UNKNOWN"
        df_valid["unrecognized_channel_val"] = ""

    # 6. Type Enforcement & Consistency Validations
    df_valid["latitude"] = raw_lats
    df_valid["longitude"] = raw_lons

    # NY State Plane Coordinates (Sanity Bounds [900000..1075000, 110000..285000])
    if "x_coordinate_state_plane" in df_valid.columns:
        x_num = pd.to_numeric(df_valid["x_coordinate_state_plane"], errors="coerce")
        df_valid["x_coordinate_state_plane"] = np.where(
            x_num.between(NYC_STATE_PLANE_BOUNDS["x_min"], NYC_STATE_PLANE_BOUNDS["x_max"]),
            x_num,
            np.nan,
        )
    if "y_coordinate_state_plane" in df_valid.columns:
        y_num = pd.to_numeric(df_valid["y_coordinate_state_plane"], errors="coerce")
        df_valid["y_coordinate_state_plane"] = np.where(
            y_num.between(NYC_STATE_PLANE_BOUNDS["y_min"], NYC_STATE_PLANE_BOUNDS["y_max"]),
            y_num,
            np.nan,
        )

    # Council District: NYC Charter Chapter 2, Section 22 defines exactly 51 Council Districts (1-51)
    if "council_district" in df_valid.columns:
        c_num = pd.to_numeric(df_valid["council_district"], errors="coerce")
        c_valid = c_num.where(c_num.between(NYC_COUNCIL_DISTRICT_MIN, NYC_COUNCIL_DISTRICT_MAX))
        df_valid["council_district"] = c_valid.astype("Int16")

    # Police Precinct: NYC has 77 NYPD precincts (ranging 1-123)
    if "police_precinct" in df_valid.columns:
        precinct_extracted = df_valid["police_precinct"].astype(str).str.extract(r"(\d+)")[0]
        precinct_num = pd.to_numeric(precinct_extracted, errors="coerce")
        p_valid = precinct_num.where(precinct_num.between(1, 123))
        df_valid["clean_police_precinct"] = p_valid.astype("Int16")

    # Secondary Timestamps: Parse to UTC
    if "due_date" in df_valid.columns:
        df_valid["due_date_utc"] = to_utc_datetime(df_valid["due_date"])
    if "resolution_action_updated_date" in df_valid.columns:
        df_valid["resolution_action_updated_date_utc"] = to_utc_datetime(df_valid["resolution_action_updated_date"])

    # 7. Quality Tagging & Observability Audit Notes
    # Instant Closures (Closed at creation: 0.0 duration)
    df_valid["is_instant_closure"] = df_valid["is_ticket_closed"] & (df_valid["resolution_duration_hours"] == 0.0)

    # Status vs. Closed Date Contradiction:
    # (a) Agency status is OPEN/IN PROGRESS/PENDING/etc., but closed_date is populated
    # (b) Agency status is CLOSED, but closed_date is missing (NaT)
    open_statuses = {"OPEN", "IN PROGRESS", "PENDING", "ASSIGNED", "STARTED"}
    closed_statuses = {"CLOSED"}
    if "status" in df_valid.columns:
        cond_open_with_closed_date = df_valid["is_ticket_closed"] & df_valid["status"].isin(open_statuses)
        cond_closed_without_closed_date = (~df_valid["is_ticket_closed"]) & df_valid["status"].isin(closed_statuses)
        df_valid["is_status_conflict"] = cond_open_with_closed_date | cond_closed_without_closed_date
    else:
        cond_open_with_closed_date = pd.Series(False, index=df_valid.index)
        cond_closed_without_closed_date = pd.Series(False, index=df_valid.index)
        df_valid["is_status_conflict"] = False

    # Extreme Outlier Durations (> 2 years / 17,520 hours)
    df_valid["is_extreme_duration"] = df_valid["is_ticket_closed"] & (df_valid["resolution_duration_hours"] > 17520.0)

    # Agency Registry Soft Check (Identifies primary municipal agencies vs external / newly added entities)
    df_valid["is_known_agency"] = df_valid["agency"].isin(KNOWN_NYC_AGENCIES)

    # Vectorized Observability Audit String Construction (avoids slow row-by-row .iterrows())
    df_valid["quality_audit_notes"] = None
    any_audit_flag = (
        df_valid["is_instant_closure"]
        | df_valid["is_status_conflict"]
        | df_valid["is_extreme_duration"]
        | df_valid["is_out_of_state_zip"]
        | (~df_valid["is_valid_borough"])
        | df_valid["is_malformed_agency"]
        | ((~df_valid["is_known_agency"]) & (df_valid["agency"] != "UNKNOWN"))
        | (df_valid["unrecognized_channel_val"] != "")
        | (df_valid["unrecognized_status_val"] != "")
    )
    if any_audit_flag.any():
        sub = df_valid.loc[any_audit_flag]
        status_vals = sub["status"].astype(str) if "status" in sub.columns else pd.Series("UNKNOWN", index=sub.index)
        zip_vals = sub["clean_incident_zip"].astype(str) if "clean_incident_zip" in sub.columns else pd.Series("", index=sub.index)
        agency_vals = sub["agency"].astype(str) if "agency" in sub.columns else pd.Series("UNKNOWN", index=sub.index)
        dur_vals = sub["resolution_duration_hours"].astype(str)

        note_parts = pd.DataFrame(index=sub.index)
        note_parts["n1"] = np.where(sub["is_instant_closure"], "INSTANT_CLOSURE_0SEC", "")
        note_parts["n2"] = np.where(
            cond_open_with_closed_date.loc[sub.index],
            "STATUS_CONFLICT(status=" + status_vals + "_with_closed_date)",
            np.where(
                cond_closed_without_closed_date.loc[sub.index],
                "STATUS_CONFLICT(status=" + status_vals + "_without_closed_date)",
                "",
            ),
        )
        note_parts["n3"] = np.where(sub["is_extreme_duration"], "EXTREME_DURATION(" + dur_vals + "h)", "")
        note_parts["n4"] = np.where(sub["is_out_of_state_zip"], "OUT_OF_STATE_ZIP(" + zip_vals + ")", "")
        note_parts["n5"] = np.where(~sub["is_valid_borough"], "UNRECOGNIZED_BOROUGH(" + sub["unrecognized_borough_val"] + ")", "")
        note_parts["n6"] = np.where(
            sub["is_malformed_agency"],
            "MALFORMED_AGENCY(" + sub["malformed_agency_val"] + ")",
            np.where(
                (~sub["is_known_agency"]) & (sub["agency"] != "UNKNOWN"),
                "UNREGISTERED_AGENCY(" + agency_vals + ")",
                "",
            ),
        )
        note_parts["n7"] = np.where(sub["unrecognized_channel_val"] != "", "UNRECOGNIZED_CHANNEL(" + sub["unrecognized_channel_val"] + ")", "")
        note_parts["n8"] = np.where(sub["unrecognized_status_val"] != "", "UNRECOGNIZED_STATUS(" + sub["unrecognized_status_val"] + ")", "")

        df_valid.loc[any_audit_flag, "quality_audit_notes"] = [
            "; ".join(val for val in row if val)
            for row in note_parts.to_numpy()
        ]

    # Clean up temporary string helpers from output schema
    df_valid.drop(
        columns=[
            "unrecognized_borough_val",
            "is_malformed_agency",
            "malformed_agency_val",
            "unrecognized_channel_val",
            "unrecognized_status_val",
        ],
        inplace=True,
    )

    # Silver Audit Metadata
    df_valid["_silver_processed_at_utc"] = processed_at_utc

    # 7. Partitioned Parquet Write: year=YYYY / month=MM / clean_311.parquet
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
    conflict_count = int(df_valid["is_status_conflict"].sum())
    instant_count = int(df_valid["is_instant_closure"].sum())

    valid_dist = df_valid["grid_distance_km"].dropna()
    avg_grid_dist = round(float(valid_dist.mean()), 2) if len(valid_dist) > 0 else 0.0
    p95_grid_dist = round(float(valid_dist.quantile(0.95)), 2) if len(valid_dist) > 0 else 0.0

    return {
        "dataset": "silver_311",
        "input_rows": input_rows,
        "duplicate_rows_removed": duplicate_rows_removed,
        "valid_rows_accepted": len(df_valid),
        "avg_grid_distance_km": avg_grid_dist,
        "p95_grid_distance_km": p95_grid_dist,
        "closed_tickets_count": closed_count,
        "open_tickets_count": open_count,
        "status_conflict_count": conflict_count,
        "instant_closure_count": instant_count,
        "quarantined_rows": len(df_quarantined),
        "spatial_resolution_breakdown": spatial_stats,
        "partitions_written": written_files,
        "duration_seconds": round(elapsed, 2),
        "status": "success",
    }


if __name__ == "__main__":
    clean_and_validate_311()
