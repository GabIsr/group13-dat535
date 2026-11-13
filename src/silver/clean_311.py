"""
Silver Layer: NYC 311 Service Requests Cleaning, Deduplication, and Spatial Imputation.
100% Pure PySpark Pipeline:
Applies CDC deduplication, UTC timestamp normalization, temporal duration validation,
3-level hierarchical spatial imputation (Coordinates -> ZIP Centroid -> Borough Centroid),
comprehensive institutional & spatial consistency checks, and routes invalid rows to quarantine.
Author: Group 13
"""

import logging
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    array,
    array_compact,
    asin,
    avg,
    broadcast,
    col,
    concat,
    concat_ws,
    cos,
    date_format,
    date_trunc,
    dayofweek,
    desc,
    from_utc_timestamp,
    hour,
    initcap,
    length,
    lit,
    month as spark_month,
    percentile_approx,
    radians,
    regexp_extract,
    round as spark_round,
    row_number,
    sin,
    sqrt,
    substring,
    sum as spark_sum,
    to_timestamp,
    to_utc_timestamp,
    trim,
    upper,
    when,
)
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel

from src.silver.spatial_cleaning import normalize_all_spatial_features
from src.silver.incident_clustering import (
    generate_hierarchical_location_keys,
    cluster_citizen_call_bursts,
)
from src.silver.resolution_text_mining import (
    classify_resolution_outcomes,
    upsert_unclassified_resolutions_ledger,
)
from src.silver.semantic_conformance import (
    conform_complaint_taxonomy,
    upsert_unmapped_taxonomy_ledger,
)
from src.utils.spark import get_spark_session
from src.silver.config import (
    BOROUGH_CENTROIDS,
    BRONZE_DIR,
    KNOWN_311_STATUSES,
    KNOWN_NYC_AGENCIES,
    MAX_UNMAPPED_TAXONOMY_RATE_PCT,
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
    TAXONOMY_REFERENCE_PATH,
    UNMAPPED_TAXONOMY_LEDGER_PATH,
    VALID_NYC_BOROUGHS,
    VALID_OPEN_DATA_CHANNELS,
    ZIP_CENTROIDS_PATH,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("clean_311")


# =========================================================================
# Standalone Helper Functions (Kept for unit test compatibility)
# =========================================================================

def load_zip_reference(zip_ref_path: Path = ZIP_CENTROIDS_PATH) -> pd.DataFrame:
    """Load the precomputed NYC ZIP centroid reference table (kept for unit test compatibility)."""
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
    Safeguards:
      1. Defensive dimension check: returns NaN Series with a warning if input lengths mismatch.
      2. Operates positionally using NumPy to prevent accidental Pandas index misalignment.
      3. Validates geographic coordinate bounds: latitude in [-90, 90], longitude in [-180, 180].
      4. Returns NaN for missing, out-of-bounds, or physically invalid coordinates.
      5. Preserves the index of lat1 on the returned Series.
    """
    target_index = getattr(lat1, "index", None)
    try:
        n1, n2, n3, n4 = len(lat1), len(lon1), len(lat2), len(lon2)
    except (TypeError, AttributeError):
        logger.warning("haversine_distance_km: Invalid input types received. Returning empty NaN Series.")
        return pd.Series(np.nan, index=target_index, dtype="float64")

    if not (n1 == n2 == n3 == n4):
        logger.warning(
            f"haversine_distance_km: Dimension mismatch (lat1={n1}, lon1={n2}, lat2={n3}, lon2={n4}). "
            "Defensively returning NaN Series to prevent pipeline crash."
        )
        return pd.Series(np.nan, index=target_index, dtype="float64")

    v_lat1 = pd.to_numeric(pd.Series(lat1).to_numpy(), errors="coerce").astype(float)
    v_lon1 = pd.to_numeric(pd.Series(lon1).to_numpy(), errors="coerce").astype(float)
    v_lat2 = pd.to_numeric(pd.Series(lat2).to_numpy(), errors="coerce").astype(float)
    v_lon2 = pd.to_numeric(pd.Series(lon2).to_numpy(), errors="coerce").astype(float)

    valid_mask = (
        ~np.isnan(v_lat1)
        & ~np.isnan(v_lon1)
        & ~np.isnan(v_lat2)
        & ~np.isnan(v_lon2)
        & (v_lat1 >= -90.0)
        & (v_lat1 <= 90.0)
        & (v_lat2 >= -90.0)
        & (v_lat2 <= 90.0)
        & (v_lon1 >= -180.0)
        & (v_lon1 <= 180.0)
        & (v_lon2 >= -180.0)
        & (v_lon2 <= 180.0)
    )

    dist = np.full(n1, np.nan, dtype=np.float64)
    if np.any(valid_mask):
        r_lat1 = np.radians(v_lat1[valid_mask])
        r_lon1 = np.radians(v_lon1[valid_mask])
        r_lat2 = np.radians(v_lat2[valid_mask])
        r_lon2 = np.radians(v_lon2[valid_mask])

        dlat = r_lat2 - r_lat1
        dlon = r_lon2 - r_lon1

        a = np.sin(dlat / 2.0) ** 2 + np.cos(r_lat1) * np.cos(r_lat2) * np.sin(dlon / 2.0) ** 2
        a = np.clip(a, 0.0, 1.0)
        c = 2.0 * np.arcsin(np.sqrt(a))
        dist[valid_mask] = np.round(6371.0 * c, 2)

    return pd.Series(dist, index=target_index, dtype="float64")


def spark_haversine_distance_km(lat1_col, lon1_col, lat2_col, lon2_col):
    """
    Compute Great-Circle Haversine distance in km using pure PySpark SQL functions.
    Fully optimized inside Catalyst optimizer without Python serialization overhead.
    Safeguards:
      1. Validates coordinate bounds: lat in [-90, 90], lon in [-180, 180].
         Returns null for invalid or missing coordinates.
      2. Clips intermediate values to [0.0, 1.0] to safeguard against floating-point NaN in asin.
    """
    valid_coords = (
        lat1_col.isNotNull()
        & lon1_col.isNotNull()
        & lat2_col.isNotNull()
        & lon2_col.isNotNull()
        & (lat1_col >= lit(-90.0))
        & (lat1_col <= lit(90.0))
        & (lat2_col >= lit(-90.0))
        & (lat2_col <= lit(90.0))
        & (lon1_col >= lit(-180.0))
        & (lon1_col <= lit(180.0))
        & (lon2_col >= lit(-180.0))
        & (lon2_col <= lit(180.0))
    )

    r_lat1 = radians(lat1_col)
    r_lon1 = radians(lon1_col)
    r_lat2 = radians(lat2_col)
    r_lon2 = radians(lon2_col)
    dlat = r_lat2 - r_lat1
    dlon = r_lon2 - r_lon1
    a = (sin(dlat / 2.0) ** 2) + cos(r_lat1) * cos(r_lat2) * (sin(dlon / 2.0) ** 2)
    a_safe = when(a > 1.0, lit(1.0)).when(a < 0.0, lit(0.0)).otherwise(a)
    dist = spark_round(lit(6371.0) * lit(2.0) * asin(sqrt(a_safe)), 2)
    return when(valid_coords, dist).otherwise(lit(None).cast("double"))


# =========================================================================
# Pure PySpark Silver 311 Pipeline Modular Functions
# =========================================================================

def normalize_timestamps_and_durations(df: DataFrame, now_epoch: int) -> DataFrame:
    """
    Normalize 311 local Eastern Time timestamps to UTC and derive temporal duration metrics.
    Computes ticket status categories, resolution durations, active ticket age, and calendar features.
    100% pure PySpark.
    """
    cols = set(df.columns)
    created_ts = to_utc_timestamp(to_timestamp(col("created_date")), "America/New_York")
    df_proc = df.withColumn("created_at_utc", created_ts)

    if "closed_date" in cols:
        closed_ts = to_utc_timestamp(to_timestamp(col("closed_date")), "America/New_York")
        df_proc = df_proc.withColumn("closed_at_utc", closed_ts)
    else:
        df_proc = df_proc.withColumn("closed_at_utc", lit(None).cast("timestamp"))

    df_proc = df_proc.withColumn("is_ticket_closed", col("closed_at_utc").isNotNull())
    df_proc = df_proc.withColumn(
        "ticket_status_category",
        when(col("is_ticket_closed"), lit("CLOSED")).otherwise(lit("OPEN")),
    )

    # Resolution duration in hours (only applicable to closed tickets)
    duration_secs = col("closed_at_utc").cast("long") - col("created_at_utc").cast("long")
    df_proc = df_proc.withColumn(
        "resolution_duration_hours",
        when(col("is_ticket_closed"), spark_round(duration_secs / 3600.0, 2)).otherwise(lit(None).cast("double")),
    )

    # Active queue age in days (relative to current UTC runtime)
    open_age_secs = lit(now_epoch) - col("created_at_utc").cast("long")
    df_proc = df_proc.withColumn(
        "open_ticket_age_days",
        when(~col("is_ticket_closed"), spark_round(open_age_secs / 86400.0, 2)).otherwise(lit(None).cast("double")),
    )

    # Source timestamps normalization (Socrata :created_at and :updated_at)
    if "source_updated_at" in cols:
        df_proc = df_proc.withColumn("source_updated_at", to_timestamp(col("source_updated_at")))

    if "source_created_at" in cols:
        src_cr_ts = to_timestamp(col("source_created_at"))
        df_proc = df_proc.withColumn("source_created_at", src_cr_ts)
        intake_lag_secs = src_cr_ts.cast("long") - col("created_at_utc").cast("long")
        df_proc = df_proc.withColumn(
            "source_intake_lag_hours",
            when(src_cr_ts.isNotNull() & col("created_at_utc").isNotNull(), spark_round(intake_lag_secs / 3600.0, 2))
            .otherwise(lit(None).cast("double")),
        )

    # Hourly join key for UTC weather alignment
    df_proc = df_proc.withColumn("weather_join_hour_utc", date_trunc("hour", col("created_at_utc")))

    # Calendar row attributes derived from local New York time in PySpark
    local_ts = from_utc_timestamp(col("created_at_utc"), "America/New_York")
    df_proc = (
        df_proc.withColumn("created_hour_utc", hour(col("created_at_utc")))
        .withColumn("created_hour_local", hour(local_ts))
        .withColumn("day_of_week", (dayofweek(local_ts) + 5) % 7)
        .withColumn("is_weekend", col("day_of_week").isin([5, 6]))
        .withColumn("month_of_year", spark_month(local_ts))
    )

    return df_proc


def quarantine_invalid_311_records(
    df: DataFrame,
    now_epoch: int,
    quarantine_dir: Path,
) -> Tuple[DataFrame, int]:
    """
    Evaluate strict boundary validation predicates in Spark SQL.
    Routes corrupted/unkeyed/negative-duration/out-of-bounds records to quarantine Parquet
    BEFORE deduplication so corrupted records are never collapsed as nulls.
    100% pure PySpark.
    """
    parsed_keys = col("unique_key").cast("long")
    launch_epoch = int(datetime(2003, 3, 1, tzinfo=timezone.utc).timestamp())
    future_grace_epoch = now_epoch + 7200
    duration_secs = col("closed_at_utc").cast("long") - col("created_at_utc").cast("long")

    cond_missing_created = col("created_at_utc").isNull()
    cond_negative_duration = col("is_ticket_closed") & (duration_secs < 0)
    cond_invalid_key = parsed_keys.isNull() | (parsed_keys <= 0)
    cond_pre_epoch = col("created_at_utc").isNotNull() & (col("created_at_utc").cast("long") < lit(launch_epoch))
    cond_future_created = col("created_at_utc").isNotNull() & (col("created_at_utc").cast("long") > lit(future_grace_epoch))
    cond_future_closed = col("closed_at_utc").isNotNull() & (col("closed_at_utc").cast("long") > lit(future_grace_epoch))

    if "_batch_id" in df.columns:
        b_str = trim(col("_batch_id").cast("string"))
        cond_corrupt_lineage = col("_batch_id").isNull() | b_str.isin(["", "nan", "None", "<NA>"])
    else:
        cond_corrupt_lineage = lit(False)

    quarantine_cond = (
        cond_missing_created
        | cond_negative_duration
        | cond_invalid_key
        | cond_pre_epoch
        | cond_future_created
        | cond_future_closed
        | cond_corrupt_lineage
    )

    df_quarantined = df.filter(quarantine_cond)
    quarantined_count = df_quarantined.count()

    if quarantined_count > 0:
        rule_checks = array_compact(array(
            when(cond_missing_created, lit("MISSING_CREATED_TIMESTAMP")),
            when(cond_invalid_key, lit("INVALID_OR_NON_NUMERIC_UNIQUE_KEY")),
            when(cond_pre_epoch, lit("PRE_311_EPOCH_DATE")),
            when(cond_future_created, lit("FUTURE_CREATED_DATE")),
            when(cond_future_closed, lit("FUTURE_CLOSED_DATE")),
            when(cond_negative_duration, lit("NEGATIVE_RESOLUTION_DURATION")),
            when(cond_corrupt_lineage, lit("CORRUPTED_INGESTION_METADATA")),
        ))
        df_quarantined = df_quarantined.withColumn("quarantine_reason", concat_ws("; ", rule_checks))
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        q_path = quarantine_dir / "quarantined_311.parquet"
        df_quarantined.coalesce(1).write.mode("overwrite").parquet(str(q_path))
        logger.warning(f"Quarantined {quarantined_count:,} invalid records to {q_path}")

    df_valid_candidates = df.filter(~quarantine_cond)
    return df_valid_candidates, quarantined_count


def deduplicate_311_records(df: DataFrame) -> Tuple[DataFrame, int]:
    """
    Perform Change Data Capture (CDC) deduplication via PySpark Window on valid records.
    Prioritizes Socrata source update timestamp over arrival timestamp, ensuring historical
    backfills never overwrite newer ticket states.
    100% pure PySpark.
    """
    cols = set(df.columns)
    order_cols = []
    if "source_updated_at" in cols:
        order_cols.append(col("source_updated_at").desc())
    if "_ingested_at_utc" in cols:
        order_cols.append(col("_ingested_at_utc").desc())
    if "created_date" in cols:
        order_cols.append(col("created_date").desc())
    if not order_cols:
        order_cols.append(col("unique_key").desc())

    # Ensure unique_key is cast to LongType
    df_keyed = df.withColumn("unique_key", col("unique_key").cast("long"))

    w_dedup = Window.partitionBy("unique_key").orderBy(*order_cols)
    df_dedup = df_keyed.withColumn("_rn", row_number().over(w_dedup)).filter(col("_rn") == 1).drop("_rn")

    valid_count = df_dedup.count()
    candidate_count = df.count()
    duplicate_rows_removed = candidate_count - valid_count
    if duplicate_rows_removed > 0:
        logger.info(f"Deduplication (PySpark): removed {duplicate_rows_removed:,} duplicate records on unique_key.")

    return df_dedup, duplicate_rows_removed


def impute_spatial_and_canonicalize_routes(
    df: DataFrame,
    spark: SparkSession,
    zip_ref_path: Path = ZIP_CENTROIDS_PATH,
) -> DataFrame:
    """
    Phase 1: Apply 3-level hierarchical spatial imputation (Native GPS -> ZIP Centroid -> Borough Centroid),
    quantify spatial distance uncertainty via Catalyst PySpark Haversine,
    and normalize spatial features & evaluate 6-route hierarchical location keys (loc_key).
    100% pure PySpark.
    """
    cols = set(df.columns)
    raw_lats = col("latitude").cast("double") if "latitude" in cols else lit(None).cast("double")
    raw_lons = col("longitude").cast("double") if "longitude" in cols else lit(None).cast("double")

    if "incident_zip" in cols:
        extracted_zips = regexp_extract(col("incident_zip").cast("string"), r"(\d{5})", 1)
        clean_zip_expr = when(extracted_zips.isin(["", "nan", "None", "00000_invalid"]), lit(None)).otherwise(extracted_zips)
    else:
        clean_zip_expr = lit(None).cast("string")

    df = df.withColumn("clean_incident_zip", clean_zip_expr)

    # Level 1: Native GPS Coordinates within NYC bounds
    mask_l1 = (
        raw_lats.isNotNull()
        & raw_lons.isNotNull()
        & (raw_lats >= lit(NYC_LAT_MIN))
        & (raw_lats <= lit(NYC_LAT_MAX))
        & (raw_lons >= lit(NYC_LON_MIN))
        & (raw_lons <= lit(NYC_LON_MAX))
    )
    clean_lat_l1 = spark_round(raw_lats, 6)
    clean_lon_l1 = spark_round(raw_lons, 6)
    grid_lat_1 = spark_round(raw_lats, 1).cast("string")
    grid_lon_1 = spark_round(raw_lons, 1).cast("string")
    grid_id_l1 = concat(lit("GRID_"), grid_lat_1, lit("_"), grid_lon_1)

    # Level 2: ZIP Centroid Lookup (via Broadcast Join of Reference Parquet)
    if zip_ref_path.exists():
        df_zip_ref = (
            spark.read.parquet(str(zip_ref_path))
            .select(
                col("zip5").alias("_ref_zip5"),
                col("centroid_lat").alias("_ref_lat"),
                col("centroid_lon").alias("_ref_lon"),
                col("grid_id").alias("_ref_grid"),
            )
        )
        df = df.join(
            broadcast(df_zip_ref),
            col("clean_incident_zip") == col("_ref_zip5"),
            how="left",
        ).drop("_ref_zip5")
    else:
        df = (
            df.withColumn("_ref_lat", lit(None).cast("double"))
            .withColumn("_ref_lon", lit(None).cast("double"))
            .withColumn("_ref_grid", lit(None).cast("string"))
        )

    # Level 3: Borough Centroid Fallback
    b_lat_expr = lit(None).cast("double")
    b_lon_expr = lit(None).cast("double")
    b_grid_expr = lit("UNKNOWN")
    b_col = upper(trim(col("borough"))) if "borough" in cols else lit("UNSPECIFIED")
    for b_name, (lat_val, lon_val, grid_val) in BOROUGH_CENTROIDS.items():
        b_cond = b_col == b_name
        b_lat_expr = when(b_cond, lit(lat_val)).otherwise(b_lat_expr)
        b_lon_expr = when(b_cond, lit(lon_val)).otherwise(b_lon_expr)
        b_grid_expr = when(b_cond, lit(grid_val)).otherwise(b_grid_expr)

    # Coalesce Hierarchical Resolution
    df = df.withColumn(
        "clean_latitude",
        when(mask_l1, clean_lat_l1)
        .when(col("_ref_lat").isNotNull(), col("_ref_lat"))
        .when(b_lat_expr.isNotNull(), b_lat_expr)
        .otherwise(lit(None).cast("double")),
    )
    df = df.withColumn(
        "clean_longitude",
        when(mask_l1, clean_lon_l1)
        .when(col("_ref_lon").isNotNull(), col("_ref_lon"))
        .when(b_lon_expr.isNotNull(), b_lon_expr)
        .otherwise(lit(None).cast("double")),
    )
    df = df.withColumn(
        "grid_id",
        when(mask_l1, grid_id_l1)
        .when(col("_ref_grid").isNotNull(), col("_ref_grid"))
        .when(b_grid_expr != lit("UNKNOWN"), b_grid_expr)
        .otherwise(lit("UNKNOWN")),
    )
    df = df.withColumn(
        "spatial_resolution_level",
        when(mask_l1, lit("EXACT_COORDINATES"))
        .when(col("_ref_lat").isNotNull(), lit("ZIP_APPROXIMATION"))
        .when(b_lat_expr.isNotNull(), lit("BOROUGH_APPROXIMATION"))
        .otherwise(lit("UNLOCATED")),
    ).drop("_ref_lat", "_ref_lon", "_ref_grid")

    # Spatial Uncertainty Quantification (Haversine Distance in Catalyst PySpark)
    grid_pattern = r"^GRID_([0-9.-]+)_([0-9.-]+)$"
    grid_center_lat = regexp_extract(col("grid_id"), grid_pattern, 1).cast("double")
    grid_center_lon = regexp_extract(col("grid_id"), grid_pattern, 2).cast("double")
    has_coords = (
        col("clean_latitude").isNotNull()
        & col("clean_longitude").isNotNull()
        & grid_center_lat.isNotNull()
        & grid_center_lon.isNotNull()
    )

    df = df.withColumn(
        "grid_distance_km",
        when(
            has_coords,
            spark_haversine_distance_km(
                col("clean_latitude"),
                col("clean_longitude"),
                grid_center_lat,
                grid_center_lon,
            ),
        ).otherwise(lit(None).cast("double")),
    )

    # PySpark Spatial Feature Normalization, Hierarchical Routing
    logger.info("Executing PySpark Spatial Normalization & Canonicalization...")
    if "complaint_type" in cols:
        df = df.withColumn(
            "complaint_type",
            when(col("complaint_type").isNull() | (trim(col("complaint_type")) == ""), lit("Unknown"))
            .otherwise(initcap(trim(col("complaint_type")))),
        )
    df = normalize_all_spatial_features(df)

    logger.info("Evaluating 6-Route Hierarchical Spatial Routing in PySpark...")
    df = generate_hierarchical_location_keys(df)

    return df


def impute_spatial_hierarchy_311(
    df: DataFrame,
    spark: SparkSession,
    zip_ref_path: Path = ZIP_CENTROIDS_PATH,
) -> DataFrame:
    """
    Backwards-compatible wrapper executing spatial imputation, canonicalization,
    and adaptive citizen call burst clustering in PySpark.
    """
    df_spatial = impute_spatial_and_canonicalize_routes(df, spark, zip_ref_path)
    logger.info("Executing PySpark Ticket-1 Anchored Burst Deduplication Engine (Adaptive Window)...")
    return cluster_citizen_call_bursts(df_spatial, burst_window_hours=6.0)


def standardize_categorical_and_administrative_attributes(df: DataFrame) -> DataFrame:
    """
    Standardize Boroughs, agency acronyms, complaint types, descriptors, channels,
    State Plane coordinates, and administrative districts (Council District, Police Precinct).
    Derive soft tags (out-of-state ZIP, known agency) in 100% pure PySpark.
    """
    cols = set(df.columns)

    # Out-of-state ZIP tag (first 3 digits outside NYC metro prefixes)
    prefix_3 = substring(col("clean_incident_zip"), 1, 3)
    df = df.withColumn(
        "is_out_of_state_zip",
        col("clean_incident_zip").isNotNull() & (~prefix_3.isin(list(NYC_METRO_ZIP_PREFIXES))),
    )

    # Coordinates cast to double
    if "latitude" in cols:
        df = df.withColumn("latitude", col("latitude").cast("double"))
    if "longitude" in cols:
        df = df.withColumn("longitude", col("longitude").cast("double"))

    # Borough normalization & soft tagging
    if "borough" in cols:
        b_str = upper(trim(col("borough").cast("string")))
        b_clean = when(b_str.isNull() | b_str.isin(["", "NAN", "NONE", "NULL", "<NA>"]), lit("UNSPECIFIED")).otherwise(b_str)
        is_rec_b = b_clean.isin(list(VALID_NYC_BOROUGHS))
        df = (
            df.withColumn("is_valid_borough", is_rec_b)
            .withColumn("unrecognized_borough_val", when(~is_rec_b, b_clean).otherwise(lit("")))
            .withColumn("borough", when(is_rec_b, b_clean).otherwise(lit("UNSPECIFIED")))
        )
    else:
        df = (
            df.withColumn("borough", lit("UNSPECIFIED"))
            .withColumn("is_valid_borough", lit(True))
            .withColumn("unrecognized_borough_val", lit(""))
        )

    # Complaint type and descriptor
    if "complaint_type" in cols:
        ct_str = initcap(trim(col("complaint_type").cast("string")))
        df = df.withColumn(
            "complaint_type",
            when(ct_str.isNull() | ct_str.isin(["", "Nan", "None", "Null", "<Na>"]), lit("Unknown")).otherwise(ct_str),
        )
    else:
        df = df.withColumn("complaint_type", lit("Unknown"))

    if "descriptor" in cols:
        d_str = trim(col("descriptor").cast("string"))
        df = df.withColumn(
            "descriptor",
            when(d_str.isNull() | upper(d_str).isin(["", "NAN", "NONE", "NULL", "<NA>"]), lit("Not Stated")).otherwise(d_str),
        )
    else:
        df = df.withColumn("descriptor", lit("Not Stated"))

    # Status standardization & validation
    if "status" in cols:
        st_str = upper(trim(col("status").cast("string")))
        st_clean = when(st_str.isNull() | st_str.isin(["", "NAN", "NONE", "NULL", "<NA>"]), lit("UNKNOWN")).otherwise(st_str)
        is_unrec_st = ~st_clean.isin(list(KNOWN_311_STATUSES))
        df = (
            df.withColumn("unrecognized_status_val", when(is_unrec_st, st_clean).otherwise(lit("")))
            .withColumn("status", st_clean)
        )
    else:
        df = df.withColumn("status", lit("UNKNOWN")).withColumn("unrecognized_status_val", lit(""))

    # Agency syntactic & registry validation
    if "agency" in cols:
        ag_str = upper(trim(col("agency").cast("string")))
        ag_clean = when(ag_str.isNull() | ag_str.isin(["", "NAN", "NONE", "NULL", "<NA>"]), lit("UNKNOWN")).otherwise(ag_str)
        cond_numeric = ag_clean.rlike(r"^\d+$") & (~ag_clean.isin(["311", "3-1-1"]))
        cond_bad_len = (ag_clean != "UNKNOWN") & ((length(ag_clean) < 2) | (length(ag_clean) > 10))
        cond_bad_chars = (ag_clean != "UNKNOWN") & (~ag_clean.rlike(r"^[A-Z0-9\-]+$"))
        is_malformed_ag = cond_numeric | cond_bad_len | cond_bad_chars

        df = (
            df.withColumn("is_malformed_agency", is_malformed_ag)
            .withColumn("malformed_agency_val", when(is_malformed_ag, ag_clean).otherwise(lit("")))
            .withColumn("agency", when(~is_malformed_ag, ag_clean).otherwise(lit("UNKNOWN")))
        )
    else:
        df = (
            df.withColumn("agency", lit("UNKNOWN"))
            .withColumn("is_malformed_agency", lit(False))
            .withColumn("malformed_agency_val", lit(""))
        )

    # Agency registry check
    df = df.withColumn("is_known_agency", col("agency").isin(list(KNOWN_NYC_AGENCIES)))

    # Open Data Channel Type validation
    if "open_data_channel_type" in cols:
        ch_str = upper(trim(col("open_data_channel_type").cast("string")))
        ch_clean = when(ch_str.isNull() | ch_str.isin(["", "NAN", "NONE", "NULL", "<NA>"]), lit("UNKNOWN")).otherwise(ch_str)
        is_unrec_ch = ~ch_clean.isin(list(VALID_OPEN_DATA_CHANNELS))
        df = (
            df.withColumn("unrecognized_channel_val", when(is_unrec_ch, ch_clean).otherwise(lit("")))
            .withColumn("open_data_channel_type", when(~is_unrec_ch, ch_clean).otherwise(lit("UNKNOWN")))
        )
    else:
        df = df.withColumn("open_data_channel_type", lit("UNKNOWN")).withColumn("unrecognized_channel_val", lit(""))

    # NY State Plane Coordinates (Sanity Bounds [900000..1075000, 110000..285000])
    if "x_coordinate_state_plane" in cols:
        x_num = col("x_coordinate_state_plane").cast("double")
        cond_x = (x_num >= lit(NYC_STATE_PLANE_BOUNDS["x_min"])) & (x_num <= lit(NYC_STATE_PLANE_BOUNDS["x_max"]))
        df = df.withColumn("x_coordinate_state_plane", when(cond_x, x_num).otherwise(lit(None).cast("double")))
    if "y_coordinate_state_plane" in cols:
        y_num = col("y_coordinate_state_plane").cast("double")
        cond_y = (y_num >= lit(NYC_STATE_PLANE_BOUNDS["y_min"])) & (y_num <= lit(NYC_STATE_PLANE_BOUNDS["y_max"]))
        df = df.withColumn("y_coordinate_state_plane", when(cond_y, y_num).otherwise(lit(None).cast("double")))

    # Council District: NYC Charter Chapter 2, Section 22 defines exactly 51 Council Districts (1-51)
    if "council_district" in cols:
        c_num = col("council_district").cast("short")
        cond_c = (c_num >= lit(NYC_COUNCIL_DISTRICT_MIN)) & (c_num <= lit(NYC_COUNCIL_DISTRICT_MAX))
        df = df.withColumn("council_district", when(cond_c, c_num).otherwise(lit(None).cast("short")))

    # Police Precinct: NYC NYPD precincts (ranging 1-123)
    if "police_precinct" in cols:
        p_extr = regexp_extract(col("police_precinct").cast("string"), r"(\d+)", 1)
        p_num = when(p_extr != "", p_extr.cast("short")).otherwise(lit(None).cast("short"))
        cond_p = (p_num >= 1) & (p_num <= 123)
        df = df.withColumn("clean_police_precinct", when(cond_p, p_num).otherwise(lit(None).cast("short")))

    # Secondary Timestamps: Parse to UTC
    if "due_date" in cols:
        df = df.withColumn("due_date_utc", to_utc_timestamp(to_timestamp(col("due_date")), "America/New_York"))
    if "resolution_action_updated_date" in cols:
        df = df.withColumn(
            "resolution_action_updated_date_utc",
            to_utc_timestamp(to_timestamp(col("resolution_action_updated_date")), "America/New_York"),
        )

    return df


def attach_quality_audit_tags(df: DataFrame, processed_at_utc: str) -> DataFrame:
    """
    Derive instant closure, status conflict, and extreme duration flags.
    Construct human-readable quality audit notes string using pure PySpark Catalyst functions.
    Attach Silver metadata and prune intermediate string helpers.
    """
    valid_dur_secs = col("closed_at_utc").cast("long") - col("created_at_utc").cast("long")
    df = df.withColumn("is_instant_closure", col("is_ticket_closed") & (valid_dur_secs == 0))

    # Status vs. Closed Date Contradiction
    open_statuses = ["OPEN", "IN PROGRESS", "PENDING", "ASSIGNED", "STARTED"]
    closed_statuses = ["CLOSED"]
    cond_open_with_closed_date = col("is_ticket_closed") & col("status").isin(open_statuses)
    cond_closed_without_closed_date = (~col("is_ticket_closed")) & col("status").isin(closed_statuses)
    df = df.withColumn("is_status_conflict", cond_open_with_closed_date | cond_closed_without_closed_date)

    # Extreme Outlier Durations (> 2 years / 17,520 hours)
    df = df.withColumn(
        "is_extreme_duration",
        col("is_ticket_closed") & (col("resolution_duration_hours") > 17520.0),
    )

    # Observability Audit String Construction in PySpark Catalyst
    n1 = when(col("is_instant_closure"), lit("INSTANT_CLOSURE_0SEC"))
    n2 = (
        when(cond_open_with_closed_date, concat(lit("STATUS_CONFLICT(status="), col("status"), lit("_with_closed_date)")))
        .when(cond_closed_without_closed_date, concat(lit("STATUS_CONFLICT(status="), col("status"), lit("_without_closed_date)")))
    )
    n3 = when(col("is_extreme_duration"), concat(lit("EXTREME_DURATION("), col("resolution_duration_hours").cast("string"), lit("h)")))
    n4 = when(col("is_out_of_state_zip"), concat(lit("OUT_OF_STATE_ZIP("), col("clean_incident_zip"), lit(")")))
    n5 = when(~col("is_valid_borough"), concat(lit("UNRECOGNIZED_BOROUGH("), col("unrecognized_borough_val"), lit(")")))
    n6 = (
        when(col("is_malformed_agency"), concat(lit("MALFORMED_AGENCY("), col("malformed_agency_val"), lit(")")))
        .when((~col("is_known_agency")) & (col("agency") != "UNKNOWN"), concat(lit("UNREGISTERED_AGENCY("), col("agency"), lit(")")))
    )
    n7 = when(col("unrecognized_channel_val") != "", concat(lit("UNRECOGNIZED_CHANNEL("), col("unrecognized_channel_val"), lit(")")))
    n8 = when(col("unrecognized_status_val") != "", concat(lit("UNRECOGNIZED_STATUS("), col("unrecognized_status_val"), lit(")")))

    audit_array = array_compact(array(n1, n2, n3, n4, n5, n6, n7, n8))
    notes_str = concat_ws("; ", audit_array)
    df = df.withColumn(
        "quality_audit_notes",
        when(trim(notes_str) != "", notes_str).otherwise(lit(None).cast("string")),
    )

    # Drop temporary string helper columns from output schema
    df = df.drop(
        "unrecognized_borough_val",
        "is_malformed_agency",
        "malformed_agency_val",
        "unrecognized_channel_val",
        "unrecognized_status_val",
    )

    df = df.withColumn("_silver_processed_at_utc", lit(processed_at_utc))
    return df


def write_silver_311(df: DataFrame, silver_311_dir: Path) -> int:
    """
    Save validated Silver 311 records to partitioned Parquet files:
    year=<YYYY>/month=<MM>/
    100% pure PySpark native streaming partition write.
    """
    silver_311_dir.mkdir(parents=True, exist_ok=True)
    df_part = (
        df.withColumn("year", date_format(col("created_at_utc"), "yyyy"))
        .withColumn("month", date_format(col("created_at_utc"), "MM"))
    )
    (
        df_part.write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(str(silver_311_dir))
    )
    return len(list(silver_311_dir.glob("**/*.parquet")))


def clean_and_validate_311(
    bronze_311_dir: Path = BRONZE_DIR / "nyc311",
    silver_311_dir: Path = SILVER_DIR / "nyc311",
    quarantine_dir: Path = QUARANTINE_DIR / "nyc311",
    staging_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    100% pure PySpark Silver Layer 311 Orchestrator:
    1. Ingest Bronze Parquet files.
    2. Normalize Eastern Time timestamps to UTC and compute durations.
    3. Quarantine invalid boundary violations directly to Parquet.
    4. CDC Deduplicate valid records by unique_key.
    5. Hierarchical spatial imputation (Level 1 GPS -> Level 2 ZIP -> Level 3 Borough).
    6. Standardize categorical and administrative features.
    7. Attach quality audit notes and flags.
    8. Write clean partitioned Parquet tables via pure PySpark.
    """
    start_time = time.time()
    processed_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    now_epoch = int(datetime.now(timezone.utc).timestamp())

    if staging_dir is None:
        staging_dir = silver_311_dir.parent / ".staging_311"

    bronze_files = sorted(bronze_311_dir.glob("**/*.parquet"))
    if not bronze_files:
        logger.warning(f"No Bronze 311 files found in {bronze_311_dir}")
        return {"status": "skipped", "reason": "no_input_files"}

    logger.info(f"Loading {len(bronze_files)} Bronze 311 partition files into PySpark...")
    spark = get_spark_session("DAT535-Silver-Clean-311")
    file_paths = [str(f.resolve()) for f in bronze_files]
    df_raw = spark.read.parquet(*file_paths)
    input_rows = df_raw.count()
    logger.info(f"Loaded {input_rows:,} raw 311 records from Bronze via PySpark.")

    # 1. Temporal Normalization & Duration Derivations
    df_proc = normalize_timestamps_and_durations(df_raw, now_epoch)

    # 2. Strict Boundary Quarantine Validation
    df_valid_candidates, quarantined_count = quarantine_invalid_311_records(
        df_proc, now_epoch, quarantine_dir
    )

    candidate_count = df_valid_candidates.count()
    if candidate_count == 0:
        elapsed = time.time() - start_time
        logger.info("Silver 311 processing complete: 0 valid records retained.")
        return {
            "dataset": "silver_311",
            "input_rows": input_rows,
            "duplicate_rows_removed": 0,
            "valid_rows_accepted": 0,
            "avg_grid_distance_km": 0.0,
            "p95_grid_distance_km": 0.0,
            "closed_tickets_count": 0,
            "open_tickets_count": 0,
            "status_conflict_count": 0,
            "instant_closure_count": 0,
            "quarantined_rows": quarantined_count,
            "spatial_resolution_breakdown": {},
            "incident_clustering": {
                "total_locatable_records": 0,
                "unlocatable_records": 0,
                "duplicate_burst_calls_collapsed": 0,
                "burst_collapse_rate_pct": 0.0,
                "distinct_physical_incidents": 0,
                "route_breakdown": {},
            },
            "partitions_written": 0,
            "duration_seconds": round(elapsed, 2),
            "status": "success",
        }

    # =========================================================================
    # PHASE 1: Row-Level Normalization, Spatial Imputation & Street Canonicalization
    # =========================================================================
    logger.info("Executing Silver 311 Phase 1: Row-level normalization & spatial canonicalization...")
    df_dedup, duplicate_rows_removed = deduplicate_311_records(df_valid_candidates)
    df_spatial_clean = impute_spatial_and_canonicalize_routes(df_dedup, spark)

    # MATERIALIZE PHASE 1 TO TRANSIENT STAGING: Completely truncates Catalyst AST lineage
    staging_dir.mkdir(parents=True, exist_ok=True)
    df_spatial_clean.write.mode("overwrite").parquet(str(staging_dir))
    logger.info(f"Phase 1 materialized to transient staging: {staging_dir}")

    success = False
    try:
        # =========================================================================
        # PHASE 2: Stateful Burst Clustering, Enrichment & Final Partition Write
        # =========================================================================
        logger.info("Executing Silver 311 Phase 2: Burst clustering from clean staging...")
        df_staging = spark.read.parquet(str(staging_dir))

        df_clustered = cluster_citizen_call_bursts(df_staging, burst_window_hours=6.0)

        # Categorical Schema Drift & Semantic Conformance (Issue #8)
        logger.info("Executing PySpark Categorical Schema Drift & Semantic Conformance...")
        df_conformed = conform_complaint_taxonomy(df_clustered, spark=spark, reference_path=TAXONOMY_REFERENCE_PATH)
        upsert_unmapped_taxonomy_ledger(df_conformed, spark, ledger_path=UNMAPPED_TAXONOMY_LEDGER_PATH)

        # Unstructured Resolution Text Mining & Phantom Closure Auditing (Issue #6)
        logger.info("Executing PySpark Unstructured Resolution Text Mining & Semantic Auditing...")
        df_resolved = classify_resolution_outcomes(df_conformed)
        upsert_unclassified_resolutions_ledger(df_resolved, spark)

        df_std = standardize_categorical_and_administrative_attributes(df_resolved)
        df_silver = attach_quality_audit_tags(df_std, processed_at_utc)

        # Pure PySpark Partitioned Parquet Write (streaming distributed write)
        written_files = write_silver_311(df_silver, silver_311_dir)

        # 8. Compute Audit Metrics in PySpark directly from written Parquet (avoids driver heap bloat & plan re-eval)
        written_silver_files = sorted(silver_311_dir.glob("**/*.parquet"))
        if written_silver_files:
            df_written = spark.read.parquet(str(silver_311_dir))
            valid_rows_accepted = df_written.count()
        else:
            df_written = None
            valid_rows_accepted = 0

        if valid_rows_accepted > 0 and df_written is not None:
            cols_silver = set(df_written.columns)
            loc_expr = (
                spark_sum(when(col("loc_key").isNotNull() & (trim(col("loc_key")) != ""), 1).otherwise(0)).alias("total_loc")
                if "loc_key" in cols_silver
                else lit(0).alias("total_loc")
            )
            dup_burst_expr = (
                spark_sum(when(col("is_burst_duplicate") == True, 1).otherwise(0)).alias("dup_burst_cnt")
                if "is_burst_duplicate" in cols_silver
                else lit(0).alias("dup_burst_cnt")
            )
            field_ver_expr = (
                spark_sum(when(col("is_field_verified") == True, 1).otherwise(0)).alias("field_ver_cnt")
                if "is_field_verified" in cols_silver
                else lit(0).alias("field_ver_cnt")
            )
            phantom_expr = (
                spark_sum(when(col("is_phantom_closure") == True, 1).otherwise(0)).alias("phantom_cnt")
                if "is_phantom_closure" in cols_silver
                else lit(0).alias("phantom_cnt")
            )
            unclass_expr = (
                spark_sum(when(col("is_unclassified_resolution") == True, 1).otherwise(0)).alias("unclass_cnt")
                if "is_unclassified_resolution" in cols_silver
                else lit(0).alias("unclass_cnt")
            )
            unmapped_expr = (
                spark_sum(when(col("is_unmapped_complaint_type") == True, 1).otherwise(0)).alias("unmapped_cnt")
                if "is_unmapped_complaint_type" in cols_silver
                else lit(0).alias("unmapped_cnt")
            )
            legacy_expr = (
                spark_sum(when(col("is_legacy_complaint_name") == True, 1).otherwise(0)).alias("legacy_cnt")
                if "is_legacy_complaint_name" in cols_silver
                else lit(0).alias("legacy_cnt")
            )

            agg = df_written.select(
                spark_sum(when(col("is_ticket_closed"), 1).otherwise(0)).alias("closed_count"),
                spark_sum(when(col("is_status_conflict"), 1).otherwise(0)).alias("conflict_count"),
                spark_sum(when(col("is_instant_closure"), 1).otherwise(0)).alias("instant_count"),
                avg("grid_distance_km").alias("avg_dist"),
                percentile_approx("grid_distance_km", 0.95).alias("p95_dist"),
                loc_expr,
                dup_burst_expr,
                field_ver_expr,
                phantom_expr,
                unclass_expr,
                unmapped_expr,
                legacy_expr,
            ).first()

            closed_count = int(agg["closed_count"] or 0)
            open_count = valid_rows_accepted - closed_count
            conflict_count = int(agg["conflict_count"] or 0)
            instant_count = int(agg["instant_count"] or 0)
            avg_grid_dist = round(float(agg["avg_dist"]), 2) if agg["avg_dist"] is not None else 0.0
            p95_grid_dist = round(float(agg["p95_dist"]), 2) if agg["p95_dist"] is not None else 0.0
            total_loc = int(agg["total_loc"] or 0)
            unloc_cnt = valid_rows_accepted - total_loc
            dup_burst_cnt = int(agg["dup_burst_cnt"] or 0)
            distinct_inc = valid_rows_accepted - dup_burst_cnt
            burst_rate = round((dup_burst_cnt / total_loc * 100.0), 2) if total_loc > 0 else 0.0
            field_ver_cnt = int(agg["field_ver_cnt"] or 0)
            phantom_cnt = int(agg["phantom_cnt"] or 0)
            unclass_cnt = int(agg["unclass_cnt"] or 0)
            field_ver_rate = round((field_ver_cnt / closed_count * 100.0), 2) if closed_count > 0 else 0.0
            phantom_rate = round((phantom_cnt / closed_count * 100.0), 2) if closed_count > 0 else 0.0
            unclass_rate = round((unclass_cnt / closed_count * 100.0), 2) if closed_count > 0 else 0.0

            unmapped_cnt = int(agg["unmapped_cnt"] or 0)
            legacy_cnt = int(agg["legacy_cnt"] or 0)
            conformed_cnt = valid_rows_accepted - unmapped_cnt
            unmapped_rate = round((unmapped_cnt / valid_rows_accepted * 100.0), 2) if valid_rows_accepted > 0 else 0.0
            conformed_rate = round((conformed_cnt / valid_rows_accepted * 100.0), 2) if valid_rows_accepted > 0 else 0.0

            spatial_rows = df_written.groupBy("spatial_resolution_level").count().collect()
            spatial_stats = {r["spatial_resolution_level"]: int(r["count"]) for r in spatial_rows}

            if "loc_routing_type" in cols_silver:
                route_rows = df_written.groupBy("loc_routing_type").count().collect()
                routes_dict = {r["loc_routing_type"]: int(r["count"]) for r in route_rows}
            else:
                routes_dict = {}

            if "resolution_outcome_category" in cols_silver:
                res_rows = df_written.groupBy("resolution_outcome_category").count().collect()
                res_dict = {r["resolution_outcome_category"]: int(r["count"]) for r in res_rows}
            else:
                res_dict = {}

            if "canonical_service_domain" in cols_silver:
                domain_rows = df_written.groupBy("canonical_service_domain").count().collect()
                domain_dict = {r["canonical_service_domain"]: int(r["count"]) for r in domain_rows}
            else:
                domain_dict = {}
        else:
            closed_count = 0
            open_count = 0
            conflict_count = 0
            instant_count = 0
            avg_grid_dist = 0.0
            p95_grid_dist = 0.0
            total_loc = 0
            unloc_cnt = 0
            dup_burst_cnt = 0
            distinct_inc = 0
            burst_rate = 0.0
            field_ver_cnt = 0
            phantom_cnt = 0
            phantom_rate = 0.0
            unmapped_cnt = 0
            legacy_cnt = 0
            conformed_cnt = 0
            unmapped_rate = 0.0
            conformed_rate = 0.0
            spatial_stats = {}
            routes_dict = {}
            res_dict = {}
            domain_dict = {}

        elapsed = time.time() - start_time
        logger.info(
            f"Completed Silver 311 processing: {valid_rows_accepted:,} valid records | "
            f"{quarantined_count:,} quarantined | {written_files} partition files written | {elapsed:.2f}s"
        )

        success = True
        return {
            "dataset": "silver_311",
            "input_rows": input_rows,
            "duplicate_rows_removed": duplicate_rows_removed,
            "valid_rows_accepted": valid_rows_accepted,
            "avg_grid_distance_km": avg_grid_dist,
            "p95_grid_distance_km": p95_grid_dist,
            "closed_tickets_count": closed_count,
            "open_tickets_count": open_count,
            "status_conflict_count": conflict_count,
            "instant_closure_count": instant_count,
            "quarantined_rows": quarantined_count,
            "spatial_resolution_breakdown": spatial_stats,
            "incident_clustering": {
                "total_locatable_records": total_loc,
                "unlocatable_records": unloc_cnt,
                "duplicate_burst_calls_collapsed": dup_burst_cnt,
                "burst_collapse_rate_pct": burst_rate,
                "distinct_physical_incidents": distinct_inc,
                "route_breakdown": routes_dict,
            },
            "resolution_text_mining": {
                "field_verified_remediations": field_ver_cnt,
                "field_verified_rate_pct": field_ver_rate,
                "phantom_closures": phantom_cnt,
                "phantom_closure_rate_pct": phantom_rate,
                "unclassified_resolutions": unclass_cnt,
                "unclassified_rate_pct": unclass_rate,
                "outcome_breakdown": res_dict,
            },
            "semantic_conformance": {
                "total_records": valid_rows_accepted,
                "conformed_records": conformed_cnt,
                "conformed_rate_pct": conformed_rate,
                "unmapped_records": unmapped_cnt,
                "unmapped_rate_pct": unmapped_rate,
                "legacy_mapped_records": legacy_cnt,
                "domain_breakdown": domain_dict,
            },
            "partitions_written": written_files,
            "duration_seconds": round(elapsed, 2),
            "status": "success",
        }
    finally:
        # EPHEMERAL CLEANUP: Intermediate staging scratch directory is cleaned up on success
        if success and staging_dir.exists():
            shutil.rmtree(staging_dir, ignore_errors=True)
            logger.info(f"Ephemeral staging directory cleaned up successfully: {staging_dir}")
        elif not success and staging_dir.exists():
            logger.warning(f"Preserving intermediate staging directory for root-cause triage: {staging_dir}")


if __name__ == "__main__":
    clean_and_validate_311()
