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
from pyspark.sql.functions import (
    asin,
    broadcast,
    col,
    concat,
    cos,
    date_trunc,
    dayofweek,
    desc,
    from_utc_timestamp,
    hour,
    lit,
    month as spark_month,
    radians,
    regexp_extract,
    round as spark_round,
    row_number,
    sin,
    sqrt,
    to_timestamp,
    to_utc_timestamp,
    trim,
    upper,
    when,
)
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel

from src.utils.spark import get_spark_session

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
    Safeguards:
      1. Defensive dimension check: returns NaN Series with a warning if input lengths mismatch (never raises).
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

    # Extract positional 1D NumPy float arrays to prevent Pandas index misalignment
    v_lat1 = pd.to_numeric(pd.Series(lat1).to_numpy(), errors="coerce").astype(float)
    v_lon1 = pd.to_numeric(pd.Series(lon1).to_numpy(), errors="coerce").astype(float)
    v_lat2 = pd.to_numeric(pd.Series(lat2).to_numpy(), errors="coerce").astype(float)
    v_lon2 = pd.to_numeric(pd.Series(lon2).to_numpy(), errors="coerce").astype(float)

    # Geographic boundary check (-90..90 for lat, -180..180 for lon)
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

    logger.info(f"Loading {len(bronze_files)} Bronze 311 partition files into PySpark...")
    spark = get_spark_session("DAT535-Silver-Clean-311")
    file_paths = [str(f.resolve()) for f in bronze_files]
    df_raw_spark = spark.read.parquet(*file_paths)
    input_rows = df_raw_spark.count()
    logger.info(f"Loaded {input_rows:,} raw 311 records from Bronze via PySpark.")

    # 1. PySpark Timestamp Normalization (America/New_York -> UTC) & Temporal Validation on Raw Bronze
    # Normalize created_date to UTC timestamp directly within Spark SQL
    cols = set(df_raw_spark.columns)
    created_ts = to_utc_timestamp(to_timestamp(col("created_date")), "America/New_York")
    df_spark_proc = df_raw_spark.withColumn("created_at_utc", created_ts)

    if "closed_date" in df_spark_proc.columns:
        closed_ts = to_utc_timestamp(to_timestamp(col("closed_date")), "America/New_York")
        df_spark_proc = df_spark_proc.withColumn("closed_at_utc", closed_ts)
    else:
        df_spark_proc = df_spark_proc.withColumn("closed_at_utc", lit(None).cast("timestamp"))

    # Open vs. Closed Ticket Semantics in PySpark
    df_spark_proc = df_spark_proc.withColumn("is_ticket_closed", col("closed_at_utc").isNotNull())
    df_spark_proc = df_spark_proc.withColumn(
        "ticket_status_category",
        when(col("is_ticket_closed"), lit("CLOSED")).otherwise(lit("OPEN")),
    )

    # Resolution duration in hours (only applicable to closed tickets)
    duration_secs = col("closed_at_utc").cast("long") - col("created_at_utc").cast("long")
    df_spark_proc = df_spark_proc.withColumn(
        "resolution_duration_hours",
        when(col("is_ticket_closed"), spark_round(duration_secs / 3600.0, 2)).otherwise(lit(None).cast("double")),
    )

    # Active queue age in days (relative to current UTC runtime)
    now_utc = datetime.now(timezone.utc)
    now_epoch = int(now_utc.timestamp())
    open_age_secs = lit(now_epoch) - col("created_at_utc").cast("long")
    df_spark_proc = df_spark_proc.withColumn(
        "open_ticket_age_days",
        when(~col("is_ticket_closed"), spark_round(open_age_secs / 86400.0, 2)).otherwise(lit(None).cast("double")),
    )

    # Hourly join key for UTC weather alignment
    df_spark_proc = df_spark_proc.withColumn("weather_join_hour_utc", date_trunc("hour", col("created_at_utc")))

    # Calendar row attributes derived from local New York time in PySpark
    local_ts = from_utc_timestamp(col("created_at_utc"), "America/New_York")
    df_spark_proc = df_spark_proc.withColumn("hour_of_day", hour(col("created_at_utc")))
    df_spark_proc = df_spark_proc.withColumn("created_hour_local", hour(local_ts))
    df_spark_proc = df_spark_proc.withColumn("day_of_week", (dayofweek(local_ts) + 5) % 7)
    df_spark_proc = df_spark_proc.withColumn("is_weekend", col("day_of_week").isin([5, 6]))
    df_spark_proc = df_spark_proc.withColumn("month_of_year", spark_month(local_ts))

    # 2. Hard Quarantine Boundary Validation (Spark SQL Boolean Predicates on Raw Stream)
    # Evaluated BEFORE deduplication so corrupted/unkeyed records are NEVER dropped as duplicates of null!
    parsed_keys = col("unique_key").cast("long")
    launch_epoch = int(datetime(2003, 3, 1, tzinfo=timezone.utc).timestamp())
    future_grace_epoch = now_epoch + 7200

    cond_missing_created = col("created_at_utc").isNull()
    cond_negative_duration = col("is_ticket_closed") & (col("resolution_duration_hours") < 0.0)
    cond_invalid_key = parsed_keys.isNull() | (parsed_keys <= 0)
    cond_pre_epoch = col("created_at_utc").isNotNull() & (col("created_at_utc").cast("long") < lit(launch_epoch))
    cond_future_created = col("created_at_utc").isNotNull() & (col("created_at_utc").cast("long") > lit(future_grace_epoch))
    cond_future_closed = col("closed_at_utc").isNotNull() & (col("closed_at_utc").cast("long") > lit(future_grace_epoch))

    if "_batch_id" in df_spark_proc.columns:
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

    df_quarantined_spark = df_spark_proc.filter(quarantine_cond)
    df_valid_candidates = df_spark_proc.filter(~quarantine_cond)

    # 3. CDC Deduplication via PySpark Window (on Valid Records with Known Unique Key)
    if "_ingested_at_utc" in cols:
        order_col = col("_ingested_at_utc").desc()
    elif "created_date" in cols:
        order_col = col("created_date").desc()
    else:
        order_col = col("unique_key").desc()

    w_dedup = Window.partitionBy("unique_key").orderBy(order_col)
    df_valid_spark = df_valid_candidates.withColumn("_rn", row_number().over(w_dedup)).filter(col("_rn") == 1).drop("_rn")
    df_valid_spark.persist(StorageLevel.MEMORY_AND_DISK)
    valid_count = df_valid_spark.count()
    duplicate_rows_removed = df_valid_candidates.count() - valid_count
    if duplicate_rows_removed > 0:
        logger.info(f"Deduplication (PySpark): removed {duplicate_rows_removed:,} duplicate records on unique_key.")

    # 3b. Hierarchical Spatial Imputation & Haversine Distance in PySpark
    raw_lats = col("latitude").cast("double") if "latitude" in cols else lit(None).cast("double")
    raw_lons = col("longitude").cast("double") if "longitude" in cols else lit(None).cast("double")

    if "incident_zip" in cols:
        extracted_zips = regexp_extract(col("incident_zip").cast("string"), r"(\d{5})", 1)
        clean_zip_expr = when(extracted_zips.isin(["", "nan", "None", "00000_invalid"]), lit(None)).otherwise(extracted_zips)
    else:
        clean_zip_expr = lit(None).cast("string")

    df_valid_spark = df_valid_spark.withColumn("clean_incident_zip", clean_zip_expr)

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

    # Level 2: ZIP Centroid Lookup (via Broadcast Join of Reference Table)
    zip_ref = load_zip_reference()
    if not zip_ref.empty:
        z_sub = zip_ref[["zip5", "centroid_lat", "centroid_lon", "grid_id"]].rename(
            columns={
                "zip5": "_ref_zip5",
                "centroid_lat": "_ref_lat",
                "centroid_lon": "_ref_lon",
                "grid_id": "_ref_grid",
            }
        )
        df_zip_ref = spark.createDataFrame(z_sub)
        df_valid_spark = df_valid_spark.join(
            broadcast(df_zip_ref),
            col("clean_incident_zip") == col("_ref_zip5"),
            how="left",
        ).drop("_ref_zip5")
    else:
        df_valid_spark = (
            df_valid_spark.withColumn("_ref_lat", lit(None).cast("double"))
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
    df_valid_spark = df_valid_spark.withColumn(
        "clean_latitude",
        when(mask_l1, clean_lat_l1)
        .when(col("_ref_lat").isNotNull(), col("_ref_lat"))
        .when(b_lat_expr.isNotNull(), b_lat_expr)
        .otherwise(lit(None).cast("double")),
    )
    df_valid_spark = df_valid_spark.withColumn(
        "clean_longitude",
        when(mask_l1, clean_lon_l1)
        .when(col("_ref_lon").isNotNull(), col("_ref_lon"))
        .when(b_lon_expr.isNotNull(), b_lon_expr)
        .otherwise(lit(None).cast("double")),
    )
    df_valid_spark = df_valid_spark.withColumn(
        "grid_id",
        when(mask_l1, grid_id_l1)
        .when(col("_ref_grid").isNotNull(), col("_ref_grid"))
        .when(b_grid_expr != lit("UNKNOWN"), b_grid_expr)
        .otherwise(lit("UNKNOWN")),
    )
    df_valid_spark = df_valid_spark.withColumn(
        "spatial_resolution_level",
        when(mask_l1, lit("EXACT_COORDINATES"))
        .when(col("_ref_lat").isNotNull(), lit("ZIP_APPROXIMATION"))
        .when(b_lat_expr.isNotNull(), lit("BOROUGH_APPROXIMATION"))
        .otherwise(lit("UNLOCATED")),
    ).drop("_ref_lat", "_ref_lon", "_ref_grid")

    # 4. Spatial Uncertainty Quantification (Great-Circle Haversine Distance in Catalyst PySpark)
    grid_pattern = r"^GRID_([0-9.-]+)_([0-9.-]+)$"
    grid_center_lat = regexp_extract(col("grid_id"), grid_pattern, 1).cast("double")
    grid_center_lon = regexp_extract(col("grid_id"), grid_pattern, 2).cast("double")
    has_coords = (
        col("clean_latitude").isNotNull()
        & col("clean_longitude").isNotNull()
        & grid_center_lat.isNotNull()
        & grid_center_lon.isNotNull()
    )

    df_valid_spark = df_valid_spark.withColumn(
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

    df_valid_spark.persist(StorageLevel.MEMORY_AND_DISK)

    df_quarantined = df_quarantined_spark.toPandas().copy()
    if len(df_quarantined) > 0:
        df_quarantined["created_at_utc"] = pd.to_datetime(df_quarantined["created_at_utc"], utc=True)
        if "closed_at_utc" in df_quarantined.columns:
            df_quarantined["closed_at_utc"] = pd.to_datetime(df_quarantined["closed_at_utc"], utc=True)
        q_keys = pd.to_numeric(df_quarantined["unique_key"], errors="coerce")
        q_now = pd.Timestamp.now(tz=timezone.utc)
        q_grace = q_now + pd.Timedelta(hours=2)
        q_launch = pd.Timestamp(NYC_311_LAUNCH_DATE)

        q_missing_cr = df_quarantined["created_at_utc"].isna()
        q_neg_dur = df_quarantined["is_ticket_closed"] & (df_quarantined["resolution_duration_hours"] < 0.0)
        q_bad_key = q_keys.isna() | (q_keys <= 0)
        q_pre_ep = df_quarantined["created_at_utc"].notna() & (df_quarantined["created_at_utc"] < q_launch)
        q_fut_cr = df_quarantined["created_at_utc"].notna() & (df_quarantined["created_at_utc"] > q_grace)
        q_fut_cl = df_quarantined["closed_at_utc"].notna() & (df_quarantined["closed_at_utc"] > q_grace)
        q_corrupt = (
            (df_quarantined["_batch_id"].isna() | df_quarantined["_batch_id"].astype(str).str.strip().isin(["", "nan", "None", "<NA>"]))
            if "_batch_id" in df_quarantined.columns
            else pd.Series(False, index=df_quarantined.index)
        )
        q_idx = df_quarantined.index
        rule_checks = [
            (q_missing_cr.loc[q_idx], "MISSING_CREATED_TIMESTAMP"),
            (q_bad_key.loc[q_idx], "INVALID_OR_NON_NUMERIC_UNIQUE_KEY"),
            (q_pre_ep.loc[q_idx], "PRE_311_EPOCH_DATE"),
            (q_fut_cr.loc[q_idx], "FUTURE_CREATED_DATE"),
            (q_fut_cl.loc[q_idx], "FUTURE_CLOSED_DATE"),
            (q_neg_dur.loc[q_idx], "NEGATIVE_RESOLUTION_DURATION"),
            (q_corrupt.loc[q_idx], "CORRUPTED_INGESTION_METADATA"),
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
    df_valid = df_valid_spark.toPandas().copy()
    df_valid_spark.unpersist()

    df_valid["created_at_utc"] = pd.to_datetime(df_valid["created_at_utc"], utc=True)
    if "closed_at_utc" in df_valid.columns:
        df_valid["closed_at_utc"] = pd.to_datetime(df_valid["closed_at_utc"], utc=True)
    df_valid["unique_key"] = pd.to_numeric(df_valid["unique_key"], errors="coerce").astype("Int64")

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

    # 3. Spatial Resolution & Uncertainty Metrics (Computed natively in PySpark Catalyst)
    # Parse coordinates to numeric for schema compliance
    raw_lats = pd.to_numeric(df_valid["latitude"], errors="coerce") if "latitude" in df_valid.columns else pd.Series(np.nan, index=df_valid.index)
    raw_lons = pd.to_numeric(df_valid["longitude"], errors="coerce") if "longitude" in df_valid.columns else pd.Series(np.nan, index=df_valid.index)

    # Validate and normalize NYC Boroughs (5 Legal Boroughs + UNSPECIFIED)
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

    # Fallback to vectorized NumPy/Pandas only if grid_distance_km was not precomputed by PySpark
    if "grid_distance_km" not in df_valid.columns or df_valid["grid_distance_km"].isna().all():
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
