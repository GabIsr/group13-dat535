"""
Silver Layer: NYC 311 Service Requests Cleaning, Deduplication, and Spatial Imputation.
100% Pure PySpark Pipeline:
Applies Universal Schema Conformance, Strict Boundary Quarantining, CDC Deduplication,
Temporal/Operational Feature Enrichment, 3-level Hierarchical Spatial Imputation
(Coordinates -> ZIP Centroid -> Borough Centroid), 6-Route Location Key Generation,
Zero-Copy Arrow Burst Clustering, Taxonomy Conformance, and Resolution Text Mining.

Author: Group 13
"""

import logging
import shutil
from functools import reduce
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    array,
    array_compact,
    asin,
    avg,
    broadcast,
    coalesce,
    col,
    concat,
    concat_ws,
    cos,
    date_format,
    date_trunc,
    dayofweek,
    from_utc_timestamp,
    greatest,
    hour,
    initcap,
    input_file_name,
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
    to_date,
    to_timestamp,
    to_utc_timestamp,
    trim,
    upper,
    when,
)
from pyspark.sql.window import Window

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
from src.utils.benchmark import get_profiler
from src.silver.config import (
    BOROUGH_CENTROIDS,
    BRONZE_DIR,
    KNOWN_311_STATUSES,
    KNOWN_NYC_AGENCIES,
    NYC_COUNCIL_DISTRICT_MAX,
    NYC_COUNCIL_DISTRICT_MIN,
    NYC_311_LAUNCH_DATE,
    NYC_LAT_MAX,
    NYC_LAT_MIN,
    NYC_LON_MAX,
    NYC_LON_MIN,
    NYC_METRO_ZIP_PREFIXES,
    NYC_STATE_PLANE_BOUNDS,
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

def spark_haversine_distance_km(lat1_col, lon1_col, lat2_col, lon2_col):
    """
    Compute Great-Circle Haversine distance in km directly in PySpark Catalyst.
    Safeguards against out-of-bounds coordinates and clips intermediate values
    to [0.0, 1.0] to prevent floating-point NaN in asin.
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


# Conformance rule tables (single source of truth for Step 1)
NULL_SENTINELS = ["", "NAN", "NONE", "NULL", "<NA>", "N/A"]
TITLE_CASE_COLS = {"complaint_type", "descriptor", "descriptor_2", "agency_name"}
UPPER_CASE_COLS = {
    "borough", "agency", "status", "open_data_channel_type", "address_type", "city",
    "incident_address", "street_name", "cross_street_1", "cross_street_2",
    "intersection_street_1", "intersection_street_2", "landmark", "facility_type",
    "park_facility_name", "park_borough", "community_board", "bridge_highway_name",
    "bridge_highway_direction", "bridge_highway_segment", "road_ramp",
    "taxi_company_borough", "taxi_pick_up_location", "vehicle_type", "resolution_description",
}
NUMERIC_CASTS = {
    "unique_key": "long", "council_district": "short",
    "latitude": "double", "longitude": "double",
    "x_coordinate_state_plane": "double", "y_coordinate_state_plane": "double",
}
TIMESTAMP_COLS = {  # raw America/New_York string -> UTC timestamp
    "created_date": "created_at_utc",
    "closed_date": "closed_at_utc",
    "due_date": "due_date_utc",
    "resolution_action_updated_date": "resolution_action_updated_date_utc",
}
# Columns downstream steps rely on; added as NULL if Bronze lacks them (replaces per-step "if col in cols" branches)
REQUIRED_RAW_COLS = (
    list(NUMERIC_CASTS) + list(TIMESTAMP_COLS)
    + ["police_precinct", "borough", "agency", "agency_name", "complaint_type", "descriptor",
       "status", "open_data_channel_type", "incident_zip", "resolution_description"]
)
OPEN_STATUSES = ["OPEN", "IN PROGRESS", "PENDING", "ASSIGNED", "STARTED"]
CLOSED_STATUSES = ["CLOSED"]
EXTREME_DURATION_HOURS = 17520.0  # > 2 years


def _ensure_cols(df: DataFrame, spec: Dict[str, str]) -> DataFrame:
    """Add any missing columns as typed NULLs (no-op when all exist)."""
    missing = [(c, t) for c, t in spec.items() if c not in df.columns]
    return df.select("*", *[lit(None).cast(t).alias(c) for c, t in missing]) if missing else df


def conform_and_cast_311_schema(df: DataFrame) -> DataFrame:
    """
    Step 1: Universal Schema Conformance in ONE projection:
    trim -> sentinel-to-NULL (case-insensitive) -> casing (initcap/upper) -> numeric casts
    -> all 4 timestamps converted America/New_York -> UTC.
    """
    df = _ensure_cols(df, {c: "string" for c in REQUIRED_RAW_COLS})
    exprs, cleaned = [], {}
    for c, c_type in df.dtypes:
        e = col(c)
        if c_type == "string" and not c.startswith("_"):
            trimmed = trim(e)
            e = when(upper(trimmed).isin(NULL_SENTINELS), lit(None)).otherwise(trimmed)
            if c in TITLE_CASE_COLS:
                e = initcap(e)
            elif c in UPPER_CASE_COLS:
                e = upper(e)
        cleaned[c] = e
        if c in NUMERIC_CASTS:
            e = e.cast(NUMERIC_CASTS[c])
        elif c == "police_precinct":
            digits = regexp_extract(e, r"(\d+)", 1)
            e = when(digits != "", digits.cast("short"))
        exprs.append(e.alias(c))

    exprs += [
        to_utc_timestamp(to_timestamp(cleaned[src]), "America/New_York").alias(dst)
        for src, dst in TIMESTAMP_COLS.items()
    ]
    return df.select(*exprs)

def quarantine_invalid_311_records(
    df: DataFrame,
    now_epoch: int,
    quarantine_dir: Path,
) -> Tuple[DataFrame, int]:
    """
    Step 2: Evaluate strict boundary validation predicates directly on typed fields.
    Routes corrupted, unkeyed, negative-duration, or future records to quarantine Parquet
    BEFORE deduplication so corrupted records are never collapsed as nulls.
    """
    launch_dt = datetime.fromisoformat(NYC_311_LAUNCH_DATE.replace("Z", "+00:00"))
    launch_epoch = int(launch_dt.timestamp())
    future_grace_epoch = now_epoch + 7200

    cond_missing_created = col("created_at_utc").isNull()
    cond_invalid_key = col("unique_key").isNull() | (col("unique_key") <= 0)
    cond_pre_epoch = col("created_at_utc").isNotNull() & (col("created_at_utc").cast("long") < lit(launch_epoch))
    cond_future_created = col("created_at_utc").isNotNull() & (col("created_at_utc").cast("long") > lit(future_grace_epoch))
    cond_future_closed = col("closed_at_utc").isNotNull() & (col("closed_at_utc").cast("long") > lit(future_grace_epoch))
    cond_negative_duration = col("closed_at_utc").isNotNull() & (col("closed_at_utc").cast("long") < col("created_at_utc").cast("long"))

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
        df_quarantined.coalesce(1).write.mode("append").parquet(str(q_path))
        logger.warning(f"Quarantined {quarantined_count:,} invalid records to {q_path}")

    df_valid_candidates = df.filter(~quarantine_cond)
    return df_valid_candidates, quarantined_count

def deduplicate_311_records(df: DataFrame, candidate_count: Optional[int] = None) -> Tuple[DataFrame, int]:
    """
    Step 3: Perform CDC deduplication via PySpark Window on clean, valid records.
    Prioritizes:
      1. Authoritative business mutation timestamps:
         greatest(coalesce(closed_at_utc, created_at_utc), coalesce(resolution_action_updated_at_utc, created_at_utc)) DESC
      2. Lakehouse pipeline arrival timestamp & batch lineage:
         _ingested_at_utc DESC, _batch_id DESC
      3. Deterministic tie-breaker:
         unique_key DESC
    """
    created = col("created_at_utc")
    order_cols = [
        greatest(
            coalesce(col("closed_at_utc"), created),
            coalesce(col("resolution_action_updated_date_utc"), created),
        ).desc()
    ]
    order_cols += [col(c).desc() for c in ("_ingested_at_utc", "_batch_id") if c in df.columns]
    order_cols.append(col("unique_key").desc())  # deterministic tie-breaker

    w_dedup = Window.partitionBy("unique_key").orderBy(*order_cols)
    df_dedup = df.withColumn("_rn", row_number().over(w_dedup)).filter(col("_rn") == 1).drop("_rn")

    valid_count = df_dedup.count()
    if candidate_count is None:
        candidate_count = df.count()
    duplicate_rows_removed = candidate_count - valid_count
    if duplicate_rows_removed > 0:
        logger.info(f"Deduplication (PySpark): removed {duplicate_rows_removed:,} duplicate records on unique_key.")

    return df_dedup, duplicate_rows_removed

def enrich_temporal_metrics(df: DataFrame, now_epoch: int) -> DataFrame:
    """
    Step 4: Operational Duration & Calendar Metrics Enrichment.
    Executes strictly post-deduplication on clean, validated records.
    Computes ticket status categories, durations, active ticket age, calendar features,
    and duration/status-derived quality flags (all depend only on Step 1 outputs).
    """
    is_closed = col("closed_at_utc").isNotNull()
    duration_secs = col("closed_at_utc").cast("long") - col("created_at_utc").cast("long")
    duration_hours = spark_round(duration_secs / 3600.0, 2)
    local_ts = from_utc_timestamp(col("created_at_utc"), "America/New_York")
    status = coalesce(col("status"), lit("UNKNOWN"))
    open_with_closed_date = is_closed & status.isin(OPEN_STATUSES)
    closed_without_closed_date = (~is_closed) & status.isin(CLOSED_STATUSES)

    return (
        df.withColumn("is_ticket_closed", is_closed)
        .withColumn("is_instant_closure", is_closed & (duration_secs == 0))
        .withColumn("is_status_conflict", open_with_closed_date | closed_without_closed_date)
        .withColumn("is_extreme_duration", is_closed & (duration_hours > EXTREME_DURATION_HOURS))
        .withColumn("ticket_status_category", when(is_closed, lit("CLOSED")).otherwise(lit("OPEN")))
        .withColumn("resolution_duration_hours", when(is_closed, duration_hours))
        .withColumn("weather_join_hour_utc", date_trunc("hour", col("created_at_utc")))
        .withColumn("created_hour_utc", hour(col("created_at_utc")))
        .withColumn("created_hour_local", hour(local_ts))
        .withColumn("day_of_week", (dayofweek(local_ts) + 5) % 7)
        .withColumn("is_weekend", col("day_of_week").isin([5, 6]))
        .withColumn("month_of_year", spark_month(local_ts))
    )

def impute_spatial_and_canonicalize_routes(
    df: DataFrame,
    spark: SparkSession,
    zip_ref_path: Path = ZIP_CENTROIDS_PATH,
) -> DataFrame:
    """
    Step 5: Apply 3-level hierarchical spatial imputation (GPS -> ZIP Centroid -> Borough Centroid),
    quantify spatial distance uncertainty via Catalyst PySpark Haversine,
    and evaluate 6-route hierarchical location keys (loc_key).
    """
    raw_lats, raw_lons = col("latitude"), col("longitude")

    extracted_zips = regexp_extract(col("incident_zip"), r"(\d{5})", 1)
    df = df.withColumn("clean_incident_zip", when(extracted_zips != "", extracted_zips))

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

    # Level 3: Borough Centroid Fallback (chained CASE WHEN over the 5 boroughs, one per output)
    def _borough_pick(idx: int, default):
        return reduce(
            lambda acc, kv: when(col("borough") == kv[0], lit(kv[1][idx])).otherwise(acc),
            BOROUGH_CENTROIDS.items(),
            default,
        )

    b_lat_expr = _borough_pick(0, lit(None).cast("double"))
    b_lon_expr = _borough_pick(1, lit(None).cast("double"))
    b_grid_expr = _borough_pick(2, lit("UNKNOWN"))

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

    df = df.withColumn(
        "grid_distance_km",
        spark_haversine_distance_km(
            col("clean_latitude"),
            col("clean_longitude"),
            grid_center_lat,
            grid_center_lon,
        ),
    )

    # PySpark Spatial Feature Normalization, Hierarchical Routing
    logger.info("Executing PySpark Spatial Normalization & Canonicalization...")
    df = normalize_all_spatial_features(df)
    
    # Truncate Catalyst AST lineage between regex cleaning and 6-route routing
    # to keep WholeStageCodegen JVM bytecode < 30 KB
    df = df.localCheckpoint(eager=True)

    logger.info("Evaluating 6-Route Hierarchical Spatial Routing in PySpark...")
    df = generate_hierarchical_location_keys(df)

    return df

def _validate_domain(df: DataFrame, name: str, allowed, default: str, val_col: str, replace_unrecognized: bool = True) -> DataFrame:
    """Park unrecognized raw value in `val_col` ('' if NULL/valid); NULL -> `default`; unrecognized -> `default` only if replace_unrecognized."""
    c = col(name)
    unrecognized = ~c.isin(list(allowed))
    kept = when(~unrecognized, c) if replace_unrecognized else c
    return (
        df.withColumn(val_col, when(unrecognized, c).otherwise(lit("")))
        .withColumn(name, coalesce(kept, lit(default)))
    )


def validate_administrative_bounds(df: DataFrame) -> DataFrame:
    """
    Step 9: Domain validation of already-typed fields: boroughs, agencies, statuses, channels,
    State Plane coordinates, council districts, police precincts; NULL defaults for names.
    Derives soft tags (out-of-state ZIP, valid borough, known/malformed agency).
    """
    # Out-of-state ZIP tag (first 3 digits outside NYC metro prefixes)
    prefix_3 = substring(col("clean_incident_zip"), 1, 3)
    df = df.withColumn(
        "is_out_of_state_zip",
        col("clean_incident_zip").isNotNull() & (~prefix_3.isin(list(NYC_METRO_ZIP_PREFIXES))),
    )

    # Borough normalization & soft tagging
    b_clean = coalesce(col("borough"), lit("UNSPECIFIED"))
    is_rec_b = b_clean.isin(list(VALID_NYC_BOROUGHS))
    df = (
        df.withColumn("is_valid_borough", is_rec_b)
        .withColumn("unrecognized_borough_val", when(~is_rec_b, b_clean).otherwise(lit("")))
        .withColumn("borough", when(is_rec_b, b_clean).otherwise(lit("UNSPECIFIED")))
    )

    # Agency syntactic & registry validation (NULL -> UNKNOWN; numeric-only (except 311), bad length or bad chars = malformed)
    agency = coalesce(col("agency"), lit("UNKNOWN"))
    is_known_token = agency != "UNKNOWN"
    is_malformed_a = (
        (agency.rlike(r"^\d+$") & ~agency.isin("311", "3-1-1"))
        | (is_known_token & ((length(agency) < 2) | (length(agency) > 10)))
        | (is_known_token & ~agency.rlike(r"^[A-Z0-9\-]+$"))
    )
    final_agency = when(is_malformed_a, lit("UNKNOWN")).otherwise(agency)
    df = (
        df.withColumn("is_malformed_agency", is_malformed_a)
        .withColumn("malformed_agency_val", when(is_malformed_a, agency).otherwise(lit("")))
        .withColumn("agency", final_agency)
        .withColumn("is_known_agency", col("agency").isin(list(KNOWN_NYC_AGENCIES)))
    )

    # Closed-vocabulary categorical validation
    df = _validate_domain(df, "status", KNOWN_311_STATUSES, "UNKNOWN", "unrecognized_status_val", replace_unrecognized=False)
    df = _validate_domain(df, "open_data_channel_type", VALID_OPEN_DATA_CHANNELS, "UNKNOWN", "unrecognized_channel_val")

    # Descriptive name defaults
    for name_col, default in (("complaint_type", "Unknown"), ("descriptor", "Not Stated"), ("agency_name", "Unknown")):
        df = df.withColumn(name_col, coalesce(col(name_col), lit(default)))

    # Numeric sanity bounds (out-of-range -> NULL): State Plane, NYC Charter 51 council districts, NYPD precincts 1-123
    bounds = {
        "x_coordinate_state_plane": (NYC_STATE_PLANE_BOUNDS["x_min"], NYC_STATE_PLANE_BOUNDS["x_max"]),
        "y_coordinate_state_plane": (NYC_STATE_PLANE_BOUNDS["y_min"], NYC_STATE_PLANE_BOUNDS["y_max"]),
        "council_district": (NYC_COUNCIL_DISTRICT_MIN, NYC_COUNCIL_DISTRICT_MAX),
        "police_precinct": (1, 123),
    }
    for name, (lo, hi) in bounds.items():
        df = df.withColumn(name, when(col(name).between(lo, hi), col(name)))

    return df

def attach_quality_audit_tags(df: DataFrame, processed_at_utc: str) -> DataFrame:
    """
    Step 9b: Construct human-readable quality audit notes string from precomputed flags.
    Attach Silver metadata and prune intermediate string helpers.
    """
    status_conflict_suffix = when(col("is_ticket_closed"), lit("_with_closed_date)")).otherwise(lit("_without_closed_date)"))

    # Observability Audit String Construction in PySpark Catalyst
    n1 = when(col("is_instant_closure"), lit("INSTANT_CLOSURE_0SEC"))
    n2 = when(col("is_status_conflict"), concat(lit("STATUS_CONFLICT(status="), col("status"), status_conflict_suffix))
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


# =========================================================================
# Reconciliation Audit Report (computed from written Silver Parquet)
# =========================================================================
_FLAG_COUNTS = {  # report key -> boolean Silver column
    "closed_count": "is_ticket_closed",
    "conflict_count": "is_status_conflict",
    "instant_count": "is_instant_closure",
    "dup_burst_cnt": "is_burst_duplicate",
    "field_ver_cnt": "is_field_verified",
    "phantom_cnt": "is_phantom_closure",
    "unclass_cnt": "is_unclassified_resolution",
    "unmapped_cnt": "is_unmapped_complaint_type",
    "legacy_cnt": "is_legacy_complaint_name",
}


def _pct(num: int, den: int) -> float:
    return round(num / den * 100.0, 2) if den > 0 else 0.0


def _build_silver_report(
    df_written: Optional[DataFrame],
    input_rows: int,
    duplicate_rows_removed: int,
    valid_rows: int,
    quarantined_count: int,
    written_files: int,
    elapsed: float,
) -> Dict[str, Any]:
    """Aggregate audit metrics in one Spark pass (+1 small groupBy per breakdown); zeroed when nothing was written."""
    agg: Dict[str, Any] = {}
    breakdown: Dict[str, Dict[str, int]] = {}
    if df_written is not None and valid_rows > 0:
        cols = set(df_written.columns)
        exprs = [
            (spark_sum(when(col(c), 1).otherwise(0)) if c in cols else lit(0)).alias(k)
            for k, c in _FLAG_COUNTS.items()
        ]
        exprs.append(
            (spark_sum(when(col("loc_key").isNotNull() & (trim(col("loc_key")) != ""), 1).otherwise(0))
             if "loc_key" in cols else lit(0)).alias("total_loc")
        )
        exprs += [avg("grid_distance_km").alias("avg_dist"), percentile_approx("grid_distance_km", 0.95).alias("p95_dist")]
        agg = df_written.select(*exprs).first().asDict()
        breakdown = {
            c: {str(r[0]): int(r["count"]) for r in df_written.filter(col(c).isNotNull()).groupBy(c).count().collect()}
            for c in ("spatial_resolution_level", "loc_routing_type", "resolution_outcome_category", "canonical_service_domain")
            if c in cols
        }

    g = lambda k: int(agg.get(k) or 0)
    closed, total_loc, dup_burst, unmapped = g("closed_count"), g("total_loc"), g("dup_burst_cnt"), g("unmapped_cnt")
    return {
        "dataset": "silver_311",
        "input_rows": input_rows,
        "duplicate_rows_removed": duplicate_rows_removed,
        "valid_rows_accepted": valid_rows,
        "avg_grid_distance_km": round(float(agg.get("avg_dist") or 0.0), 2),
        "p95_grid_distance_km": round(float(agg.get("p95_dist") or 0.0), 2),
        "closed_tickets_count": closed,
        "open_tickets_count": valid_rows - closed,
        "status_conflict_count": g("conflict_count"),
        "instant_closure_count": g("instant_count"),
        "quarantined_rows": quarantined_count,
        "spatial_resolution_breakdown": breakdown.get("spatial_resolution_level", {}),
        "incident_clustering": {
            "total_locatable_records": total_loc,
            "unlocatable_records": valid_rows - total_loc,
            "duplicate_burst_calls_collapsed": dup_burst,
            "burst_collapse_rate_pct": _pct(dup_burst, total_loc),
            "distinct_physical_incidents": valid_rows - dup_burst,
            "route_breakdown": breakdown.get("loc_routing_type", {}),
        },
        "resolution_text_mining": {
            "field_verified_remediations": g("field_ver_cnt"),
            "field_verified_rate_pct": _pct(g("field_ver_cnt"), closed),
            "phantom_closures": g("phantom_cnt"),
            "phantom_closure_rate_pct": _pct(g("phantom_cnt"), closed),
            "unclassified_resolutions": g("unclass_cnt"),
            "unclassified_rate_pct": _pct(g("unclass_cnt"), closed),
            "outcome_breakdown": breakdown.get("resolution_outcome_category", {}),
        },
        "semantic_conformance": {
            "total_records": valid_rows,
            "conformed_records": valid_rows - unmapped,
            "conformed_rate_pct": _pct(valid_rows - unmapped, valid_rows),
            "unmapped_records": unmapped,
            "unmapped_rate_pct": _pct(unmapped, valid_rows),
            "legacy_mapped_records": g("legacy_cnt"),
            "domain_breakdown": breakdown.get("canonical_service_domain", {}),
        },
        "partitions_written": written_files,
        "duration_seconds": round(elapsed, 2),
        "status": "success",
    }



# =========================================================================
# Main Silver 311 Pipeline Orchestrator
# =========================================================================
def clean_and_validate_311(
    bronze_311_dir: Path = BRONZE_DIR / "nyc311",
    silver_311_dir: Path = SILVER_DIR / "nyc311",
    quarantine_dir: Path = QUARANTINE_DIR / "nyc311",
    staging_dir: Optional[Path] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Silver Layer 311 Orchestrator:
    1. Ingest Bronze Parquet files (with optional start_date/end_date partition pruning).
    2. Step 1: Universal schema conformance, type casting, and standard casing.
    3. Step 2: Strict boundary quarantine validation.
    4. Step 3: CDC deduplication on unique_key.
    5. Step 4: Operational & calendar duration metrics enrichment.
    6. Step 5: Hierarchical spatial imputation & 6-route location keys.
    7. Step 6: Stateful burst clustering via mapInArrow.
    8. Step 7: Semantic taxonomy conformance.
    9. Step 8: Unstructured resolution text mining.
    10. Step 9: Administrative bounds validation & quality audit notes.
    11. Step 10: Partitioned Parquet write.
    """
    start_time = time.time()
    processed_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    now_epoch = int(datetime.now(timezone.utc).timestamp())

    if staging_dir is None:
        staging_dir = silver_311_dir.parent / "staging_311"

    bronze_files = sorted([f for f in bronze_311_dir.glob("**/*.parquet") if "_staging" not in f.parts])
    if not bronze_files:
        logger.warning(f"No Bronze 311 files found in {bronze_311_dir}")
        return {"status": "skipped", "reason": "no_input_files"}

    profiler = get_profiler()

    with profiler.measure_step("silver_311", "bronze_scan_and_pruning") as step_scan:
        logger.info(f"Loading Bronze 311 partition data into PySpark from {bronze_311_dir}...")
        spark = get_spark_session("DAT535-Silver-Clean-311")
        df_raw = spark.read.parquet(str(bronze_311_dir))

        # Attach file-level lineage dynamically if not present in raw row
        if "_source_file" not in df_raw.columns:
            df_raw = df_raw.withColumn("_source_file", input_file_name())
        if "_batch_id" not in df_raw.columns:
            extracted_id = regexp_extract(col("_source_file"), r"(311_\d{8}_\d{3})", 1)
            df_raw = df_raw.withColumn(
                "_batch_id",
                when(extracted_id != "", extracted_id).otherwise(lit("batch_unknown")),
            )

        # Step 1: Universal 100% Schema Conformance & Type Casting
        df_conformed = conform_and_cast_311_schema(df_raw)

        # Optional date range filtering on conformed UTC timestamp
        # (Preserve NULL created_at_utc so corrupted records are properly quarantined in Step 2)
        if start_date:
            logger.info(f"Applying start_date filter on conformed UTC timestamp: created_at_utc >= {start_date[:10]}")
            df_conformed = df_conformed.filter(
                col("created_at_utc").isNull() | (to_date(col("created_at_utc")) >= lit(start_date[:10]))
            )
        if end_date:
            logger.info(f"Applying end_date filter on conformed UTC timestamp: created_at_utc <= {end_date[:10]}")
            df_conformed = df_conformed.filter(
                col("created_at_utc").isNull() | (to_date(col("created_at_utc")) <= lit(end_date[:10]))
            )

        input_rows = df_conformed.count()
        step_scan.set_input_rows(input_rows)
        step_scan.set_output_rows(input_rows)
    logger.info(f"Loaded {input_rows:,} raw 311 records from Bronze via PySpark.")

    with profiler.measure_step("silver_311", "schema_conformance_and_quarantine", input_rows=input_rows) as step_quar:
        # Step 2: Strict Boundary Quarantine Validation (Evaluated on typed fields)
        df_valid_candidates, quarantined_count = quarantine_invalid_311_records(
            df_conformed, now_epoch, quarantine_dir
        )
        candidate_count = input_rows - quarantined_count  # conformance is 1:1, so no extra scan needed
        step_quar.set_output_rows(candidate_count)

    if candidate_count == 0:
        elapsed = time.time() - start_time
        logger.info("Silver 311 processing complete: 0 valid records retained.")
        return _build_silver_report(None, input_rows, 0, 0, quarantined_count, 0, elapsed)

    # Step 3: CDC Deduplication
    logger.info("Executing Silver 311 Step 3: CDC Deduplication on validated unique_key...")
    with profiler.measure_step("silver_311", "cdc_deduplication", input_rows=candidate_count) as step_dedup:
        df_dedup, duplicate_rows_removed = deduplicate_311_records(df_valid_candidates, candidate_count=candidate_count)
        step_dedup.set_output_rows(candidate_count - duplicate_rows_removed)

    # Step 4: Operational & Calendar Duration Metrics Enrichment
    logger.info("Executing Silver 311 Step 4: Operational & calendar duration metrics enrichment...")
    with profiler.measure_step("silver_311", "temporal_metrics_enrichment", input_rows=candidate_count - duplicate_rows_removed) as step_enrich:
        df_enriched = enrich_temporal_metrics(df_dedup, now_epoch)
        step_enrich.set_output_rows(candidate_count - duplicate_rows_removed)

    # Step 5: Spatial Imputation & 6-Route Location Key Generation
    with profiler.measure_step("silver_311", "spatial_imputation_and_canonicalization", input_rows=candidate_count - duplicate_rows_removed) as step_spat:
        df_spatial_clean = impute_spatial_and_canonicalize_routes(df_enriched, spark)
        step_spat.set_output_rows(candidate_count - duplicate_rows_removed)

    try:
        # Step 6: Stateful Citizen Call Burst Clustering Sessionization
        logger.info("Executing Silver 311 Step 6: Zero-copy Arrow burst clustering...")
        with profiler.measure_step("silver_311", "burst_clustering_sessionization", input_rows=candidate_count - duplicate_rows_removed) as step_cluster:
            df_clustered = cluster_citizen_call_bursts(df_spatial_clean, burst_window_hours=6.0)
            df_clustered = df_clustered.localCheckpoint(eager=True)
            step_cluster.set_output_rows(candidate_count - duplicate_rows_removed)

        # Step 7: Categorical Schema Drift & Semantic Conformance
        logger.info("Executing Silver 311 Step 7: PySpark Semantic Conformance...")
        with profiler.measure_step("silver_311", "taxonomy_conformance_and_ledger", input_rows=candidate_count - duplicate_rows_removed) as step_tax:
            df_conformed = conform_complaint_taxonomy(df_clustered, spark=spark, reference_path=TAXONOMY_REFERENCE_PATH)
            upsert_unmapped_taxonomy_ledger(df_conformed, spark, ledger_path=UNMAPPED_TAXONOMY_LEDGER_PATH)
            step_tax.set_output_rows(candidate_count - duplicate_rows_removed)

        # Step 8: Unstructured Resolution Text Mining & Phantom Closure Auditing
        logger.info("Executing Silver 311 Step 8: PySpark Unstructured Resolution Text Mining...")
        with profiler.measure_step("silver_311", "resolution_text_mining_and_ledger", input_rows=candidate_count - duplicate_rows_removed) as step_mine:
            df_resolved = classify_resolution_outcomes(df_conformed)
            upsert_unclassified_resolutions_ledger(df_resolved, spark)
            step_mine.set_output_rows(candidate_count - duplicate_rows_removed)

        # Step 9: Administrative Sanity Bounds & Quality Audit Notes
        with profiler.measure_step("silver_311", "attribute_standardization_and_audit_tags", input_rows=candidate_count - duplicate_rows_removed) as step_std:
            df_std = validate_administrative_bounds(df_resolved)
            df_silver = attach_quality_audit_tags(df_std, processed_at_utc)
            step_std.set_output_rows(candidate_count - duplicate_rows_removed)

        # Step 10: Distributed Partitioned Parquet Write
        with profiler.measure_step("silver_311", "partitioned_parquet_write", input_rows=candidate_count - duplicate_rows_removed) as step_write:
            written_files = write_silver_311(df_silver, silver_311_dir)
            step_write.set_output_rows(candidate_count - duplicate_rows_removed)

        # Step 11: Compute Audit Metrics in PySpark directly from written Parquet
        with profiler.measure_step("silver_311", "reconciliation_audit_aggregation") as step_audit:
            written_silver_files = sorted(silver_311_dir.glob("**/*.parquet"))
            if written_silver_files:
                df_written = spark.read.parquet(str(silver_311_dir))
                if start_date:
                    df_written = df_written.filter(to_date(col("created_at_utc")) >= lit(start_date[:10]))
                if end_date:
                    df_written = df_written.filter(to_date(col("created_at_utc")) <= lit(end_date[:10]))
                valid_rows_accepted = df_written.count()
            else:
                df_written = None
                valid_rows_accepted = 0

            step_audit.set_input_rows(valid_rows_accepted)
            step_audit.set_output_rows(valid_rows_accepted)

        elapsed = time.time() - start_time
        report = _build_silver_report(
            df_written, input_rows, duplicate_rows_removed, valid_rows_accepted,
            quarantined_count, written_files, elapsed,
        )
        logger.info(
            f"Completed Silver 311 processing: {valid_rows_accepted:,} valid records | "
            f"{quarantined_count:,} quarantined | {written_files} partition files written | {elapsed:.2f}s"
        )
        return report
    finally:
        if staging_dir and staging_dir.exists():
            shutil.rmtree(staging_dir, ignore_errors=True)


if __name__ == "__main__":
    clean_and_validate_311()
