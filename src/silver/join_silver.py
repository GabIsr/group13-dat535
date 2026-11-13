"""
Silver Layer: Unified Analytical Fact Table Join (311 + ERA5-Land Weather).
Performs broadcast left-join on (grid_id, weather_join_hour_utc == timestamp_utc).
Eliminates distributed shuffle overhead by broadcasting the compact weather dimension.
Author: Group 13
"""

import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from pyspark.sql.functions import broadcast, col, date_format, lit, sum as spark_sum, when
from pyspark.storagelevel import StorageLevel

from src.silver.config import PROJECT_ROOT, SILVER_DIR
from src.utils.spark import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("join_silver")


def join_silver_datasets(
    silver_311_dir: Path = SILVER_DIR / "nyc311",
    silver_weather_dir: Path = SILVER_DIR / "weather",
    output_joined_dir: Path = SILVER_DIR / "311_weather_joined",
) -> Dict[str, Any]:
    """
    Join clean 311 service requests with local hourly ERA5-Land meteorological reanalysis.
    Joins on: 311.grid_id == weather.grid_id AND 311.weather_join_hour_utc == weather.timestamp_utc.
    Uses PySpark BroadcastHashJoin (Lab 3 requirement) to broadcast the weather dimension.
    """
    start_time = time.time()
    joined_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 1. Load Silver 311
    files_311 = sorted(silver_311_dir.glob("**/*.parquet"))
    if not files_311:
        raise FileNotFoundError(f"No Silver 311 files found in {silver_311_dir}")

    # 2. Load Silver Weather (The Broadcast Dimension)
    files_weather = sorted(silver_weather_dir.glob("**/*.parquet"))
    if not files_weather:
        raise FileNotFoundError(f"No Silver weather files found in {silver_weather_dir}")

    logger.info("Initializing PySpark session for Silver Broadcast Join...")
    spark = get_spark_session("DAT535-Silver-Join")

    file_paths_311 = [str(f.resolve()) for f in files_311]
    df_311_spark = spark.read.parquet(*file_paths_311)
    total_311 = df_311_spark.count()
    logger.info(f"Loaded {total_311:,} clean 311 records into PySpark.")

    file_paths_weather = [str(f.resolve()) for f in files_weather]
    df_weather_spark = spark.read.parquet(*file_paths_weather)
    logger.info(f"Loaded {df_weather_spark.count():,} hourly weather observations into PySpark.")

    # 3. Weather Column Selection (Column Pruning early - Lab 3 optimization)
    weather_join_cols = [
        "grid_id",
        "timestamp_utc",
        "temperature_2m",
        "apparent_temperature",
        "precipitation",
        "precip_past_3h_mm",
        "precip_past_6h_mm",
        "precip_past_24h_mm",
        "rolling_24h_hours_count",
        "is_rolling_24h_partial",
        "rain",
        "snowfall",
        "snow_depth",
        "weather_code",
        "relative_humidity_2m",
        "wind_direction_10m",
        "surface_pressure",
        "wind_speed_10m",
        "wind_gusts_10m",
        "max_wind_gust_past_6h_ms",
        "min_temp_past_24h_c",
        "max_temp_past_24h_c",
        "is_freezing",
        "is_raining",
        "is_extreme_weather",
        "is_ground_frozen",
        "is_soil_saturated",
        "is_overcast",
        "soil_temperature_0_to_7cm",
        "soil_moisture_0_to_7cm",
        "cloud_cover",
        "model_elevation",
    ]
    w_cols = [c for c in weather_join_cols if c in df_weather_spark.columns]
    df_w_sub = df_weather_spark.select(*w_cols)

    # 4. True PySpark broadcast() Join (Lab 3 requirement)
    logger.info("Executing PySpark Broadcast Join (311 [Fact] LEFT JOIN broadcast(Weather) [Dimension])...")
    df_w_alias = broadcast(df_w_sub).alias("w")
    df_311_alias = df_311_spark.alias("t")

    join_cond = (col("t.grid_id") == col("w.grid_id")) & (col("t.weather_join_hour_utc") == col("w.timestamp_utc"))
    df_joined_spark = df_311_alias.join(df_w_alias, join_cond, how="left").drop(col("w.grid_id"))

    # Log Catalyst execution plan (Lab 3 optimization verification)
    logger.info("PySpark Catalyst Plan for Broadcast Join:")
    df_joined_spark.explain(False)

    df_joined_spark.persist(StorageLevel.MEMORY_AND_DISK)
    joined_rows = df_joined_spark.count()

    # 4b. Hard Data Quality Gate: Prevent 1-to-Many Cartesian Join Multiplication
    if joined_rows != total_311:
        df_joined_spark.unpersist()
        raise ValueError(
            f"Cartesian join explosion detected! "
            f"Input 311 rows ({total_311:,}) != Joined rows ({joined_rows:,}). "
            f"Weather dimension contains duplicate (grid_id, timestamp_utc) keys."
        )

    # 1. Native PySpark nullable type casting (Zero toPandas Anti-Pattern)
    for int16_col in ["weather_code", "relative_humidity_2m", "wind_direction_10m", "rolling_24h_hours_count"]:
        if int16_col in df_joined_spark.columns:
            df_joined_spark = df_joined_spark.withColumn(int16_col, col(int16_col).cast("smallint"))

    for int32_col in ["cluster_size", "burst_sequence_num"]:
        if int32_col in df_joined_spark.columns:
            df_joined_spark = df_joined_spark.withColumn(int32_col, col(int32_col).cast("integer"))

    if "incident_first_created_at_utc" in df_joined_spark.columns:
        df_joined_spark = df_joined_spark.withColumn(
            "incident_first_created_at_utc", col("incident_first_created_at_utc").cast("timestamp")
        )

    # 2. Add match status, metadata, and partition columns in pure Catalyst expressions
    df_joined_spark = (
        df_joined_spark
        .withColumn("is_weather_matched", col("temperature_2m").isNotNull())
        .withColumn("is_weather_pending", col("temperature_2m").isNull())
        .withColumn("_silver_joined_at_utc", lit(joined_at_utc))
        .withColumn("year", date_format(col("created_at_utc"), "yyyy"))
        .withColumn("month", date_format(col("created_at_utc"), "MM"))
    )

    # 3. Audit match status natively in PySpark (1-row summary tuple over Py4J, zero driver data collection)
    audit_row = df_joined_spark.select(
        spark_sum(when(col("is_weather_matched"), 1).otherwise(0)).alias("matched_count")
    ).collect()[0]
    matched_count = int(audit_row["matched_count"] or 0)
    unmatched_count = total_311 - matched_count
    match_rate = round(matched_count / total_311 * 100.0, 2) if total_311 > 0 else 0.0

    logger.info(
        f"Join Results: {matched_count:,} matched ({match_rate}%) | {unmatched_count:,} unmatched."
    )

    # 4. Write Partitioned Fact Table directly with PySpark distributed write
    output_joined_dir.mkdir(parents=True, exist_ok=True)
    (
        df_joined_spark.write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(str(output_joined_dir))
    )
    df_joined_spark.unpersist()

    written_files = len(list(output_joined_dir.glob("**/*.parquet")))
    elapsed = time.time() - start_time
    logger.info(
        f"Saved Unified Fact Table: {total_311:,} rows | "
        f"{written_files} partition files | {elapsed:.2f}s"
    )

    return {
        "dataset": "silver_311_weather_joined",
        "total_311_records": total_311,
        "matched_weather_records": matched_count,
        "unmatched_weather_records": unmatched_count,
        "weather_match_rate_pct": match_rate,
        "partitions_written": written_files,
        "duration_seconds": round(elapsed, 2),
        "status": "success",
    }


if __name__ == "__main__":
    join_silver_datasets()
