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
from src.utils.benchmark import get_profiler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("join_silver")


def join_silver_datasets(
    silver_311_dir: Path = SILVER_DIR / "nyc311",
    silver_weather_dir: Path = SILVER_DIR / "weather",
    output_joined_dir: Path = SILVER_DIR / "311_weather_joined",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Join clean 311 service requests with local hourly ERA5-Land meteorological reanalysis.
    Joins on: 311.grid_id == weather.grid_id AND 311.weather_join_hour_utc == weather.timestamp_utc.
    Uses PySpark BroadcastHashJoin (Lab 3 requirement) to broadcast the weather dimension.
    Supports optional start_date and end_date partition pruning for bounded-memory execution.
    """
    start_time = time.time()
    joined_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    # 1. Load Silver 311
    files_311 = sorted(silver_311_dir.glob("**/*.parquet"))
    if not files_311:
        raise FileNotFoundError(f"No Silver 311 files found in {silver_311_dir}")

    # 2. Load Silver Weather (The Broadcast Dimension)
    files_weather = sorted(silver_weather_dir.glob("**/*.parquet"))
    if not files_weather:
        raise FileNotFoundError(f"No Silver weather files found in {silver_weather_dir}")

    profiler = get_profiler()

    with profiler.measure_step("unified_fact_join", "scans_and_partition_pruning") as step_scan:
        logger.info("Initializing PySpark session for Silver Broadcast Join...")
        spark = get_spark_session("DAT535-Silver-Join")

        file_paths_311 = [str(f.resolve()) for f in files_311]
        df_311_spark = spark.read.parquet(*file_paths_311)

        # Optional partition pruning if date range is specified
        if start_date:
            logger.info(f"Applying start_date filter on Silver 311: created_at_utc >= {start_date}")
            df_311_spark = df_311_spark.filter(col("created_at_utc") >= lit(start_date))
        if end_date:
            logger.info(f"Applying end_date filter on Silver 311: created_at_utc <= {end_date}")
            df_311_spark = df_311_spark.filter(col("created_at_utc") <= lit(end_date))

        total_311 = df_311_spark.count()
        logger.info(f"Loaded {total_311:,} clean 311 records into PySpark.")

        file_paths_weather = [str(f.resolve()) for f in files_weather]
        df_weather_spark = spark.read.parquet(*file_paths_weather)

        if start_date:
            df_weather_spark = df_weather_spark.filter(col("timestamp_utc") >= lit(start_date))
        if end_date:
            df_weather_spark = df_weather_spark.filter(col("timestamp_utc") <= lit(end_date))

        total_weather = df_weather_spark.count()
        logger.info(f"Loaded {total_weather:,} hourly weather observations into PySpark.")
        step_scan.set_input_rows(total_311)
        step_scan.set_output_rows(total_311)

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
    with profiler.measure_step("unified_fact_join", "broadcast_hash_join_execution", input_rows=total_311) as step_join:
        df_w_alias = broadcast(df_w_sub).alias("w")
        df_311_alias = df_311_spark.alias("t")

        join_cond = (col("t.grid_id") == col("w.grid_id")) & (col("t.weather_join_hour_utc") == col("w.timestamp_utc"))
        df_joined_spark = df_311_alias.join(df_w_alias, join_cond, how="left").drop(col("w.grid_id"))

        # Log Catalyst execution plan (Lab 3 optimization verification)
        logger.info("PySpark Catalyst Plan for Broadcast Join:")
        df_joined_spark.explain(False)

        joined_rows = df_joined_spark.count()

        # 4b. Hard Data Quality Gate: Prevent 1-to-Many Cartesian Join Multiplication
        if joined_rows != total_311:
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
        step_join.set_output_rows(joined_rows)

    # 3. Audit match status natively in PySpark (1-row summary tuple over Py4J, zero driver data collection)
    with profiler.measure_step("unified_fact_join", "reconciliation_audit", input_rows=total_311) as step_audit:
        audit_row = df_joined_spark.select(
            spark_sum(when(col("is_weather_matched"), 1).otherwise(0)).alias("matched_count")
        ).collect()[0]
        matched_count = int(audit_row["matched_count"] or 0)
        unmatched_count = total_311 - matched_count
        match_rate = round(matched_count / total_311 * 100.0, 2) if total_311 > 0 else 0.0
        step_audit.set_output_rows(total_311)

    logger.info(
        f"Join Results: {matched_count:,} matched ({match_rate}%) | {unmatched_count:,} unmatched."
    )

    # 4. Write Partitioned Fact Table directly with PySpark distributed write
    with profiler.measure_step("unified_fact_join", "fact_table_partition_write", input_rows=total_311) as step_write:
        output_joined_dir.mkdir(parents=True, exist_ok=True)
        (
            df_joined_spark.write
            .mode("overwrite")
            .partitionBy("year", "month")
            .parquet(str(output_joined_dir))
        )
        step_write.set_output_rows(total_311)

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
