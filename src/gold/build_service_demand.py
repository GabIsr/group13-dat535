"""
Gold Layer: Daily Service Demand Mart Builder.
Grain: date × grid_id × borough × complaint_type
Aggregates 311 demand and local daily meteorological conditions to measure environmental elasticity of citizen complaints.
Uses PySpark (Labs 2 & 3) for scalable distributed aggregation and broadcast joins.
Author: Group 13
"""

import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from pyspark.sql.functions import (
    broadcast,
    coalesce,
    col,
    count as spark_count,
    countDistinct,
    dayofweek,
    lit,
    max as spark_max,
    mean as spark_mean,
    min as spark_min,
    month as spark_month,
    round as spark_round,
    sum as spark_sum,
    to_date,
    when,
    year as spark_year,
)
from pyspark.storagelevel import StorageLevel

from src.gold.config import GOLD_MARTS_DIR, SILVER_311_DIR, SILVER_WEATHER_DIR
from src.utils.spark import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("build_service_demand")


def build_daily_service_demand_mart(
    silver_311_dir: Path = SILVER_311_DIR,
    silver_weather_dir: Path = SILVER_WEATHER_DIR,
    output_dir: Path = GOLD_MARTS_DIR,
) -> Dict[str, Any]:
    """
    Construct the Daily Service Demand Mart using PySpark.
    Pre-aggregates 311 complaints and meteorological observations to the daily grain before joining,
    ensuring zero row-loss reconciliation and complete weather-coverage tracking.
    """
    start_time = time.time()
    built_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 1. Discover Silver 311 Records
    files_311 = sorted(silver_311_dir.glob("**/*.parquet"))
    if not files_311:
        raise FileNotFoundError(f"No Silver 311 files found in {silver_311_dir}")

    # 2. Discover Silver Weather Observations
    files_weather = sorted(silver_weather_dir.glob("**/*.parquet"))
    if not files_weather:
        raise FileNotFoundError(f"No Silver weather files found in {silver_weather_dir}")

    logger.info("Initializing PySpark session for Daily Service Demand Mart...")
    spark = get_spark_session("DAT535-Gold-Service-Demand")

    file_paths_311 = [str(f.resolve()) for f in files_311]
    df_311_spark = spark.read.parquet(*file_paths_311)
    total_311_input = df_311_spark.count()
    logger.info(f"Loaded {total_311_input:,} clean 311 records into PySpark.")

    file_paths_weather = [str(f.resolve()) for f in files_weather]
    df_weather_spark = spark.read.parquet(*file_paths_weather)
    weather_input_rows = df_weather_spark.count()
    logger.info(f"Loaded {weather_input_rows:,} hourly weather observations into PySpark.")

    # Guard against missing optional weather columns in historical/partial schemas
    weather_cols = set(df_weather_spark.columns)
    if "precipitation" not in weather_cols:
        df_weather_spark = df_weather_spark.withColumn("precipitation", lit(0.0))
    if "wind_gusts_10m" not in weather_cols:
        df_weather_spark = df_weather_spark.withColumn("wind_gusts_10m", lit(None).cast("double"))
    if "temperature_2m" not in weather_cols:
        df_weather_spark = df_weather_spark.withColumn("temperature_2m", lit(None).cast("double"))
    if "is_extreme_weather" not in weather_cols:
        df_weather_spark = df_weather_spark.withColumn("is_extreme_weather", lit(False))

    # 3. Aggregate Weather to Daily Grain (date × grid_id) via PySpark (Lab 2 requirement)
    logger.info("Aggregating daily weather metrics with PySpark...")
    daily_weather_spark = (
        df_weather_spark.withColumn("date", to_date(col("timestamp_utc")))
        .groupBy("date", "grid_id")
        .agg(
            spark_round(spark_sum("precipitation"), 2).alias("total_precipitation_mm"),
            spark_sum(when(col("precipitation") > 0.0, 1).otherwise(0)).alias("hours_of_precipitation"),
            spark_round(spark_max("wind_gusts_10m"), 2).alias("max_wind_gust_ms"),
            spark_round(spark_mean("temperature_2m"), 2).alias("avg_temperature_c"),
            spark_round(spark_min("temperature_2m"), 2).alias("min_temperature_c"),
            spark_round(spark_max("temperature_2m"), 2).alias("max_temperature_c"),
            spark_max(when(col("is_extreme_weather") == True, lit(True)).otherwise(lit(False))).alias("is_extreme_weather_day"),
            spark_sum(when(col("temperature_2m").isNotNull(), 1).otherwise(0)).alias("valid_weather_obs_count"),
        )
    )

    # 4. Aggregate 311 to Daily Demand Grain (date × grid_id × borough × complaint_type) via PySpark
    logger.info("Aggregating daily 311 service demand with PySpark...")
    cols_311 = set(df_311_spark.columns)
    has_burst_dup = "is_burst_duplicate" in cols_311
    inc_count_expr = spark_sum(when(col("is_burst_duplicate") == False, 1).otherwise(0)) if has_burst_dup else spark_count("unique_key")
    dup_burst_expr = spark_sum(when(col("is_burst_duplicate") == True, 1).otherwise(0)) if has_burst_dup else lit(0)

    daily_311_spark = (
        df_311_spark.withColumn("date", to_date(col("created_at_utc")))
        .groupBy("date", "grid_id", "borough", "complaint_type")
        .agg(
            spark_count("unique_key").alias("complaint_count"),
            inc_count_expr.alias("distinct_incidents_count"),
            dup_burst_expr.alias("duplicate_burst_calls_count"),
            spark_sum(when(col("is_ticket_closed") == True, 1).otherwise(0)).alias("closed_complaint_count"),
            countDistinct("agency").alias("distinct_agencies_handling"),
        )
        .withColumn(
            "demand_inflation_ratio",
            when(
                col("distinct_incidents_count") > 0,
                spark_round(col("complaint_count") / col("distinct_incidents_count"), 2)
            ).otherwise(lit(1.0))
        )
    )

    # 5. Left Join 311 Daily Demand with Daily Local Weather (Broadcast Hash Join - Lab 3)
    logger.info("Joining Daily 311 Demand with Daily Weather Reanalysis on (date, grid_id)...")
    mart_spark = daily_311_spark.join(
        broadcast(daily_weather_spark),
        on=["date", "grid_id"],
        how="left",
    )

    # 6. Add Calendar and Coverage Attributes in pure PySpark (Zero toPandas Anti-Pattern)
    mart_spark = (
        mart_spark
        .withColumn(
            "weather_data_coverage_pct",
            spark_round((coalesce(col("valid_weather_obs_count"), lit(0)) / 24.0) * 100.0, 1)
        )
        .withColumn("is_extreme_weather_day", coalesce(col("is_extreme_weather_day"), lit(False)))
        .withColumn("hours_of_precipitation", coalesce(col("hours_of_precipitation"), lit(0)))
        .withColumn("closed_complaint_count", coalesce(col("closed_complaint_count"), lit(0)))
        .withColumn("year", spark_year(col("date")))
        .withColumn("month", spark_month(col("date")))
        .withColumn("day_of_week", (dayofweek(col("date")) + 5) % 7)
        .withColumn("is_weekend", col("day_of_week").isin([5, 6]))
        .withColumn("_gold_built_at_utc", lit(built_at_utc))
    )

    mart_spark.persist(StorageLevel.MEMORY_AND_DISK)
    mart_rows = mart_spark.count()

    reconciled_total_complaints = int(mart_spark.agg(spark_sum("complaint_count")).collect()[0][0])
    if reconciled_total_complaints != total_311_input:
        mart_spark.unpersist()
        raise ValueError(
            f"Row count reconciliation violation in Daily Demand Mart! "
            f"Silver input ({total_311_input}) != Mart total ({reconciled_total_complaints})"
        )

    # 7. Verification: Ensure Grain Uniqueness in PySpark
    distinct_grains = mart_spark.select("date", "grid_id", "borough", "complaint_type").distinct().count()
    if distinct_grains != mart_rows:
        mart_spark.unpersist()
        raise ValueError(f"Grain uniqueness violation: {mart_rows - distinct_grains} duplicate keys on grain")

    # 8. Write Parquet Mart using PySpark distributed write
    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / "gold_daily_service_demand.parquet"
    mart_spark.coalesce(1).write.mode("overwrite").parquet(str(out_file))

    distinct_dates = mart_spark.select("date").distinct().count()
    distinct_grid_cells = mart_spark.select("grid_id").distinct().count()
    distinct_complaint_types = mart_spark.select("complaint_type").distinct().count()
    weather_matched_rows = mart_spark.filter(col("weather_data_coverage_pct") > 0).count()
    mart_spark.unpersist()

    elapsed = time.time() - start_time
    logger.info(
        f"Built Gold Daily Service Demand Mart with PySpark: {mart_rows:,} rows | "
        f"{reconciled_total_complaints:,} complaints reconciled | {elapsed:.2f}s | Output: {out_file.name}"
    )

    return {
        "mart": "gold_daily_service_demand",
        "rows": mart_rows,
        "total_complaints_reconciled": reconciled_total_complaints,
        "distinct_dates": distinct_dates,
        "distinct_grid_cells": distinct_grid_cells,
        "distinct_complaint_types": distinct_complaint_types,
        "weather_matched_rows": weather_matched_rows,
        "output_path": str(out_file),
        "duration_seconds": round(elapsed, 2),
        "status": "success",
    }


if __name__ == "__main__":
    build_daily_service_demand_mart()
