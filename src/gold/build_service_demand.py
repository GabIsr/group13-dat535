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

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql.functions import (
    broadcast,
    col,
    count as spark_count,
    countDistinct,
    lit,
    max as spark_max,
    mean as spark_mean,
    min as spark_min,
    round as spark_round,
    sum as spark_sum,
    to_date,
    when,
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
    daily_311_spark = (
        df_311_spark.withColumn("date", to_date(col("created_at_utc")))
        .groupBy("date", "grid_id", "borough", "complaint_type")
        .agg(
            spark_count("unique_key").alias("complaint_count"),
            spark_sum(when(col("is_ticket_closed") == True, 1).otherwise(0)).alias("closed_complaint_count"),
            countDistinct("agency").alias("distinct_agencies_handling"),
        )
    )

    # 5. Left Join 311 Daily Demand with Daily Local Weather (Broadcast Hash Join - Lab 3)
    logger.info("Joining Daily 311 Demand with Daily Weather Reanalysis on (date, grid_id)...")
    mart_spark = daily_311_spark.join(
        broadcast(daily_weather_spark),
        on=["date", "grid_id"],
        how="left",
    )

    mart_spark.persist(StorageLevel.MEMORY_AND_DISK)
    mart_df = mart_spark.toPandas().copy()
    mart_spark.unpersist()

    # Weather coverage percentage (24 hours in a full day)
    mart_df["weather_data_coverage_pct"] = (
        (mart_df["valid_weather_obs_count"].fillna(0) / 24.0) * 100.0
    ).round(1)

    # Impute missing weather flags for unlocated or unobserved grid cells
    mart_df["weather_data_coverage_pct"] = mart_df["weather_data_coverage_pct"].fillna(0.0)
    mart_df["is_extreme_weather_day"] = mart_df["is_extreme_weather_day"].fillna(False).astype(bool)
    mart_df["hours_of_precipitation"] = mart_df["hours_of_precipitation"].fillna(0).astype(int)
    mart_df["closed_complaint_count"] = mart_df["closed_complaint_count"].fillna(0).astype(int)

    reconciled_total_complaints = int(mart_df["complaint_count"].sum())
    if reconciled_total_complaints != total_311_input:
        raise ValueError(
            f"Row count reconciliation violation in Daily Demand Mart! "
            f"Silver input ({total_311_input}) != Mart total ({reconciled_total_complaints})"
        )

    # 6. Add Calendar and Temporal Context Attributes
    dt_series = pd.to_datetime(mart_df["date"])
    mart_df["year"] = dt_series.dt.year
    mart_df["month"] = dt_series.dt.month
    mart_df["day_of_week"] = dt_series.dt.dayofweek  # 0=Monday, 6=Sunday
    mart_df["is_weekend"] = mart_df["day_of_week"].isin([5, 6])
    mart_df["_gold_built_at_utc"] = built_at_utc

    # 7. Verification: Ensure Grain Uniqueness
    grain_cols = ["date", "grid_id", "borough", "complaint_type"]
    duplicate_grain_count = int(mart_df.duplicated(subset=grain_cols).sum())
    if duplicate_grain_count > 0:
        raise ValueError(f"Grain uniqueness violation: {duplicate_grain_count} duplicate keys on {grain_cols}")

    # 8. Write Parquet Mart
    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / "gold_daily_service_demand.parquet"
    table = pa.Table.from_pandas(mart_df, preserve_index=False)
    pq.write_table(table, out_file, compression="snappy")

    elapsed = time.time() - start_time
    logger.info(
        f"Built Gold Daily Service Demand Mart with PySpark: {len(mart_df):,} rows | "
        f"{reconciled_total_complaints:,} complaints reconciled | {elapsed:.2f}s | Output: {out_file.name}"
    )

    return {
        "mart": "gold_daily_service_demand",
        "rows": len(mart_df),
        "total_complaints_reconciled": reconciled_total_complaints,
        "distinct_dates": mart_df["date"].nunique(),
        "distinct_grid_cells": mart_df["grid_id"].nunique(),
        "distinct_complaint_types": mart_df["complaint_type"].nunique(),
        "weather_matched_rows": int((mart_df["weather_data_coverage_pct"] > 0).sum()),
        "output_path": str(out_file),
        "duration_seconds": round(elapsed, 2),
        "status": "success",
    }


if __name__ == "__main__":
    build_daily_service_demand_mart()
