"""
Gold Layer: Agency Resolution & SLA Performance Mart Builder.
Grain: reporting_date × borough × agency
Measures operational agency throughput, resolution SLAs (median & p90 durations via PySpark percentile_approx),
active backlog accumulation, and long-open ticket share under prevailing weather conditions.
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
    col,
    count as spark_count,
    lit,
    max as spark_max,
    mean as spark_mean,
    percentile_approx,
    round as spark_round,
    sum as spark_sum,
    to_date,
    trim,
    upper,
    when,
)
from pyspark.storagelevel import StorageLevel

from src.gold.config import (
    GOLD_MARTS_DIR,
    LONG_OPEN_BACKLOG_DAYS,
    SILVER_311_DIR,
    SILVER_WEATHER_DIR,
    STORM_PRECIP_THRESHOLD_MM,
    STORM_WIND_THRESHOLD_MS,
)
from src.utils.spark import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("build_agency_performance")


def build_agency_resolution_performance_mart(
    silver_311_dir: Path = SILVER_311_DIR,
    silver_weather_dir: Path = SILVER_WEATHER_DIR,
    output_dir: Path = GOLD_MARTS_DIR,
    long_open_days: float = LONG_OPEN_BACKLOG_DAYS,
) -> Dict[str, Any]:
    """
    Construct the Agency Resolution & SLA Performance Mart using PySpark.
    Conceptually separates creation cohorts (incoming demand), closure cohorts (agency output),
    and active backlog accumulation as of each reporting date.
    Utilizes PySpark percentile_approx for robust SLA distribution modeling (Lab 3 requirement).
    """
    start_time = time.time()
    built_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 1. Discover Clean Silver 311 Records
    files_311 = sorted(silver_311_dir.glob("**/*.parquet"))
    if not files_311:
        raise FileNotFoundError(f"No Silver 311 files found in {silver_311_dir}")

    logger.info("Initializing PySpark session for Agency Performance Mart...")
    spark = get_spark_session("DAT535-Gold-Agency-Performance")

    file_paths_311 = [str(f.resolve()) for f in files_311]
    df_311_spark = spark.read.parquet(*file_paths_311)
    total_311_input = df_311_spark.count()
    logger.info(f"Loaded {total_311_input:,} clean 311 records into PySpark.")

    # 2. Compute Creation Cohort (Incoming Demand on reporting_date) via PySpark
    logger.info("Computing Creation Cohort (incoming ticket volume) with PySpark...")
    df_created_spark = (
        df_311_spark.withColumn("reporting_date", to_date(col("created_at_utc")))
        .groupBy("reporting_date", "borough", "agency")
        .agg(spark_count("unique_key").alias("tickets_created_today"))
    )

    # 3. Compute Closure Cohort (Agency Output & Resolution Durations on reporting_date) via PySpark
    logger.info("Computing Closure Cohort (median & P90 SLAs via PySpark percentile_approx)...")
    cols_311 = set(df_311_spark.columns)
    has_closed_date = "closed_at_utc" in cols_311

    if has_closed_date and "is_ticket_closed" in cols_311:
        df_closed_subset_spark = df_311_spark.filter(
            (col("is_ticket_closed") == True) & col("closed_at_utc").isNotNull()
        ).withColumn("reporting_date", to_date(col("closed_at_utc")))

        df_closed_spark = (
            df_closed_subset_spark.groupBy("reporting_date", "borough", "agency")
            .agg(
                spark_count("unique_key").alias("tickets_closed_today"),
                spark_round(percentile_approx(col("resolution_duration_hours"), lit(0.5)), 2).alias("median_resolution_duration_hours"),
                spark_round(percentile_approx(col("resolution_duration_hours"), lit(0.9)), 2).alias("p90_resolution_duration_hours"),
                spark_round(spark_mean("resolution_duration_hours"), 2).alias("avg_resolution_duration_hours"),
            )
        )
    else:
        df_closed_spark = df_created_spark.select(
            "reporting_date", "borough", "agency",
            lit(0).alias("tickets_closed_today"),
            lit(None).cast("double").alias("median_resolution_duration_hours"),
            lit(None).cast("double").alias("p90_resolution_duration_hours"),
            lit(None).cast("double").alias("avg_resolution_duration_hours"),
        )

    # Merge Creation and Closure cohorts
    df_perf_spark = df_created_spark.join(
        df_closed_spark,
        on=["reporting_date", "borough", "agency"],
        how="outer",
    )

    df_perf_spark.persist(StorageLevel.MEMORY_AND_DISK)
    df_perf = df_perf_spark.toPandas().copy()
    df_perf_spark.unpersist()

    df_perf["tickets_created_today"] = df_perf["tickets_created_today"].fillna(0).astype(int)
    df_perf["tickets_closed_today"] = df_perf["tickets_closed_today"].fillna(0).astype(int)

    # Convert df_311 to pandas for date-by-date active queue accumulation
    df_311 = df_311_spark.toPandas().copy()
    df_311["created_date_only"] = pd.to_datetime(df_311["created_at_utc"], utc=True).dt.date
    df_311["closed_date_only"] = (
        pd.to_datetime(df_311["closed_at_utc"], utc=True).dt.date
        if "closed_at_utc" in df_311.columns
        else pd.Series(pd.NaT, index=df_311.index)
    )

    # 4. Compute Active Backlog & Aging as of each reporting_date
    logger.info("Computing Backlog Cohort (active pending tickets and aging share > 7 days)...")
    reporting_dates = sorted(df_perf["reporting_date"].unique())

    backlog_records = []
    for rep_date in reporting_dates:
        cond_created = df_311["created_date_only"] <= rep_date
        cond_open = df_311["closed_date_only"].isna() | (df_311["closed_date_only"] > rep_date)
        active_mask = cond_created & cond_open
        df_active = df_311[active_mask]

        if df_active.empty:
            continue

        age_in_days = (rep_date - df_active["created_date_only"]).apply(lambda delta: delta.days)
        df_active_grouped = df_active.assign(is_long_open=(age_in_days > long_open_days)).groupby(
            ["borough", "agency"]
        ).agg(
            active_open_backlog_count=("unique_key", "count"),
            long_open_backlog_count=("is_long_open", "sum"),
        ).reset_index()

        df_active_grouped["reporting_date"] = rep_date
        backlog_records.append(df_active_grouped)

    if backlog_records:
        df_backlog_all = pd.concat(backlog_records, ignore_index=True)
        df_perf = pd.merge(df_perf, df_backlog_all, on=["reporting_date", "borough", "agency"], how="left")
    else:
        df_perf["active_open_backlog_count"] = 0
        df_perf["long_open_backlog_count"] = 0

    df_perf["active_open_backlog_count"] = df_perf["active_open_backlog_count"].fillna(0).astype(int)
    df_perf["long_open_backlog_count"] = df_perf["long_open_backlog_count"].fillna(0).astype(int)

    # Long-open share %
    df_perf["long_open_share_pct"] = np.where(
        df_perf["active_open_backlog_count"] > 0,
        ((df_perf["long_open_backlog_count"] / df_perf["active_open_backlog_count"]) * 100.0).round(1),
        0.0,
    )

    # 5. Load and Aggregate Borough-Level Weather Context via PySpark
    logger.info("Computing Borough-level daily environmental context with PySpark...")
    files_weather = sorted(silver_weather_dir.glob("**/*.parquet"))
    if files_weather:
        file_paths_weather = [str(f.resolve()) for f in files_weather]
        df_w_spark = spark.read.parquet(*file_paths_weather)

        w_cols = set(df_w_spark.columns)
        has_hint = "borough_hint" in w_cols

        if has_hint:
            if "precipitation" not in w_cols:
                df_w_spark = df_w_spark.withColumn("precipitation", lit(0.0))
            if "wind_gusts_10m" not in w_cols:
                df_w_spark = df_w_spark.withColumn("wind_gusts_10m", lit(None).cast("double"))
            if "temperature_2m" not in w_cols:
                df_w_spark = df_w_spark.withColumn("temperature_2m", lit(None).cast("double"))
            if "is_extreme_weather" not in w_cols:
                df_w_spark = df_w_spark.withColumn("is_extreme_weather", lit(False))

            borough_weather_spark = (
                df_w_spark.withColumn("reporting_date", to_date(col("timestamp_utc")))
                .withColumn("borough", upper(trim(col("borough_hint"))))
                .groupBy("reporting_date", "borough")
                .agg(
                    spark_round(spark_sum("precipitation"), 2).alias("borough_daily_precip_mm"),
                    spark_round(spark_max("wind_gusts_10m"), 2).alias("borough_max_wind_gust_ms"),
                    spark_round(spark_mean("temperature_2m"), 2).alias("borough_avg_temp_c"),
                    spark_max(when(col("is_extreme_weather") == True, lit(True)).otherwise(lit(False))).alias("is_extreme_weather_event"),
                )
            )
            borough_weather = borough_weather_spark.toPandas().copy()

            borough_weather["is_storm_day"] = (
                (borough_weather["borough_daily_precip_mm"] >= STORM_PRECIP_THRESHOLD_MM) |
                (borough_weather["borough_max_wind_gust_ms"] >= STORM_WIND_THRESHOLD_MS)
            )

            df_perf = pd.merge(df_perf, borough_weather, on=["reporting_date", "borough"], how="left")
        else:
            df_perf["borough_daily_precip_mm"] = np.nan
            df_perf["borough_max_wind_gust_ms"] = np.nan
            df_perf["borough_avg_temp_c"] = np.nan
            df_perf["is_storm_day"] = False
    else:
        df_perf["borough_daily_precip_mm"] = np.nan
        df_perf["borough_max_wind_gust_ms"] = np.nan
        df_perf["borough_avg_temp_c"] = np.nan
        df_perf["is_storm_day"] = False

    df_perf["is_storm_day"] = df_perf["is_storm_day"].astype("boolean").fillna(False).astype(bool)

    # 6. Calendar Context
    dt_series = pd.to_datetime(df_perf["reporting_date"])
    df_perf["year"] = dt_series.dt.year
    df_perf["month"] = dt_series.dt.month
    df_perf["day_of_week"] = dt_series.dt.dayofweek
    df_perf["is_weekend"] = df_perf["day_of_week"].isin([5, 6])
    df_perf["_gold_built_at_utc"] = built_at_utc

    # 7. Grain Uniqueness Verification
    grain_cols = ["reporting_date", "borough", "agency"]
    dup_count = int(df_perf.duplicated(subset=grain_cols).sum())
    if dup_count > 0:
        raise ValueError(f"Grain uniqueness violation: {dup_count} duplicate keys on {grain_cols}")

    # 8. Write Parquet Mart
    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / "gold_agency_resolution_performance.parquet"
    table = pa.Table.from_pandas(df_perf, preserve_index=False)
    pq.write_table(table, out_file, compression="snappy")

    elapsed = time.time() - start_time
    logger.info(
        f"Built Gold Agency Resolution Performance Mart with PySpark: {len(df_perf):,} rows | "
        f"{df_perf['reporting_date'].nunique()} dates | {df_perf['agency'].nunique()} agencies | "
        f"{elapsed:.2f}s | Output: {out_file.name}"
    )

    return {
        "mart": "gold_agency_resolution_performance",
        "rows": len(df_perf),
        "distinct_dates": df_perf["reporting_date"].nunique(),
        "distinct_boroughs": df_perf["borough"].nunique(),
        "distinct_agencies": df_perf["agency"].nunique(),
        "total_tickets_created_tracked": int(df_perf["tickets_created_today"].sum()),
        "total_tickets_closed_tracked": int(df_perf["tickets_closed_today"].sum()),
        "output_path": str(out_file),
        "duration_seconds": round(elapsed, 2),
        "status": "success",
    }


if __name__ == "__main__":
    build_agency_resolution_performance_mart()
