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

from pyspark.sql.functions import (
    broadcast,
    col,
    count as spark_count,
    countDistinct,
    datediff,
    dayofweek,
    lit,
    max as spark_max,
    mean as spark_mean,
    month as spark_month,
    percentile_approx,
    round as spark_round,
    sum as spark_sum,
    to_date,
    trim,
    upper,
    when,
    year as spark_year,
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
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Construct the Agency Resolution & SLA Performance Mart using PySpark.
    Conceptually separates creation cohorts (incoming demand), closure cohorts (agency output),
    and active backlog accumulation as of each reporting date.
    Utilizes PySpark percentile_approx for robust SLA distribution modeling (Lab 3 requirement).
    Supports optional start_date and end_date partition pruning.
    """
    start_time = time.time()
    built_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    # 1. Discover Clean Silver 311 Records
    files_311 = sorted(silver_311_dir.glob("**/*.parquet"))
    if not files_311:
        raise FileNotFoundError(f"No Silver 311 files found in {silver_311_dir}")

    logger.info("Initializing PySpark session for Agency Performance Mart...")
    spark = get_spark_session("DAT535-Gold-Agency-Performance")

    file_paths_311 = [str(f.resolve()) for f in files_311]
    df_311_spark = spark.read.parquet(*file_paths_311)

    if start_date:
        logger.info(f"Applying start_date filter on Silver 311: created_at_utc >= {start_date}")
        df_311_spark = df_311_spark.filter(col("created_at_utc") >= lit(start_date))
    if end_date:
        logger.info(f"Applying end_date filter on Silver 311: created_at_utc <= {end_date}")
        df_311_spark = df_311_spark.filter(col("created_at_utc") <= lit(end_date))

    total_311_input = df_311_spark.count()
    logger.info(f"Loaded {total_311_input:,} clean 311 records into PySpark.")

    # 2. Compute Creation Cohort (Incoming Demand on reporting_date) via PySpark
    logger.info("Computing Creation Cohort (incoming ticket volume) with PySpark...")
    cols_311 = set(df_311_spark.columns)
    has_burst_dup = "is_burst_duplicate" in cols_311
    inc_created_expr = spark_sum(when(col("is_burst_duplicate") == False, 1).otherwise(0)) if has_burst_dup else spark_count("unique_key")

    df_created_spark = (
        df_311_spark.withColumn("reporting_date", to_date(col("created_at_utc")))
        .groupBy("reporting_date", "borough", "agency")
        .agg(
            spark_count("unique_key").alias("tickets_created_today"),
            inc_created_expr.alias("distinct_incidents_created_today"),
        )
    )

    # 3. Compute Closure Cohort (Agency Output & Resolution Durations on reporting_date) via PySpark
    logger.info("Computing Closure Cohort (median & P90 SLAs via PySpark percentile_approx)...")
    cols_311 = set(df_311_spark.columns)
    has_closed_date = "closed_at_utc" in cols_311
    has_field_verified = "is_field_verified" in cols_311
    has_field_hours = "field_remediation_hours" in cols_311
    has_phantom = "is_phantom_closure" in cols_311

    if has_closed_date and "is_ticket_closed" in cols_311:
        df_closed_subset_spark = df_311_spark.filter(
            (col("is_ticket_closed") == True) & col("closed_at_utc").isNotNull()
        ).withColumn("reporting_date", to_date(col("closed_at_utc")))

        closure_aggs = [
            spark_count("unique_key").alias("tickets_closed_today"),
            spark_round(percentile_approx(col("resolution_duration_hours"), lit(0.5)), 2).alias("median_resolution_duration_hours"),
            spark_round(percentile_approx(col("resolution_duration_hours"), lit(0.9)), 2).alias("p90_resolution_duration_hours"),
            spark_round(spark_mean("resolution_duration_hours"), 2).alias("avg_resolution_duration_hours"),
        ]

        if has_field_verified:
            closure_aggs.append(
                spark_sum(when(col("is_field_verified") == True, 1).otherwise(0)).alias("field_verified_closed_today")
            )
        if has_phantom:
            closure_aggs.append(
                spark_sum(when(col("is_phantom_closure") == True, 1).otherwise(0)).alias("phantom_closed_today")
            )
            closure_aggs.append(
                spark_round(
                    spark_sum(when(col("is_phantom_closure") == True, 1).otherwise(0)) /
                    when(spark_count("unique_key") > 0, spark_count("unique_key")).otherwise(1) * 100.0,
                    2
                ).alias("phantom_closure_rate_pct")
            )
        has_unclass = "is_unclassified_resolution" in cols_311
        if has_unclass:
            closure_aggs.append(
                spark_sum(when(col("is_unclassified_resolution") == True, 1).otherwise(0)).alias("unclassified_closed_today")
            )
        if has_field_hours:
            closure_aggs.append(
                spark_round(percentile_approx(col("field_remediation_hours"), lit(0.5)), 2).alias("median_field_remediation_hours")
            )
            closure_aggs.append(
                spark_round(percentile_approx(col("field_remediation_hours"), lit(0.9)), 2).alias("p90_field_remediation_hours")
            )
            closure_aggs.append(
                spark_round(spark_mean("field_remediation_hours"), 2).alias("avg_field_remediation_hours")
            )

        df_closed_spark = (
            df_closed_subset_spark.groupBy("reporting_date", "borough", "agency")
            .agg(*closure_aggs)
        )
    else:
        select_cols = [
            "reporting_date", "borough", "agency",
            lit(0).alias("tickets_closed_today"),
            lit(None).cast("double").alias("median_resolution_duration_hours"),
            lit(None).cast("double").alias("p90_resolution_duration_hours"),
            lit(None).cast("double").alias("avg_resolution_duration_hours"),
        ]
        if has_field_verified:
            select_cols.append(lit(0).alias("field_verified_closed_today"))
        if has_phantom:
            select_cols.append(lit(0).alias("phantom_closed_today"))
            select_cols.append(lit(0.0).alias("phantom_closure_rate_pct"))
        if has_field_hours:
            select_cols.append(lit(None).cast("double").alias("median_field_remediation_hours"))
            select_cols.append(lit(None).cast("double").alias("p90_field_remediation_hours"))
            select_cols.append(lit(None).cast("double").alias("avg_field_remediation_hours"))

        df_closed_spark = df_created_spark.select(*select_cols)

    # Merge Creation and Closure cohorts in PySpark
    df_perf_spark = df_created_spark.join(
        df_closed_spark,
        on=["reporting_date", "borough", "agency"],
        how="outer",
    ).fillna(0, subset=["tickets_created_today", "tickets_closed_today"])

    zero_fill_cols = [c for c in ["field_verified_closed_today", "phantom_closed_today"] if c in df_perf_spark.columns]
    if zero_fill_cols:
        df_perf_spark = df_perf_spark.fillna(0, subset=zero_fill_cols)

    if "distinct_incidents_created_today" in df_perf_spark.columns:
        df_perf_spark = df_perf_spark.fillna(0, subset=["distinct_incidents_created_today"])

    # 4. Compute Active Backlog & Aging as of each reporting_date in pure PySpark (Zero toPandas)
    logger.info("Computing Backlog Cohort (active pending tickets and aging share > 7 days) with PySpark...")
    df_dates_spark = df_perf_spark.select("reporting_date").distinct()

    df_311_backlog = (
        df_311_spark
        .select(
            to_date(col("created_at_utc")).alias("created_date_only"),
            (to_date(col("closed_at_utc")) if has_closed_date else lit(None).cast("date")).alias("closed_date_only"),
            col("borough"),
            col("agency"),
            col("unique_key"),
        )
    )

    # Broadcast join reporting dates with active 311 tickets
    df_active_cross = df_311_backlog.join(
        broadcast(df_dates_spark),
        col("created_date_only") <= col("reporting_date"),
    )
    df_active_filtered = df_active_cross.filter(
        col("closed_date_only").isNull() | (col("closed_date_only") > col("reporting_date"))
    )
    df_active_with_aging = df_active_filtered.withColumn(
        "is_long_open",
        datediff(col("reporting_date"), col("created_date_only")) > lit(long_open_days),
    )
    df_backlog_summary = (
        df_active_with_aging
        .groupBy("reporting_date", "borough", "agency")
        .agg(
            spark_count("unique_key").alias("active_open_backlog_count"),
            spark_sum(when(col("is_long_open"), 1).otherwise(0)).alias("long_open_backlog_count"),
        )
    )

    df_perf_spark = (
        df_perf_spark
        .join(df_backlog_summary, on=["reporting_date", "borough", "agency"], how="left")
        .fillna(0, subset=["active_open_backlog_count", "long_open_backlog_count"])
        .withColumn(
            "long_open_share_pct",
            when(
                col("active_open_backlog_count") > 0,
                spark_round((col("long_open_backlog_count") / col("active_open_backlog_count")) * 100.0, 1),
            ).otherwise(lit(0.0)),
        )
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
                .withColumn(
                    "is_storm_day",
                    (col("borough_daily_precip_mm") >= lit(STORM_PRECIP_THRESHOLD_MM)) |
                    (col("borough_max_wind_gust_ms") >= lit(STORM_WIND_THRESHOLD_MS)),
                )
            )

            df_perf_spark = (
                df_perf_spark
                .join(borough_weather_spark, on=["reporting_date", "borough"], how="left")
                .fillna(False, subset=["is_storm_day"])
            )
        else:
            df_perf_spark = (
                df_perf_spark
                .withColumn("borough_daily_precip_mm", lit(None).cast("double"))
                .withColumn("borough_max_wind_gust_ms", lit(None).cast("double"))
                .withColumn("borough_avg_temp_c", lit(None).cast("double"))
                .withColumn("is_storm_day", lit(False))
            )
    else:
        df_perf_spark = (
            df_perf_spark
            .withColumn("borough_daily_precip_mm", lit(None).cast("double"))
            .withColumn("borough_max_wind_gust_ms", lit(None).cast("double"))
            .withColumn("borough_avg_temp_c", lit(None).cast("double"))
            .withColumn("is_storm_day", lit(False))
        )

    # 6. Calendar Context in PySpark
    df_perf_spark = (
        df_perf_spark
        .withColumn("year", spark_year(col("reporting_date")))
        .withColumn("month", spark_month(col("reporting_date")))
        .withColumn("day_of_week", (dayofweek(col("reporting_date")) + 5) % 7)
        .withColumn("is_weekend", col("day_of_week").isin([5, 6]))
        .withColumn("_gold_built_at_utc", lit(built_at_utc))
    )

    # 7. Grain Uniqueness Verification in PySpark
    grain_cols = ["reporting_date", "borough", "agency"]
    total_perf_rows = df_perf_spark.count()
    distinct_grain_count = df_perf_spark.select(*grain_cols).distinct().count()

    if distinct_grain_count != total_perf_rows:
        raise ValueError(f"Grain uniqueness violation: {total_perf_rows - distinct_grain_count} duplicate keys on {grain_cols}")

    # 8. Write Parquet Mart using PySpark distributed coalesce(1) write
    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / "gold_agency_resolution_performance.parquet"
    df_perf_spark.coalesce(1).write.mode("overwrite").parquet(str(out_file))

    # Compute summary metrics natively via PySpark aggregate (1-row tuple over Py4J)
    summary_metrics = df_perf_spark.select(
        countDistinct("reporting_date").alias("distinct_dates"),
        countDistinct("borough").alias("distinct_boroughs"),
        countDistinct("agency").alias("distinct_agencies"),
        spark_sum("tickets_created_today").alias("total_created"),
        spark_sum("tickets_closed_today").alias("total_closed"),
    ).collect()[0]

    elapsed = time.time() - start_time
    logger.info(
        f"Built Gold Agency Resolution Performance Mart with PySpark: {total_perf_rows:,} rows | "
        f"{summary_metrics['distinct_dates']} dates | {summary_metrics['distinct_agencies']} agencies | "
        f"{elapsed:.2f}s | Output: {out_file.name}"
    )

    return {
        "mart": "gold_agency_resolution_performance",
        "rows": total_perf_rows,
        "distinct_dates": int(summary_metrics["distinct_dates"] or 0),
        "distinct_boroughs": int(summary_metrics["distinct_boroughs"] or 0),
        "distinct_agencies": int(summary_metrics["distinct_agencies"] or 0),
        "total_tickets_created_tracked": int(summary_metrics["total_created"] or 0),
        "total_tickets_closed_tracked": int(summary_metrics["total_closed"] or 0),
        "output_path": str(out_file),
        "duration_seconds": round(elapsed, 2),
        "status": "success",
    }


if __name__ == "__main__":
    build_agency_resolution_performance_mart()
