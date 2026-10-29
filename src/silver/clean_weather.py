"""
Silver Layer: Open-Meteo ERA5-Land Weather Cleaning, Physical Bound Validation,
Atmospheric Law Verification, and Sensor Anomaly Ledgering with Apache Spark.
Filters corrupt/sentinel records, applies physical sanity bounds, tags extreme weather events,
masks corrupted fields to NaN without row loss, builds chronological rolling window metrics via PySpark Window functions,
and maintains a complete forensic Audit Ledger.
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

from pyspark.sql import Window
from pyspark.sql.functions import (
    array,
    array_compact,
    col,
    concat,
    concat_ws,
    date_format,
    lit,
    max as spark_max,
    min as spark_min,
    round as spark_round,
    row_number,
    sum as spark_sum,
    to_timestamp,
    trim,
    when,
)
from pyspark.storagelevel import StorageLevel

from src.silver.config import (
    BRONZE_DIR,
    PROJECT_ROOT,
    QUARANTINE_DIR,
    SENSOR_ANOMALY_LEDGER_PATH,
    SILVER_DIR,
    VALID_WMO_WEATHER_CODES,
    WEATHER_EXTREME_THRESHOLDS,
    WEATHER_HARD_LIMITS,
)
from src.utils.spark import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("clean_weather")


def clean_and_validate_weather(
    bronze_weather_dir: Path = BRONZE_DIR / "weather",
    silver_weather_dir: Path = SILVER_DIR / "weather",
    anomaly_ledger_path: Path = SENSOR_ANOMALY_LEDGER_PATH,
    quarantine_dir: Path = QUARANTINE_DIR / "weather",
) -> Dict[str, Any]:
    """
    Load Bronze weather records, apply physical bounds checking and atmospheric consistency laws,
    mask corrupted sensors to NULL (avoiding row loss), record detailed forensic anomaly logs,
    quarantine unkeyed/unparseable rows, build windowed rolling aggregations, and save partitioned Silver Parquet tables.
    """
    start_time = time.time()
    processed_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    bronze_files = sorted(bronze_weather_dir.glob("**/*.parquet"))
    if not bronze_files:
        logger.warning(f"No Bronze weather files found in {bronze_weather_dir}")
        return {"status": "skipped", "reason": "no_input_files"}

    spark = get_spark_session(app_name="DAT535-Silver-Clean-Weather")
    spark.conf.set("spark.sql.ansi.enabled", "false")
    logger.info(f"Loading {len(bronze_files)} Bronze weather partition files into PySpark...")

    # Load raw records with PySpark
    df_raw = spark.read.parquet(*[str(f) for f in bronze_files])
    input_rows = df_raw.count()
    logger.info(f"Loaded {input_rows:,} raw weather observations from Bronze.")

    cols = df_raw.columns

    # 1. Row-level Primary Key Check, Temporal Standardization & Unkeyed Quarantine
    cond_missing_grid = (
        col("grid_id").isNull()
        | trim(col("grid_id")).isin(["", "nan", "None", "NONE", "NAN", "null", "NULL"])
        if "grid_id" in cols
        else lit(True)
    )
    cond_missing_time = (
        col("time").isNull()
        | trim(col("time")).isin(["", "nan", "None", "NONE", "NAN", "null", "NULL"])
        if "time" in cols
        else lit(True)
    )
    parsed_ts = to_timestamp(col("time")) if "time" in cols else lit(None)
    cond_invalid_ts = (~cond_missing_time) & parsed_ts.isNull()
    cond_corrupt_lineage = (
        (col("_batch_id").isNull() | trim(col("_batch_id")).isin(["", "nan", "None", "<NA>"]))
        if "_batch_id" in cols
        else lit(False)
    )

    quarantine_mask = cond_missing_grid | cond_missing_time | cond_invalid_ts | cond_corrupt_lineage

    quarantine_reasons = array_compact(
        array(
            when(cond_missing_grid, lit("MISSING_GRID_ID")),
            when(cond_missing_time, lit("MISSING_TIME_KEY")),
            when(cond_invalid_ts, lit("INVALID_TIMESTAMP_FORMAT")),
            when(cond_corrupt_lineage, lit("CORRUPTED_INGESTION_METADATA")),
        )
    )

    df_evaluated = (
        df_raw.withColumn("_quarantine_reason", concat_ws("; ", quarantine_reasons))
        .withColumn("_is_quarantined", quarantine_mask)
        .withColumn("timestamp_utc", parsed_ts)
    )

    # Persist evaluated DataFrame before branching into valid and quarantined tables (prevents double execution)
    df_evaluated.persist(StorageLevel.MEMORY_AND_DISK)

    df_quarantined = df_evaluated.filter(col("_is_quarantined") == True)
    quarantined_weather_rows = df_quarantined.count()

    if quarantined_weather_rows > 0:
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        q_path = quarantine_dir / "quarantined_weather.parquet"
        save_q = df_quarantined.withColumn("quarantine_reason", col("_quarantine_reason")) \
                               .withColumn("_quarantined_at_utc", lit(processed_at_utc)) \
                               .drop("_is_quarantined", "_quarantine_reason")
        save_q.toPandas().to_parquet(q_path, index=False, compression="snappy")
        logger.warning(f"Quarantined {quarantined_weather_rows:,} unkeyed/unparseable weather rows to {q_path}")

    # Retain only valid rows
    df_valid = df_evaluated.filter(col("_is_quarantined") == False).drop("_is_quarantined", "_quarantine_reason")

    # Deduplicate: if multiple batches contain the same (grid_id, timestamp_utc), keep latest ingestion
    before_dedup_count = df_valid.count()
    w_dedup = Window.partitionBy("grid_id", "timestamp_utc").orderBy(
        col("_ingested_at_utc").desc() if "_ingested_at_utc" in cols else col("timestamp_utc").desc()
    )
    df_clean_spark = df_valid.withColumn("_rn", row_number().over(w_dedup)).filter(col("_rn") == 1).drop("_rn")
    df_evaluated.unpersist()

    valid_count = df_clean_spark.count()
    duplicate_rows_dropped = before_dedup_count - valid_count

    if duplicate_rows_dropped > 0:
        logger.info(
            f"Deduplicated Bronze weather records: dropped {duplicate_rows_dropped:,} "
            f"duplicate observations on (grid_id, timestamp_utc)."
        )

    # Convert to Pandas for detailed physical laws auditing & forensic anomaly ledger construction
    df_clean = df_clean_spark.toPandas().copy()
    df_clean_spark.unpersist()
    df_clean["timestamp_utc"] = pd.to_datetime(df_clean["timestamp_utc"], utc=True)

    # 2. Comprehensive Sensor Physical Laws & Bounds Validation
    anomaly_records: List[Dict[str, Any]] = []
    audit_notes_map: Dict[int, List[str]] = {idx: [] for idx in df_clean.index}

    def evaluate_sensor_rule(
        mask_condition: pd.Series,
        col_name: str,
        rule_name: str,
        reason_template: str,
        deviation_calc=None,
    ):
        violating_indices = df_clean[mask_condition].index
        if len(violating_indices) == 0:
            return

        for idx in violating_indices:
            raw_val = df_clean.at[idx, col_name]
            r_ts = df_clean.at[idx, "timestamp_utc"]
            r_grid = df_clean.at[idx, "grid_id"]

            dev_str = str(deviation_calc(df_clean.loc[idx])) if deviation_calc else "OUT_OF_BOUNDS"
            reason_str = reason_template.format(val=raw_val, dev=dev_str)

            anomaly_records.append({
                "timestamp_utc": r_ts,
                "grid_id": r_grid,
                "field_name": col_name,
                "raw_value": float(raw_val) if pd.notna(raw_val) and isinstance(raw_val, (int, float, np.number)) else str(raw_val),
                "rule_violated": rule_name,
                "reason": reason_str,
                "action_taken": "MASKED_TO_NAN",
                "masked_at_utc": processed_at_utc,
            })
            audit_notes_map[idx].append(f"MASKED {col_name}={raw_val} ({rule_name})")

        df_clean.loc[mask_condition, col_name] = None

    # A. Temperature Physical Bounds [-35°C, 45°C]
    if "temperature_2m" in df_clean.columns:
        corrupt_temp = df_clean["temperature_2m"].notna() & ~df_clean["temperature_2m"].between(
            WEATHER_HARD_LIMITS["temp_min"], WEATHER_HARD_LIMITS["temp_max"]
        )
        evaluate_sensor_rule(corrupt_temp, "temperature_2m", "TEMPERATURE_OUT_OF_BOUNDS", "Value {val}°C exceeds NYC physical climate range [-35°C, 45°C]")

    # B. Apparent Temperature Bounds [-45°C, 55°C]
    if "apparent_temperature" in df_clean.columns:
        corrupt_app_temp = df_clean["apparent_temperature"].notna() & ~df_clean["apparent_temperature"].between(
            WEATHER_HARD_LIMITS["apparent_temp_min"], WEATHER_HARD_LIMITS["apparent_temp_max"]
        )
        evaluate_sensor_rule(corrupt_app_temp, "apparent_temperature", "APPARENT_TEMPERATURE_OUT_OF_BOUNDS", "Value {val}°C exceeds heat index / wind chill range [-45°C, 55°C]")

    # C. Atmospheric Thermodynamic Law: Dew Point <= Temperature
    if "dew_point_2m" in df_clean.columns and "temperature_2m" in df_clean.columns:
        corrupt_dew = (
            df_clean["dew_point_2m"].notna()
            & df_clean["temperature_2m"].notna()
            & (df_clean["dew_point_2m"] > (df_clean["temperature_2m"] + 0.1))
        )
        evaluate_sensor_rule(
            corrupt_dew,
            "dew_point_2m",
            "DEW_POINT_EXCEEDS_TEMPERATURE",
            "Dew point {val}°C physically exceeds ambient temperature ({dev})",
            deviation_calc=lambda row: f"temp={row['temperature_2m']}°C",
        )

    # D. Relative Humidity Bounds [0%, 100%]
    if "relative_humidity_2m" in df_clean.columns:
        corrupt_rh = df_clean["relative_humidity_2m"].notna() & ~df_clean["relative_humidity_2m"].between(
            WEATHER_HARD_LIMITS["humidity_min"], WEATHER_HARD_LIMITS["humidity_max"]
        )
        evaluate_sensor_rule(corrupt_rh, "relative_humidity_2m", "RELATIVE_HUMIDITY_OUT_OF_BOUNDS", "Humidity {val}% must strictly be between 0% and 100%")

    # E. Precipitation Bounds [0.0, 150.0 mm/hr]
    if "precipitation" in df_clean.columns:
        corrupt_precip = df_clean["precipitation"].notna() & (
            (df_clean["precipitation"] < WEATHER_HARD_LIMITS["precip_min"])
            | (df_clean["precipitation"] > WEATHER_HARD_LIMITS["precip_max"])
        )
        evaluate_sensor_rule(corrupt_precip, "precipitation", "PRECIPITATION_OUT_OF_BOUNDS", "Precipitation {val} mm/hr outside realistic physical limits [0, 150]")

    # F. Rain Bounds (rain >= 0 and rain <= precipitation)
    if "rain" in df_clean.columns and "precipitation" in df_clean.columns:
        corrupt_rain = df_clean["rain"].notna() & (
            (df_clean["rain"] < 0.0)
            | (df_clean["precipitation"].notna() & (df_clean["rain"] > (df_clean["precipitation"] + 0.05)))
        )
        evaluate_sensor_rule(
            corrupt_rain,
            "rain",
            "RAIN_EXCEEDS_TOTAL_PRECIP",
            "Rain component {val} mm exceeds total precipitation ({dev})",
            deviation_calc=lambda row: f"precip={row['precipitation']} mm",
        )

    # G. Snowfall and Snow Depth
    if "snowfall" in df_clean.columns:
        corrupt_snow = df_clean["snowfall"].notna() & (df_clean["snowfall"] < 0.0)
        evaluate_sensor_rule(corrupt_snow, "snowfall", "NEGATIVE_SNOWFALL", "Snowfall {val} cm cannot be negative")

    if "snow_depth" in df_clean.columns:
        corrupt_snowdepth = df_clean["snow_depth"].notna() & (df_clean["snow_depth"] < 0.0)
        evaluate_sensor_rule(corrupt_snowdepth, "snow_depth", "NEGATIVE_SNOW_DEPTH", "Snow depth {val} m cannot be negative")

    # H. Sustained Wind Speed [0.0, 65.0 m/s]
    if "wind_speed_10m" in df_clean.columns:
        corrupt_wind = df_clean["wind_speed_10m"].notna() & ~df_clean["wind_speed_10m"].between(
            WEATHER_HARD_LIMITS["wind_speed_min"], WEATHER_HARD_LIMITS["wind_speed_max"]
        )
        evaluate_sensor_rule(corrupt_wind, "wind_speed_10m", "WIND_SPEED_OUT_OF_BOUNDS", "Sustained wind speed {val} m/s outside physical sanity bounds [0, 65]")

    # I. Aerodynamic Law: Wind Gust >= Sustained Wind Speed
    if "wind_gusts_10m" in df_clean.columns and "wind_speed_10m" in df_clean.columns:
        corrupt_gust = (
            df_clean["wind_gusts_10m"].notna()
            & df_clean["wind_speed_10m"].notna()
            & (df_clean["wind_gusts_10m"] < (df_clean["wind_speed_10m"] - 0.01))
        )
        evaluate_sensor_rule(
            corrupt_gust,
            "wind_gusts_10m",
            "GUST_LESS_THAN_SUSTAINED_SPEED",
            "Peak gust {val} m/s physically cannot be slower than sustained speed ({dev})",
            deviation_calc=lambda row: f"sustained={row['wind_speed_10m']} m/s",
        )

    # J. Wind Direction [0°, 360°]
    if "wind_direction_10m" in df_clean.columns:
        corrupt_wind_dir = df_clean["wind_direction_10m"].notna() & ~df_clean["wind_direction_10m"].between(
            WEATHER_HARD_LIMITS["wind_dir_min"], WEATHER_HARD_LIMITS["wind_dir_max"]
        )
        evaluate_sensor_rule(corrupt_wind_dir, "wind_direction_10m", "WIND_DIRECTION_OUT_OF_BOUNDS", "Compass wind direction {val}° outside [0, 360]")

    # K. Surface Barometric Pressure [870, 1085 hPa]
    if "surface_pressure" in df_clean.columns:
        corrupt_pressure = df_clean["surface_pressure"].notna() & ~df_clean["surface_pressure"].between(
            WEATHER_HARD_LIMITS["pressure_min"], WEATHER_HARD_LIMITS["pressure_max"]
        )
        evaluate_sensor_rule(corrupt_pressure, "surface_pressure", "SURFACE_PRESSURE_OUT_OF_BOUNDS", "Surface pressure {val} hPa outside terrestrial extremes [870, 1085]")

    # L. World Meteorological Organization (WMO) Code Table 4677 Validity
    if "weather_code" in df_clean.columns:
        corrupt_wmo = df_clean["weather_code"].notna() & ~df_clean["weather_code"].isin(VALID_WMO_WEATHER_CODES)
        evaluate_sensor_rule(corrupt_wmo, "weather_code", "INVALID_WMO_WEATHER_CODE", "Weather code {val} not found in WMO Code Table 4677 standard")

    # Attach Row-Level Observability Metadata
    df_clean["quality_audit_notes"] = [
        "; ".join(audit_notes_map[idx]) if audit_notes_map[idx] else None
        for idx in df_clean.index
    ]
    df_clean["has_corrupted_sensor_fields"] = df_clean["quality_audit_notes"].notna()
    corrupted_fields_rows = int(df_clean["has_corrupted_sensor_fields"].sum())

    # Write Sensor Anomaly Audit Ledger Parquet
    anomaly_ledger_path.parent.mkdir(parents=True, exist_ok=True)
    if anomaly_records:
        df_ledger = pd.DataFrame(anomaly_records)
        logger.warning(
            f"Sensor Anomaly Audit: Logged {len(anomaly_records):,} corrupt sensor readings "
            f"across {corrupted_fields_rows:,} rows to {anomaly_ledger_path}"
        )
    else:
        df_ledger = pd.DataFrame(
            columns=["timestamp_utc", "grid_id", "field_name", "raw_value", "rule_violated", "reason", "action_taken", "masked_at_utc"]
        )
    pq.write_table(pa.Table.from_pandas(df_ledger, preserve_index=False), anomaly_ledger_path, compression="snappy")

    # 3. Cumulative & Lagged Weather Features (Chronological Rolling Windows via PySpark Window functions - Lab 3)
    # Computed on MASKED sensor values so corrupted readings do not pollute rolling metrics!
    # Uses true chronological time windows rangeBetween(-2 * 3600, 0) on epoch seconds.
    df_window_spark = spark.createDataFrame(df_clean)
    df_window_spark = df_window_spark.withColumn("_ts_secs", col("timestamp_utc").cast("long"))

    w_grid_3 = Window.partitionBy("grid_id").orderBy("_ts_secs").rangeBetween(-2 * 3600, 0)
    w_grid_6 = Window.partitionBy("grid_id").orderBy("_ts_secs").rangeBetween(-5 * 3600, 0)
    w_grid_24 = Window.partitionBy("grid_id").orderBy("_ts_secs").rangeBetween(-23 * 3600, 0)

    if "precipitation" in df_window_spark.columns:
        df_window_spark = (
            df_window_spark.withColumn("precip_past_3h_mm", spark_round(spark_sum("precipitation").over(w_grid_3), 2))
            .withColumn("precip_past_6h_mm", spark_round(spark_sum("precipitation").over(w_grid_6), 2))
            .withColumn("precip_past_24h_mm", spark_round(spark_sum("precipitation").over(w_grid_24), 2))
        )
    else:
        df_window_spark = (
            df_window_spark.withColumn("precip_past_3h_mm", lit(None).cast("double"))
            .withColumn("precip_past_6h_mm", lit(None).cast("double"))
            .withColumn("precip_past_24h_mm", lit(None).cast("double"))
        )

    if "wind_gusts_10m" in df_window_spark.columns:
        df_window_spark = df_window_spark.withColumn(
            "max_wind_gust_past_6h_ms", spark_round(spark_max("wind_gusts_10m").over(w_grid_6), 2)
        )
    else:
        df_window_spark = df_window_spark.withColumn("max_wind_gust_past_6h_ms", lit(None).cast("double"))

    if "temperature_2m" in df_window_spark.columns:
        df_window_spark = (
            df_window_spark.withColumn("min_temp_past_24h_c", spark_round(spark_min("temperature_2m").over(w_grid_24), 2))
            .withColumn("max_temp_past_24h_c", spark_round(spark_max("temperature_2m").over(w_grid_24), 2))
        )
    else:
        df_window_spark = (
            df_window_spark.withColumn("min_temp_past_24h_c", lit(None).cast("double"))
            .withColumn("max_temp_past_24h_c", lit(None).cast("double"))
        )

    df_window_spark = df_window_spark.drop("_ts_secs")
    df_clean = df_window_spark.toPandas().copy()
    df_window_spark.unpersist()

    # 4. Soft Quality Tagging (Extreme Weather Flags - NEVER DISCARD REAL EXTREMES)
    cond_cold = df_clean["temperature_2m"] < WEATHER_EXTREME_THRESHOLDS["temp_extreme_cold"]
    cond_heat = df_clean["temperature_2m"] > WEATHER_EXTREME_THRESHOLDS["temp_extreme_heat"]
    cond_torrential = df_clean["precipitation"] >= WEATHER_EXTREME_THRESHOLDS["precip_torrential_hr"]
    cond_severe_wind = (
        df_clean["wind_gusts_10m"] >= WEATHER_EXTREME_THRESHOLDS["wind_gust_severe"]
        if "wind_gusts_10m" in df_clean.columns
        else pd.Series(False, index=df_clean.index)
    )

    df_clean["is_extreme_weather"] = (cond_cold | cond_heat | cond_torrential | cond_severe_wind).fillna(False)
    extreme_rows_count = int(df_clean["is_extreme_weather"].sum())
    logger.info(f"Tagged {extreme_rows_count:,} observations as extreme weather events (preserved intact).")

    # 5. Standardization and Derived Row Attributes (Pure Metric: Celsius and m/s)
    df_clean["is_freezing"] = df_clean["temperature_2m"] <= 0.0
    df_clean["is_raining"] = df_clean["precipitation"] > 0.0

    for int16_col in ["weather_code", "relative_humidity_2m", "wind_direction_10m"]:
        if int16_col in df_clean.columns:
            df_clean[int16_col] = pd.to_numeric(df_clean[int16_col], errors="coerce").astype("Int16")

    # 6. Silver Audit Metadata
    df_final_pd = df_clean
    df_final_pd["timestamp_utc"] = pd.to_datetime(df_final_pd["timestamp_utc"], utc=True)
    df_final_pd["_silver_processed_at_utc"] = processed_at_utc
    df_final_pd["_partition_year"] = df_final_pd["timestamp_utc"].dt.strftime("%Y")

    # 7. Write Partitioned Parquet: grid_id=GRID_XX_YY / year=YYYY / clean_weather.parquet
    silver_weather_dir.mkdir(parents=True, exist_ok=True)
    written_files = 0
    for (grid_id, year), sub_df in df_final_pd.groupby(["grid_id", "_partition_year"]):
        partition_dir = silver_weather_dir / f"grid_id={grid_id}" / f"year={year}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        out_path = partition_dir / "clean_weather.parquet"
        save_df = sub_df.drop(columns=["_partition_year"])

        table = pa.Table.from_pandas(save_df, preserve_index=False)
        pq.write_table(table, out_path, compression="snappy")
        written_files += 1

    df_evaluated.unpersist()
    df_clean_spark.unpersist()

    elapsed = time.time() - start_time
    logger.info(
        f"Completed Silver Weather processing with PySpark: {len(df_final_pd):,} valid rows | "
        f"{written_files} partition files written | {len(anomaly_records)} sensor anomalies ledgered | {elapsed:.2f}s"
    )

    return {
        "dataset": "silver_weather",
        "input_rows": input_rows,
        "valid_rows": len(df_final_pd),
        "dropped_unkeyed_rows": quarantined_weather_rows,
        "quarantined_rows": quarantined_weather_rows,
        "duplicate_rows_dropped": duplicate_rows_dropped,
        "corrupted_fields_masked": corrupted_fields_rows,
        "sensor_anomalies_logged": len(anomaly_records),
        "extreme_weather_tagged_rows": extreme_rows_count,
        "distinct_grid_cells": int(df_final_pd["grid_id"].nunique()),
        "partitions_written": written_files,
        "duration_seconds": round(elapsed, 2),
        "status": "success",
    }


if __name__ == "__main__":
    clean_and_validate_weather()
