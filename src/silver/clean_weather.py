"""
Silver Layer: Open-Meteo ERA5-Land Weather Cleaning and Quality Tagging.
Filters corrupt/sentinel records, applies physical sanity bounds, tags extreme weather events,
and standardizes timestamps and units.
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

from src.silver.config import (
    BRONZE_DIR,
    PROJECT_ROOT,
    SILVER_DIR,
    WEATHER_EXTREME_THRESHOLDS,
    WEATHER_HARD_LIMITS,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("clean_weather")


def clean_and_validate_weather(
    bronze_weather_dir: Path = BRONZE_DIR / "weather",
    silver_weather_dir: Path = SILVER_DIR / "weather",
) -> Dict[str, Any]:
    """
    Load Bronze weather records, apply physical bounds checking, tag extreme weather,
    derive standardized units (Fahrenheit, freezing/rain flags), and write to Silver Parquet.
    """
    start_time = time.time()
    processed_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    bronze_files = sorted(bronze_weather_dir.glob("**/*.parquet"))
    if not bronze_files:
        logger.warning(f"No Bronze weather files found in {bronze_weather_dir}")
        return {"status": "skipped", "reason": "no_input_files"}

    logger.info(f"Loading {len(bronze_files)} Bronze weather partition files...")
    df_raw = pd.concat([pd.read_parquet(f) for f in bronze_files], ignore_index=True)
    input_rows = len(df_raw)
    logger.info(f"Loaded {input_rows:,} raw weather observations from Bronze.")

    # 1. Row-level Primary Key Check & Temporal Standardization
    valid_keys_mask = df_raw["time"].notna() & df_raw["grid_id"].notna()
    dropped_unkeyed_rows = int((~valid_keys_mask).sum())
    df_clean = df_raw[valid_keys_mask].copy()

    df_clean["timestamp_utc"] = pd.to_datetime(df_clean["time"], utc=True, errors="coerce")
    valid_ts_mask = df_clean["timestamp_utc"].notna()
    dropped_invalid_ts = int((~valid_ts_mask).sum())
    df_clean = df_clean[valid_ts_mask].copy()

    # Deduplicate: if multiple batches contain the same (grid_id, timestamp_utc), keep latest ingestion
    before_dedup_count = len(df_clean)
    if "_ingested_at_utc" in df_clean.columns:
        df_clean = df_clean.sort_values("_ingested_at_utc", ascending=False)
    df_clean = df_clean.drop_duplicates(subset=["grid_id", "timestamp_utc"], keep="first").copy()
    duplicate_rows_dropped = int(before_dedup_count - len(df_clean))
    if duplicate_rows_dropped > 0:
        logger.info(
            f"Deduplicated Bronze weather records: dropped {duplicate_rows_dropped:,} "
            f"duplicate observations on (grid_id, timestamp_utc)."
        )

    # 2. Column-Level Nullification (NEVER drop the whole row for a single sensor glitch)
    # Check physical bounds on each individual measurement:
    corrupt_temp_mask = ~df_clean["temperature_2m"].between(
        WEATHER_HARD_LIMITS["temp_min"], WEATHER_HARD_LIMITS["temp_max"]
    )
    corrupt_precip_mask = df_clean["precipitation"] < WEATHER_HARD_LIMITS["precip_min"]
    corrupt_wind_mask = df_clean["wind_speed_10m"] < WEATHER_HARD_LIMITS["wind_min"]
    corrupt_pressure_mask = ~df_clean["surface_pressure"].between(
        WEATHER_HARD_LIMITS["pressure_min"], WEATHER_HARD_LIMITS["pressure_max"]
    )

    # Nullify only the affected column, preserving all other valid readings
    df_clean.loc[corrupt_temp_mask, "temperature_2m"] = None
    df_clean.loc[corrupt_precip_mask, "precipitation"] = None
    df_clean.loc[corrupt_wind_mask, "wind_speed_10m"] = None
    df_clean.loc[corrupt_pressure_mask, "surface_pressure"] = None

    # Track if row had any corrupted sensor field
    df_clean["has_corrupted_sensor_fields"] = (
        corrupt_temp_mask | corrupt_precip_mask | corrupt_wind_mask | corrupt_pressure_mask
    )
    corrupted_fields_rows = int(df_clean["has_corrupted_sensor_fields"].sum())

    # 3. Soft Quality Tagging (Extreme Weather Flags - NEVER DISCARD REAL EXTREMES)
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

    # 4. Standardization and Derived Row Attributes (Pure Metric: Celsius and m/s)
    if "timestamp_utc" not in df_clean.columns:
        df_clean["timestamp_utc"] = pd.to_datetime(df_clean["time"], utc=True)
    df_clean["is_freezing"] = df_clean["temperature_2m"] <= 0.0
    df_clean["is_raining"] = df_clean["precipitation"] > 0.0

    # Cast discrete integer weather features to nullable Int16 (prevents silent float64 upcasting in left joins)
    for int16_col in ["weather_code", "relative_humidity_2m", "wind_direction_10m"]:
        if int16_col in df_clean.columns:
            df_clean[int16_col] = pd.to_numeric(df_clean[int16_col], errors="coerce").astype("Int16")

    # 5. Cumulative & Lagged Weather Features (Chronological Rolling Windows per Grid Cell)
    # Sort chronologically by grid_id and timestamp_utc to guarantee window integrity
    df_clean = df_clean.sort_values(["grid_id", "timestamp_utc"]).reset_index(drop=True)

    # Rolling precipitation accumulations: 3-hour, 6-hour, and 24-hour windows
    df_clean["precip_past_3h_mm"] = df_clean.groupby("grid_id")["precipitation"].transform(
        lambda s: s.rolling(window=3, min_periods=1).sum().round(2)
    )
    df_clean["precip_past_6h_mm"] = df_clean.groupby("grid_id")["precipitation"].transform(
        lambda s: s.rolling(window=6, min_periods=1).sum().round(2)
    )
    df_clean["precip_past_24h_mm"] = df_clean.groupby("grid_id")["precipitation"].transform(
        lambda s: s.rolling(window=24, min_periods=1).sum().round(2)
    )

    # Rolling peak wind gusts: 6-hour window (captures storm severity causing tree & power line damage)
    if "wind_gusts_10m" in df_clean.columns:
        df_clean["max_wind_gust_past_6h_ms"] = df_clean.groupby("grid_id")["wind_gusts_10m"].transform(
            lambda s: s.rolling(window=6, min_periods=1).max().round(2)
        )
    else:
        df_clean["max_wind_gust_past_6h_ms"] = np.nan

    # Rolling temperature extremes: 24-hour minimum (freezing pipe risk) and maximum (heat wave risk)
    df_clean["min_temp_past_24h_c"] = df_clean.groupby("grid_id")["temperature_2m"].transform(
        lambda s: s.rolling(window=24, min_periods=1).min().round(2)
    )
    df_clean["max_temp_past_24h_c"] = df_clean.groupby("grid_id")["temperature_2m"].transform(
        lambda s: s.rolling(window=24, min_periods=1).max().round(2)
    )

    # Silver Audit Metadata
    df_clean["_silver_processed_at_utc"] = processed_at_utc

    # 4. Write Partitioned Parquet: grid_id=GRID_XX_YY / year=YYYY / part_XXXX.parquet
    silver_weather_dir.mkdir(parents=True, exist_ok=True)
    df_clean["_partition_year"] = df_clean["timestamp_utc"].dt.strftime("%Y")

    written_files = 0
    for (grid_id, year), sub_df in df_clean.groupby(["grid_id", "_partition_year"]):
        partition_dir = silver_weather_dir / f"grid_id={grid_id}" / f"year={year}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        out_path = partition_dir / "clean_weather.parquet"
        save_df = sub_df.drop(columns=["_partition_year"])

        table = pa.Table.from_pandas(save_df, preserve_index=False)
        pq.write_table(table, out_path, compression="snappy")
        written_files += 1

    elapsed = time.time() - start_time
    logger.info(
        f"Completed Silver Weather processing: {len(df_clean):,} valid rows | "
        f"{written_files} partition files written | {elapsed:.2f}s"
    )

    return {
        "dataset": "silver_weather",
        "input_rows": input_rows,
        "valid_rows": len(df_clean),
        "dropped_unkeyed_rows": dropped_unkeyed_rows + dropped_invalid_ts,
        "duplicate_rows_dropped": duplicate_rows_dropped,
        "corrupted_fields_masked": corrupted_fields_rows,
        "extreme_weather_tagged_rows": extreme_rows_count,
        "distinct_grid_cells": df_clean["grid_id"].nunique(),
        "partitions_written": written_files,
        "duration_seconds": round(elapsed, 2),
        "status": "success",
    }


if __name__ == "__main__":
    clean_and_validate_weather()
