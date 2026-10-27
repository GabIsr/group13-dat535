"""
Silver Layer: Open-Meteo ERA5-Land Weather Cleaning, Physical Bound Validation,
Atmospheric Law Verification, and Sensor Anomaly Ledgering.
Filters corrupt/sentinel records, applies physical sanity bounds, tags extreme weather events,
masks corrupted fields to NaN without row loss, and maintains a complete forensic Audit Ledger.
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
    QUARANTINE_DIR,
    SENSOR_ANOMALY_LEDGER_PATH,
    SILVER_DIR,
    VALID_WMO_WEATHER_CODES,
    WEATHER_EXTREME_THRESHOLDS,
    WEATHER_HARD_LIMITS,
)

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
    mask corrupted sensors to NaN (avoiding row loss), record detailed forensic anomaly logs,
    quarantine unkeyed/unparseable rows, and save partitioned Silver Parquet tables.
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

    # 1. Row-level Primary Key Check, Temporal Standardization & Unkeyed Quarantine
    parsed_ts = (
        pd.to_datetime(df_raw["time"], utc=True, errors="coerce")
        if "time" in df_raw.columns
        else pd.Series(pd.NaT, index=df_raw.index)
    )
    cond_missing_grid = (
        df_raw["grid_id"].isna()
        | df_raw["grid_id"].astype(str).str.strip().isin(["", "nan", "None", "NONE", "NAN", "null", "NULL"])
        if "grid_id" in df_raw.columns
        else pd.Series(True, index=df_raw.index)
    )
    cond_missing_time = (
        df_raw["time"].isna()
        | df_raw["time"].astype(str).str.strip().isin(["", "nan", "None", "NONE", "NAN", "null", "NULL"])
        if "time" in df_raw.columns
        else pd.Series(True, index=df_raw.index)
    )
    cond_invalid_ts = (~cond_missing_time) & parsed_ts.isna()
    cond_corrupt_lineage = (
        (df_raw["_batch_id"].isna() | df_raw["_batch_id"].astype(str).str.strip().isin(["", "nan", "None", "<NA>"]))
        if "_batch_id" in df_raw.columns
        else pd.Series(False, index=df_raw.index)
    )

    quarantine_mask = cond_missing_grid | cond_missing_time | cond_invalid_ts | cond_corrupt_lineage
    dropped_unkeyed_rows = int((cond_missing_grid | cond_missing_time).sum())
    dropped_invalid_ts = int(cond_invalid_ts.sum())
    quarantined_weather_rows = int(quarantine_mask.sum())

    if quarantined_weather_rows > 0:
        df_quarantined = df_raw[quarantine_mask].copy()
        q_idx = df_quarantined.index
        w_rules = [
            (cond_missing_grid.loc[q_idx], "MISSING_GRID_ID"),
            (cond_missing_time.loc[q_idx], "MISSING_TIME_KEY"),
            (cond_invalid_ts.loc[q_idx], "INVALID_TIMESTAMP_FORMAT"),
            (cond_corrupt_lineage.loc[q_idx], "CORRUPTED_INGESTION_METADATA"),
        ]
        df_quarantined["quarantine_reason"] = [
            "; ".join(label for mask, label in w_rules if mask.at[idx])
            for idx in q_idx
        ]
        df_quarantined["_quarantined_at_utc"] = processed_at_utc
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        q_path = quarantine_dir / "quarantined_weather.parquet"
        pq.write_table(pa.Table.from_pandas(df_quarantined, preserve_index=False), q_path, compression="snappy")
        logger.warning(f"Quarantined {quarantined_weather_rows:,} unkeyed/unparseable weather rows to {q_path}")

    df_clean = df_raw[~quarantine_mask].copy()
    df_clean["timestamp_utc"] = parsed_ts[~quarantine_mask]

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

    # 2. Comprehensive Sensor Physical Laws & Bounds Validation
    # We maintain a forensic anomaly log of every single masked field.
    anomaly_records: List[Dict[str, Any]] = []
    audit_notes_map: Dict[int, List[str]] = {idx: [] for idx in df_clean.index}

    def evaluate_sensor_rule(
        mask_condition: pd.Series,
        col_name: str,
        rule_name: str,
        reason_template: str,
        deviation_calc=None,
    ):
        """Helper to record forensic anomalies, mask corrupt values to NaN, and build audit notes."""
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

        # Mask corrupted values to None (NaN)
        df_clean.loc[mask_condition, col_name] = None

    # A. Temperature Physical Bounds [-35°C, 45°C] (NOAA Central Park all-time records)
    if "temperature_2m" in df_clean.columns:
        corrupt_temp = df_clean["temperature_2m"].notna() & ~df_clean["temperature_2m"].between(
            WEATHER_HARD_LIMITS["temp_min"], WEATHER_HARD_LIMITS["temp_max"]
        )
        evaluate_sensor_rule(
            corrupt_temp,
            "temperature_2m",
            "TEMPERATURE_OUT_OF_BOUNDS",
            "Value {val}°C exceeds NYC physical climate range [-35°C, 45°C]",
        )

    # B. Apparent Temperature Bounds [-45°C, 55°C]
    if "apparent_temperature" in df_clean.columns:
        corrupt_app_temp = df_clean["apparent_temperature"].notna() & ~df_clean["apparent_temperature"].between(
            WEATHER_HARD_LIMITS["apparent_temp_min"], WEATHER_HARD_LIMITS["apparent_temp_max"]
        )
        evaluate_sensor_rule(
            corrupt_app_temp,
            "apparent_temperature",
            "APPARENT_TEMPERATURE_OUT_OF_BOUNDS",
            "Value {val}°C exceeds heat index / wind chill range [-45°C, 55°C]",
        )

    # C. Atmospheric Thermodynamic Law: Dew Point <= Temperature
    # (In the physical atmosphere, dew point can never exceed dry-bulb temperature; RH <= 100%)
    if "dew_point_2m" in df_clean.columns and "temperature_2m" in df_clean.columns:
        corrupt_dew = (
            df_clean["dew_point_2m"].notna() &
            df_clean["temperature_2m"].notna() &
            (df_clean["dew_point_2m"] > (df_clean["temperature_2m"] + 0.1))
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
        evaluate_sensor_rule(
            corrupt_rh,
            "relative_humidity_2m",
            "RELATIVE_HUMIDITY_OUT_OF_BOUNDS",
            "Humidity {val}% must strictly be between 0% and 100%",
        )

    # E. Precipitation Bounds [0.0, 150.0 mm/hr]
    if "precipitation" in df_clean.columns:
        corrupt_precip = df_clean["precipitation"].notna() & (
            (df_clean["precipitation"] < WEATHER_HARD_LIMITS["precip_min"]) |
            (df_clean["precipitation"] > WEATHER_HARD_LIMITS["precip_max"])
        )
        evaluate_sensor_rule(
            corrupt_precip,
            "precipitation",
            "PRECIPITATION_OUT_OF_BOUNDS",
            "Precipitation {val} mm/hr outside realistic physical limits [0, 150]",
        )

    # F. Rain Bounds (rain >= 0 and rain <= precipitation)
    if "rain" in df_clean.columns and "precipitation" in df_clean.columns:
        corrupt_rain = df_clean["rain"].notna() & (
            (df_clean["rain"] < 0.0) |
            (df_clean["precipitation"].notna() & (df_clean["rain"] > (df_clean["precipitation"] + 0.05)))
        )
        evaluate_sensor_rule(
            corrupt_rain,
            "rain",
            "RAIN_EXCEEDS_TOTAL_PRECIP",
            "Rain component {val} mm exceeds total precipitation ({dev})",
            deviation_calc=lambda row: f"precip={row['precipitation']} mm",
        )

    # G. Snowfall and Snow Depth (must be non-negative)
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
        evaluate_sensor_rule(
            corrupt_wind,
            "wind_speed_10m",
            "WIND_SPEED_OUT_OF_BOUNDS",
            "Sustained wind speed {val} m/s outside physical sanity bounds [0, 65]",
        )

    # I. Aerodynamic Law: Wind Gust >= Sustained Wind Speed
    # (By definition, maximum instantaneous 3-second gust must be >= sustained average hourly wind)
    if "wind_gusts_10m" in df_clean.columns and "wind_speed_10m" in df_clean.columns:
        corrupt_gust = (
            df_clean["wind_gusts_10m"].notna() &
            df_clean["wind_speed_10m"].notna() &
            (df_clean["wind_gusts_10m"] < (df_clean["wind_speed_10m"] - 0.01))
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
        evaluate_sensor_rule(
            corrupt_wind_dir,
            "wind_direction_10m",
            "WIND_DIRECTION_OUT_OF_BOUNDS",
            "Compass wind direction {val}° outside [0, 360]",
        )

    # K. Surface Barometric Pressure [870, 1085 hPa]
    if "surface_pressure" in df_clean.columns:
        corrupt_pressure = df_clean["surface_pressure"].notna() & ~df_clean["surface_pressure"].between(
            WEATHER_HARD_LIMITS["pressure_min"], WEATHER_HARD_LIMITS["pressure_max"]
        )
        evaluate_sensor_rule(
            corrupt_pressure,
            "surface_pressure",
            "SURFACE_PRESSURE_OUT_OF_BOUNDS",
            "Surface pressure {val} hPa outside terrestrial extremes [870, 1085]",
        )

    # L. World Meteorological Organization (WMO) Code Table 4677 Validity
    if "weather_code" in df_clean.columns:
        corrupt_wmo = df_clean["weather_code"].notna() & ~df_clean["weather_code"].isin(VALID_WMO_WEATHER_CODES)
        evaluate_sensor_rule(
            corrupt_wmo,
            "weather_code",
            "INVALID_WMO_WEATHER_CODE",
            "Weather code {val} not found in WMO Code Table 4677 standard",
        )

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
    df_clean["is_freezing"] = df_clean["temperature_2m"] <= 0.0
    df_clean["is_raining"] = df_clean["precipitation"] > 0.0

    # Cast discrete integer weather features to nullable Int16 (prevents silent float64 upcasting in left joins)
    for int16_col in ["weather_code", "relative_humidity_2m", "wind_direction_10m"]:
        if int16_col in df_clean.columns:
            df_clean[int16_col] = pd.to_numeric(df_clean[int16_col], errors="coerce").astype("Int16")

    # 5. Cumulative & Lagged Weather Features (Chronological Rolling Windows per Grid Cell)
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

    # Rolling peak wind gusts: 6-hour window
    if "wind_gusts_10m" in df_clean.columns:
        df_clean["max_wind_gust_past_6h_ms"] = df_clean.groupby("grid_id")["wind_gusts_10m"].transform(
            lambda s: s.rolling(window=6, min_periods=1).max().round(2)
        )
    else:
        df_clean["max_wind_gust_past_6h_ms"] = np.nan

    # Rolling temperature extremes: 24-hour minimum and maximum
    df_clean["min_temp_past_24h_c"] = df_clean.groupby("grid_id")["temperature_2m"].transform(
        lambda s: s.rolling(window=24, min_periods=1).min().round(2)
    )
    df_clean["max_temp_past_24h_c"] = df_clean.groupby("grid_id")["temperature_2m"].transform(
        lambda s: s.rolling(window=24, min_periods=1).max().round(2)
    )

    # Silver Audit Metadata
    df_clean["_silver_processed_at_utc"] = processed_at_utc

    # 6. Write Partitioned Parquet: grid_id=GRID_XX_YY / year=YYYY / clean_weather.parquet
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
        f"{written_files} partition files written | {len(anomaly_records)} sensor anomalies ledgered | {elapsed:.2f}s"
    )

    return {
        "dataset": "silver_weather",
        "input_rows": input_rows,
        "valid_rows": len(df_clean),
        "dropped_unkeyed_rows": quarantined_weather_rows,
        "quarantined_rows": quarantined_weather_rows,
        "duplicate_rows_dropped": duplicate_rows_dropped,
        "corrupted_fields_masked": corrupted_fields_rows,
        "sensor_anomalies_logged": len(anomaly_records),
        "extreme_weather_tagged_rows": extreme_rows_count,
        "distinct_grid_cells": df_clean["grid_id"].nunique(),
        "partitions_written": written_files,
        "duration_seconds": round(elapsed, 2),
        "status": "success",
    }


if __name__ == "__main__":
    clean_and_validate_weather()
