"""
Gold Layer: Daily Service Demand Mart Builder.
Grain: date × grid_id × borough × complaint_type
Aggregates 311 demand and local daily meteorological conditions to measure environmental elasticity of citizen complaints.
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

from src.gold.config import GOLD_MARTS_DIR, SILVER_311_DIR, SILVER_WEATHER_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("build_service_demand")


def build_daily_service_demand_mart(
    silver_311_dir: Path = SILVER_311_DIR,
    silver_weather_dir: Path = SILVER_WEATHER_DIR,
    output_dir: Path = GOLD_MARTS_DIR,
) -> Dict[str, Any]:
    """
    Construct the Daily Service Demand Mart.
    Pre-aggregates 311 complaints and meteorological observations to the daily grain before joining,
    ensuring zero row-loss reconciliation and complete weather-coverage tracking.
    """
    start_time = time.time()
    built_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 1. Load Clean Silver 311 Records
    files_311 = sorted(silver_311_dir.glob("**/*.parquet"))
    if not files_311:
        raise FileNotFoundError(f"No Silver 311 files found in {silver_311_dir}")
    logger.info(f"Loading {len(files_311)} Silver 311 partition files...")
    df_311 = pd.concat([pd.read_parquet(f) for f in files_311], ignore_index=True)
    total_311_input = len(df_311)
    logger.info(f"Loaded {total_311_input:,} clean 311 records.")

    # 2. Load Clean Silver Weather Observations
    files_weather = sorted(silver_weather_dir.glob("**/*.parquet"))
    if not files_weather:
        raise FileNotFoundError(f"No Silver weather files found in {silver_weather_dir}")
    logger.info(f"Loading {len(files_weather)} Silver weather partition files across grid cells...")
    df_weather = pd.concat([pd.read_parquet(f) for f in files_weather], ignore_index=True)
    logger.info(f"Loaded {len(df_weather):,} hourly weather observations across {df_weather['grid_id'].nunique()} grid cells.")

    # 3. Aggregate Weather to Daily Grain (date × grid_id)
    df_weather["date"] = pd.to_datetime(df_weather["timestamp_utc"], utc=True).dt.date
    daily_weather = df_weather.groupby(["date", "grid_id"]).agg(
        total_precipitation_mm=("precipitation", lambda s: round(float(s.sum()), 2)),
        hours_of_precipitation=("precipitation", lambda s: int((s > 0.0).sum())),
        max_wind_gust_ms=("wind_gusts_10m", lambda s: round(float(s.max()), 2) if s.notna().any() else np.nan),
        avg_temperature_c=("temperature_2m", lambda s: round(float(s.mean()), 2) if s.notna().any() else np.nan),
        min_temperature_c=("temperature_2m", lambda s: round(float(s.min()), 2) if s.notna().any() else np.nan),
        max_temperature_c=("temperature_2m", lambda s: round(float(s.max()), 2) if s.notna().any() else np.nan),
        is_extreme_weather_day=("is_extreme_weather", lambda s: bool(s.any())),
        valid_weather_obs_count=("temperature_2m", lambda s: int(s.notna().sum())),
    ).reset_index()

    # Weather coverage percentage (24 hours in a full day)
    daily_weather["weather_data_coverage_pct"] = (
        (daily_weather["valid_weather_obs_count"] / 24.0) * 100.0
    ).round(1)

    # 4. Aggregate 311 to Daily Demand Grain (date × grid_id × borough × complaint_type)
    df_311["date"] = pd.to_datetime(df_311["created_at_utc"], utc=True).dt.date
    daily_311 = df_311.groupby(["date", "grid_id", "borough", "complaint_type"]).agg(
        complaint_count=("unique_key", "count"),
        closed_complaint_count=("is_ticket_closed", lambda s: int(s.sum())),
        distinct_agencies_handling=("agency", "nunique"),
    ).reset_index()

    reconciled_total_complaints = int(daily_311["complaint_count"].sum())
    if reconciled_total_complaints != total_311_input:
        raise ValueError(
            f"Row count reconciliation violation in Daily Demand Mart! "
            f"Silver input ({total_311_input}) != Mart total ({reconciled_total_complaints})"
        )

    # 5. Left Join 311 Daily Demand with Daily Local Weather
    logger.info("Joining Daily 311 Demand with Daily Weather Reanalysis on (date, grid_id)...")
    mart_df = pd.merge(daily_311, daily_weather, on=["date", "grid_id"], how="left")

    # Impute missing weather flags for unlocated or unobserved grid cells
    mart_df["weather_data_coverage_pct"] = mart_df["weather_data_coverage_pct"].fillna(0.0)
    mart_df["is_extreme_weather_day"] = mart_df["is_extreme_weather_day"].astype("boolean").fillna(False).astype(bool)

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
        f"Built Gold Daily Service Demand Mart: {len(mart_df):,} rows | "
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
