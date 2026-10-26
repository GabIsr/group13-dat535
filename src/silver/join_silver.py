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

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.silver.config import PROJECT_ROOT, SILVER_DIR

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
    """
    start_time = time.time()
    joined_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 1. Load Silver 311
    files_311 = sorted(silver_311_dir.glob("**/*.parquet"))
    if not files_311:
        raise FileNotFoundError(f"No Silver 311 files found in {silver_311_dir}")

    logger.info(f"Loading {len(files_311)} Silver 311 partition files...")
    df_311 = pd.concat([pd.read_parquet(f) for f in files_311], ignore_index=True)
    total_311 = len(df_311)
    logger.info(f"Loaded {total_311:,} clean 311 records.")

    # 2. Load Silver Weather (The Broadcast Dimension)
    files_weather = sorted(silver_weather_dir.glob("**/*.parquet"))
    if not files_weather:
        raise FileNotFoundError(f"No Silver weather files found in {silver_weather_dir}")

    logger.info(f"Loading {len(files_weather)} Silver weather partition files across grid cells...")
    df_weather = pd.concat([pd.read_parquet(f) for f in files_weather], ignore_index=True)
    logger.info(f"Loaded {len(df_weather):,} hourly weather observations across {df_weather['grid_id'].nunique()} grid cells.")

    # 3. Prepare Join Keys and Columns
    # Weather select subset to avoid duplicate column collisions (e.g. borough_hint vs borough)
    weather_join_cols = [
        "grid_id",
        "timestamp_utc",
        "temperature_2m",
        "apparent_temperature",
        "precipitation",
        "precip_past_3h_mm",
        "precip_past_6h_mm",
        "precip_past_24h_mm",
        "rain",
        "snowfall",
        "snow_depth",
        "weather_code",
        "surface_pressure",
        "wind_speed_10m",
        "wind_gusts_10m",
        "max_wind_gust_past_6h_ms",
        "min_temp_past_24h_c",
        "max_temp_past_24h_c",
        "is_freezing",
        "is_raining",
        "is_extreme_weather",
        "model_elevation",
    ]
    df_w_sub = df_weather[[c for c in weather_join_cols if c in df_weather.columns]].copy()

    # Ensure UTC timezone awareness on join keys
    df_311["weather_join_hour_utc"] = pd.to_datetime(df_311["weather_join_hour_utc"], utc=True)
    df_w_sub["timestamp_utc"] = pd.to_datetime(df_w_sub["timestamp_utc"], utc=True)

    # 4. Execute Left Join
    logger.info("Executing Left Join (311 [Fact] LEFT JOIN Weather [Dimension])...")
    df_joined = df_311.merge(
        df_w_sub,
        left_on=["grid_id", "weather_join_hour_utc"],
        right_on=["grid_id", "timestamp_utc"],
        how="left",
    )

    # Audit match status
    df_joined["is_weather_matched"] = df_joined["temperature_2m"].notna()
    matched_count = int(df_joined["is_weather_matched"].sum())
    unmatched_count = total_311 - matched_count
    match_rate = round(matched_count / total_311 * 100.0, 2) if total_311 > 0 else 0.0

    logger.info(
        f"Join Results: {matched_count:,} matched ({match_rate}%) | {unmatched_count:,} unmatched."
    )

    df_joined["_silver_joined_at_utc"] = joined_at_utc

    # 5. Write Partitioned Fact Table: year=YYYY / month=MM / fact_311_weather.parquet
    output_joined_dir.mkdir(parents=True, exist_ok=True)
    df_joined["_partition_year"] = df_joined["created_at_utc"].dt.strftime("%Y")
    df_joined["_partition_month"] = df_joined["created_at_utc"].dt.strftime("%m")

    written_files = 0
    for (year, month), sub_df in df_joined.groupby(["_partition_year", "_partition_month"]):
        partition_dir = output_joined_dir / f"year={year}" / f"month={month}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        out_path = partition_dir / "fact_311_weather.parquet"
        save_df = sub_df.drop(columns=["_partition_year", "_partition_month"])

        table = pa.Table.from_pandas(save_df, preserve_index=False)
        pq.write_table(table, out_path, compression="snappy")
        written_files += 1

    elapsed = time.time() - start_time
    logger.info(
        f"Saved Unified Fact Table: {len(df_joined):,} rows | "
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
