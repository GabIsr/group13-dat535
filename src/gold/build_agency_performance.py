"""
Gold Layer: Agency Resolution & SLA Performance Mart Builder.
Grain: reporting_date × borough × agency
Measures operational agency throughput, resolution SLAs (median & p90 durations),
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

from src.gold.config import (
    GOLD_MARTS_DIR,
    LONG_OPEN_BACKLOG_DAYS,
    SILVER_311_DIR,
    SILVER_WEATHER_DIR,
    STORM_PRECIP_THRESHOLD_MM,
    STORM_WIND_THRESHOLD_MS,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("build_agency_performance")


def build_agency_resolution_performance_mart(
    silver_311_dir: Path = SILVER_311_DIR,
    silver_weather_dir: Path = SILVER_WEATHER_DIR,
    output_dir: Path = GOLD_MARTS_DIR,
    long_open_days: float = LONG_OPEN_BACKLOG_DAYS,
) -> Dict[str, Any]:
    """
    Construct the Agency Resolution & SLA Performance Mart.
    Conceptually separates creation cohorts (incoming demand), closure cohorts (agency output),
    and active backlog accumulation as of each reporting date.
    """
    start_time = time.time()
    built_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 1. Load Clean Silver 311 Records
    files_311 = sorted(silver_311_dir.glob("**/*.parquet"))
    if not files_311:
        raise FileNotFoundError(f"No Silver 311 files found in {silver_311_dir}")
    logger.info(f"Loading {len(files_311)} Silver 311 partition files...")
    df_311 = pd.concat([pd.read_parquet(f) for f in files_311], ignore_index=True)
    logger.info(f"Loaded {len(df_311):,} clean 311 records for performance aggregation.")

    # Standardize dates to date-only objects
    df_311["created_date_only"] = pd.to_datetime(df_311["created_at_utc"], utc=True).dt.date
    df_311["closed_date_only"] = pd.to_datetime(df_311.get("closed_at_utc"), utc=True).dt.date

    # 2. Compute Creation Cohort (Incoming Demand on reporting_date)
    logger.info("Computing Creation Cohort (incoming ticket volume)...")
    df_created = df_311.groupby(["created_date_only", "borough", "agency"]).agg(
        tickets_created_today=("unique_key", "count"),
    ).reset_index().rename(columns={"created_date_only": "reporting_date"})

    # 3. Compute Closure Cohort (Agency Output & Resolution Durations on reporting_date)
    logger.info("Computing Closure Cohort (agency output, median, and P90 resolution SLAs)...")
    df_closed_subset = df_311[df_311["is_ticket_closed"] & df_311["closed_date_only"].notna()]
    df_closed = df_closed_subset.groupby(["closed_date_only", "borough", "agency"]).agg(
        tickets_closed_today=("unique_key", "count"),
        median_resolution_duration_hours=("resolution_duration_hours", lambda s: round(float(s.median()), 2)),
        p90_resolution_duration_hours=("resolution_duration_hours", lambda s: round(float(s.quantile(0.90)), 2)),
        avg_resolution_duration_hours=("resolution_duration_hours", lambda s: round(float(s.mean()), 2)),
    ).reset_index().rename(columns={"closed_date_only": "reporting_date"})

    # Merge Creation and Closure cohorts
    df_perf = pd.merge(df_created, df_closed, on=["reporting_date", "borough", "agency"], how="outer")
    df_perf["tickets_created_today"] = df_perf["tickets_created_today"].fillna(0).astype(int)
    df_perf["tickets_closed_today"] = df_perf["tickets_closed_today"].fillna(0).astype(int)

    # 4. Compute Active Backlog & Aging as of each reporting_date
    logger.info("Computing Backlog Cohort (active pending tickets and aging share > 7 days)...")
    reporting_dates = sorted(df_perf["reporting_date"].unique())

    backlog_records = []
    for rep_date in reporting_dates:
        # Active on rep_date: created <= rep_date AND (closed > rep_date OR never closed)
        cond_created = df_311["created_date_only"] <= rep_date
        cond_open = df_311["closed_date_only"].isna() | (df_311["closed_date_only"] > rep_date)
        active_mask = cond_created & cond_open
        df_active = df_311[active_mask]

        if df_active.empty:
            continue

        # Check aging > long_open_days relative to rep_date
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

    # 5. Load and Aggregate Borough-Level Weather Context
    logger.info("Computing Borough-level daily environmental context...")
    files_weather = sorted(silver_weather_dir.glob("**/*.parquet"))
    if files_weather:
        df_weather = pd.concat([pd.read_parquet(f) for f in files_weather], ignore_index=True)
        df_weather["date"] = pd.to_datetime(df_weather["timestamp_utc"], utc=True).dt.date
        df_weather["borough_clean"] = df_weather["borough_hint"].astype(str).str.strip().str.upper()

        borough_weather = df_weather.groupby(["date", "borough_clean"]).agg(
            borough_daily_precip_mm=("precipitation", lambda s: round(float(s.sum()), 2)),
            borough_max_wind_gust_ms=("wind_gusts_10m", lambda s: round(float(s.max()), 2) if s.notna().any() else np.nan),
            borough_avg_temp_c=("temperature_2m", lambda s: round(float(s.mean()), 2) if s.notna().any() else np.nan),
            is_extreme_weather_event=("is_extreme_weather", lambda s: bool(s.any())),
        ).reset_index().rename(columns={"date": "reporting_date", "borough_clean": "borough"})

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
        f"Built Gold Agency Resolution Performance Mart: {len(df_perf):,} rows | "
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
