"""
Open-Meteo ERA5-Land Weather Ingestion Engine (Landing & Bronze Layers).
Ingests the 19 Native NYC ERA5-Land Grid Cells in a single batch API call.
Supports Backfill and Incremental updates.
Complies with INGESTION_METADATA_SPECIFICATION.md.
Author: Group 13
"""

import argparse
import gzip
import hashlib
import json
import logging
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.utils.config import (
    BRONZE_DIR,
    DATASET_WEATHER,
    ERA5_NYC_GRID_POINTS,
    LANDING_DIR,
    MANIFESTS_DIR,
    OPEN_METEO_ARCHIVE_ENDPOINT,
    OPEN_METEO_DATASET_ID,
    PROJECT_ROOT,
    SCHEMA_VERSION,
    SOURCE_NAME_WEATHER,
    WEATHER_HOURLY_VARIABLES,
)
from src.utils.manifest import (
    compute_file_sha256,
    compute_schema_fingerprint,
    create_manifest_dict,
    generate_batch_id,
    get_git_commit_hash,
    get_latest_watermark,
    is_batch_completed,
    save_manifest,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ingest_weather")


def compute_weather_row_hashes(df: pd.DataFrame) -> List[str]:
    """Compute deterministic SHA-256 hash for each hourly observation row."""
    # Hash time + grid_id + temperature + precipitation + wind
    hash_cols = [c for c in df.columns if not c.startswith("_")]
    records = df[hash_cols].fillna("").to_dict(orient="records")
    return [
        hashlib.sha256(json.dumps(r, sort_keys=True, default=str).encode("utf-8")).hexdigest()
        for r in records
    ]


def fetch_open_meteo_batch(
    grid_points: List[Dict[str, Any]],
    start_date: str,
    end_date: str,
    hourly_vars: List[str],
    timeout: int = 60,
) -> Tuple[List[Dict[str, Any]], bytes]:
    """
    Fetch all 19 NYC grid coordinates in a single multi-location HTTP request.
    Open-Meteo returns a JSON list of location objects matching coordinate order.
    """
    lats = [str(pt["grid_lat"]) for pt in grid_points]
    lons = [str(pt["grid_lon"]) for pt in grid_points]

    params = {
        "latitude": ",".join(lats),
        "longitude": ",".join(lons),
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(hourly_vars),
        "wind_speed_unit": "ms",
    }

    query_str = urllib.parse.urlencode(params)
    url = f"{OPEN_METEO_ARCHIVE_ENDPOINT}?{query_str}"
    logger.info(f"Querying Open-Meteo multi-location API for {len(grid_points)} grid cells...")

    req = urllib.request.Request(url, headers={"User-Agent": "DAT535-UiS-Ingestion/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        raw_bytes = response.read()

    data = json.loads(raw_bytes.decode("utf-8"))
    if isinstance(data, dict) and "error" in data:
        raise RuntimeError(f"Open-Meteo API Error: {data.get('reason')}")

    # If single point requested, API returns dict; if multi-location, returns list of dicts
    locations_list = data if isinstance(data, list) else [data]
    return locations_list, raw_bytes


def run_weather_ingestion(
    mode: str = "incremental",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    env: str = "dev",
    manifests_dir: Path = MANIFESTS_DIR,
) -> Optional[Dict[str, Any]]:
    """
    Main orchestration routine for Open-Meteo ERA5-Land weather ingestion.
    Writes uncorrupted raw JSON to landing/ and partitioned Parquet to bronze/.
    """
    start_time = time.time()
    retrieved_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    batch_id = generate_batch_id("wth", manifests_dir=manifests_dir)

    logger.info(f"Starting Weather Ingestion | Batch ID: {batch_id} | Mode: {mode} | Env: {env}")

    if is_batch_completed(batch_id, DATASET_WEATHER, manifests_dir=manifests_dir):
        logger.info(f"Batch {batch_id} has already succeeded. Skipping execution.")
        return None

    # Determine date intervals
    default_reanalysis_end = (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d")

    if mode == "incremental":
        last_watermark = get_latest_watermark(DATASET_WEATHER, manifests_dir=manifests_dir)
        if last_watermark:
            # Increment by 1 day past last watermark
            last_date_obj = datetime.strptime(last_watermark[:10], "%Y-%m-%d")
            effective_start = (last_date_obj + timedelta(days=1)).strftime("%Y-%m-%d")
            logger.info(f"Discovered previous weather watermark: {last_watermark}. New start: {effective_start}")
        else:
            logger.warning("No previous weather watermark found. Defaulting to 311 baseline (2026-07-15).")
            effective_start = "2026-07-15"
        effective_end = end_date or default_reanalysis_end
    else:
        effective_start = start_date or "2026-07-15"
        effective_end = end_date or default_reanalysis_end

    # Guard against querying future dates
    if effective_start > effective_end:
        logger.info(
            f"Weather dataset is already up to date (start {effective_start} > end {effective_end}). Nothing to ingest."
        )
        return None

    logger.info(f"Temporal interval for acquisition: {effective_start} to {effective_end}")

    # Step 1: Fetch raw data in a single multi-location API call
    locations_data, raw_bytes = fetch_open_meteo_batch(
        grid_points=ERA5_NYC_GRID_POINTS,
        start_date=effective_start,
        end_date=effective_end,
        hourly_vars=WEATHER_HOURLY_VARIABLES,
    )

    # Step 2: Write raw payload to landing zone
    landing_batch_dir = LANDING_DIR / "weather" / f"batch_{batch_id}"
    landing_batch_dir.mkdir(parents=True, exist_ok=True)
    landing_file = landing_batch_dir / "raw_weather_grid19.json.gz"

    compressed_bytes = gzip.compress(raw_bytes)
    with open(landing_file, "wb") as f:
        f.write(compressed_bytes)

    landing_sha256 = compute_file_sha256(landing_file)
    landing_bytes = landing_file.stat().st_size

    manifest_files_list: List[Dict[str, Any]] = [
        {
            "file_name": landing_file.name,
            "relative_path": landing_file.relative_to(PROJECT_ROOT).as_posix(),
            "layer": "landing",
            "rows": sum(len(loc.get("hourly", {}).get("time", [])) for loc in locations_data),
            "bytes": landing_bytes,
            "sha256": landing_sha256,
        }
    ]

    # Step 3: Unpack, enrich with spatial metadata, and write Bronze Parquet
    bronze_weather_dir = BRONZE_DIR / "weather"
    total_rows = 0
    all_col_types: Dict[str, str] = {}

    for idx, loc_data in enumerate(locations_data):
        target_grid = ERA5_NYC_GRID_POINTS[idx]
        hourly = loc_data.get("hourly", {})
        if not hourly or "time" not in hourly:
            continue

        df_grid = pd.DataFrame(hourly)
        if df_grid.empty:
            continue

        # Spatial enrichment (Section 4 of spec)
        df_grid["grid_id"] = target_grid["grid_id"]
        df_grid["grid_lat"] = target_grid["grid_lat"]
        df_grid["grid_lon"] = target_grid["grid_lon"]
        df_grid["borough_hint"] = target_grid["borough_hint"]
        df_grid["model_elevation"] = loc_data.get("elevation", 0.0)

        # Tier 2 Row-Level System Columns (Section 3 of spec)
        row_hashes = compute_weather_row_hashes(df_grid)
        df_grid["_batch_id"] = batch_id
        df_grid["_ingested_at_utc"] = retrieved_at_utc
        df_grid["_source_file"] = landing_file.relative_to(PROJECT_ROOT).as_posix()
        df_grid["_source_name"] = SOURCE_NAME_WEATHER
        df_grid["_extraction_mode"] = mode
        df_grid["_schema_version"] = SCHEMA_VERSION
        df_grid["_raw_row_hash"] = row_hashes

        # Track schema fingerprint
        for col in df_grid.columns:
            all_col_types[col] = str(df_grid[col].dtype)

        # Write partitioned Parquet: grid_id=GRID_XX_YY / year=YYYY / part_{batch_id}.parquet
        df_grid["_year"] = pd.to_datetime(df_grid["time"]).dt.strftime("%Y")
        for year, year_group in df_grid.groupby("_year"):
            partition_dir = bronze_weather_dir / f"grid_id={target_grid['grid_id']}" / f"year={year}"
            partition_dir.mkdir(parents=True, exist_ok=True)

            parquet_file = partition_dir / f"part_{batch_id}.parquet"
            clean_sub_df = year_group.drop(columns=["_year"])

            table = pa.Table.from_pandas(clean_sub_df, preserve_index=False)
            pq.write_table(table, parquet_file, compression="snappy")

            p_sha256 = compute_file_sha256(parquet_file)
            p_bytes = parquet_file.stat().st_size

            manifest_files_list.append({
                "file_name": parquet_file.name,
                "relative_path": parquet_file.relative_to(PROJECT_ROOT).as_posix(),
                "layer": "bronze",
                "rows": len(year_group),
                "bytes": p_bytes,
                "sha256": p_sha256,
            })
            total_rows += len(year_group)

    duration = time.time() - start_time
    code_version = get_git_commit_hash(PROJECT_ROOT)
    schema_fingerprint = compute_schema_fingerprint(all_col_types)

    manifest_dict = create_manifest_dict(
        batch_id=batch_id,
        dataset_name=DATASET_WEATHER,
        source_name=SOURCE_NAME_WEATHER,
        source_dataset_id=OPEN_METEO_DATASET_ID,
        source_url=OPEN_METEO_ARCHIVE_ENDPOINT,
        retrieved_at_utc=retrieved_at_utc,
        source_data_start=effective_start,
        source_data_end=effective_end,
        extraction_mode=mode,
        environment=env,
        code_version=code_version,
        duration_seconds=duration,
        byte_size=len(raw_bytes),
        row_count=total_rows,
        schema_fingerprint=schema_fingerprint,
        status="success" if total_rows > 0 else "failed",
        files=manifest_files_list,
    )

    manifest_path = save_manifest(manifest_dict, manifests_dir=manifests_dir)
    logger.info(
        f"Completed Weather Ingestion: {total_rows:,} rows | "
        f"{len(manifest_files_list)} files | {duration:.2f}s | Manifest: {manifest_path.name}"
    )
    return manifest_dict


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest ERA5-Land Weather data to Landing and Bronze layers.")
    parser.add_argument("--mode", choices=["incremental", "backfill"], default="incremental", help="Ingestion mode")
    parser.add_argument("--start-date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--env", choices=["dev", "prod"], default="dev", help="Execution environment")

    args = parser.parse_args()
    run_weather_ingestion(
        mode=args.mode,
        start_date=args.start_date,
        end_date=args.end_date,
        env=args.env,
    )
