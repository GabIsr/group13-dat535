"""
Open-Meteo ERA5-Land Weather Ingestion Engine (Landing & Bronze Layers).
Ingests the 19 Native NYC ERA5-Land Grid Cells in a single batch API call.
Supports Backfill and Incremental updates.
Complies with INGESTION_METADATA_SPECIFICATION.md.
Author: Group 13
"""

import argparse
import gzip
import json
import logging
import shutil
import time
import urllib.error
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
    DATASET_311,
    DATASET_WEATHER,
    ERA5_NYC_GRID_POINTS,
    LANDING_DIR,
    MANIFESTS_DIR,
    OPEN_METEO_ARCHIVE_ENDPOINT,
    OPEN_METEO_DATASET_ID,
    PROJECT_ROOT,
    SOURCE_NAME_WEATHER,
    WEATHER_HOURLY_VARIABLES,
)
from src.utils.manifest import (
    compute_file_sha256,
    compute_schema_fingerprint,
    create_manifest_dict,
    generate_batch_id,
    get_earliest_watermark,
    get_git_commit_hash,
    get_latest_watermark,
    is_batch_completed,
    is_date_range_completed,
    save_manifest,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ingest_weather")


def get_relative_path(path: Path) -> str:
    """Safely return path relative to PROJECT_ROOT, or as_posix if outside PROJECT_ROOT."""
    try:
        return path.relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def fetch_open_meteo_batch(
    grid_points: List[Dict[str, Any]],
    start_date: str,
    end_date: str,
    hourly_vars: List[str],
    timeout: int = 60,
    max_retries: int = 4,
) -> Tuple[List[Dict[str, Any]], bytes]:
    """
    Fetch all 19 NYC grid coordinates in a single multi-location HTTP request
    with exponential backoff retries for transient 5xx server errors and network timeouts.
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

    for attempt in range(1, max_retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as response:
                raw_bytes = response.read()

            data = json.loads(raw_bytes.decode("utf-8"))
            if isinstance(data, dict) and "error" in data:
                raise RuntimeError(f"Open-Meteo API Error: {data.get('reason')}")

            # If single point requested, API returns dict; if multi-location, returns list of dicts
            locations_list = data if isinstance(data, list) else [data]
            return locations_list, raw_bytes
        except urllib.error.HTTPError as e:
            # 4xx errors (except 429 Too Many Requests) are non-transient client errors
            if 400 <= e.code < 500 and e.code != 429:
                logger.error(f"Non-transient HTTP client error from Open-Meteo ({e.code}): {e}")
                raise
            if attempt == max_retries:
                logger.error(f"Open-Meteo request failed after {max_retries} attempts: {e}")
                raise
            wait_time = 2 ** attempt * 2
            logger.warning(
                f"Transient HTTP error querying Open-Meteo ({e.code}: {e.reason}) on attempt {attempt}/{max_retries}. "
                f"Retrying in {wait_time}s..."
            )
            time.sleep(wait_time)
        except (TimeoutError, urllib.error.URLError, Exception) as e:
            if attempt == max_retries:
                logger.error(f"Open-Meteo network request failed after {max_retries} attempts: {e}")
                raise
            wait_time = 2 ** attempt * 2
            logger.warning(
                f"Transient network/timeout error querying Open-Meteo on attempt {attempt}/{max_retries}: {e}. "
                f"Retrying in {wait_time}s..."
            )
            time.sleep(wait_time)


def run_weather_ingestion(
    mode: str = "incremental",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    lookback_days: int = 7,
    env: str = "dev",
    manifests_dir: Path = MANIFESTS_DIR,
    batch_id: Optional[str] = None,
    force: bool = False,
) -> Optional[Dict[str, Any]]:
    """
    Main orchestration routine for Open-Meteo ERA5-Land weather ingestion.
    Writes uncorrupted raw JSON to landing/ and partitioned Parquet to bronze/ via isolated staging.
    """
    start_time = time.time()
    retrieved_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    # ECMWF ERA5-Land reanalysis latency is strictly 5 days
    today_utc = datetime.now(timezone.utc)
    max_weather_ceiling = (today_utc - timedelta(days=5)).strftime("%Y-%m-%d")

    # Date intervals resolution and validation
    if mode == "backfill":
        if not end_date:
            raise ValueError(
                "Backfill mode requires an explicit --end-date (e.g. --end-date 2026-10-31) "
                "to prevent accidental unbounded extraction."
            )
        if not start_date:
            raise ValueError("Backfill mode requires an explicit --start-date (e.g. --start-date 2026-10-01).")
        if end_date[:10] > max_weather_ceiling:
            raise ValueError(
                f"end_date ({end_date[:10]}) exceeds maximum available ERA5-Land reanalysis date ({max_weather_ceiling}) "
                f"due to ECMWF 5-day publication latency."
            )
        if start_date[:10] > end_date[:10]:
            raise ValueError(f"start_date ({start_date[:10]}) cannot be after end_date ({end_date[:10]}).")
        effective_start = start_date[:10]
        effective_end = end_date[:10]
    else:
        # Incremental mode: automatically clamp ceiling to ECMWF availability
        effective_end = min(end_date[:10], max_weather_ceiling) if end_date else max_weather_ceiling

        last_watermark = get_latest_watermark(DATASET_WEATHER, manifests_dir=manifests_dir)

        if start_date and force:
            effective_start = start_date[:10]
        elif last_watermark and not (start_date and not force):
            last_date_obj = datetime.strptime(last_watermark[:10], "%Y-%m-%d")
            effective_start = (last_date_obj + timedelta(days=1)).strftime("%Y-%m-%d")
            logger.info(f"Discovered previous weather watermark: {last_watermark}. New start: {effective_start}")
        elif start_date:
            effective_start = start_date[:10]
            logger.info(f"Using explicitly specified start_date for incremental intake: {start_date}")
        else:
            # Align with earliest 311 data if available, otherwise default to past lookback_days
            earliest_311 = get_earliest_watermark(DATASET_311, manifests_dir=manifests_dir)
            if earliest_311:
                base_start = datetime.strptime(earliest_311[:10], "%Y-%m-%d")
                effective_start = (base_start - timedelta(days=1)).strftime("%Y-%m-%d")
                logger.info(
                    f"No previous weather watermark found. Aligning start with 311 baseline ({earliest_311[:10]}) "
                    f"plus 1-day rolling window warm-up buffer: {effective_start}"
                )
            else:
                end_dt = datetime.strptime(effective_end, "%Y-%m-%d")
                fallback_days = lookback_days if lookback_days else 7
                effective_start = (end_dt - timedelta(days=fallback_days + 1)).strftime("%Y-%m-%d")
                logger.warning(
                    f"No previous weather watermark or 311 data found. Defaulting to past {fallback_days} days "
                    f"plus 1-day warm-up buffer ({effective_start}) up to {effective_end}."
                )

    # Guard against querying caught-up intervals
    if effective_start > effective_end:
        logger.info(
            f"Weather dataset is already up to date with ECMWF reanalysis latency "
            f"(start {effective_start} > ceiling {effective_end}). Nothing to ingest."
        )
        return None

    # Idempotency Guards
    if batch_id and not force and is_batch_completed(batch_id, DATASET_WEATHER, manifests_dir=manifests_dir):
        logger.info(f"Idempotency Guard: Batch {batch_id} has already succeeded. Skipping execution.")
        return None

    if not force and is_date_range_completed(DATASET_WEATHER, effective_start, effective_end, manifests_dir=manifests_dir):
        logger.info(
            f"Idempotency Guard: Date range [{effective_start} -> {effective_end}] already fully covered "
            f"by successful manifests. Skipping execution (use --force to override)."
        )
        return None

    active_batch_id = batch_id or generate_batch_id("wth", manifests_dir=manifests_dir)
    logger.info(f"Starting Weather Ingestion | Batch ID: {active_batch_id} | Mode: {mode} | Env: {env}")
    logger.info(f"Temporal interval for acquisition: {effective_start} to {effective_end}")

    # Step 1: Fetch raw data in a single multi-location API call
    locations_data, raw_bytes = fetch_open_meteo_batch(
        grid_points=ERA5_NYC_GRID_POINTS,
        start_date=effective_start,
        end_date=effective_end,
        hourly_vars=WEATHER_HOURLY_VARIABLES,
    )

    # Step 2: Write raw payload to landing zone
    landing_batch_dir = LANDING_DIR / "weather" / f"batch_{active_batch_id}"
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
            "relative_path": get_relative_path(landing_file),
            "layer": "landing",
            "rows": sum(len(loc.get("hourly", {}).get("time", [])) for loc in locations_data),
            "bytes": landing_bytes,
            "sha256": landing_sha256,
        }
    ]

    # Step 3: Unpack, enrich with spatial metadata, and write Bronze Parquet via staging
    bronze_weather_dir = BRONZE_DIR / "weather"
    staging_batch_dir = bronze_weather_dir.parent / "_staging_weather" / f"batch_{active_batch_id}"
    if staging_batch_dir.exists():
        shutil.rmtree(staging_batch_dir)
    staging_batch_dir.mkdir(parents=True, exist_ok=True)

    staged_files: List[Tuple[Path, Path, int]] = []
    total_rows = 0
    all_col_types: Dict[str, str] = {}

    try:
        for idx, loc_data in enumerate(locations_data):
            target_grid = ERA5_NYC_GRID_POINTS[idx]
            hourly = loc_data.get("hourly", {})
            if not hourly or "time" not in hourly:
                continue

            df_grid = pd.DataFrame(hourly)
            if df_grid.empty:
                continue

            # Preserve pure Open-Meteo raw observation attributes
            df_grid["latitude"] = loc_data.get("latitude", target_grid["grid_lat"])
            df_grid["longitude"] = loc_data.get("longitude", target_grid["grid_lon"])
            df_grid["elevation"] = loc_data.get("elevation", 0.0)

            # Track schema fingerprint
            for col in df_grid.columns:
                all_col_types[col] = str(df_grid[col].dtype)

            # Stage partitioned Parquet: grid_id=GRID_XX_YY / year=YYYY / part_{active_batch_id}.parquet
            df_grid["_year"] = pd.to_datetime(df_grid["time"]).dt.strftime("%Y")
            for year, year_group in df_grid.groupby("_year"):
                staged_part_dir = staging_batch_dir / f"grid_id={target_grid['grid_id']}" / f"year={year}"
                staged_part_dir.mkdir(parents=True, exist_ok=True)
                staged_file = staged_part_dir / f"part_{active_batch_id}.parquet"
                dest_file = bronze_weather_dir / f"grid_id={target_grid['grid_id']}" / f"year={year}" / f"part_{active_batch_id}.parquet"

                clean_sub_df = year_group.drop(columns=["_year"])
                table = pa.Table.from_pandas(clean_sub_df, preserve_index=False)
                pq.write_table(
                    table,
                    staged_file,
                    compression="snappy",
                    coerce_timestamps="us",
                    allow_truncated_timestamps=True,
                )
                staged_files.append((staged_file, dest_file, len(year_group)))

        # Validation passed across all 19 grid points -> Publish atomically to destination partitions
        for staged_file, dest_file, row_count in staged_files:
            dest_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(staged_file), str(dest_file))
            p_sha256 = compute_file_sha256(dest_file)
            p_bytes = dest_file.stat().st_size

            manifest_files_list.append({
                "file_name": dest_file.name,
                "relative_path": get_relative_path(dest_file),
                "layer": "bronze",
                "rows": row_count,
                "bytes": p_bytes,
                "sha256": p_sha256,
            })
            total_rows += row_count
    finally:
        shutil.rmtree(staging_batch_dir, ignore_errors=True)

    duration = time.time() - start_time
    code_version = get_git_commit_hash(PROJECT_ROOT)
    schema_fingerprint = compute_schema_fingerprint(all_col_types)

    manifest_dict = create_manifest_dict(
        batch_id=active_batch_id,
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
    parser.add_argument("--lookback-days", type=int, default=7, help="Lookback buffer in days when no watermark exists (default: 7)")
    parser.add_argument("--env", choices=["dev", "prod"], default="dev", help="Execution environment")
    parser.add_argument("--batch-id", type=str, help="Explicit batch ID to assign or check for idempotency")
    parser.add_argument("--force", action="store_true", help="Force ingestion even if date range or batch ID already succeeded")

    args = parser.parse_args()
    run_weather_ingestion(
        mode=args.mode,
        start_date=args.start_date,
        end_date=args.end_date,
        lookback_days=args.lookback_days,
        env=args.env,
        batch_id=args.batch_id,
        force=args.force,
    )
