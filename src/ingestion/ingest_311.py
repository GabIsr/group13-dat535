"""
NYC 311 Service Requests Ingestion Engine (Landing & Bronze Layers).
Supports Bootstrap, Backfill, and Incremental CDC (Change Data Capture) updates.
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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.utils.config import (
    BRONZE_DIR,
    DATASET_311,
    LANDING_DIR,
    MANIFESTS_DIR,
    PROJECT_ROOT,
    SCHEMA_VERSION,
    SOCRATA_311_ENDPOINT,
    SOCRATA_DATASET_ID,
    SOURCE_NAME_311,
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
logger = logging.getLogger("ingest_311")


def compute_row_hashes(df: pd.DataFrame, exclude_cols: Optional[List[str]] = None) -> List[str]:
    """Compute SHA-256 hash for each row based on sorted raw content."""
    cols_to_hash = [c for c in df.columns if exclude_cols is None or c not in exclude_cols]
    df_hashable = df[cols_to_hash].fillna("")
    
    # Fast vectorized-like row string serialization with robust fallback encoder
    rows_as_strings = [
        json.dumps(row, sort_keys=True, ensure_ascii=False, default=str)
        for row in df_hashable.to_dict(orient="records")
    ]
    return [hashlib.sha256(s.encode("utf-8")).hexdigest() for s in rows_as_strings]


def fetch_socrata_page(
    where_clause: Optional[str] = None,
    limit: int = 50000,
    offset: int = 0,
    app_token: Optional[str] = None,
    timeout: int = 60,
) -> Tuple[List[Dict[str, Any]], bytes]:
    """Fetch a single page of records from Socrata API."""
    params: Dict[str, str] = {
        "$limit": str(limit),
        "$offset": str(offset),
        "$order": "created_date ASC",
    }
    if where_clause:
        params["$where"] = where_clause

    query_str = urllib.parse.urlencode(params)
    url = f"{SOCRATA_311_ENDPOINT}?{query_str}"

    headers = {"User-Agent": "DAT535-UiS-Ingestion/1.0"}
    if app_token:
        headers["X-App-Token"] = app_token

    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as response:
        raw_bytes = response.read()

    data = json.loads(raw_bytes.decode("utf-8"))
    return data, raw_bytes


def write_bronze_partitioned(
    df: pd.DataFrame,
    batch_id: str,
    part_idx: int,
    base_bronze_dir: Path,
) -> List[Dict[str, Any]]:
    """
    Write DataFrame to Bronze partitioned by year and month.
    Returns file metadata list for the manifest.
    """
    written_files = []
    
    # Ensure year and month partition columns exist
    if "created_date" in df.columns:
        dt_series = pd.to_datetime(df["created_date"], errors="coerce")
        years = dt_series.dt.strftime("%Y").fillna("unknown")
        months = dt_series.dt.strftime("%m").fillna("unknown")
    else:
        years = pd.Series(["unknown"] * len(df), index=df.index)
        months = pd.Series(["unknown"] * len(df), index=df.index)

    df["_partition_year"] = years
    df["_partition_month"] = months

    # Group by partition values and write individual files
    for (year, month), sub_df in df.groupby(["_partition_year", "_partition_month"]):
        partition_dir = base_bronze_dir / f"year={year}" / f"month={month}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        filename = f"part_{batch_id}_{part_idx:04d}.parquet"
        file_path = partition_dir / filename

        # Drop internal partition helper columns before writing
        clean_sub_df = sub_df.drop(columns=["_partition_year", "_partition_month"])

        table = pa.Table.from_pandas(clean_sub_df, preserve_index=False)
        pq.write_table(table, file_path, compression="snappy")

        file_sha256 = compute_file_sha256(file_path)
        file_bytes = file_path.stat().st_size

        rel_path = file_path.relative_to(PROJECT_ROOT).as_posix()
        written_files.append({
            "file_name": filename,
            "relative_path": rel_path,
            "layer": "bronze",
            "rows": len(sub_df),
            "bytes": file_bytes,
            "sha256": file_sha256,
        })

    return written_files


def run_311_ingestion(
    mode: str = "incremental",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    batch_size: int = 50000,
    max_records: Optional[int] = None,
    lookback_days: int = 14,
    app_token: Optional[str] = None,
    env: str = "dev",
    manifests_dir: Path = MANIFESTS_DIR,
) -> Optional[Dict[str, Any]]:
    """
    Main orchestration routine for NYC 311 Ingestion.
    Supports incremental (CDC), backfill, and bootstrap modes.
    """
    start_time = time.time()
    retrieved_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    batch_id = generate_batch_id("311", manifests_dir=manifests_dir)

    logger.info(f"Starting NYC 311 Ingestion | Batch ID: {batch_id} | Mode: {mode} | Env: {env}")

    # Check idempotency
    if is_batch_completed(batch_id, DATASET_311, manifests_dir=manifests_dir):
        logger.info(f"Batch {batch_id} has already succeeded. Skipping execution.")
        return None

    landing_batch_dir = LANDING_DIR / "nyc311" / f"batch_{batch_id}"
    landing_batch_dir.mkdir(parents=True, exist_ok=True)
    bronze_311_dir = BRONZE_DIR / "nyc311"

    manifest_files_list: List[Dict[str, Any]] = []
    total_rows = 0
    total_bytes = 0
    all_col_types: Dict[str, str] = {}
    source_min_date: Optional[str] = None
    source_max_date: Optional[str] = None

    if mode == "bootstrap":
        # Bootstrap from pre-downloaded 3-month raw parquet files
        source_dir = PROJECT_ROOT / "notebooks" / "data" / "nyc311_last_3_months"
        if not source_dir.exists():
            raise FileNotFoundError(f"Bootstrap source directory not found: {source_dir}")

        parquet_files = sorted(source_dir.glob("*.parquet"))
        logger.info(f"Found {len(parquet_files)} parquet parts to bootstrap from {source_dir}")

        for part_idx, pf in enumerate(parquet_files):
            df_part = pd.read_parquet(pf)
            if df_part.empty:
                continue

            # Track temporal bounds
            if "created_date" in df_part.columns:
                part_min = str(df_part["created_date"].min())
                part_max = str(df_part["created_date"].max())
                source_min_date = part_min if source_min_date is None else min(source_min_date, part_min)
                source_max_date = part_max if source_max_date is None else max(source_max_date, part_max)

            # Write landing JSON.GZ
            landing_filename = f"raw_part_{part_idx:04d}.json.gz"
            landing_path = landing_batch_dir / landing_filename
            json_str = df_part.to_json(orient="records", date_format="iso")
            compressed_bytes = gzip.compress(json_str.encode("utf-8"))
            with open(landing_path, "wb") as f:
                f.write(compressed_bytes)

            landing_sha256 = compute_file_sha256(landing_path)
            landing_bytes = landing_path.stat().st_size
            total_bytes += len(json_str.encode("utf-8"))

            manifest_files_list.append({
                "file_name": landing_filename,
                "relative_path": landing_path.relative_to(PROJECT_ROOT).as_posix(),
                "layer": "landing",
                "rows": len(df_part),
                "bytes": landing_bytes,
                "sha256": landing_sha256,
            })

            # Inject Tier 2 Row-Level Metadata Columns
            row_hashes = compute_row_hashes(df_part)
            df_part["_batch_id"] = batch_id
            df_part["_ingested_at_utc"] = retrieved_at_utc
            df_part["_source_file"] = landing_path.relative_to(PROJECT_ROOT).as_posix()
            df_part["_source_name"] = SOURCE_NAME_311
            df_part["_extraction_mode"] = "full_snapshot"
            df_part["_schema_version"] = SCHEMA_VERSION
            df_part["_raw_row_hash"] = row_hashes

            # Record schema fingerprint info
            for col in df_part.columns:
                all_col_types[col] = str(df_part[col].dtype)

            # Write Bronze Parquet
            bronze_meta = write_bronze_partitioned(df_part, batch_id, part_idx, bronze_311_dir)
            manifest_files_list.extend(bronze_meta)
            total_rows += len(df_part)

            if max_records and total_rows >= max_records:
                logger.info(f"Reached max records threshold ({max_records}). Stopping bootstrap.")
                break

    else:
        # Network API Ingestion (incremental or backfill)
        where_conditions: List[str] = []

        if mode == "incremental":
            last_watermark = get_latest_watermark(DATASET_311, manifests_dir=manifests_dir)
            if last_watermark:
                logger.info(f"Discovered previous watermark: {last_watermark}")
                try:
                    # Subtract lookback window to catch late status updates and ticket closures on existing records
                    dt_watermark = pd.to_datetime(last_watermark)
                    lookback_dt = dt_watermark - pd.Timedelta(days=lookback_days)
                    lookback_iso = lookback_dt.strftime("%Y-%m-%dT%H:%M:%S.000")
                    logger.info(
                        f"Incremental CDC Strategy: Querying created_date >= '{lookback_iso}' "
                        f"(watermark: {last_watermark} with {lookback_days}-day lookback window for updates)"
                    )
                    where_conditions.append(f"created_date >= '{lookback_iso}'")
                except Exception as e:
                    logger.warning(f"Could not calculate lookback date from watermark '{last_watermark}': {e}. Falling back to strict watermark.")
                    where_conditions.append(f"created_date > '{last_watermark}'")
            else:
                logger.warning("No previous watermark found for incremental mode. Ingesting past 7 days.")
                where_conditions.append("created_date >= '2026-10-15T00:00:00.000'")

        if start_date:
            where_conditions.append(f"created_date >= '{start_date}'")
        if end_date:
            where_conditions.append(f"created_date <= '{end_date}'")

        where_clause = " AND ".join(where_conditions) if where_conditions else None
        logger.info(f"Socrata SoQL filter: {where_clause}")

        offset = 0
        part_idx = 0

        while True:
            current_limit = batch_size
            if max_records and (total_rows + current_limit) > max_records:
                current_limit = max_records - total_rows
                if current_limit <= 0:
                    break

            logger.info(f"Fetching page offset {offset:,} (limit {current_limit:,})...")
            records, raw_bytes = fetch_socrata_page(
                where_clause=where_clause,
                limit=current_limit,
                offset=offset,
                app_token=app_token,
            )

            if not records:
                logger.info("No more records returned from Socrata API.")
                break

            total_bytes += len(raw_bytes)
            page_rows = len(records)
            df_page = pd.DataFrame(records)

            # Track min and max created_date
            if "created_date" in df_page.columns:
                page_min = str(df_page["created_date"].min())
                page_max = str(df_page["created_date"].max())
                source_min_date = page_min if source_min_date is None else min(source_min_date, page_min)
                source_max_date = page_max if source_max_date is None else max(source_max_date, page_max)

            # Write Landing JSON.GZ
            landing_filename = f"raw_part_{part_idx:04d}.json.gz"
            landing_path = landing_batch_dir / landing_filename
            compressed_bytes = gzip.compress(raw_bytes)
            with open(landing_path, "wb") as f:
                f.write(compressed_bytes)

            landing_sha256 = compute_file_sha256(landing_path)
            landing_bytes = landing_path.stat().st_size

            manifest_files_list.append({
                "file_name": landing_filename,
                "relative_path": landing_path.relative_to(PROJECT_ROOT).as_posix(),
                "layer": "landing",
                "rows": page_rows,
                "bytes": landing_bytes,
                "sha256": landing_sha256,
            })

            # Inject Tier 2 System Columns
            row_hashes = compute_row_hashes(df_page)
            df_page["_batch_id"] = batch_id
            df_page["_ingested_at_utc"] = retrieved_at_utc
            df_page["_source_file"] = landing_path.relative_to(PROJECT_ROOT).as_posix()
            df_page["_source_name"] = SOURCE_NAME_311
            df_page["_extraction_mode"] = mode
            df_page["_schema_version"] = SCHEMA_VERSION
            df_page["_raw_row_hash"] = row_hashes

            # Record column types
            for col in df_page.columns:
                all_col_types[col] = str(df_page[col].dtype)

            # Write Bronze Parquet
            bronze_meta = write_bronze_partitioned(df_page, batch_id, part_idx, bronze_311_dir)
            manifest_files_list.extend(bronze_meta)

            total_rows += page_rows
            part_idx += 1
            offset += page_rows

            if page_rows < current_limit or (max_records and total_rows >= max_records):
                break

    duration = time.time() - start_time
    code_version = get_git_commit_hash(PROJECT_ROOT)
    schema_fingerprint = compute_schema_fingerprint(all_col_types)

    manifest_dict = create_manifest_dict(
        batch_id=batch_id,
        dataset_name=DATASET_311,
        source_name=SOURCE_NAME_311,
        source_dataset_id=SOCRATA_DATASET_ID,
        source_url=SOCRATA_311_ENDPOINT,
        retrieved_at_utc=retrieved_at_utc,
        source_data_start=source_min_date or "N/A",
        source_data_end=source_max_date or "N/A",
        extraction_mode=mode,
        environment=env,
        code_version=code_version,
        duration_seconds=duration,
        byte_size=total_bytes,
        row_count=total_rows,
        schema_fingerprint=schema_fingerprint,
        status="success" if total_rows > 0 else "partial",
        files=manifest_files_list,
    )

    manifest_path = save_manifest(manifest_dict, manifests_dir=manifests_dir)
    logger.info(
        f"Completed NYC 311 Ingestion: {total_rows:,} rows | "
        f"{len(manifest_files_list)} files | {duration:.2f}s | Manifest: {manifest_path.name}"
    )
    return manifest_dict


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest NYC 311 Service Requests to Landing and Bronze layers.")
    parser.add_argument("--mode", choices=["incremental", "backfill", "bootstrap"], default="incremental", help="Ingestion mode")
    parser.add_argument("--start-date", type=str, help="Start date filter (e.g. 2026-10-14T00:00:00.000)")
    parser.add_argument("--end-date", type=str, help="End date filter")
    parser.add_argument("--batch-size", type=int, default=50000, help="Records per API page")
    parser.add_argument("--max-records", type=int, help="Optional record limit for testing")
    parser.add_argument("--lookback-days", type=int, default=14, help="Lookback buffer in days for CDC status updates")
    parser.add_argument("--app-token", type=str, help="Optional Socrata App Token")
    parser.add_argument("--env", choices=["dev", "prod"], default="dev", help="Execution environment")

    args = parser.parse_args()
    run_311_ingestion(
        mode=args.mode,
        start_date=args.start_date,
        end_date=args.end_date,
        batch_size=args.batch_size,
        max_records=args.max_records,
        lookback_days=args.lookback_days,
        app_token=args.app_token,
        env=args.env,
    )
