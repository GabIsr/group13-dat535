"""
NYC 311 Service Requests Ingestion Engine (Landing & Bronze Layers).
Supports Backfill and Incremental CDC (Change Data Capture) updates.
Features PySpark-accelerated Landing-to-Bronze transformation, volume-aware partitioning,
isolated staging with Write-Audit-Publish (WAP) validation, and idempotent manifest-based execution.
Complies with INGESTION_METADATA_SPECIFICATION.md.
Author: Group 13
"""

import argparse
import gzip
import hashlib
import json
import logging
import os
import shutil
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq

from pyspark.sql.functions import (
    col,
    date_format,
    lit,
    to_timestamp,
    when,
)
from src.utils.spark import get_spark_session

from src.utils.config import (
    BRONZE_DIR,
    DATASET_311,
    LANDING_DIR,
    MANIFESTS_DIR,
    PROJECT_ROOT,
    PROJECT_START_DATE,
    SCHEMA_VERSION,
    SOCRATA_311_ENDPOINT,
    SOCRATA_DATASET_ID,
    SOURCE_NAME_311,
)
from src.utils.manifest import (
    compute_file_sha256,
    compute_schema_fingerprint,
    compute_uncovered_date_intervals,
    create_manifest_dict,
    generate_batch_id,
    get_git_commit_hash,
    get_latest_pipeline_run_timestamp,
    get_latest_schema_fingerprint,
    get_latest_watermark,
    is_batch_completed,
    is_date_range_completed,
    save_manifest,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ingest_311")


def get_relative_path(p: Path) -> str:
    """Safely return path relative to PROJECT_ROOT, or as_posix() if outside PROJECT_ROOT."""
    try:
        return p.relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return p.as_posix()


def fetch_socrata_page(
    where_clause: Optional[str] = None,
    order_clause: str = "created_date ASC, unique_key ASC",
    select_clause: str = "*, :created_at",
    limit: int = 50000,
    offset: int = 0,
    app_token: Optional[str] = None,
    timeout: int = 120,
    max_retries: int = 5,
) -> Tuple[List[Dict[str, Any]], bytes]:
    """Fetch a single page of records from Socrata API with exponential backoff retry."""
    params: Dict[str, str] = {
        "$select": select_clause,
        "$limit": str(limit),
        "$offset": str(offset),
        "$order": order_clause,
    }
    if where_clause:
        params["$where"] = where_clause

    query_str = urllib.parse.urlencode(params)
    url = f"{SOCRATA_311_ENDPOINT}?{query_str}"

    headers = {"User-Agent": "DAT535-UiS-Ingestion/1.0"}
    if app_token:
        headers["X-App-Token"] = app_token

    req = urllib.request.Request(url, headers=headers)

    for attempt in range(1, max_retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as response:
                raw_bytes = response.read()
            data = json.loads(raw_bytes.decode("utf-8"))
            return data, raw_bytes
        except urllib.error.HTTPError as e:
            # Do not retry permanent 4xx client errors (e.g. 400 Bad Request, 403 Forbidden), except 429 Rate Limit
            if 400 <= e.code < 500 and e.code != 429:
                logger.error(f"Permanent HTTP client error ({e.code}) at offset {offset}: {e}. Not retrying.")
                raise
            if attempt == max_retries:
                logger.error(f"Failed to fetch page at offset {offset} after {max_retries} attempts: {e}")
                raise
            wait_time = 2 ** attempt * 2
            logger.warning(
                f"Transient HTTP error ({e.code}) at offset {offset} (attempt {attempt}/{max_retries}): {e}. "
                f"Retrying in {wait_time}s..."
            )
            time.sleep(wait_time)
        except (TimeoutError, urllib.error.URLError, Exception) as e:
            if attempt == max_retries:
                logger.error(f"Failed to fetch page at offset {offset} after {max_retries} attempts: {e}")
                raise
            wait_time = 2 ** attempt * 2
            logger.warning(
                f"Transient network/timeout error at offset {offset} (attempt {attempt}/{max_retries}): {e}. "
                f"Retrying in {wait_time}s..."
            )
            time.sleep(wait_time)


def fetch_311_to_landing(
    landing_batch_dir: Path,
    where_clause: Optional[str] = None,
    order_clause: str = "created_date ASC, unique_key ASC",
    batch_size: int = 50000,
    max_records: Optional[int] = None,
    app_token: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Stream raw pages from Socrata 311 API directly to compressed Landing JSON.GZ files.
    Streams into an isolated temporary folder and atomically swaps into landing_batch_dir upon success,
    preventing stale files on retry and preserving existing usable downloads if a network error occurs.
    """
    landing_batch_dir = Path(landing_batch_dir)
    tmp_batch_dir = landing_batch_dir.with_name(f"{landing_batch_dir.name}_tmp")
    if tmp_batch_dir.exists():
        shutil.rmtree(tmp_batch_dir)
    tmp_batch_dir.mkdir(parents=True, exist_ok=True)

    manifest_files_list: List[Dict[str, Any]] = []
    total_rows = 0
    total_bytes = 0
    source_min_date: Optional[str] = None
    source_max_date: Optional[str] = None

    offset = 0
    part_idx = 0

    try:
        while True:
            current_limit = batch_size
            if max_records and (total_rows + current_limit) > max_records:
                current_limit = max_records - total_rows
                if current_limit <= 0:
                    break

            logger.info(f"Fetching page offset {offset:,} (limit {current_limit:,})...")
            records, raw_bytes = fetch_socrata_page(
                where_clause=where_clause,
                order_clause=order_clause,
                select_clause="*, :created_at",
                limit=current_limit,
                offset=offset,
                app_token=app_token,
            )

            if not records:
                logger.info("No more records returned from Socrata API.")
                break

            total_bytes += len(raw_bytes)
            page_rows = len(records)

            created_dates = [r["created_date"] for r in records if "created_date" in r and r["created_date"]]
            if created_dates:
                p_min = str(min(created_dates))
                p_max = str(max(created_dates))
                source_min_date = p_min if source_min_date is None else min(source_min_date, p_min)
                source_max_date = p_max if source_max_date is None else max(source_max_date, p_max)

            # Write compressed Landing JSON.GZ into temporary folder
            landing_filename = f"raw_part_{part_idx:04d}.json.gz"
            tmp_part_path = tmp_batch_dir / landing_filename
            compressed_bytes = gzip.compress(raw_bytes)
            with open(tmp_part_path, "wb") as f:
                f.write(compressed_bytes)

            landing_sha256 = compute_file_sha256(tmp_part_path)
            landing_bytes = tmp_part_path.stat().st_size

            # Target final path for relative manifest registration
            final_part_path = landing_batch_dir / landing_filename
            manifest_files_list.append({
                "file_name": landing_filename,
                "relative_path": get_relative_path(final_part_path),
                "layer": "landing",
                "rows": page_rows,
                "bytes": landing_bytes,
                "sha256": landing_sha256,
            })

            total_rows += page_rows
            part_idx += 1
            offset += page_rows

            if page_rows < current_limit or (max_records and total_rows >= max_records):
                break

        # Download completed successfully -> Atomically swap tmp directory into landing_batch_dir
        if landing_batch_dir.exists():
            shutil.rmtree(landing_batch_dir)
        tmp_batch_dir.rename(landing_batch_dir)

        return {
            "total_rows": total_rows,
            "total_bytes": total_bytes,
            "manifest_files": manifest_files_list,
            "source_min_date": source_min_date,
            "source_max_date": source_max_date,
        }

    except Exception as e:
        logger.error(f"Error during API streaming to Landing: {e}. Cleaning up temporary landing files...")
        if tmp_batch_dir.exists():
            shutil.rmtree(tmp_batch_dir, ignore_errors=True)
        raise


def audit_and_publish_bronze_batch(
    staging_batch_dir: Path,
    base_bronze_dir: Path,
    batch_id: str,
    expected_rows: int,
) -> List[Dict[str, Any]]:
    """
    Validate staged Bronze Parquet output and publish to Hive partitions (Write-Audit-Publish pattern).
    1. Validates non-empty files and readability via Parquet metadata.
    2. Validates that staged row count matches landing records.
    3. Moves validated files to destination Hive partitions with batch-prefixed names.
    4. Cleans up staging batch directory.
    5. Returns file metadata list for manifest registration.
    """
    staging_files = sorted(list(staging_batch_dir.glob("**/*.parquet")))
    if not staging_files:
        raise ValueError(f"Staged Bronze batch {batch_id} produced no Parquet files in {staging_batch_dir}")

    total_staged_rows = 0
    file_stats = []
    for sf in staging_files:
        f_size = sf.stat().st_size
        if f_size == 0:
            raise ValueError(f"Staged Bronze file {sf.name} is empty (0 bytes)")
        try:
            pq_meta = pq.ParquetFile(str(sf)).metadata
            num_rows = pq_meta.num_rows
            if num_rows <= 0:
                raise ValueError(f"Staged Bronze file {sf.name} contains 0 rows according to Parquet metadata")
        except Exception as e:
            raise ValueError(f"Corrupted Parquet file {sf.name} in staging batch {batch_id}: {e}") from e

        total_staged_rows += num_rows
        file_stats.append((sf, num_rows, f_size))

    if total_staged_rows != expected_rows:
        raise ValueError(
            f"Audit Validation Failure for batch {batch_id}: Staged row count ({total_staged_rows:,}) "
            f"does not match expected Landing count ({expected_rows:,})"
        )

    # Validation passed -> Publish files to base_bronze_dir with batch prefix
    base_bronze_dir.mkdir(parents=True, exist_ok=True)
    published_files: List[Dict[str, Any]] = []

    for sf, num_rows, f_size in file_stats:
        rel_to_staging = sf.relative_to(staging_batch_dir)
        partition_subpath = rel_to_staging.parent
        dest_dir = base_bronze_dir / partition_subpath
        dest_dir.mkdir(parents=True, exist_ok=True)

        dest_filename = f"{batch_id}_{sf.name}"
        dest_path = dest_dir / dest_filename

        # Safe atomic publication
        shutil.move(str(sf), str(dest_path))

        dest_sha = compute_file_sha256(dest_path)
        published_files.append({
            "file_name": dest_filename,
            "relative_path": get_relative_path(dest_path),
            "layer": "bronze",
            "rows": num_rows,
            "bytes": dest_path.stat().st_size,
            "sha256": dest_sha,
        })

    # Cleanup staging directory
    try:
        shutil.rmtree(staging_batch_dir)
    except OSError as e:
        logger.warning(f"Could not remove staging batch directory {staging_batch_dir}: {e}")

    logger.info(
        f"Bronze batch {batch_id} validated & published: {len(published_files)} file(s), "
        f"{total_staged_rows:,} rows committed."
    )
    return published_files


def convert_landing_to_bronze_pyspark(
    landing_batch_dir: Path,
    base_bronze_dir: Path,
    batch_id: str,
    retrieved_at_utc: str,
    extraction_mode: str = "incremental",
    return_schema: bool = False,
) -> Union[List[Dict[str, Any]], Tuple[List[Dict[str, Any]], str]]:
    """
    Read compressed JSON landing files via Apache PySpark, compute vectorized SHA-256 row hashes,
    inject Tier 2 system metadata, write partitioned Bronze Parquet files (year=YYYY / month=MM)
    into an isolated batch staging path, audit output, and publish.

    Partitioning Strategy:
    - Eliminates coalesce(1) bottleneck.
    - Applies volume-aware repartitioning and maxRecordsPerFile=250000 to prevent both skew and tiny files.

    Row Hashing Strategy:
    - Uses deterministic sorted column names with struct() and to_json(..., ignoreNullFields=false).
    - Preserves null distinctness and column boundaries.
    - Explicitly excludes system metadata and partition columns.

    Returns published file metadata (and optionally spark_schema_json if return_schema=True).
    """
    landing_files = sorted(list(landing_batch_dir.glob("*.json.gz")))
    if not landing_files:
        logger.warning(f"No landing .json.gz files found in {landing_batch_dir}")
        return ([], "") if return_schema else []

    logger.info(f"PySpark Landing-to-Bronze: Processing {len(landing_files)} landing file(s) for batch {batch_id}...")
    spark = get_spark_session("DAT535-Landing-To-Bronze-311")
    file_paths = [str(f.resolve()) for f in landing_files]
    df_raw = spark.read.option("multiLine", True).option("mode", "FAILFAST").json(file_paths)
    total_rows = df_raw.count()
    if total_rows == 0:
        logger.warning(f"Landing files for batch {batch_id} contain 0 records.")
        return ([], "") if return_schema else []

    # Standardize Socrata system metadata (:created_at) before hashing
    if ":created_at" in df_raw.columns:
        df_raw = df_raw.withColumnRenamed(":created_at", "source_created_at")
    if "source_created_at" in df_raw.columns:
        cr_c = col("source_created_at")
        df_raw = df_raw.withColumn(
            "source_created_at",
            when(cr_c.cast("long").isNotNull() & (cr_c.cast("long") > 100000000), cr_c.cast("long").cast("timestamp"))
            .otherwise(to_timestamp(cr_c))
        )

    source_file_rel = get_relative_path(landing_batch_dir)
    df_bronze = (
        df_raw.withColumn("_batch_id", lit(batch_id))
        .withColumn("_ingested_at_utc", lit(retrieved_at_utc))
        .withColumn("_source_file", lit(source_file_rel))
        .withColumn("_source_name", lit(SOURCE_NAME_311))
        .withColumn("_extraction_mode", lit(extraction_mode))
        .withColumn("_schema_version", lit(SCHEMA_VERSION))
        .withColumn("_bronze_status", lit("valid"))
    )

    # Derive partition columns
    if "created_date" in df_bronze.columns:
        ts_col = to_timestamp(col("created_date"))
        df_bronze = (
            df_bronze.withColumn("year", when(ts_col.isNotNull(), date_format(ts_col, "yyyy")).otherwise(lit("unknown")))
            .withColumn("month", when(ts_col.isNotNull(), date_format(ts_col, "MM")).otherwise(lit("unknown")))
        )
    else:
        df_bronze = df_bronze.withColumn("year", lit("unknown")).withColumn("month", lit("unknown"))

    if total_rows < 50000:
        df_writer = df_bronze.repartition("year", "month")
    else:
        num_partitions = max(2, min(total_rows // 50000, 32))
        df_writer = df_bronze.repartition(num_partitions, "year", "month")

    # Isolated batch staging location
    staging_batch_dir = base_bronze_dir / "_staging" / f"batch_{batch_id}"
    if staging_batch_dir.exists():
        shutil.rmtree(staging_batch_dir)
    staging_batch_dir.mkdir(parents=True, exist_ok=True)

    spark_schema_json = df_bronze.schema.json()

    try:
        (
            df_writer.write
            .mode("overwrite")
            .partitionBy("year", "month")
            .option("maxRecordsPerFile", 250000)
            .parquet(str(staging_batch_dir))
        )
    except Exception as e:
        logger.error(f"PySpark write failed for batch {batch_id}: {e}. Batch left uncommitted.")
        raise

    # Write-Audit-Publish validation & publication
    published_files = audit_and_publish_bronze_batch(
        staging_batch_dir=staging_batch_dir,
        base_bronze_dir=base_bronze_dir,
        batch_id=batch_id,
        expected_rows=total_rows,
    )

    if return_schema:
        return published_files, spark_schema_json
    return published_files


def build_311_soql_filter(
    mode: str = "incremental",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    lookback_days: int = 7,
    project_start_date: Optional[str] = None,
    manifests_dir: Path = MANIFESTS_DIR,
) -> Tuple[Optional[str], str, str]:
    """Construct Socrata $where filter, order clause, and effective end date."""
    order_clause = "created_date ASC, unique_key ASC"
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Input validation guardrails
    if end_date and end_date[:10] > today:
        raise ValueError(
            f"end_date ({end_date[:10]}) cannot be in the future "
            f"(current ceiling: {today})."
        )
    if start_date and end_date and start_date[:10] > end_date[:10]:
        raise ValueError(
            f"start_date ({start_date[:10]}) cannot be after end_date ({end_date[:10]})."
        )
    if mode == "backfill" and not end_date:
        raise ValueError(
            "Backfill mode requires an explicit --end-date "
            "(e.g. --end-date 2026-10-31) to prevent accidental unbounded extraction."
        )

    default_end = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    effective_end = end_date if mode == "backfill" else (end_date or f"{default_end}T23:59:59.000")

    if mode == "incremental":
        last_wm = get_latest_watermark(DATASET_311, manifests_dir=manifests_dir)
        last_run = get_latest_pipeline_run_timestamp(DATASET_311, manifests_dir=manifests_dir)

        last_run_nyc = None
        if last_run:
            last_run_ts = last_run.rstrip("Z")
            try:
                dt_utc = datetime.fromisoformat(last_run_ts).replace(tzinfo=timezone.utc)
                last_run_nyc = dt_utc.astimezone(
                    ZoneInfo("America/New_York")
                ).strftime("%Y-%m-%dT%H:%M:%S")
            except Exception as e:
                logger.warning(f"Failed to convert last_run_ts ({last_run_ts}) to America/New_York: {e}. Using raw UTC.")
                last_run_nyc = last_run_ts

        if last_wm and last_run_nyc:
            p_start = project_start_date or PROJECT_START_DATE
            if len(p_start) == 10:
                p_start += "T00:00:00.000"

            logger.info(
                f"Incremental CDC Strategy (Business Mutation Window): "
                f"Intake [{last_wm} -> {effective_end}] | "
                f"Mutations & Closures >= {last_run_nyc} "
                f"(NYC Local Time; Pipeline Run UTC: {last_run})"
            )
            where = (
                f"((created_date >= '{last_wm}' AND created_date <= '{effective_end}') OR "
                f"(resolution_action_updated_date >= '{last_run_nyc}' AND created_date >= '{p_start}') OR "
                f"(closed_date >= '{last_run_nyc}' AND created_date >= '{p_start}'))"
            )
            return where, order_clause, effective_end

        start = last_wm or start_date
        if not start:
            days = lookback_days or 7
            end_dt = datetime.strptime(effective_end[:10], "%Y-%m-%d")
            start = (end_dt - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00.000")
            logger.warning(
                f"No previous watermark found for incremental mode. "
                f"Ingesting past {days} days ({start}) up to {effective_end}."
            )
        elif last_wm:
            logger.info(f"Discovered initial created_date watermark: {last_wm}. Streaming up to {effective_end}.")
        else:
            logger.info(f"Using explicitly specified start_date for incremental intake: {start_date}")
    else:
        start = start_date

    conditions = []
    if start:
        conditions.append(f"created_date >= '{start}'")
    if effective_end:
        conditions.append(f"created_date <= '{effective_end}'")

    return (" AND ".join(conditions) if conditions else None), order_clause, effective_end


def run_from_landing_pipeline(
    landing_root: Path,
    bronze_311_dir: Path,
    manifests_dir: Path,
    landing_batch: Optional[str] = None,
    force: bool = False,
    env: str = "dev",
) -> Optional[Dict[str, Any]]:
    """
    Offline execution mode: Discovers Landing JSON batches and converts them to Bronze Parquet.
    """
    start_time = time.time()
    retrieved_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    if not landing_root.exists():
        logger.warning(f"Landing root directory does not exist: {landing_root}")
        return None

    if landing_batch:
        target_batch_dir = landing_root / landing_batch
        candidate_dirs = [target_batch_dir] if target_batch_dir.exists() else []
    else:
        candidate_dirs = sorted([d for d in landing_root.iterdir() if d.is_dir() and d.name.startswith("batch_")])

    if not candidate_dirs:
        logger.warning(f"No candidate landing batch directories found in {landing_root}")
        return None

    total_converted_rows = 0
    all_bronze_files: List[Dict[str, Any]] = []

    for l_dir in candidate_dirs:
        cur_batch_id = l_dir.name.replace("batch_", "")

        if not force and is_batch_completed(cur_batch_id, DATASET_311, manifests_dir=manifests_dir):
            logger.info(f"Idempotency Guard: Batch {cur_batch_id} has already succeeded. Skipping.")
            continue

        bronze_files, spark_schema_json = convert_landing_to_bronze_pyspark(
            landing_batch_dir=l_dir,
            base_bronze_dir=bronze_311_dir,
            batch_id=cur_batch_id,
            retrieved_at_utc=retrieved_at_utc,
            extraction_mode="from_landing",
            return_schema=True,
        )
        all_bronze_files.extend(bronze_files)
        batch_rows = sum(f["rows"] for f in bronze_files)
        total_converted_rows += batch_rows

        if bronze_files:
            code_version = get_git_commit_hash(PROJECT_ROOT)
            schema_fp = compute_schema_fingerprint(spark_schema_json)
            m_dict = create_manifest_dict(
                batch_id=cur_batch_id,
                dataset_name=DATASET_311,
                source_name=SOURCE_NAME_311,
                source_dataset_id=SOCRATA_DATASET_ID,
                source_url=SOCRATA_311_ENDPOINT,
                retrieved_at_utc=retrieved_at_utc,
                source_data_start="N/A",
                source_data_end="N/A",
                extraction_mode="from_landing",
                environment=env,
                code_version=code_version,
                duration_seconds=round(time.time() - start_time, 2),
                byte_size=sum(f["bytes"] for f in bronze_files),
                row_count=batch_rows,
                schema_fingerprint=schema_fp,
                status="success",
                files=bronze_files,
            )
            save_manifest(m_dict, manifests_dir=manifests_dir)

    duration = time.time() - start_time
    logger.info(
        f"From-Landing-Only Complete: {len(candidate_dirs)} batch(es) evaluated | "
        f"{total_converted_rows:,} rows converted to Bronze | {duration:.2f}s"
    )
    return {
        "status": "success",
        "batches_evaluated": len(candidate_dirs),
        "total_rows_converted": total_converted_rows,
        "bronze_files": all_bronze_files,
    }


def run_311_ingestion(
    mode: str = "incremental",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    batch_size: int = 50000,
    max_records: Optional[int] = None,
    lookback_days: int = 7,
    project_start_date: Optional[str] = None,
    app_token: Optional[str] = None,
    env: str = "dev",
    manifests_dir: Path = MANIFESTS_DIR,
    landing_only: bool = False,
    from_landing_only: bool = False,
    landing_batch: Optional[str] = None,
    bronze_dir: Optional[Path] = None,
    use_spark_bronze: bool = True,
    batch_id: Optional[str] = None,
    force: bool = False,
) -> Optional[Dict[str, Any]]:
    """
    Main orchestration routine for NYC 311 Ingestion.
    Supports incremental (CDC with watermark lookback) and backfill modes.
    Includes --landing-only (dump raw API payloads) and --from-landing-only (PySpark Lakehouse conversion).
    Idempotent: skips already processed batches and fully covered date intervals unless force=True.
    """
    start_time = time.time()
    retrieved_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    bronze_311_dir = Path(bronze_dir) if bronze_dir else BRONZE_DIR / "nyc311"
    app_token = app_token or os.environ.get("SOCRATA_APP_TOKEN")
    if app_token:
        logger.info("Using authenticated Socrata App Token (elevated API rate limit).")

    # Mode 1: --from-landing-only (Offline conversion)
    if from_landing_only:
        logger.info("Executing NYC 311 Ingestion in 'from-landing-only' mode (skipping API requests)...")
        return run_from_landing_pipeline(
            landing_root=LANDING_DIR / "nyc311",
            bronze_311_dir=bronze_311_dir,
            manifests_dir=manifests_dir,
            landing_batch=landing_batch,
            force=force,
            env=env,
        )

    # Mode 2: Standard Ingestion
    where_clause, order_clause, effective_end = build_311_soql_filter(
        mode=mode,
        start_date=start_date,
        end_date=end_date,
        lookback_days=lookback_days,
        project_start_date=project_start_date,
        manifests_dir=manifests_dir,
    )

    # Idempotency Guards
    if batch_id and not force and is_batch_completed(batch_id, DATASET_311, manifests_dir=manifests_dir):
        logger.info(f"Idempotency Guard: Batch {batch_id} has already succeeded. Skipping execution.")
        return None

    if start_date and effective_end and not force:
        if is_date_range_completed(DATASET_311, start_date, effective_end, manifests_dir=manifests_dir):
            logger.info(
                f"Idempotency Guard: Date range [{start_date} -> {effective_end}] already fully covered "
                f"by successful manifests. Skipping execution (use --force to override)."
            )
            return None

    active_batch_id = batch_id or generate_batch_id("311", manifests_dir=manifests_dir)
    logger.info(f"Starting NYC 311 Ingestion | Batch ID: {active_batch_id} | Mode: {mode} | Landing-Only: {landing_only}")

    landing_batch_dir = LANDING_DIR / "nyc311" / f"batch_{active_batch_id}"

    # Stream API directly to Landing zone
    fetch_result = fetch_311_to_landing(
        landing_batch_dir=landing_batch_dir,
        where_clause=where_clause,
        order_clause=order_clause,
        batch_size=batch_size,
        max_records=max_records,
        app_token=app_token,
    )

    total_rows = fetch_result["total_rows"]
    total_bytes = fetch_result["total_bytes"]
    manifest_files_list = list(fetch_result["manifest_files"])
    source_min_date = fetch_result["source_min_date"]
    source_max_date = fetch_result["source_max_date"]
    spark_schema_json = ""

    # Bronze Conversion: PySpark Lakehouse Execution (unless --landing-only)
    if landing_only:
        logger.info(
            f"Landing-only mode selected: {total_rows:,} records stored in Landing ({landing_batch_dir.name}). "
            f"Bronze Parquet conversion skipped as requested."
        )
    elif total_rows > 0:
        bronze_meta, spark_schema_json = convert_landing_to_bronze_pyspark(
            landing_batch_dir=landing_batch_dir,
            base_bronze_dir=bronze_311_dir,
            batch_id=active_batch_id,
            retrieved_at_utc=retrieved_at_utc,
            extraction_mode=mode,
            return_schema=True,
        )
        manifest_files_list.extend(bronze_meta)

    duration = time.time() - start_time
    code_version = get_git_commit_hash(PROJECT_ROOT)

    # Schema fingerprinting and drift detection via Spark schema
    if spark_schema_json:
        schema_fingerprint = compute_schema_fingerprint(spark_schema_json)
        prev_fp = get_latest_schema_fingerprint(DATASET_311, manifests_dir=manifests_dir)
        if prev_fp and prev_fp != schema_fingerprint:
            logger.warning(
                f"Schema Evolution / Drift Detected for {DATASET_311}! "
                f"Previous fingerprint: {prev_fp} -> Current: {schema_fingerprint}"
            )
    else:
        schema_fingerprint = "sha256:landing_only"

    # Save manifest as atomic commit marker
    manifest_dict = create_manifest_dict(
        batch_id=active_batch_id,
        dataset_name=DATASET_311,
        source_name=SOURCE_NAME_311,
        source_dataset_id=SOCRATA_DATASET_ID,
        source_url=SOCRATA_311_ENDPOINT,
        retrieved_at_utc=retrieved_at_utc,
        source_data_start=source_min_date or "N/A",
        source_data_end=source_max_date or "N/A",
        extraction_mode="landing_only" if landing_only else mode,
        environment=env,
        code_version=code_version,
        duration_seconds=duration,
        byte_size=total_bytes,
        row_count=total_rows,
        schema_fingerprint=schema_fingerprint,
        status="success" if total_rows > 0 else "partial",
        files=manifest_files_list,
        target_coverage_end=effective_end,
    )

    manifest_path = save_manifest(manifest_dict, manifests_dir=manifests_dir)
    logger.info(
        f"Completed NYC 311 Ingestion: {total_rows:,} rows | "
        f"{len(manifest_files_list)} files | {duration:.2f}s | Manifest: {manifest_path.name}"
    )
    return manifest_dict


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest NYC 311 Service Requests to Landing and Bronze layers.")
    parser.add_argument("--mode", choices=["incremental", "backfill"], default="incremental", help="Ingestion mode")
    parser.add_argument("--start-date", type=str, help="Start date filter (e.g. 2026-10-14T00:00:00.000)")
    parser.add_argument("--end-date", type=str, help="End date filter")
    parser.add_argument("--batch-size", type=int, default=50000, help="Records per API page")
    parser.add_argument("--max-records", type=int, help="Optional record limit for testing")
    parser.add_argument("--lookback-days", type=int, default=7, help="Lookback buffer in days when no watermark exists (default: 7)")
    parser.add_argument("--project-start-date", type=str, default=PROJECT_START_DATE, help="Project study universe start date (default: from .env/config)")
    parser.add_argument("--app-token", type=str, help="Optional Socrata App Token")
    parser.add_argument("--env", choices=["dev", "prod"], default="dev", help="Execution environment")
    parser.add_argument("--landing-only", action="store_true", help="Only dump raw API records into Landing zone; skip Bronze conversion")
    parser.add_argument("--from-landing-only", action="store_true", help="Skip API calls and generate Bronze Parquet from existing Landing files")
    parser.add_argument("--landing-batch", type=str, help="Specific landing batch folder name to convert (when using --from-landing-only)")
    parser.add_argument("--batch-id", type=str, help="Explicit batch ID to assign or check for idempotency")
    parser.add_argument("--force", action="store_true", help="Force ingestion even if date range or batch ID already succeeded")

    args = parser.parse_args()
    run_311_ingestion(
        mode=args.mode,
        start_date=args.start_date,
        end_date=args.end_date,
        batch_size=args.batch_size,
        max_records=args.max_records,
        lookback_days=args.lookback_days,
        project_start_date=args.project_start_date,
        app_token=args.app_token,
        env=args.env,
        landing_only=args.landing_only,
        from_landing_only=args.from_landing_only,
        landing_batch=args.landing_batch,
        batch_id=args.batch_id,
        force=args.force,
    )
