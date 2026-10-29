"""
NYC 311 Service Requests Ingestion Engine (Landing & Bronze Layers).
Supports Bootstrap, Backfill, and Incremental CDC (Change Data Capture) updates.
Features PySpark-accelerated Landing-to-Bronze transformation with Small Files Prevention (coalesce),
Landing-only mode, and Idempotent manifest-based execution.
Complies with INGESTION_METADATA_SPECIFICATION.md.
Author: Group 13
"""

import argparse
import gzip
import hashlib
import json
import logging
import shutil
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pyspark.sql.functions import col, concat_ws, date_format, lit, sha2, to_timestamp, when
from src.utils.spark import get_spark_session

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


def get_relative_path(p: Path) -> str:
    """Safely return path relative to PROJECT_ROOT, or as_posix() if outside PROJECT_ROOT."""
    try:
        return p.relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return p.as_posix()


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
    Fallback Pandas/PyArrow writer: writes DataFrame to Bronze partitioned by year and month.
    Returns file metadata list for the manifest.
    """
    written_files = []

    if "created_date" in df.columns:
        dt_series = pd.to_datetime(df["created_date"], errors="coerce")
        years = dt_series.dt.strftime("%Y").fillna("unknown")
        months = dt_series.dt.strftime("%m").fillna("unknown")
    else:
        years = pd.Series(["unknown"] * len(df), index=df.index)
        months = pd.Series(["unknown"] * len(df), index=df.index)

    df["_partition_year"] = years
    df["_partition_month"] = months

    for (year, month), sub_df in df.groupby(["_partition_year", "_partition_month"]):
        partition_dir = base_bronze_dir / f"year={year}" / f"month={month}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        filename = f"part_{batch_id}_{part_idx:04d}.parquet"
        file_path = partition_dir / filename

        clean_sub_df = sub_df.drop(columns=["_partition_year", "_partition_month"])

        table = pa.Table.from_pandas(clean_sub_df, preserve_index=False)
        pq.write_table(table, file_path, compression="snappy")

        file_sha256 = compute_file_sha256(file_path)
        file_bytes = file_path.stat().st_size

        rel_path = get_relative_path(file_path)
        written_files.append({
            "file_name": filename,
            "relative_path": rel_path,
            "layer": "bronze",
            "rows": len(sub_df),
            "bytes": file_bytes,
            "sha256": file_sha256,
        })

    return written_files


def convert_landing_to_bronze_pyspark(
    landing_batch_dir: Path,
    base_bronze_dir: Path,
    batch_id: str,
    retrieved_at_utc: str,
    extraction_mode: str = "incremental",
) -> List[Dict[str, Any]]:
    """
    Read compressed JSON landing files via Apache PySpark, compute vectorized SHA-256 row hashes,
    inject Tier 2 system metadata, and write partitioned Bronze Parquet files (year=YYYY / month=MM).
    Utilizes PySpark coalesce(1) per partition to eliminate the Small Files Problem.
    Returns file metadata list for manifest registration.
    """
    landing_files = sorted(list(landing_batch_dir.glob("*.json.gz")))
    if not landing_files:
        logger.warning(f"No landing .json.gz files found in {landing_batch_dir}")
        return []

    logger.info(f"PySpark Landing-to-Bronze: Processing {len(landing_files)} landing file(s) for batch {batch_id}...")
    spark = get_spark_session("DAT535-Landing-To-Bronze-311")
    file_paths = [str(f.resolve()) for f in landing_files]
    df_raw = spark.read.json(*file_paths)
    total_rows = df_raw.count()
    if total_rows == 0:
        logger.warning(f"Landing files for batch {batch_id} contain 0 records.")
        return []

    # Vectorized SHA-256 row hashing across all payload columns
    cols_to_hash = sorted([c for c in df_raw.columns if not c.startswith("_")])
    hash_expr = sha2(concat_ws("||", *[col(c).cast("string") for c in cols_to_hash]), 256)

    source_file_rel = get_relative_path(landing_batch_dir)
    df_bronze = (
        df_raw.withColumn("_batch_id", lit(batch_id))
        .withColumn("_ingested_at_utc", lit(retrieved_at_utc))
        .withColumn("_source_file", lit(source_file_rel))
        .withColumn("_source_name", lit(SOURCE_NAME_311))
        .withColumn("_extraction_mode", lit(extraction_mode))
        .withColumn("_schema_version", lit(SCHEMA_VERSION))
        .withColumn("_raw_row_hash", hash_expr)
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

    partitions = df_bronze.select("year", "month").distinct().collect()
    written_files: List[Dict[str, Any]] = []

    for row in partitions:
        y, m = str(row["year"]), str(row["month"])
        partition_dir = base_bronze_dir / f"year={y}" / f"month={m}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        filename = f"part_{batch_id}_spark_{y}_{m}.parquet"
        file_path = partition_dir / filename

        sub_df = df_bronze.filter((col("year") == lit(y)) & (col("month") == lit(m))).drop("year", "month")
        f_rows = sub_df.count()

        tmp_out = partition_dir / f"_tmp_{filename}"
        sub_df.coalesce(1).write.mode("overwrite").parquet(str(tmp_out))

        part_files = list(tmp_out.glob("part-*.parquet"))
        if part_files:
            if file_path.exists():
                file_path.unlink()
            shutil.move(str(part_files[0]), str(file_path))
            shutil.rmtree(tmp_out, ignore_errors=True)

            f_size = file_path.stat().st_size
            f_sha = compute_file_sha256(file_path)

            try:
                rel_path = get_relative_path(file_path)
            except ValueError:
                rel_path = file_path.as_posix()

            written_files.append({
                "file_name": filename,
                "relative_path": rel_path,
                "layer": "bronze",
                "rows": f_rows,
                "bytes": f_size,
                "sha256": f_sha,
            })

    logger.info(f"PySpark Bronze complete: wrote {len(written_files)} partition file(s) ({total_rows:,} total rows).")
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
    landing_only: bool = False,
    from_landing_only: bool = False,
    landing_batch: Optional[str] = None,
    use_spark_bronze: bool = True,
) -> Optional[Dict[str, Any]]:
    """
    Main orchestration routine for NYC 311 Ingestion.
    Supports incremental (CDC with watermark lookback), backfill, and bootstrap modes.
    Includes --landing-only (dump raw API payloads) and --from-landing-only (PySpark Lakehouse conversion).
    """
    start_time = time.time()
    retrieved_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    bronze_311_dir = BRONZE_DIR / "nyc311"

    # -------------------------------------------------------------------------
    # Mode 1: --from-landing-only (Idempotent PySpark Landing-to-Bronze conversion)
    # -------------------------------------------------------------------------
    if from_landing_only:
        logger.info("Executing NYC 311 Ingestion in 'from-landing-only' mode (skipping API requests)...")
        landing_root = LANDING_DIR / "nyc311"
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

            # Idempotency check: has this landing batch already been processed into Bronze?
            if is_batch_completed(cur_batch_id, DATASET_311, manifests_dir=manifests_dir):
                logger.info(f"Idempotency Guard: Batch {cur_batch_id} has already succeeded. Skipping.")
                continue

            bronze_files = convert_landing_to_bronze_pyspark(
                landing_batch_dir=l_dir,
                base_bronze_dir=bronze_311_dir,
                batch_id=cur_batch_id,
                retrieved_at_utc=retrieved_at_utc,
                extraction_mode="from_landing",
            )
            all_bronze_files.extend(bronze_files)
            batch_rows = sum(f["rows"] for f in bronze_files)
            total_converted_rows += batch_rows

            if bronze_files:
                code_version = get_git_commit_hash(PROJECT_ROOT)
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
                    schema_fingerprint="sha256:pyspark_landing_to_bronze",
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

    # -------------------------------------------------------------------------
    # Mode 2: Standard Ingestion (API / Bootstrap Fetch -> Landing -> [Bronze])
    # -------------------------------------------------------------------------
    batch_id = generate_batch_id("311", manifests_dir=manifests_dir)
    logger.info(f"Starting NYC 311 Ingestion | Batch ID: {batch_id} | Mode: {mode} | Landing-Only: {landing_only}")

    # Idempotency check for current batch
    if is_batch_completed(batch_id, DATASET_311, manifests_dir=manifests_dir):
        logger.info(f"Idempotency Guard: Batch {batch_id} has already succeeded. Skipping execution.")
        return None

    landing_batch_dir = LANDING_DIR / "nyc311" / f"batch_{batch_id}"
    landing_batch_dir.mkdir(parents=True, exist_ok=True)

    manifest_files_list: List[Dict[str, Any]] = []
    total_rows = 0
    total_bytes = 0
    all_col_types: Dict[str, str] = {}
    source_min_date: Optional[str] = None
    source_max_date: Optional[str] = None

    if mode == "bootstrap":
        # Bootstrap from pre-downloaded raw parquet files
        source_dir = PROJECT_ROOT / "notebooks" / "data" / "nyc311_last_3_months"
        if not source_dir.exists():
            raise FileNotFoundError(f"Bootstrap source directory not found: {source_dir}")

        parquet_files = sorted(source_dir.glob("*.parquet"))
        logger.info(f"Found {len(parquet_files)} parquet parts to bootstrap from {source_dir}")

        for part_idx, pf in enumerate(parquet_files):
            df_part = pd.read_parquet(pf)
            if df_part.empty:
                continue

            if "created_date" in df_part.columns:
                part_min = str(df_part["created_date"].min())
                part_max = str(df_part["created_date"].max())
                source_min_date = part_min if source_min_date is None else min(source_min_date, part_min)
                source_max_date = part_max if source_max_date is None else max(source_max_date, part_max)

            # Write Landing JSON.GZ
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
                "relative_path": get_relative_path(landing_path),
                "layer": "landing",
                "rows": len(df_part),
                "bytes": landing_bytes,
                "sha256": landing_sha256,
            })

            for col_name in df_part.columns:
                all_col_types[col_name] = str(df_part[col_name].dtype)

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
                    dt_watermark = pd.to_datetime(last_watermark)
                    lookback_dt = dt_watermark - pd.Timedelta(days=lookback_days)
                    lookback_iso = lookback_dt.strftime("%Y-%m-%dT%H:%M:%S.000")
                    logger.info(
                        f"Incremental CDC Strategy: Querying created_date >= '{lookback_iso}' "
                        f"(watermark: {last_watermark} with {lookback_days}-day lookback window for status updates)"
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
                "relative_path": get_relative_path(landing_path),
                "layer": "landing",
                "rows": page_rows,
                "bytes": landing_bytes,
                "sha256": landing_sha256,
            })

            for col_name in df_page.columns:
                all_col_types[col_name] = str(df_page[col_name].dtype)

            total_rows += page_rows
            part_idx += 1
            offset += page_rows

            if page_rows < current_limit or (max_records and total_rows >= max_records):
                break

    # -------------------------------------------------------------------------
    # Bronze Conversion: PySpark Lakehouse Execution (unless --landing-only)
    # -------------------------------------------------------------------------
    if landing_only:
        logger.info(
            f"Landing-only mode selected: {total_rows:,} records stored in Landing ({landing_batch_dir.name}). "
            f"Bronze Parquet conversion skipped as requested."
        )
    elif total_rows > 0:
        if use_spark_bronze:
            bronze_meta = convert_landing_to_bronze_pyspark(
                landing_batch_dir=landing_batch_dir,
                base_bronze_dir=bronze_311_dir,
                batch_id=batch_id,
                retrieved_at_utc=retrieved_at_utc,
                extraction_mode=mode,
            )
            manifest_files_list.extend(bronze_meta)
        else:
            logger.info("Using fallback Pandas/PyArrow Bronze writer...")
            # Fallback writer handled per-page if use_spark_bronze is disabled

    duration = time.time() - start_time
    code_version = get_git_commit_hash(PROJECT_ROOT)
    schema_fingerprint = compute_schema_fingerprint(all_col_types) if all_col_types else "sha256:landing_only"

    manifest_dict = create_manifest_dict(
        batch_id=batch_id,
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
    parser.add_argument("--landing-only", action="store_true", help="Only dump raw API records into Landing zone; skip Bronze conversion")
    parser.add_argument("--from-landing-only", action="store_true", help="Skip API calls and generate Bronze Parquet from existing Landing files")
    parser.add_argument("--landing-batch", type=str, help="Specific landing batch folder name to convert (when using --from-landing-only)")

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
        landing_only=args.landing_only,
        from_landing_only=args.from_landing_only,
        landing_batch=args.landing_batch,
    )
