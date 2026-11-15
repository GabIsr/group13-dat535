"""
Tier 1 Ingestion-Batch Manifest and Metadata Utilities.
Complies strictly with INGESTION_METADATA_SPECIFICATION.md.
Author: Group 13
"""

import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from src.utils.config import MANIFESTS_DIR


def get_git_commit_hash(repo_dir: Optional[Path] = None) -> str:
    """Return the current short git commit hash, or fallback if unavailable."""
    try:
        cmd = ["git", "rev-parse", "--short", "HEAD"]
        kwargs = {"cwd": str(repo_dir)} if repo_dir else {}
        output = subprocess.check_output(cmd, stderr=subprocess.DEVNULL, **kwargs)
        return f"git_commit_{output.decode('utf-8').strip()}"
    except Exception:
        # Fallback if run outside a direct git checkout
        return "git_commit_dev"


def compute_file_sha256(filepath: Path, chunk_size: int = 65536) -> str:
    """Compute the SHA-256 hash of a file on disk."""
    hasher = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(chunk_size):
            hasher.update(chunk)
    return hasher.hexdigest()


def compute_schema_fingerprint(columns_and_types: Union[Dict[str, str], str]) -> str:
    """
    Compute a deterministic canonical schema fingerprint from column names/types dict or Spark schema JSON string.
    Detects schema drift across ingestion batches.
    """
    if isinstance(columns_and_types, str):
        try:
            parsed = json.loads(columns_and_types)
            canonical_repr = json.dumps(parsed, sort_keys=True)
        except Exception:
            canonical_repr = columns_and_types
    else:
        sorted_items = sorted(columns_and_types.items(), key=lambda x: x[0])
        canonical_repr = ",".join(f"{col}:{dtype}" for col, dtype in sorted_items)
    return f"sha256:{hashlib.sha256(canonical_repr.encode('utf-8')).hexdigest()}"


def get_latest_schema_fingerprint(dataset_name: str, manifests_dir: Path = MANIFESTS_DIR) -> Optional[str]:
    """Inspect latest successful manifest to retrieve previous schema fingerprint."""
    manifests_dir = Path(manifests_dir)
    if not manifests_dir.exists():
        return None
    pattern = f"{dataset_name}_*.json"
    manifest_files = sorted(list(manifests_dir.glob(pattern)), key=lambda p: p.stat().st_mtime, reverse=True)
    for mf in manifest_files:
        try:
            with open(mf, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("status") == "success" and data.get("schema_fingerprint"):
                return data["schema_fingerprint"]
        except Exception:
            continue
    return None


def get_committed_files(dataset_name: str, layer: str = "bronze", manifests_dir: Path = MANIFESTS_DIR) -> List[str]:
    """Retrieve relative paths of all files published in successful manifests for a given dataset and layer."""
    manifests_dir = Path(manifests_dir)
    if not manifests_dir.exists():
        return []
    pattern = f"{dataset_name}_*.json"
    manifest_files = list(manifests_dir.glob(pattern))
    committed_files: List[str] = []
    for mf in manifest_files:
        try:
            with open(mf, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("status") != "success":
                continue
            for f_info in data.get("files", []):
                if f_info.get("layer") == layer and "relative_path" in f_info:
                    committed_files.append(f_info["relative_path"])
        except Exception:
            continue
    return committed_files


def generate_batch_id(dataset_prefix: str, date_str: Optional[str] = None, manifests_dir: Path = MANIFESTS_DIR) -> str:
    """
    Generate a deterministic batch identifier formatted as {prefix}_{YYYYMMDD}_{seq:03d}.
    Automatically increments sequence based on existing manifests for that day.
    """
    if date_str is None:
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")

    manifests_dir = Path(manifests_dir)
    pattern = f"*{dataset_prefix}_{date_str}_*.json"
    existing = list(manifests_dir.glob(pattern))
    seq = len(existing) + 1
    return f"{dataset_prefix}_{date_str}_{seq:03d}"


def get_latest_watermark(dataset_name: str, manifests_dir: Path = MANIFESTS_DIR) -> Optional[str]:
    """
    Inspect existing successful manifests for a dataset to determine the highest source_data_end watermark.
    Returns None if no previous successful batches exist.
    """
    manifests_dir = Path(manifests_dir)
    if not manifests_dir.exists():
        return None

    pattern = f"{dataset_name}_*.json"
    manifest_files = list(manifests_dir.glob(pattern))
    if not manifest_files:
        return None

    latest_watermark = None
    for mf in manifest_files:
        try:
            with open(mf, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("status") == "success" and data.get("source_data_end"):
                end_val = str(data["source_data_end"])
                if latest_watermark is None or end_val > latest_watermark:
                    latest_watermark = end_val
        except Exception:
            continue

    return latest_watermark


def get_earliest_watermark(dataset_name: str, manifests_dir: Path = MANIFESTS_DIR) -> Optional[str]:
    """
    Inspect existing successful manifests for a dataset to determine the earliest source_data_start.
    Returns None if no previous successful batches exist.
    """
    manifests_dir = Path(manifests_dir)
    if not manifests_dir.exists():
        return None

    pattern = f"{dataset_name}_*.json"
    manifest_files = list(manifests_dir.glob(pattern))
    if not manifest_files:
        return None

    earliest_watermark = None
    for mf in manifest_files:
        try:
            with open(mf, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("status") == "success" and data.get("source_data_start") and str(data["source_data_start"]) != "N/A":
                start_val = str(data["source_data_start"])
                if earliest_watermark is None or start_val < earliest_watermark:
                    earliest_watermark = start_val
        except Exception:
            continue

    return earliest_watermark


def get_latest_pipeline_run_timestamp(dataset_name: str, manifests_dir: Path = MANIFESTS_DIR) -> Optional[str]:
    """
    Inspect existing successful manifests for a dataset to determine the highest retrieved_at_utc execution timestamp.
    Used as the lower bound for mutation stream CDC queries (resolution_action_updated_date >= last_run_timestamp
    and closed_date >= last_run_timestamp).
    Returns None if no previous successful batches exist.
    """
    manifests_dir = Path(manifests_dir)
    if not manifests_dir.exists():
        return None

    pattern = f"{dataset_name}_*.json"
    manifest_files = list(manifests_dir.glob(pattern))
    if not manifest_files:
        return None

    latest_run_ts = None
    for mf in manifest_files:
        try:
            with open(mf, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("status") == "success" and data.get("retrieved_at_utc"):
                val = str(data["retrieved_at_utc"]).rstrip("Z")
                if latest_run_ts is None or val > latest_run_ts:
                    latest_run_ts = val
        except Exception:
            continue

    return latest_run_ts


def is_batch_completed(batch_id: str, dataset_name: str, manifests_dir: Path = MANIFESTS_DIR) -> bool:
    """Check if an ingestion batch manifest already exists with status 'success' (idempotency check)."""
    manifest_path = Path(manifests_dir) / f"{dataset_name}_{batch_id}.json"
    if not manifest_path.exists():
        return False
    try:
        with open(manifest_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("status") == "success"
    except Exception:
        return False


def _parse_manifest_date(val: Optional[str], is_end: bool = False) -> Optional[datetime]:
    """Helper to parse varied date/datetime formats from manifest files into datetime objects."""
    if not val or val == "N/A":
        return None
    val = str(val).rstrip("Z").strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(val, fmt)
        except ValueError:
            pass
    if len(val) == 10:
        try:
            dt = datetime.strptime(val, "%Y-%m-%d")
            return dt.replace(hour=23, minute=59, second=59, microsecond=999999) if is_end else dt
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(val)
    except Exception:
        return None


def compute_uncovered_date_intervals(
    dataset_name: str,
    requested_start: str,
    requested_end: str,
    manifests_dir: Path = MANIFESTS_DIR,
) -> List[Tuple[str, str]]:
    """
    Given a requested date range [requested_start, requested_end], inspect all successful manifests
    for dataset_name and compute the uncovered date intervals (subtraction of covered ranges).
    Returns a list of (start_iso, end_iso) tuples representing the gaps that still need to be ingested.
    If the requested range is fully covered, returns an empty list [].
    """
    req_start_dt = _parse_manifest_date(requested_start, is_end=False)
    req_end_dt = _parse_manifest_date(requested_end, is_end=True)
    if not req_start_dt or not req_end_dt or req_start_dt > req_end_dt:
        return [(requested_start, requested_end)]

    manifests_dir = Path(manifests_dir)
    if not manifests_dir.exists():
        return [(req_start_dt.isoformat(), req_end_dt.isoformat())]

    covered_intervals: List[Tuple[datetime, datetime]] = []
    pattern = f"{dataset_name}_*.json"
    for mf in manifests_dir.glob(pattern):
        try:
            with open(mf, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("status") != "success":
                continue
            m_start = _parse_manifest_date(data.get("source_data_start"), is_end=False)
            m_end = _parse_manifest_date(data.get("source_data_end") or data.get("target_coverage_end"), is_end=True)
            if m_start and m_end and m_start <= m_end:
                covered_intervals.append((m_start, m_end))
        except Exception:
            continue

    if not covered_intervals:
        return [(req_start_dt.isoformat(), req_end_dt.isoformat())]

    # Interval subtraction algorithm:
    uncovered: List[Tuple[datetime, datetime]] = [(req_start_dt, req_end_dt)]

    for cov_start, cov_end in covered_intervals:
        next_uncovered: List[Tuple[datetime, datetime]] = []
        for unc_start, unc_end in uncovered:
            # Case 1: No overlap
            if cov_end < unc_start or cov_start > unc_end:
                next_uncovered.append((unc_start, unc_end))
            else:
                # Case 2: Left remaining slice
                if unc_start < cov_start:
                    next_uncovered.append((unc_start, cov_start))
                # Case 3: Right remaining slice
                if unc_end > cov_end:
                    next_uncovered.append((cov_end, unc_end))
        uncovered = next_uncovered
        if not uncovered:
            break

    return [(s.isoformat(), e.isoformat()) for s, e in uncovered]


def is_date_range_completed(
    dataset_name: str,
    start_date: str,
    end_date: str,
    manifests_dir: Path = MANIFESTS_DIR,
) -> bool:
    """Check if the requested date range [start_date, end_date] is 100% covered by successful manifests."""
    uncovered = compute_uncovered_date_intervals(dataset_name, start_date, end_date, manifests_dir=manifests_dir)
    return len(uncovered) == 0


def create_manifest_dict(
    batch_id: str,
    dataset_name: str,
    source_name: str,
    source_dataset_id: str,
    source_url: str,
    retrieved_at_utc: str,
    source_data_start: str,
    source_data_end: str,
    extraction_mode: str,
    environment: str,
    code_version: str,
    duration_seconds: float,
    byte_size: int,
    row_count: int,
    schema_fingerprint: str,
    status: str,
    files: List[Dict[str, Any]],
    target_coverage_end: Optional[str] = None,
) -> Dict[str, Any]:
    """Construct a Tier 1 manifest dictionary complying strictly with specification."""
    d = {
        "batch_id": batch_id,
        "dataset_name": dataset_name,
        "source_name": source_name,
        "source_dataset_id": source_dataset_id,
        "source_url": source_url,
        "retrieved_at_utc": retrieved_at_utc,
        "source_data_start": source_data_start,
        "source_data_end": source_data_end,
        "extraction_mode": extraction_mode,
        "environment": environment,
        "code_version": code_version,
        "duration_seconds": round(duration_seconds, 2),
        "byte_size": byte_size,
        "row_count": row_count,
        "schema_fingerprint": schema_fingerprint,
        "status": status,
        "files": files,
    }
    if target_coverage_end:
        d["target_coverage_end"] = target_coverage_end
    return d


def save_manifest(
    manifest_data: Dict[str, Any],
    output_dir: Optional[Path] = None,
    manifests_dir: Optional[Path] = None,
) -> Path:
    """Save the Tier 1 manifest document atomically as JSON."""
    target_dir = Path(output_dir or manifests_dir or MANIFESTS_DIR)
    target_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{manifest_data['dataset_name']}_{manifest_data['batch_id']}.json"
    target_path = target_dir / filename
    temp_path = target_dir / f".tmp_{filename}"

    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(manifest_data, f, indent=2, ensure_ascii=False)

    temp_path.replace(target_path)
    return target_path
