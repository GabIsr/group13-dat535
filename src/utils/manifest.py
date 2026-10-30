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
from typing import Any, Dict, List, Optional

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


def compute_schema_fingerprint(columns_and_types: Dict[str, str]) -> str:
    """
    Compute a deterministic canonical schema fingerprint from column names and types.
    Detects schema drift across ingestion batches.
    """
    sorted_items = sorted(columns_and_types.items(), key=lambda x: x[0])
    canonical_repr = ",".join(f"{col}:{dtype}" for col, dtype in sorted_items)
    return f"sha256:{hashlib.sha256(canonical_repr.encode('utf-8')).hexdigest()}"


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


def get_latest_cdc_watermark(dataset_name: str, manifests_dir: Path = MANIFESTS_DIR) -> Optional[str]:
    """
    Inspect existing successful manifests for a dataset to determine the highest cdc_updated_at_watermark.
    Returns None if no previous incremental CDC batches exist.
    """
    manifests_dir = Path(manifests_dir)
    if not manifests_dir.exists():
        return None

    pattern = f"{dataset_name}_*.json"
    manifest_files = list(manifests_dir.glob(pattern))
    if not manifest_files:
        return None

    latest_cdc_watermark = None
    for mf in manifest_files:
        try:
            with open(mf, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("status") == "success" and data.get("cdc_updated_at_watermark"):
                val = str(data["cdc_updated_at_watermark"])
                if latest_cdc_watermark is None or val > latest_cdc_watermark:
                    latest_cdc_watermark = val
        except Exception:
            continue

    return latest_cdc_watermark


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
    cdc_updated_at_watermark: Optional[str] = None,
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
    if cdc_updated_at_watermark:
        d["cdc_updated_at_watermark"] = cdc_updated_at_watermark
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
