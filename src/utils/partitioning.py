"""
DAT535 Medallion Lakehouse: Partition Discovery & Execution Windowing Utilities.
Author: Group 13
Provides robust calendar-aligned month-by-month partition batching for bounded-memory execution.
"""

import calendar
import json
import logging
import re
from datetime import date, datetime
from pathlib import Path
from typing import List, Optional, Tuple

logger = logging.getLogger("partitioning")


def get_monthly_execution_windows(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    bronze_dir: Optional[Path] = None,
    mode: str = "all",
    manifests_dir: Optional[Path] = None,
) -> List[Tuple[str, str]]:
    """
    Generate sequential month-by-month execution windows [(start_iso, end_iso), ...].
    Guarantees that PySpark execution is bounded to exactly one calendar month (~250k-350k rows)
    at a time, eliminating driver/executor OOM memory crashes regardless of historical volume.

    1. If both start_date and end_date are provided:
       Splits the date span into contiguous monthly slices expanded outward to full calendar months
       to ensure atomic PySpark dynamic partition overwrites (year=YYYY/month=MM) without data loss.
       e.g., '2026-01-15' to '2026-03-10' produces:
             [('2026-01-01', '2026-01-31'), ('2026-02-01', '2026-02-28'), ('2026-03-01', '2026-03-31')]

    2. If mode == 'incremental' and dates are omitted:
       Discovers which specific 'year=YYYY/month=MM' partitions were touched by the latest
       Bronze ingestion manifest, executing only affected partition slices (skipping untouched months).

    3. If dates are omitted or partial in 'all' / 'backfill' mode:
       Discovers all available 'year=YYYY/month=MM' Hive partition directories in bronze_dir
       and generates chronological full-month windows.
    """
    dt_start = datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else None
    dt_end = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else None

    # If both dates are provided, expand boundaries outward to the full calendar months.
    # This prevents PySpark dynamic partition overwrites (year=YYYY/month=MM) from
    # erasing untargeted dates within partial-month runs.
    if dt_start and dt_end:
        if dt_start > dt_end:
            raise ValueError(f"start_date ({start_date}) cannot be greater than end_date ({end_date})")

        windows: List[Tuple[str, str]] = []
        curr = date(dt_start.year, dt_start.month, 1)
        while curr <= dt_end:
            # Last day of current month
            _, last_day = calendar.monthrange(curr.year, curr.month)
            month_end = date(curr.year, curr.month, last_day)

            windows.append((curr.isoformat(), month_end.isoformat()))

            # Advance to first day of next month
            if curr.month == 12:
                curr = date(curr.year + 1, 1, 1)
            else:
                curr = date(curr.year, curr.month + 1, 1)
        return windows

    discovered_months: List[Tuple[int, int]] = []

    # Incremental CDC Discovery: check partitions touched in latest Bronze manifest
    if mode == "incremental":
        m_dir = manifests_dir
        if not m_dir and bronze_dir:
            candidate_m = bronze_dir.parent.parent / "metadata" / "manifests"
            if candidate_m.exists():
                m_dir = candidate_m

        if m_dir and m_dir.exists():
            manifest_files = sorted(
                m_dir.glob("nyc_311_*.json"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            if manifest_files:
                try:
                    with open(manifest_files[0], "r", encoding="utf-8") as f:
                        m_data = json.load(f)
                        for wf in m_data.get("written_files", []):
                            rel = wf.get("relative_path", "")
                            match = re.search(r"year=(\d{4})[\\/]month=(\d{1,2})", rel)
                            if match:
                                discovered_months.append((int(match.group(1)), int(match.group(2))))

                        if not discovered_months and m_data.get("source_data_start"):
                            s_dt = datetime.strptime(str(m_data["source_data_start"])[:10], "%Y-%m-%d").date()
                            discovered_months.append((s_dt.year, s_dt.month))

                    if discovered_months:
                        unique_m = sorted(list(set(discovered_months)))
                        logger.info(
                            f"Incremental CDC mode: Discovered {len(unique_m)} partition(s) touched by "
                            f"latest Bronze batch: {unique_m}"
                        )
                except Exception as e:
                    logger.debug(f"Could not read latest ingestion manifest for incremental partitioning: {e}")

    # Fallback to discovering all partitions from bronze_dir if not incremental or manifest empty
    if not discovered_months and bronze_dir and bronze_dir.exists():
        # Match Hive style year=YYYY/month=MM or year=YYYY/month=M
        for p in bronze_dir.glob("year=*/month=*"):
            m = re.search(r"year=(\d{4})[\\/]month=(\d{1,2})", str(p))
            if m:
                discovered_months.append((int(m.group(1)), int(m.group(2))))

    if not discovered_months:
        # Fallback to current year or 2026 if no directories found
        fallback_year = dt_start.year if dt_start else (dt_end.year if dt_end else 2026)
        discovered_months = [(fallback_year, m) for m in range(1, 13)]

    # Deduplicate and sort chronologically
    sorted_months = sorted(list(set(discovered_months)))

    windows = []
    for y, m in sorted_months:
        _, last_day = calendar.monthrange(y, m)
        m_start = date(y, m, 1)
        m_end = date(y, m, last_day)

        # Skip months entirely outside the provided bounds
        if dt_start and m_end < dt_start:
            continue
        if dt_end and m_start > dt_end:
            continue

        # Expand to full calendar month to ensure atomic partition-level safety
        windows.append((m_start.isoformat(), m_end.isoformat()))

    return windows
