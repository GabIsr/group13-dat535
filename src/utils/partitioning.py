"""
DAT535 Medallion Lakehouse: Partition Discovery & Execution Windowing Utilities.
Author: Group 13
Provides robust calendar-aligned month-by-month partition batching for bounded-memory execution.
"""

import calendar
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
) -> List[Tuple[str, str]]:
    """
    Generate sequential month-by-month execution windows [(start_iso, end_iso), ...].
    Guarantees that PySpark execution is bounded to exactly one calendar month (~250k-350k rows)
    at a time, eliminating driver/executor OOM memory crashes regardless of historical volume.

    1. If both start_date and end_date are provided:
       Splits the date span into contiguous monthly slices.
       e.g., '2026-01-15' to '2026-03-10' produces:
             [('2026-01-15', '2026-01-31'), ('2026-02-01', '2026-02-28'), ('2026-03-01', '2026-03-10')]

    2. If dates are omitted or partial:
       Discovers available 'year=YYYY/month=MM' Hive partition directories in bronze_dir
       and generates chronological full-month windows.
    """
    dt_start = datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else None
    dt_end = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else None

    # If both dates are provided and fall within the exact same month, return single window
    if dt_start and dt_end:
        if dt_start > dt_end:
            raise ValueError(f"start_date ({start_date}) cannot be greater than end_date ({end_date})")

        windows: List[Tuple[str, str]] = []
        curr = dt_start
        while curr <= dt_end:
            # Last day of current month
            _, last_day = calendar.monthrange(curr.year, curr.month)
            month_end = date(curr.year, curr.month, last_day)

            # Cap window end at dt_end if dt_end is earlier than month_end
            window_end = min(month_end, dt_end)
            windows.append((curr.isoformat(), window_end.isoformat()))

            # Advance to first day of next month
            if curr.month == 12:
                curr = date(curr.year + 1, 1, 1)
            else:
                curr = date(curr.year, curr.month + 1, 1)
        return windows

    # If dates are omitted or partially omitted, discover from bronze_dir partitions
    discovered_months: List[Tuple[int, int]] = []
    if bronze_dir and bronze_dir.exists():
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

        # Apply partial bounds if provided
        if dt_start and m_end < dt_start:
            continue
        if dt_end and m_start > dt_end:
            continue

        actual_start = max(m_start, dt_start) if dt_start else m_start
        actual_end = min(m_end, dt_end) if dt_end else m_end
        windows.append((actual_start.isoformat(), actual_end.isoformat()))

    return windows
