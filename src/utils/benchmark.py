"""
DAT535 Pipeline Profiling & High-Resolution Performance Benchmarking.
Measures wall-clock latency, input/output row throughput, and step-by-step
bottlenecks across Bronze Ingestion, Silver Cleaning/Joins, and Gold Marts.
Generates persistent JSON execution ledgers and Markdown tables for CI/CD.
Author: Group 13
"""

import json
import logging
import os
import platform
import shutil
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.config import METADATA_DIR

BENCHMARKS_DIR = METADATA_DIR / "benchmarks"
BENCHMARKS_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("benchmark")


class StepTracker:
    """Active container for a single measured sub-operation."""

    def __init__(self, stage: str, step_name: str, input_rows: Optional[int] = None):
        self.stage = stage
        self.step_name = step_name
        self.input_rows = input_rows
        self.output_rows: Optional[int] = None
        self.start_perf = time.perf_counter()
        self.end_perf: Optional[float] = None
        self.duration_seconds: float = 0.0
        self.details: Dict[str, Any] = {}

    def set_input_rows(self, rows: int) -> None:
        self.input_rows = rows

    def set_output_rows(self, rows: int) -> None:
        self.output_rows = rows

    def add_detail(self, key: str, value: Any) -> None:
        self.details[key] = value

    def finish(self) -> float:
        if self.end_perf is None:
            self.end_perf = time.perf_counter()
            self.duration_seconds = round(self.end_perf - self.start_perf, 3)
        return self.duration_seconds

    def to_dict(self) -> Dict[str, Any]:
        rows_evaluated = self.output_rows if self.output_rows is not None else self.input_rows
        throughput = 0.0
        if rows_evaluated and self.duration_seconds > 0:
            throughput = round(rows_evaluated / self.duration_seconds, 2)

        return {
            "stage": self.stage,
            "step_name": self.step_name,
            "duration_seconds": self.duration_seconds,
            "duration_human": format_duration(self.duration_seconds),
            "input_rows": self.input_rows,
            "output_rows": self.output_rows,
            "throughput_rows_per_sec": throughput,
            "details": self.details,
        }


def format_duration(seconds: float) -> str:
    """Format seconds into readable human string (e.g. 1m 23s or 4.12s)."""
    if seconds < 60.0:
        return f"{seconds:.2f}s"
    minutes = int(seconds // 60)
    rem_sec = seconds % 60
    return f"{minutes}m {rem_sec:04.1f}s"


class PipelineProfiler:
    """
    Central performance profiler and benchmark aggregator.
    Tracks step latencies and serializes structured JSON execution summaries.
    """

    def __init__(self, run_id: Optional[str] = None, mode: str = "incremental"):
        self.run_id = run_id or os.environ.get("DAT535_BENCHMARK_ID")
        if not self.run_id:
            now_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            self.run_id = f"bench_{now_str}"

        self.mode = mode
        self.start_wall_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.start_perf = time.perf_counter()
        self.end_perf: Optional[float] = None
        self.total_duration_seconds: float = 0.0
        self.environment = "github_actions" if os.environ.get("GITHUB_ACTIONS") == "true" else "local_environment"
        self.steps: List[StepTracker] = []
        self._load_existing_if_present()

    def _get_benchmark_path(self) -> Path:
        return BENCHMARKS_DIR / f"{self.run_id}.json"

    def _load_existing_if_present(self) -> None:
        """Load prior stage metrics if running as part of a multi-process pipeline run."""
        path = self._get_benchmark_path()
        if path.exists():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.start_wall_utc = data.get("start_time_utc", self.start_wall_utc)
                    self.mode = data.get("execution_mode", self.mode)
            except Exception as e:
                logger.debug(f"Could not read existing benchmark file {path}: {e}")

    @contextmanager
    def measure_step(self, stage: str, step_name: str, input_rows: Optional[int] = None):
        """Context manager to measure execution time of a pipeline step."""
        tracker = StepTracker(stage=stage, step_name=step_name, input_rows=input_rows)
        try:
            yield tracker
        finally:
            tracker.finish()
            self.steps.append(tracker)
            # Persist incrementally so crashed or split runs still preserve timings
            self.save_json()

    def record_step(
        self,
        stage: str,
        step_name: str,
        duration_seconds: float,
        input_rows: Optional[int] = None,
        output_rows: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Explicitly record a step without a context manager."""
        tracker = StepTracker(stage=stage, step_name=step_name, input_rows=input_rows)
        tracker.duration_seconds = round(duration_seconds, 3)
        tracker.output_rows = output_rows
        if details:
            tracker.details.update(details)
        self.steps.append(tracker)
        self.save_json()

    def finish(self) -> float:
        """Mark end of execution and compute total elapsed seconds."""
        if self.end_perf is None:
            self.end_perf = time.perf_counter()
            self.total_duration_seconds = round(self.end_perf - self.start_perf, 3)
        return self.total_duration_seconds

    def to_dict(self) -> Dict[str, Any]:
        """Compile complete structured dictionary for JSON export."""
        self.finish()

        # Read any prior stages recorded by other subprocesses in this run session
        prior_steps_dict: Dict[str, Dict[str, Any]] = {}
        path = self._get_benchmark_path()
        if path.exists():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    existing = json.load(f)
                    for s in existing.get("steps", []):
                        key = f"{s.get('stage')}_{s.get('step_name')}"
                        prior_steps_dict[key] = s
            except Exception:
                pass

        # Merge current in-memory steps over prior steps
        for st in self.steps:
            st_dict = st.to_dict()
            key = f"{st_dict['stage']}_{st_dict['step_name']}"
            prior_steps_dict[key] = st_dict

        all_steps = list(prior_steps_dict.values())
        total_time = sum(s.get("duration_seconds", 0.0) for s in all_steps)
        if total_time <= 0:
            total_time = self.total_duration_seconds

        # Group steps by stage
        stages_summary: Dict[str, Dict[str, Any]] = {}
        for s in all_steps:
            st_name = s.get("stage", "other")
            if st_name not in stages_summary:
                stages_summary[st_name] = {
                    "total_duration_seconds": 0.0,
                    "steps": [],
                }
            stages_summary[st_name]["total_duration_seconds"] = round(
                stages_summary[st_name]["total_duration_seconds"] + s.get("duration_seconds", 0.0), 3
            )
            # Add relative percentage share of total
            pct = round((s.get("duration_seconds", 0.0) / total_time * 100.0), 2) if total_time > 0 else 0.0
            s["pct_of_total"] = pct
            stages_summary[st_name]["steps"].append(s)

        for st_info in stages_summary.values():
            st_sec = st_info["total_duration_seconds"]
            st_info["duration_human"] = format_duration(st_sec)
            st_info["pct_of_pipeline"] = round((st_sec / total_time * 100.0), 2) if total_time > 0 else 0.0

        max_rows = 0
        for s in all_steps:
            r = s.get("output_rows") or s.get("input_rows") or 0
            if r > max_rows:
                max_rows = r

        overall_throughput = round(max_rows / total_time, 2) if total_time > 0 and max_rows > 0 else 0.0

        return {
            "benchmark_id": self.run_id,
            "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "start_time_utc": self.start_wall_utc,
            "environment": self.environment,
            "execution_mode": self.mode,
            "system_info": {
                "os": platform.system(),
                "os_release": platform.release(),
                "python_version": platform.python_version(),
            },
            "summary": {
                "total_duration_seconds": round(total_time, 3),
                "total_duration_human": format_duration(total_time),
                "max_rows_processed": max_rows,
                "overall_throughput_rows_per_sec": overall_throughput,
                "total_steps_executed": len(all_steps),
            },
            "stages": stages_summary,
            "steps": all_steps,
        }

    def save_json(self, output_dir: Optional[Path] = None) -> Path:
        """Write benchmark data to timestamped JSON file and update latest_benchmark.json."""
        target_dir = output_dir or BENCHMARKS_DIR
        target_dir.mkdir(parents=True, exist_ok=True)

        payload = self.to_dict()
        file_path = target_dir / f"{self.run_id}.json"
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

        # Update latest_benchmark.json convenience pointer
        latest_path = target_dir / "latest_benchmark.json"
        try:
            shutil.copy2(file_path, latest_path)
        except Exception as e:
            logger.debug(f"Could not update {latest_path}: {e}")

        return file_path

    def print_summary_table(self) -> None:
        """Print clean ASCII execution summary table to console."""
        data = self.to_dict()
        summary = data.get("summary", {})
        steps = data.get("steps", [])

        if not steps:
            return

        print("\n" + "=" * 106)
        print("                        MEDALLION PIPELINE EXECUTION & PERFORMANCE BENCHMARK                        ")
        print("=" * 106)
        header = f"{'Layer / Stage':<22} | {'Sub-Operation':<32} | {'Duration':<10} | {'% Total':<8} | {'Rows Out':<11} | {'Throughput'}"
        print(header)
        print("-" * 106)

        for s in steps:
            stage_str = s.get("stage", "other").replace("_", " ").title()[:22]
            step_str = s.get("step_name", "").replace("_", " ").title()[:32]
            dur_str = format_duration(s.get("duration_seconds", 0.0))
            pct_str = f"{s.get('pct_of_total', 0.0):.1f}%"
            rows = s.get("output_rows") if s.get("output_rows") is not None else s.get("input_rows")
            rows_str = f"{rows:,}" if rows is not None else "N/A"
            tput = s.get("throughput_rows_per_sec", 0.0)
            tput_str = f"{tput:,.0f} r/s" if tput > 0 else "-"

            print(f"{stage_str:<22} | {step_str:<32} | {dur_str:<10} | {pct_str:<8} | {rows_str:<11} | {tput_str}")

        print("=" * 106)
        tot_human = summary.get("total_duration_human", "N/A")
        tot_sec = summary.get("total_duration_seconds", 0.0)
        tot_rows = summary.get("max_rows_processed", 0)
        print(
            f"TOTAL PIPELINE RUNTIME: {tot_human} ({tot_sec:.2f}s) | "
            f"Max Rows: {tot_rows:,} | Environment: {self.environment}"
        )
        print(f"Benchmark ledger saved to: {self._get_benchmark_path()}")
        print("=" * 106 + "\n")

    def generate_markdown_table(self) -> str:
        """Generate GitHub Flavored Markdown table for CI/CD and reports."""
        data = self.to_dict()
        summary = data.get("summary", {})
        steps = data.get("steps", [])

        if not steps:
            return ""

        md: list = []
        md.append("## ⏱️ Medallion Pipeline Runtime & Performance Breakdown")
        md.append(f"**Total Runtime:** `{summary.get('total_duration_human')}` (`{summary.get('total_duration_seconds'):.2f}s`) | "
                  f"**Peak Rows Processed:** `{summary.get('max_rows_processed'):,}` | "
                  f"**Run ID:** `{self.run_id}`")
        md.append("")
        md.append("| Layer / Pipeline Stage | Key Operation / Sub-Step | Duration | Share of Total | Rows Evaluated | Throughput |")
        md.append("| :--- | :--- | :---: | :---: | :---: | :---: |")

        for s in steps:
            st = s.get("stage", "other").replace("_", " ").title()
            op = s.get("step_name", "").replace("_", " ").title()
            dur = format_duration(s.get("duration_seconds", 0.0))
            pct = f"{s.get('pct_of_total', 0.0):.1f}%"
            rows = s.get("output_rows") if s.get("output_rows") is not None else s.get("input_rows")
            r_str = f"{rows:,}" if rows is not None else "N/A"
            tput = s.get("throughput_rows_per_sec", 0.0)
            tp_str = f"`{tput:,.0f} r/s`" if tput > 0 else "-"
            md.append(f"| **{st}** | {op} | `{dur}` | `{pct}` | `{r_str}` | {tp_str} |")

        md.append("")
        return "\n".join(md)


_GLOBAL_PROFILER: Optional[PipelineProfiler] = None


def get_profiler(run_id: Optional[str] = None, mode: str = "incremental") -> PipelineProfiler:
    """Return shared profiler instance for this process."""
    global _GLOBAL_PROFILER
    if _GLOBAL_PROFILER is None:
        _GLOBAL_PROFILER = PipelineProfiler(run_id=run_id, mode=mode)
    return _GLOBAL_PROFILER


def reset_profiler() -> None:
    """Reset the global profiler instance (e.g. between tests)."""
    global _GLOBAL_PROFILER
    _GLOBAL_PROFILER = None
