"""
Unit & Integration Tests for the Medallion Gold Layer.
Validates grain uniqueness, cohort separation, and zero-row-loss reconciliation against Silver.
Author: Group 13
"""

import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from src.gold.build_agency_performance import build_agency_resolution_performance_mart
from src.gold.build_service_demand import build_daily_service_demand_mart
from src.gold.config import GOLD_MARTS_DIR


class TestGoldLayerPipeline(unittest.TestCase):

    def test_daily_service_demand_mart_logic_and_reconciliation(self):
        """Verify daily demand aggregation, grain uniqueness, and row reconciliation."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            silver_311 = tmp_path / "silver_311"
            silver_weather = tmp_path / "silver_weather"
            gold_marts = tmp_path / "gold_marts"

            silver_311.mkdir()
            silver_weather.mkdir()
            gold_marts.mkdir()

            # Mock 311 records: 3 records across 2 dates and 2 complaint types
            mock_311 = [
                {
                    "unique_key": "101",
                    "created_at_utc": "2026-10-18T10:00:00Z",
                    "closed_at_utc": "2026-10-18T14:00:00Z",
                    "is_ticket_closed": True,
                    "resolution_duration_hours": 4.0,
                    "grid_id": "GRID_40.7_-74.0",
                    "borough": "MANHATTAN",
                    "complaint_type": "Noise",
                    "agency": "NYPD",
                },
                {
                    "unique_key": "102",
                    "created_at_utc": "2026-10-18T12:00:00Z",
                    "closed_at_utc": None,
                    "is_ticket_closed": False,
                    "resolution_duration_hours": np.nan,
                    "grid_id": "GRID_40.7_-74.0",
                    "borough": "MANHATTAN",
                    "complaint_type": "Noise",
                    "agency": "NYPD",
                },
                {
                    "unique_key": "103",
                    "created_at_utc": "2026-10-19T10:00:00Z",
                    "closed_at_utc": "2026-10-19T11:00:00Z",
                    "is_ticket_closed": True,
                    "resolution_duration_hours": 1.0,
                    "grid_id": "GRID_40.8_-73.9",
                    "borough": "BRONX",
                    "complaint_type": "Flooding",
                    "agency": "DEP",
                },
            ]
            pd.DataFrame(mock_311).to_parquet(silver_311 / "clean_311.parquet", index=False)

            # Mock Weather: hourly records for the 2 days
            mock_weather = [
                {
                    "grid_id": "GRID_40.7_-74.0",
                    "timestamp_utc": f"2026-10-18T{h:02d}:00:00Z",
                    "temperature_2m": 15.0,
                    "precipitation": 2.0 if h < 3 else 0.0,
                    "wind_gusts_10m": 10.0 + h,
                    "is_extreme_weather": False,
                }
                for h in range(24)
            ]
            pd.DataFrame(mock_weather).to_parquet(silver_weather / "clean_weather.parquet", index=False)

            res = build_daily_service_demand_mart(
                silver_311_dir=silver_311,
                silver_weather_dir=silver_weather,
                output_dir=gold_marts,
            )

            self.assertEqual(res["total_complaints_reconciled"], 3)
            self.assertEqual(res["distinct_dates"], 2)

            df_mart = pd.read_parquet(res["output_path"])
            # Grain uniqueness check
            grain_cols = ["date", "grid_id", "borough", "complaint_type"]
            self.assertEqual(df_mart.duplicated(subset=grain_cols).sum(), 0)

            # Check weather metrics on date 2026-10-18 for GRID_40.7_-74.0
            row_18 = df_mart[df_mart["date"] == pd.to_datetime("2026-10-18").date()].iloc[0]
            self.assertEqual(row_18["complaint_count"], 2)
            self.assertEqual(row_18["total_precipitation_mm"], 6.0)  # 2mm * 3 hours
            self.assertEqual(row_18["hours_of_precipitation"], 3)
            self.assertEqual(row_18["weather_data_coverage_pct"], 100.0)

    def test_agency_resolution_performance_mart_cohorts_and_backlog(self):
        """Verify creation cohort vs closure cohort separation and active backlog calculations."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            silver_311 = tmp_path / "silver_311"
            silver_weather = tmp_path / "silver_weather"
            gold_marts = tmp_path / "gold_marts"

            silver_311.mkdir()
            silver_weather.mkdir()
            gold_marts.mkdir()

            mock_311 = [
                # Ticket created Oct 10, closed Oct 12 (duration: 48h)
                {
                    "unique_key": "201",
                    "created_at_utc": "2026-10-10T10:00:00Z",
                    "closed_at_utc": "2026-10-12T10:00:00Z",
                    "is_ticket_closed": True,
                    "resolution_duration_hours": 48.0,
                    "borough": "QUEENS",
                    "agency": "DOT",
                },
                # Ticket created Oct 10, still open
                {
                    "unique_key": "202",
                    "created_at_utc": "2026-10-10T10:00:00Z",
                    "closed_at_utc": None,
                    "is_ticket_closed": False,
                    "resolution_duration_hours": np.nan,
                    "borough": "QUEENS",
                    "agency": "DOT",
                },
            ]
            pd.DataFrame(mock_311).to_parquet(silver_311 / "clean_311.parquet", index=False)

            res = build_agency_resolution_performance_mart(
                silver_311_dir=silver_311,
                silver_weather_dir=silver_weather,
                output_dir=gold_marts,
                long_open_days=1.0,  # 1 day threshold for test
            )

            df_perf = pd.read_parquet(res["output_path"])

            # Grain uniqueness check
            grain_cols = ["reporting_date", "borough", "agency"]
            self.assertEqual(df_perf.duplicated(subset=grain_cols).sum(), 0)

            # On Oct 10: 2 created, 0 closed, active backlog = 2
            row_oct10 = df_perf[df_perf["reporting_date"] == pd.to_datetime("2026-10-10").date()].iloc[0]
            self.assertEqual(row_oct10["tickets_created_today"], 2)
            self.assertEqual(row_oct10["tickets_closed_today"], 0)
            self.assertEqual(row_oct10["active_open_backlog_count"], 2)

            # On Oct 12: 0 created, 1 closed (duration 48h), active backlog = 1 (ticket 202)
            row_oct12 = df_perf[df_perf["reporting_date"] == pd.to_datetime("2026-10-12").date()].iloc[0]
            self.assertEqual(row_oct12["tickets_created_today"], 0)
            self.assertEqual(row_oct12["tickets_closed_today"], 1)
            self.assertEqual(row_oct12["median_resolution_duration_hours"], 48.0)
            self.assertEqual(row_oct12["active_open_backlog_count"], 1)
            self.assertEqual(row_oct12["long_open_backlog_count"], 1)  # ticket 202 is 2 days old > 1 day


if __name__ == "__main__":
    unittest.main()
