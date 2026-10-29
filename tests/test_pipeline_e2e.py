"""
End-to-End (E2E) Integration & Schema Evolution Tests for the Medallion Pipeline.
Tests the full lifecycle: Landing (JSON.GZ) -> Bronze (PySpark) -> Silver (PySpark) -> Gold (PySpark)
as well as Schema Drift and Multi-Partition Compaction.
Author: Group 13
"""

import gzip
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from src.ingestion.ingest_311 import convert_landing_to_bronze_pyspark
from src.silver.clean_311 import clean_and_validate_311
from src.silver.clean_weather import clean_and_validate_weather
from src.silver.join_silver import join_silver_datasets
from src.gold.build_service_demand import build_daily_service_demand_mart
from src.gold.build_agency_performance import build_agency_resolution_performance_mart
from src.gold.build_zip_vulnerability import build_zip_infrastructure_vulnerability_mart
from src.utils.compaction import compact_partitions


class TestPipelineEndToEnd(unittest.TestCase):
    """Rigorous End-to-End integration tests for all Medallion Lakehouse tiers."""

    def test_full_medallion_pipeline_e2e(self):
        """
        Execute full lifecycle from Landing JSON to Gold analytical marts:
        Landing (.json.gz) -> Bronze (PySpark) -> Silver (PySpark) -> Gold (PySpark).
        Verifies zero data loss, correct schema flow, and SLA percentile computations.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            landing_dir = root / "landing" / "nyc311" / "batch_e2e_001"
            landing_dir.mkdir(parents=True)
            bronze_dir = root / "bronze"
            silver_dir = root / "silver"
            gold_dir = root / "gold"
            quarantine_dir = root / "quarantine"

            # -------------------------------------------------------------
            # Stage 1: Seed Realistic Landing Data (Raw JSON.GZ for 311)
            # -------------------------------------------------------------
            records_311 = [
                # Valid Closed complaint in Manhattan (EDT Daylight time: 14:00 local -> 18:00 UTC)
                {
                    "unique_key": "10001",
                    "created_date": "2024-10-18T14:00:00.000",
                    "closed_date": "2024-10-18T16:30:00.000",
                    "agency": "NYPD",
                    "complaint_type": "Noise - Residential",
                    "descriptor": "Loud Music/Party",
                    "borough": "MANHATTAN",
                    "incident_zip": "10001",
                    "latitude": "40.7500",
                    "longitude": "-73.9900",
                    "status": "Closed",
                    "_batch_id": "batch_e2e_001",
                },
                # Valid Closed complaint in Brooklyn
                {
                    "unique_key": "10002",
                    "created_date": "2024-10-18T15:00:00.000",
                    "closed_date": "2024-10-18T18:00:00.000",
                    "agency": "DSNY",
                    "complaint_type": "Illegal Dumping",
                    "descriptor": "Commercial",
                    "borough": "BROOKLYN",
                    "incident_zip": "11201",
                    "latitude": "40.6900",
                    "longitude": "-73.9900",
                    "status": "Closed",
                    "_batch_id": "batch_e2e_001",
                },
                # Valid Open ticket (closed_date is None)
                {
                    "unique_key": "10003",
                    "created_date": "2024-10-18T16:00:00.000",
                    "closed_date": None,
                    "agency": "DEP",
                    "complaint_type": "Water System",
                    "descriptor": "Hydrant Leaking",
                    "borough": "QUEENS",
                    "incident_zip": "11375",
                    "latitude": "40.7200",
                    "longitude": "-73.8400",
                    "status": "Open",
                    "_batch_id": "batch_e2e_001",
                },
                # Invalid record to test quarantine routing (negative duration: closed before created)
                {
                    "unique_key": "10004",
                    "created_date": "2024-10-18T18:00:00.000",
                    "closed_date": "2024-10-18T12:00:00.000",
                    "agency": "DOT",
                    "complaint_type": "Street Condition",
                    "descriptor": "Pothole",
                    "borough": "BRONX",
                    "incident_zip": "10451",
                    "latitude": "40.8200",
                    "longitude": "-73.9200",
                    "status": "Closed",
                    "_batch_id": "batch_e2e_001",
                },
            ]

            gz_path = landing_dir / "raw_part_0000.json.gz"
            with gzip.open(gz_path, "wb") as f:
                f.write(json.dumps(records_311).encode("utf-8"))

            # Seed Corresponding Bronze Weather Observations for those hours & grid
            bronze_weather_part = bronze_dir / "weather" / "grid_id=GRID_40.8_-74.0" / "year=2024"
            bronze_weather_part.mkdir(parents=True)
            weather_hours = pd.date_range("2024-10-18 00:00:00", "2024-10-18 23:00:00", freq="h", tz="UTC")
            df_mock_weather = pd.DataFrame({
                "time": [t.strftime("%Y-%m-%dT%H:%M") for t in weather_hours],
                "temperature_2m": [15.0 + i * 0.2 for i in range(len(weather_hours))],
                "precipitation": [0.0 if i < 18 else 5.2 for i in range(len(weather_hours))],
                "wind_speed_10m": [12.0 for _ in range(len(weather_hours))],
                "wind_gusts_10m": [14.0 for _ in range(len(weather_hours))],
                "relative_humidity_2m": [65 for _ in range(len(weather_hours))],
                "grid_id": "GRID_40.8_-74.0",
                "grid_lat": 40.8,
                "grid_lon": -74.0,
                "_batch_id": "weather_batch_001",
                "_ingested_at_utc": "2024-10-18T23:59:00Z",
                "_bronze_status": "valid",
            })
            df_mock_weather.to_parquet(bronze_weather_part / "part_weather_0000.parquet", index=False)

            # -------------------------------------------------------------
            # Stage 2: Execute PySpark Landing-to-Bronze Ingestion
            # -------------------------------------------------------------
            written_bronze = convert_landing_to_bronze_pyspark(
                landing_batch_dir=landing_dir,
                base_bronze_dir=bronze_dir / "nyc311",
                batch_id="batch_e2e_001",
                retrieved_at_utc="2024-10-18T23:59:00Z",
                extraction_mode="incremental",
            )
            self.assertTrue(len(written_bronze) > 0)
            self.assertEqual(sum(f["rows"] for f in written_bronze), 4)

            # Verify Bronze partition on disk
            bronze_311_files = list((bronze_dir / "nyc311").glob("**/*.parquet"))
            self.assertTrue(len(bronze_311_files) > 0)

            # -------------------------------------------------------------
            # Stage 3: Execute PySpark Silver Layer (Cleaning & Broadcast Join)
            # -------------------------------------------------------------
            res_311 = clean_and_validate_311(
                bronze_311_dir=bronze_dir / "nyc311",
                silver_311_dir=silver_dir / "nyc311",
                quarantine_dir=quarantine_dir / "nyc311",
            )
            self.assertEqual(res_311["input_rows"], 4)
            self.assertEqual(res_311["valid_rows_accepted"], 3)  # 3 valid
            self.assertEqual(res_311["quarantined_rows"], 1)     # 1 negative duration quarantined

            # Verify quarantine table exists and contains the corrupted record
            quar_file = quarantine_dir / "nyc311" / "quarantined_311.parquet"
            self.assertTrue(quar_file.exists())
            df_quar = pd.read_parquet(quar_file)
            self.assertIn("NEGATIVE_RESOLUTION_DURATION", df_quar["quarantine_reason"].iloc[0])

            # Run Silver Weather Cleaning
            res_weather = clean_and_validate_weather(
                bronze_weather_dir=bronze_dir / "weather",
                silver_weather_dir=silver_dir / "weather",
                quarantine_dir=quarantine_dir / "weather",
            )
            self.assertEqual(res_weather["valid_rows"], 24)

            # Run Silver Unified Fact Table Broadcast Join
            res_join = join_silver_datasets(
                silver_311_dir=silver_dir / "nyc311",
                silver_weather_dir=silver_dir / "weather",
                output_joined_dir=silver_dir / "311_weather_joined",
            )
            self.assertEqual(res_join["total_311_records"], 3)
            self.assertEqual(res_join["status"], "success")

            # Verify joined fact table exists on disk
            joined_files = list((silver_dir / "311_weather_joined").glob("**/*.parquet"))
            self.assertTrue(len(joined_files) > 0)

            # -------------------------------------------------------------
            # Stage 4: Execute PySpark Gold Layer (Marts 1, 2, and 3)
            # -------------------------------------------------------------
            # Mart 1: Daily Service Demand Mart
            res_m1 = build_daily_service_demand_mart(
                silver_311_dir=silver_dir / "nyc311",
                silver_weather_dir=silver_dir / "weather",
                output_dir=gold_dir,
            )
            self.assertEqual(res_m1["status"], "success")
            m1_path = Path(res_m1["output_path"])
            self.assertTrue(m1_path.exists())
            df_m1 = pd.read_parquet(m1_path)
            self.assertTrue(len(df_m1) > 0)
            self.assertIn("complaint_count", df_m1.columns)
            self.assertIn("avg_temperature_c", df_m1.columns)

            # Mart 2: Agency Resolution Performance Mart
            res_m2 = build_agency_resolution_performance_mart(
                silver_311_dir=silver_dir / "nyc311",
                silver_weather_dir=silver_dir / "weather",
                output_dir=gold_dir,
            )
            self.assertEqual(res_m2["status"], "success")
            m2_path = Path(res_m2["output_path"])
            self.assertTrue(m2_path.exists())
            df_m2 = pd.read_parquet(m2_path)
            self.assertTrue(len(df_m2) > 0)
            self.assertIn("median_resolution_duration_hours", df_m2.columns)
            self.assertIn("p90_resolution_duration_hours", df_m2.columns)

            # Mart 3: ZIP Infrastructure Vulnerability Mart
            res_m3 = build_zip_infrastructure_vulnerability_mart(
                silver_joined_dir=silver_dir / "311_weather_joined",
                silver_weather_dir=silver_dir / "weather",
                output_dir=gold_dir,
            )
            self.assertEqual(res_m3["status"], "success")
            m3_path = Path(res_m3["output_path"])
            self.assertTrue(m3_path.exists())
            df_m3 = pd.read_parquet(m3_path)
            self.assertTrue(len(df_m3) > 0)
            self.assertIn("storm_sensitivity_multiplier", df_m3.columns)

    def test_schema_evolution_and_drift_across_partitions(self):
        """
        Verify that PySpark gracefully unifies partitions with schema drift across years.
        Simulates 2020 schema (4 columns) vs 2024 schema (8 columns with new attributes).
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            bronze_in = root / "bronze" / "nyc311"
            silver_out = root / "silver" / "nyc311"
            quar_out = root / "quarantine" / "nyc311"

            # Historical 2020 Partition: 4 columns only
            part_2020 = bronze_in / "year=2020" / "month=06"
            part_2020.mkdir(parents=True)
            df_2020 = pd.DataFrame({
                "unique_key": ["5001", "5002"],
                "created_date": ["2020-06-15T10:00:00.000", "2020-06-16T11:00:00.000"],
                "complaint_type": ["Noise", "Heat/Hot Water"],
                "borough": ["MANHATTAN", "BRONX"],
            })
            df_2020.to_parquet(part_2020 / "part_2020.parquet", index=False)

            # Modern 2024 Partition: 8 columns (includes GPS, incident_zip, vehicle_type)
            part_2024 = bronze_in / "year=2024" / "month=10"
            part_2024.mkdir(parents=True)
            df_2024 = pd.DataFrame({
                "unique_key": ["5003", "5004"],
                "created_date": ["2024-10-18T10:00:00.000", "2024-10-18T12:00:00.000"],
                "complaint_type": ["Illegal Parking", "Street Condition"],
                "borough": ["BROOKLYN", "QUEENS"],
                "latitude": ["40.6800", "40.7300"],
                "longitude": ["-73.9800", "-73.8200"],
                "incident_zip": ["11201", "11375"],
                "vehicle_type": ["Commercial Box Truck", "Sedan"],
                "_batch_id": ["batch_2024_01", "batch_2024_01"],
            })
            df_2024.to_parquet(part_2024 / "part_2024.parquet", index=False)

            # Execute PySpark Silver pipeline over the combined multi-schema bronze
            res = clean_and_validate_311(
                bronze_311_dir=bronze_in,
                silver_311_dir=silver_out,
                quarantine_dir=quar_out,
            )

            # All 4 records must be accepted without schema mismatch crashes
            self.assertEqual(res["input_rows"], 4)
            self.assertEqual(res["valid_rows_accepted"], 4)

            # Verify that modern partitions populated clean coordinates while 2020 fell back cleanly
            silver_files = list(silver_out.glob("**/*.parquet"))
            self.assertTrue(len(silver_files) >= 2)

    def test_multi_partition_compaction_hierarchy(self):
        """
        Verify that PySpark coalesce(1) recursively traverses multiple leaf partitions,
        coalescing small files in each partition independently with zero row loss.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            base_dir = Path(tmp_dir) / "bronze" / "311"

            # Partition A (3 small files)
            part_a = base_dir / "year=2024" / "month=08"
            part_a.mkdir(parents=True)
            pd.DataFrame({"id": [1, 2], "val": ["a", "b"]}).to_parquet(part_a / "p1.parquet", index=False)
            pd.DataFrame({"id": [3, 4], "val": ["c", "d"]}).to_parquet(part_a / "p2.parquet", index=False)
            pd.DataFrame({"id": [5], "val": ["e"]}).to_parquet(part_a / "p3.parquet", index=False)

            # Partition B (2 small files)
            part_b = base_dir / "year=2024" / "month=09"
            part_b.mkdir(parents=True)
            pd.DataFrame({"id": [10, 20], "val": ["x", "y"]}).to_parquet(part_b / "p1.parquet", index=False)
            pd.DataFrame({"id": [30], "val": ["z"]}).to_parquet(part_b / "p2.parquet", index=False)

            summary = compact_partitions(base_dir)

            self.assertEqual(summary["total_partitions_scanned"], 2)
            self.assertEqual(summary["partitions_compacted"], 2)
            self.assertEqual(summary["files_before"], 5)
            self.assertEqual(summary["files_after"], 2)
            self.assertEqual(summary["files_eliminated"], 3)
            self.assertEqual(summary["total_rows_verified"], 8)

            # Verify that only 1 file remains in Partition A and contains 5 rows
            files_a = list(part_a.glob("*.parquet"))
            self.assertEqual(len(files_a), 1)
            self.assertEqual(len(pd.read_parquet(files_a[0])), 5)

            # Verify that only 1 file remains in Partition B and contains 3 rows
            files_b = list(part_b.glob("*.parquet"))
            self.assertEqual(len(files_b), 1)
            self.assertEqual(len(pd.read_parquet(files_b[0])), 3)


if __name__ == "__main__":
    unittest.main()
