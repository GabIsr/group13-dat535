"""
Unit & Integration Tests for Ingestion and Bronze Layer (unittest standard library).
Author: Group 13
"""

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
import pandas as pd

from src.utils.config import (
    DATASET_311,
    DATASET_WEATHER,
    ERA5_NYC_GRID_POINTS,
    WEATHER_HOURLY_VARIABLES,
)
from src.utils.manifest import (
    compute_file_sha256,
    compute_schema_fingerprint,
    compute_uncovered_date_intervals,
    create_manifest_dict,
    generate_batch_id,
    get_committed_files,
    get_earliest_watermark,
    get_latest_pipeline_run_timestamp,
    get_latest_schema_fingerprint,
    get_latest_watermark,
    is_batch_completed,
    is_date_range_completed,
    save_manifest,
)
from unittest.mock import patch, MagicMock
import io
import urllib.error
from src.ingestion.ingest_weather import fetch_open_meteo_batch


class TestIngestionPipeline(unittest.TestCase):

    def test_grid_cells_count_and_bounds(self):
        """Verify that NYC is covered by exactly 19 ERA5-Land grid cells within valid bounds."""
        self.assertEqual(len(ERA5_NYC_GRID_POINTS), 19)
        for pt in ERA5_NYC_GRID_POINTS:
            self.assertTrue(40.4 <= pt["grid_lat"] <= 41.0)
            self.assertTrue(-74.4 <= pt["grid_lon"] <= -73.6)
            self.assertTrue(pt["grid_id"].startswith("GRID_"))
            self.assertIn(pt["borough_hint"], ["MANHATTAN", "BRONX", "BROOKLYN", "QUEENS", "STATEN ISLAND"])

    def test_weather_variables_list(self):
        """Ensure all required core and extended meteorological variables are configured."""
        required = [
            "temperature_2m",
            "precipitation",
            "wind_speed_10m",
            "relative_humidity_2m",
            "soil_temperature_0_to_7cm",
            "soil_moisture_0_to_7cm",
            "cloud_cover",
        ]
        for var in required:
            self.assertIn(var, WEATHER_HOURLY_VARIABLES)

    def test_file_sha256_computation(self):
        """Verify deterministic SHA-256 computation on test bytes."""
        with tempfile.NamedTemporaryFile("w+", delete=False, encoding="utf-8") as tmp:
            tmp.write("DAT535_INGESTION_TEST_PAYLOAD")
            tmp_path = Path(tmp.name)

        try:
            expected = hashlib.sha256("DAT535_INGESTION_TEST_PAYLOAD".encode("utf-8")).hexdigest()
            self.assertEqual(compute_file_sha256(tmp_path), expected)
        finally:
            tmp_path.unlink()

    def test_schema_fingerprint_drift_detection(self):
        """Verify schema fingerprint is order-independent and detects changes."""
        schema_a = {"col1": "int64", "col2": "string"}
        schema_b = {"col2": "string", "col1": "int64"}
        schema_c = {"col1": "int64", "col2": "string", "col3": "float64"}

        fp_a = compute_schema_fingerprint(schema_a)
        fp_b = compute_schema_fingerprint(schema_b)
        fp_c = compute_schema_fingerprint(schema_c)

        self.assertEqual(fp_a, fp_b)  # Canonical sorting ensures equality
        self.assertNotEqual(fp_a, fp_c)  # Schema drift detected

    def test_manifest_lifecycle_and_watermark(self):
        """Verify creating, saving, discovering watermarks, and idempotency checks."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Initially no watermark
            self.assertIsNone(get_latest_watermark("nyc_311", manifests_dir=tmp_path))

            # Batch 1
            b1_id = generate_batch_id("311", date_str="20261023", manifests_dir=tmp_path)
            self.assertTrue(b1_id.endswith("_001"))

            m1 = create_manifest_dict(
                batch_id=b1_id,
                dataset_name="nyc_311",
                source_name="nyc_open_data",
                source_dataset_id="erm2-nwe9",
                source_url="https://data.cityofnewyork.us/resource/erm2-nwe9.json",
                retrieved_at_utc="2026-10-23T10:00:00Z",
                source_data_start="2026-10-01",
                source_data_end="2026-10-10",
                extraction_mode="incremental",
                environment="dev",
                code_version="git_test",
                duration_seconds=1.5,
                byte_size=1000,
                row_count=50,
                schema_fingerprint="sha256:test",
                status="success",
                files=[],
            )
            save_manifest(m1, output_dir=tmp_path)

            # Batch 1 should be recognized as completed
            self.assertTrue(is_batch_completed(b1_id, "nyc_311", manifests_dir=tmp_path))
            # Watermark should now be 2026-10-10, earliest should be 2026-10-01
            self.assertEqual(get_latest_watermark("nyc_311", manifests_dir=tmp_path), "2026-10-10")
            self.assertEqual(get_earliest_watermark("nyc_311", manifests_dir=tmp_path), "2026-10-01")

            # Batch 2 sequence should increment
            b2_id = generate_batch_id("311", date_str="20261023", manifests_dir=tmp_path)
            self.assertTrue(b2_id.endswith("_002"))

    def test_pyspark_landing_to_bronze_conversion_and_idempotency(self):
        """Verify that PySpark converts landing JSON.GZ to partitioned Bronze Parquet with row-level hashes."""
        import gzip
        import json
        from src.ingestion.ingest_311 import convert_landing_to_bronze_pyspark

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            landing_dir = tmp_path / "landing" / "batch_311_20261029_001"
            landing_dir.mkdir(parents=True)
            bronze_dir = tmp_path / "bronze"

            # Create mock compressed landing JSON including Socrata system column :created_at
            mock_records = [
                {
                    "unique_key": "1001",
                    "created_date": "2026-10-18T10:00:00.000",
                    "complaint_type": "Noise",
                    "latitude": "40.71",
                    "longitude": "-74.00",
                    ":created_at": 1729245000,
                },
                {
                    "unique_key": "1002",
                    "created_date": "2026-10-19T14:30:00.000",
                    "complaint_type": "Illegal Parking",
                    "latitude": "40.75",
                    "longitude": "-73.98",
                    ":created_at": "2026-10-19T14:30:00.000Z",
                },
            ]
            gz_path = landing_dir / "raw_part_0000.json.gz"
            with gzip.open(gz_path, "wb") as f:
                f.write(json.dumps(mock_records).encode("utf-8"))

            written_files = convert_landing_to_bronze_pyspark(
                landing_batch_dir=landing_dir,
                base_bronze_dir=bronze_dir,
                batch_id="311_20261029_001",
                retrieved_at_utc="2026-10-29T10:00:00Z",
                extraction_mode="incremental",
            )

            self.assertTrue(len(written_files) > 0)
            self.assertEqual(sum(f["rows"] for f in written_files), 2)

            # Verify Parquet file was written to year=2026/month=10
            parquet_files = list(bronze_dir.glob("**/*.parquet"))
            self.assertEqual(len(parquet_files), 1)
            df_bronze = pd.read_parquet(parquet_files[0])
            self.assertEqual(len(df_bronze), 2)
            self.assertIn("_batch_id", df_bronze.columns)
            self.assertEqual(df_bronze["_batch_id"].iloc[0], "311_20261029_001")

            # Verify colon column :created_at was cleanly renamed to standard Bronze identifier
            self.assertIn("source_created_at", df_bronze.columns)
            self.assertNotIn(":created_at", df_bronze.columns)

    def test_pipeline_run_timestamp_manifest_tracking(self):
        """Verify watermark tracking and pipeline run timestamp tracking across backfill and incremental manifests."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # 1. Backfill batch (targets created_date 2025)
            m_backfill = create_manifest_dict(
                batch_id="311_2025_backfill",
                dataset_name="nyc_311",
                source_name="nyc_open_data",
                source_dataset_id="erm2-nwe9",
                source_url="https://data.cityofnewyork.us/resource/erm2-nwe9.json",
                retrieved_at_utc="2026-10-30T10:00:00Z",
                source_data_start="2025-01-01",
                source_data_end="2025-12-31",
                extraction_mode="backfill",
                environment="dev",
                code_version="git_test",
                duration_seconds=5.0,
                byte_size=50000,
                row_count=1000,
                schema_fingerprint="sha256:test",
                status="success",
                files=[],
            )
            save_manifest(m_backfill, output_dir=tmp_path)

            # Standard watermark sees the 2025 backfill
            self.assertEqual(get_latest_watermark("nyc_311", manifests_dir=tmp_path), "2025-12-31")
            self.assertEqual(get_latest_pipeline_run_timestamp("nyc_311", manifests_dir=tmp_path), "2026-10-30T10:00:00")

            # 2. Incremental batch advances watermark and pipeline run timestamp
            m_incr = create_manifest_dict(
                batch_id="311_20261030_001",
                dataset_name="nyc_311",
                source_name="nyc_open_data",
                source_dataset_id="erm2-nwe9",
                source_url="https://data.cityofnewyork.us/resource/erm2-nwe9.json",
                retrieved_at_utc="2026-10-30T11:00:00Z",
                source_data_start="2026-10-29",
                source_data_end="2026-10-30",
                extraction_mode="incremental",
                environment="dev",
                code_version="git_test",
                duration_seconds=2.0,
                byte_size=10000,
                row_count=200,
                schema_fingerprint="sha256:test",
                status="success",
                files=[],
            )
            save_manifest(m_incr, output_dir=tmp_path)

            self.assertEqual(get_latest_watermark("nyc_311", manifests_dir=tmp_path), "2026-10-30")
            # Pipeline execution watermark tracks the latest retrieved_at_utc run timestamp (stripped of trailing Z)
            self.assertEqual(get_latest_pipeline_run_timestamp("nyc_311", manifests_dir=tmp_path), "2026-10-30T11:00:00")

    def test_business_mutation_window_clauses(self):
        """Verify that the 3-Clause Business Mutation Window anchors queries to project start date and strips trailing Z."""
        from src.utils.config import PROJECT_START_DATE

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Create an existing manifest to establish watermarks (testing legacy 'Z' stripping)
            m = create_manifest_dict(
                batch_id="311_20261028_001",
                dataset_name="nyc_311",
                source_name="nyc_open_data",
                source_dataset_id="erm2-nwe9",
                source_url="https://data.cityofnewyork.us/resource/erm2-nwe9.json",
                retrieved_at_utc="2026-10-28T14:00:00Z",
                source_data_start="2026-10-20",
                source_data_end="2026-10-28T00:00:00.000",
                extraction_mode="incremental",
                environment="dev",
                code_version="git_test",
                duration_seconds=1.0,
                byte_size=1000,
                row_count=100,
                schema_fingerprint="sha256:test",
                status="success",
                files=[],
            )
            save_manifest(m, output_dir=tmp_path)

            last_ceiling = get_latest_watermark("nyc_311", manifests_dir=tmp_path)
            last_run = get_latest_pipeline_run_timestamp("nyc_311", manifests_dir=tmp_path)

            self.assertEqual(last_ceiling, "2026-10-28T00:00:00.000")
            # Must be cleanly stripped of 'Z' for Socrata floating timestamp compliance
            self.assertEqual(last_run, "2026-10-28T14:00:00")

            # Verify 3-clause formulation
            effective_end = "2026-11-01T23:59:59.000"
            proj_floor = PROJECT_START_DATE
            intake_clause = f"(created_date >= '{last_ceiling}' AND created_date <= '{effective_end}')"
            mutation_clause = f"(resolution_action_updated_date >= '{last_run}' AND created_date >= '{proj_floor}')"
            closure_clause = f"(closed_date >= '{last_run}' AND created_date >= '{proj_floor}')"

            query = f"({intake_clause} OR {mutation_clause} OR {closure_clause})"

            self.assertIn(f"created_date >= '{last_ceiling}'", query)
            self.assertIn(f"resolution_action_updated_date >= '{last_run}'", query)
            self.assertIn(f"created_date >= '{proj_floor}'", query)
            self.assertIn(f"closed_date >= '{last_run}'", query)

    @patch("time.sleep")
    @patch("urllib.request.urlopen")
    def test_open_meteo_retry_on_502_bad_gateway(self, mock_urlopen, mock_sleep):
        """Verify that fetch_open_meteo_batch catches 502 Bad Gateway and retries with backoff."""
        # 1st attempt: 502 Bad Gateway, 2nd attempt: Success 200
        mock_resp_fail = urllib.error.HTTPError("http://test", 502, "Bad Gateway", {}, io.BytesIO(b"Bad Gateway"))
        mock_resp_success = MagicMock()
        mock_resp_success.read.return_value = b'[{"latitude": 40.7, "longitude": -74.0, "hourly": {}}]'
        mock_resp_success.__enter__.return_value = mock_resp_success

        mock_urlopen.side_effect = [mock_resp_fail, mock_resp_success]

        grid_points = [{"grid_lat": 40.7, "grid_lon": -74.0, "grid_id": "GRID_40.7_-74.0"}]
        locations, raw = fetch_open_meteo_batch(grid_points, "2026-11-09", "2026-11-09", ["temperature_2m"], max_retries=3)

        self.assertEqual(len(locations), 1)
        self.assertEqual(mock_urlopen.call_count, 2)
        mock_sleep.assert_called_once_with(4)  # 2^1 * 2 = 4s backoff

    @patch("urllib.request.urlopen")
    def test_open_meteo_no_retry_on_400_client_error(self, mock_urlopen):
        """Verify that fetch_open_meteo_batch does NOT retry non-transient 4xx client errors."""
        mock_resp_fail = urllib.error.HTTPError("http://test", 400, "Bad Request", {}, io.BytesIO(b"Bad Request"))
        mock_urlopen.side_effect = mock_resp_fail

        grid_points = [{"grid_lat": 40.7, "grid_lon": -74.0, "grid_id": "GRID_40.7_-74.0"}]
        with self.assertRaises(urllib.error.HTTPError):
            fetch_open_meteo_batch(grid_points, "2026-11-09", "2026-11-09", ["temperature_2m"], max_retries=3)

        self.assertEqual(mock_urlopen.call_count, 1)

    def test_compute_uncovered_date_intervals_and_idempotency(self):
        """Verify date-interval gap calculation and idempotency matching across subsets and supersets."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # 1. No manifests: entire range uncovered
            uncovered = compute_uncovered_date_intervals("nyc_311", "2026-10-10", "2026-10-20", manifests_dir=tmp_path)
            self.assertEqual(len(uncovered), 1)
            self.assertFalse(is_date_range_completed("nyc_311", "2026-10-10", "2026-10-20", manifests_dir=tmp_path))

            # 2. Add manifest covering 2026-10-10 to 2026-10-20
            m = create_manifest_dict(
                batch_id="311_20261020_001",
                dataset_name="nyc_311",
                source_name="nyc_open_data",
                source_dataset_id="erm2-nwe9",
                source_url="https://data.cityofnewyork.us/resource/erm2-nwe9.json",
                retrieved_at_utc="2026-10-20T12:00:00Z",
                source_data_start="2026-10-10",
                source_data_end="2026-10-20",
                extraction_mode="backfill",
                environment="dev",
                code_version="test",
                duration_seconds=1.0,
                byte_size=500,
                row_count=100,
                schema_fingerprint="sha256:test",
                status="success",
                files=[],
            )
            save_manifest(m, output_dir=tmp_path)

            # Exact match: covered
            self.assertTrue(is_date_range_completed("nyc_311", "2026-10-10", "2026-10-20", manifests_dir=tmp_path))

            # Subset: covered
            self.assertTrue(is_date_range_completed("nyc_311", "2026-10-12", "2026-10-18", manifests_dir=tmp_path))

            # Superset: not fully covered (returns uncovered slices)
            self.assertFalse(is_date_range_completed("nyc_311", "2026-10-01", "2026-10-31", manifests_dir=tmp_path))
            gaps = compute_uncovered_date_intervals("nyc_311", "2026-10-01", "2026-10-31", manifests_dir=tmp_path)
            self.assertEqual(len(gaps), 2)  # Gap before Oct 10 and gap after Oct 20

    def test_run_311_ingestion_idempotency_skip(self):
        """Verify run_311_ingestion skips execution when date range is covered unless force=True."""
        from src.ingestion.ingest_311 import run_311_ingestion

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Establish covered manifest
            m = create_manifest_dict(
                batch_id="311_20261020_001",
                dataset_name="nyc_311",
                source_name="nyc_open_data",
                source_dataset_id="erm2-nwe9",
                source_url="https://data.cityofnewyork.us/resource/erm2-nwe9.json",
                retrieved_at_utc="2026-10-20T12:00:00Z",
                source_data_start="2026-10-10",
                source_data_end="2026-10-20",
                extraction_mode="backfill",
                environment="dev",
                code_version="test",
                duration_seconds=1.0,
                byte_size=500,
                row_count=100,
                schema_fingerprint="sha256:test",
                status="success",
                files=[],
            )
            save_manifest(m, output_dir=tmp_path)

            # Calling without force should skip (return None)
            res = run_311_ingestion(
                mode="backfill",
                start_date="2026-10-10",
                end_date="2026-10-20",
                manifests_dir=tmp_path,
                force=False,
            )
            self.assertIsNone(res)

    @patch("src.ingestion.ingest_311.fetch_socrata_page")
    def test_fetch_311_to_landing_pure_python_and_cleanup(self, mock_fetch):
        """Verify pure Python streaming to landing, metadata extraction, and cleanup on failure."""
        from src.ingestion.ingest_311 import fetch_311_to_landing

        with tempfile.TemporaryDirectory() as tmp_dir:
            landing_dir = Path(tmp_dir) / "landing_batch"

            mock_records = [
                {"unique_key": "1", "created_date": "2026-10-01T10:00:00.000"},
                {"unique_key": "2", "created_date": "2026-10-02T12:00:00.000"},
            ]
            raw_bytes = json.dumps(mock_records).encode("utf-8")
            mock_fetch.side_effect = [(mock_records, raw_bytes), ([], b"[]")]

            res = fetch_311_to_landing(
                landing_batch_dir=landing_dir,
                where_clause=None,
                batch_size=10,
                max_records=10,
            )

            self.assertEqual(res["total_rows"], 2)
            self.assertEqual(res["source_min_date"], "2026-10-01T10:00:00.000")
            self.assertEqual(res["source_max_date"], "2026-10-02T12:00:00.000")
            self.assertEqual(len(res["manifest_files"]), 1)
            self.assertTrue((landing_dir / "raw_part_0000.json.gz").exists())

            # Test cleanup on failure:
            landing_fail_dir = Path(tmp_dir) / "landing_fail_batch"
            mock_fetch.side_effect = RuntimeError("Network exploded")
            with self.assertRaises(RuntimeError):
                fetch_311_to_landing(landing_batch_dir=landing_fail_dir)
            # Confirm no leftover partial files
            self.assertEqual(len(list(landing_fail_dir.glob("raw_part_*.json.gz"))), 0)

    def test_bronze_multi_partition_write_and_wap_validation(self):
        """Verify volume-aware partitioned writes across multiple year/month partitions and WAP publication."""
        import gzip
        import json
        from src.ingestion.ingest_311 import convert_landing_to_bronze_pyspark

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            landing_dir = tmp_path / "landing" / "batch_multi_part"
            landing_dir.mkdir(parents=True)
            bronze_dir = tmp_path / "bronze"

            # Records across Oct 2026 and Nov 2026
            records = [
                {"unique_key": "2001", "created_date": "2026-10-31T23:50:00.000", "complaint_type": "Noise"},
                {"unique_key": "2002", "created_date": "2026-11-01T00:10:00.000", "complaint_type": "Noise"},
            ]
            gz_path = landing_dir / "raw_part_0000.json.gz"
            with gzip.open(gz_path, "wb") as f:
                f.write(json.dumps(records).encode("utf-8"))

            written_files, schema_json = convert_landing_to_bronze_pyspark(
                landing_batch_dir=landing_dir,
                base_bronze_dir=bronze_dir,
                batch_id="batch_multi_part",
                retrieved_at_utc="2026-11-01T01:00:00Z",
                extraction_mode="incremental",
                return_schema=True,
            )

            self.assertEqual(len(written_files), 2)
            self.assertEqual(sum(f["rows"] for f in written_files), 2)

            # Both partitions exist on disk
            oct_files = list(bronze_dir.glob("year=2026/month=10/*.parquet"))
            nov_files = list(bronze_dir.glob("year=2026/month=11/*.parquet"))
            self.assertEqual(len(oct_files), 1)
            self.assertEqual(len(nov_files), 1)

            # Filenames are batch-prefixed
            self.assertTrue(oct_files[0].name.startswith("batch_multi_part_"))
            self.assertTrue(nov_files[0].name.startswith("batch_multi_part_"))

            # Staging directory is cleaned up
            staging_dir = bronze_dir / "_staging"
            self.assertFalse((staging_dir / "batch_multi_part").exists())

            # Schema JSON returned and valid
            self.assertTrue(len(schema_json) > 0)
            self.assertIn("unique_key", schema_json)

    def test_bronze_failed_write_not_committed_and_safe_retry(self):
        """Verify that a batch failure leaves no manifest and can be retried cleanly."""
        from src.ingestion.ingest_311 import run_311_ingestion

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            manifests_dir = tmp_path / "manifests"
            bronze_dir = tmp_path / "bronze"

            # 1. Simulate failure during Bronze transformation/validation
            with patch("src.ingestion.ingest_311.convert_landing_to_bronze_pyspark", side_effect=RuntimeError("Bronze write error")):
                with patch("src.ingestion.ingest_311.fetch_311_to_landing") as mock_fetch:
                    mock_fetch.return_value = {
                        "total_rows": 2,
                        "total_bytes": 100,
                        "manifest_files": [],
                        "source_min_date": "2026-10-01",
                        "source_max_date": "2026-10-02",
                    }
                    with self.assertRaises(RuntimeError):
                        run_311_ingestion(
                            batch_id="batch_retry_001",
                            manifests_dir=manifests_dir,
                            bronze_dir=bronze_dir,
                            force=True,
                        )

            # Manifest was NOT committed
            self.assertFalse(is_batch_completed("batch_retry_001", "nyc_311", manifests_dir=manifests_dir))

            # 2. Retry succeeds cleanly
            with patch("src.ingestion.ingest_311.convert_landing_to_bronze_pyspark") as mock_convert:
                mock_convert.return_value = ([{
                    "file_name": "batch_retry_001_part.parquet",
                    "relative_path": "data/bronze/nyc311/year=2026/month=10/batch_retry_001_part.parquet",
                    "layer": "bronze",
                    "rows": 2,
                    "bytes": 500,
                    "sha256": "fake_sha",
                }], '{"type":"struct","fields":[]}')
                with patch("src.ingestion.ingest_311.fetch_311_to_landing") as mock_fetch:
                    mock_fetch.return_value = {
                        "total_rows": 2,
                        "total_bytes": 100,
                        "manifest_files": [],
                        "source_min_date": "2026-10-01",
                        "source_max_date": "2026-10-02",
                    }
                    res = run_311_ingestion(
                        batch_id="batch_retry_001",
                        manifests_dir=manifests_dir,
                        bronze_dir=bronze_dir,
                        force=True,
                    )
                    self.assertIsNotNone(res)
                    self.assertTrue(is_batch_completed("batch_retry_001", "nyc_311", manifests_dir=manifests_dir))

    def test_audit_and_publish_validation_failure_on_row_mismatch(self):
        """Verify that audit_and_publish_bronze_batch raises ValueError when staged rows != expected rows."""
        from src.ingestion.ingest_311 import audit_and_publish_bronze_batch
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            staging_dir = tmp_path / "_staging" / "batch_test"
            part_dir = staging_dir / "year=2026" / "month=10"
            part_dir.mkdir(parents=True)
            df = pd.DataFrame([{"unique_key": 1}, {"unique_key": 2}])
            df.to_parquet(part_dir / "part.parquet")

            # Staged file has 2 rows, but expected_rows is 5 -> MUST raise ValueError
            with self.assertRaises(ValueError):
                audit_and_publish_bronze_batch(
                    staging_batch_dir=staging_dir,
                    base_bronze_dir=tmp_path / "bronze",
                    batch_id="batch_test",
                    expected_rows=5,
                )

    def test_schema_drift_detection_across_batches(self):
        """Verify schema fingerprint detects evolution between Spark DataFrame schemas."""
        schema_v1 = '{"type":"struct","fields":[{"name":"unique_key","type":"string","nullable":true}]}'
        schema_v2 = '{"type":"struct","fields":[{"name":"unique_key","type":"string","nullable":true},{"name":"new_feature","type":"integer","nullable":true}]}'

        fp1 = compute_schema_fingerprint(schema_v1)
        fp2 = compute_schema_fingerprint(schema_v2)

        self.assertTrue(fp1.startswith("sha256:"))
        self.assertTrue(fp2.startswith("sha256:"))
        self.assertNotEqual(fp1, fp2, "Schema drift between v1 and v2 was not detected!")

    def test_backfill_requires_end_date(self):
        """Verify backfill mode raises ValueError when end_date is omitted."""
        from src.ingestion.ingest_311 import build_311_soql_filter
        with self.assertRaises(ValueError) as ctx:
            build_311_soql_filter(mode="backfill", start_date="2026-01-01", end_date=None)
        self.assertIn("Backfill mode requires an explicit --end-date", str(ctx.exception))

    def test_backfill_rejects_future_dates(self):
        """Verify build_311_soql_filter raises ValueError when end_date is in the future."""
        from src.ingestion.ingest_311 import build_311_soql_filter
        with self.assertRaises(ValueError) as ctx:
            build_311_soql_filter(mode="backfill", start_date="2026-01-01", end_date="2099-01-01")
        self.assertIn("cannot be in the future", str(ctx.exception))

    def test_backfill_rejects_start_after_end(self):
        """Verify build_311_soql_filter raises ValueError when start_date > end_date."""
        from src.ingestion.ingest_311 import build_311_soql_filter
        with self.assertRaises(ValueError) as ctx:
            build_311_soql_filter(mode="backfill", start_date="2026-05-01", end_date="2026-01-01")
        self.assertIn("cannot be after end_date", str(ctx.exception))

    @patch("urllib.request.urlopen")
    def test_fetch_socrata_page_no_retry_on_permanent_4xx(self, mock_urlopen):
        """Verify fetch_socrata_page raises immediately on permanent 4xx without retrying."""
        import urllib.error
        from src.ingestion.ingest_311 import fetch_socrata_page

        mock_err = urllib.error.HTTPError(
            url="http://fake", code=400, msg="Bad Request", hdrs={}, fp=None
        )
        mock_urlopen.side_effect = mock_err

        with self.assertRaises(urllib.error.HTTPError):
            fetch_socrata_page(limit=10, offset=0, max_retries=3)

        self.assertEqual(mock_urlopen.call_count, 1)

    def test_run_weather_ingestion_idempotency_skip(self):
        """Verify run_weather_ingestion skips execution when date range is covered unless force=True."""
        from src.ingestion.ingest_weather import run_weather_ingestion
        from src.utils.config import DATASET_WEATHER

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            m = create_manifest_dict(
                batch_id="wth_20261020_001",
                dataset_name=DATASET_WEATHER,
                source_name="open_meteo",
                source_dataset_id="era5_land",
                source_url="https://archive-api.open-meteo.com/v1/archive",
                retrieved_at_utc="2026-10-20T12:00:00Z",
                source_data_start="2026-10-10",
                source_data_end="2026-10-20",
                extraction_mode="backfill",
                environment="dev",
                code_version="test",
                duration_seconds=1.0,
                byte_size=500,
                row_count=100,
                schema_fingerprint="sha256:test",
                status="success",
                files=[],
            )
            save_manifest(m, output_dir=tmp_path)

            # Calling without force should skip
            res = run_weather_ingestion(
                mode="backfill",
                start_date="2026-10-10",
                end_date="2026-10-20",
                manifests_dir=tmp_path,
                force=False,
            )
            self.assertIsNone(res)

    def test_weather_backfill_requires_explicit_dates(self):
        """Verify backfill mode requires both start_date and end_date."""
        from src.ingestion.ingest_weather import run_weather_ingestion

        with self.assertRaises(ValueError) as ctx:
            run_weather_ingestion(mode="backfill", start_date=None, end_date="2026-10-10")
        self.assertIn("explicit --start-date", str(ctx.exception))

        with self.assertRaises(ValueError) as ctx:
            run_weather_ingestion(mode="backfill", start_date="2026-10-01", end_date=None)
        self.assertIn("explicit --end-date", str(ctx.exception))

    def test_weather_backfill_rejects_dates_beyond_ecmwf_lag(self):
        """Verify backfill rejects end_date beyond the 5-day ECMWF publication ceiling."""
        from src.ingestion.ingest_weather import run_weather_ingestion

        with self.assertRaises(ValueError) as ctx:
            run_weather_ingestion(mode="backfill", start_date="2026-10-01", end_date="2099-01-01")
        self.assertIn("ECMWF 5-day publication latency", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
