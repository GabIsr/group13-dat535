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
    create_manifest_dict,
    generate_batch_id,
    get_latest_watermark,
    is_batch_completed,
    save_manifest,
)
from src.ingestion.ingest_311 import compute_row_hashes


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
        """Ensure all required core meteorological variables are configured."""
        required = ["temperature_2m", "precipitation", "wind_speed_10m", "relative_humidity_2m"]
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

    def test_row_hashing_determinism(self):
        """Verify that row hashing generates identical hashes for identical payloads."""
        df1 = pd.DataFrame([{"id": 1, "val": "abc"}, {"id": 2, "val": "def"}])
        df2 = pd.DataFrame([{"val": "abc", "id": 1}, {"val": "def", "id": 2}])

        h1 = compute_row_hashes(df1)
        h2 = compute_row_hashes(df2)
        self.assertEqual(h1, h2)
        self.assertEqual(len(h1), 2)
        self.assertEqual(len(h1[0]), 64)  # SHA-256 hex length

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
            # Watermark should now be 2026-10-10
            self.assertEqual(get_latest_watermark("nyc_311", manifests_dir=tmp_path), "2026-10-10")

            # Batch 2 sequence should increment
            b2_id = generate_batch_id("311", date_str="20261023", manifests_dir=tmp_path)
            self.assertTrue(b2_id.endswith("_002"))


if __name__ == "__main__":
    unittest.main()
