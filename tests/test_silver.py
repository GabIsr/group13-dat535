"""
Unit & Integration Tests for the Medallion Silver Layer.
Compatible with standard library unittest for headless GitHub Actions CI/CD.
Author: Group 13
"""

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
import numpy as np

from src.silver.config import (
    BOROUGH_CENTROIDS,
    NYC_LAT_MIN,
    NYC_LAT_MAX,
    NYC_LON_MIN,
    NYC_LON_MAX,
    WEATHER_EXTREME_THRESHOLDS,
    WEATHER_HARD_LIMITS,
)
from src.silver.clean_311 import clean_and_validate_311, to_utc_datetime, standardize_string_series
from src.silver.clean_weather import clean_and_validate_weather
from src.silver.join_silver import join_silver_datasets


class TestSilverLayerPipeline(unittest.TestCase):

    def test_coordinate_bounds_validation(self):
        """Verify NYC coordinate bounding box constraints."""
        valid_coords = [(40.7128, -74.0060), (40.7831, -73.9712), (40.5795, -74.1502)]
        invalid_coords = [(0.0, 0.0), (34.0522, -118.2437), (58.9700, 5.7331), (-999.0, -999.0)]

        for lat, lon in valid_coords:
            self.assertTrue(NYC_LAT_MIN <= lat <= NYC_LAT_MAX)
            self.assertTrue(NYC_LON_MIN <= lon <= NYC_LON_MAX)

        for lat, lon in invalid_coords:
            in_bounds = (NYC_LAT_MIN <= lat <= NYC_LAT_MAX) and (NYC_LON_MIN <= lon <= NYC_LON_MAX)
            self.assertFalse(in_bounds)

    def test_weather_physical_limits_and_soft_tagging(self):
        """Verify hard rejection of corrupt sentinels vs. soft tagging of real extreme weather."""
        # 1. Hard limits check
        corrupt_temps = [-999.0, 999.0, -100.0, 75.0]
        for t in corrupt_temps:
            is_valid = WEATHER_HARD_LIMITS["temp_min"] <= t <= WEATHER_HARD_LIMITS["temp_max"]
            self.assertFalse(is_valid, f"Temp {t} should be flagged as corrupt/impossible")

        # 2. Extreme weather tagging check (NYC all-time cold/heat should be preserved)
        extreme_cold = -26.0  # Cold record
        extreme_heat = 41.0   # Heat record
        torrential_rain = 30.0# Flash flood rain rate

        self.assertTrue(extreme_cold < WEATHER_EXTREME_THRESHOLDS["temp_extreme_cold"])
        self.assertTrue(extreme_heat > WEATHER_EXTREME_THRESHOLDS["temp_extreme_heat"])
        self.assertTrue(torrential_rain >= WEATHER_EXTREME_THRESHOLDS["precip_torrential_hr"])

    def test_temporal_truncation_for_weather_join(self):
        """Verify that 311 complaint timestamps truncate cleanly to hourly join keys."""
        created_dt = pd.to_datetime("2026-10-18 14:43:21", utc=True)
        expected_join_hour = pd.to_datetime("2026-10-18 14:00:00", utc=True)

        truncated_hour = created_dt.floor("h")
        self.assertEqual(truncated_hour, expected_join_hour)

    def test_resolution_duration_and_negative_duration_detection(self):
        """Verify resolution duration calculation and detection of invalid negative durations."""
        # Case A: Valid duration
        c1 = pd.to_datetime("2026-10-18 10:00:00", utc=True)
        cl1 = pd.to_datetime("2026-10-18 14:30:00", utc=True)
        duration1 = (cl1 - c1).total_seconds() / 3600.0
        self.assertEqual(duration1, 4.5)
        self.assertTrue(duration1 >= 0.0)

        # Case B: Negative duration (closed before created)
        c2 = pd.to_datetime("2026-10-18 16:00:00", utc=True)
        cl2 = pd.to_datetime("2026-10-18 12:00:00", utc=True)
        duration2 = (cl2 - c2).total_seconds() / 3600.0
        self.assertTrue(duration2 < 0.0)

    def test_hierarchical_spatial_fallback(self):
        """Verify that the 3-level fallback correctly assigns grid_id."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Create mock Bronze records covering all 4 resolution levels
            mock_records = [
                # Row 1: Valid exact coordinates (Manhattan)
                {
                    "unique_key": "1001",
                    "created_date": "2026-10-18T14:15:00.000",
                    "closed_date": "2026-10-18T18:15:00.000",
                    "complaint_type": "Noise",
                    "descriptor": "Loud Music",
                    "borough": "MANHATTAN",
                    "incident_zip": "10001",
                    "latitude": "40.7500",
                    "longitude": "-73.9900",
                    "_batch_id": "test_batch",
                    "_ingested_at_utc": "2026-10-23T10:00:00Z",
                    "_source_file": "test.json.gz",
                },
                # Row 2: Missing coordinates, valid ZIP 10001 (should fall back to ZIP)
                {
                    "unique_key": "1002",
                    "created_date": "2026-10-18T14:20:00.000",
                    "closed_date": "2026-10-18T16:20:00.000",
                    "complaint_type": "Heat/Hot Water",
                    "descriptor": "No Heat",
                    "borough": "MANHATTAN",
                    "incident_zip": "10001",
                    "latitude": None,
                    "longitude": None,
                    "_batch_id": "test_batch",
                    "_ingested_at_utc": "2026-10-23T10:00:00Z",
                    "_source_file": "test.json.gz",
                },
                # Row 3: Missing coordinates, invalid ZIP, valid Borough (fallback to Borough)
                {
                    "unique_key": "1003",
                    "created_date": "2026-10-18T14:30:00.000",
                    "closed_date": "2026-10-18T17:30:00.000",
                    "complaint_type": "Pothole",
                    "descriptor": "Cavity",
                    "borough": "BROOKLYN",
                    "incident_zip": "00000",
                    "latitude": None,
                    "longitude": None,
                    "_batch_id": "test_batch",
                    "_ingested_at_utc": "2026-10-23T10:00:00Z",
                    "_source_file": "test.json.gz",
                },
                # Row 4: Negative duration (should be quarantined)
                {
                    "unique_key": "1004",
                    "created_date": "2026-10-18T18:00:00.000",
                    "closed_date": "2026-10-18T12:00:00.000",  # closed before created
                    "complaint_type": "Illegal Parking",
                    "descriptor": "Blocked Hydrant",
                    "borough": "QUEENS",
                    "incident_zip": "11101",
                    "latitude": "40.7500",
                    "longitude": "-73.9400",
                    "_batch_id": "test_batch",
                    "_ingested_at_utc": "2026-10-23T10:00:00Z",
                    "_source_file": "test.json.gz",
                },
            ]

            bronze_in = tmp_path / "bronze"
            bronze_in.mkdir()
            df_mock = pd.DataFrame(mock_records)
            df_mock.to_parquet(bronze_in / "part_0000.parquet", index=False)

            silver_out = tmp_path / "silver"
            quar_out = tmp_path / "quarantine"

            res = clean_and_validate_311(
                bronze_311_dir=bronze_in,
                silver_311_dir=silver_out,
                quarantine_dir=quar_out,
            )

            self.assertEqual(res["input_rows"], 4)
            self.assertEqual(res["quarantined_rows"], 1)  # 1 row with negative duration quarantined
            self.assertEqual(res["valid_rows_accepted"], 3)

            # Inspect clean outputs
            clean_files = list(silver_out.glob("**/*.parquet"))
            self.assertTrue(len(clean_files) > 0)
            df_clean = pd.concat([pd.read_parquet(f) for f in clean_files])

            row1 = df_clean[df_clean["unique_key"] == "1001"].iloc[0]
            self.assertEqual(row1["spatial_resolution_level"], "EXACT_COORDINATES")
            self.assertEqual(row1["grid_id"], "GRID_40.8_-74.0")

            row2 = df_clean[df_clean["unique_key"] == "1002"].iloc[0]
            self.assertEqual(row2["spatial_resolution_level"], "ZIP_APPROXIMATION")
            self.assertTrue(row2["grid_id"].startswith("GRID_"))

            row3 = df_clean[df_clean["unique_key"] == "1003"].iloc[0]
            self.assertEqual(row3["spatial_resolution_level"], "BOROUGH_APPROXIMATION")
            self.assertEqual(row3["grid_id"], BOROUGH_CENTROIDS["BROOKLYN"][2])

    def test_timezone_localization_eastern_to_utc(self):
        """Verify proper 4-hour (EDT) and 5-hour (EST) conversion to UTC."""
        # EDT (Daylight Saving Time: UTC-4) -> 14:00 local should be 18:00 UTC
        edt_local = pd.Series(["2026-07-15T14:00:00.000"])
        edt_utc = to_utc_datetime(edt_local).iloc[0]
        expected_edt_utc = pd.Timestamp("2026-07-15 18:00:00", tz="UTC")
        self.assertEqual(edt_utc, expected_edt_utc)

        # EST (Standard Time: UTC-5) -> 14:00 local should be 19:00 UTC
        est_local = pd.Series(["2026-01-15T14:00:00.000"])
        est_utc = to_utc_datetime(est_local).iloc[0]
        expected_est_utc = pd.Timestamp("2026-01-15 19:00:00", tz="UTC")
        self.assertEqual(est_utc, expected_est_utc)

    def test_open_ticket_semantics_and_age(self):
        """Verify open vs closed ticket flags, duration calculation, and active age tracking."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            mock_records = [
                # Closed ticket: created 2 hours ago, closed 1 hour ago
                {
                    "unique_key": "2001",
                    "created_date": "2026-10-18T10:00:00.000",
                    "closed_date": "2026-10-18T11:30:00.000",
                    "complaint_type": "Noise",
                    "borough": "MANHATTAN",
                    "latitude": "40.7500",
                    "longitude": "-73.9900",
                },
                # Open ticket: created recently, closed_date is None
                {
                    "unique_key": "2002",
                    "created_date": "2026-10-18T12:00:00.000",
                    "closed_date": None,
                    "complaint_type": "Illegal Parking",
                    "borough": "QUEENS",
                    "latitude": "40.7500",
                    "longitude": "-73.9400",
                },
            ]
            bronze_in = tmp_path / "bronze"
            bronze_in.mkdir()
            pd.DataFrame(mock_records).to_parquet(bronze_in / "part_0000.parquet", index=False)

            silver_out = tmp_path / "silver"
            quar_out = tmp_path / "quarantine"

            res = clean_and_validate_311(bronze_in, silver_out, quar_out)
            self.assertEqual(res["valid_rows_accepted"], 2)
            self.assertEqual(res["closed_tickets_count"], 1)
            self.assertEqual(res["open_tickets_count"], 1)

            clean_files = list(silver_out.glob("**/*.parquet"))
            df_res = pd.concat([pd.read_parquet(f) for f in clean_files])

            row_closed = df_res[df_res["unique_key"] == "2001"].iloc[0]
            self.assertTrue(row_closed["is_ticket_closed"])
            self.assertEqual(row_closed["ticket_status_category"], "CLOSED")
            self.assertEqual(row_closed["resolution_duration_hours"], 1.5)
            self.assertTrue(pd.isna(row_closed["open_ticket_age_days"]))

            row_open = df_res[df_res["unique_key"] == "2002"].iloc[0]
            self.assertFalse(row_open["is_ticket_closed"])
            self.assertEqual(row_open["ticket_status_category"], "OPEN")
            self.assertTrue(pd.isna(row_open["resolution_duration_hours"]))
            self.assertTrue(row_open["open_ticket_age_days"] > 0)

    def test_cumulative_and_lagged_weather_features(self):
        """Verify rolling precipitation and wind gust window aggregations in Silver weather."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            mock_weather = [
                {
                    "grid_id": "GRID_40.7_-74.0",
                    "time": f"2026-10-18T{h:02d}:00",
                    "temperature_2m": 15.0 - h,
                    "precipitation": 2.0,  # 2mm each hour
                    "surface_pressure": 1013.0,
                    "wind_speed_10m": 5.0,
                    "wind_gusts_10m": 10.0 + h,
                }
                for h in range(6)
            ]
            bronze_in = tmp_path / "bronze_weather"
            bronze_in.mkdir()
            pd.DataFrame(mock_weather).to_parquet(bronze_in / "part_0000.parquet", index=False)

            silver_out = tmp_path / "silver_weather"
            res = clean_and_validate_weather(bronze_in, silver_out)
            self.assertEqual(res["valid_rows"], 6)

            clean_files = list(silver_out.glob("**/*.parquet"))
            df_w = pd.concat([pd.read_parquet(f) for f in clean_files]).sort_values("timestamp_utc").reset_index(drop=True)

            # Hour 0: 3h sum = 2.0 (only 1 period available)
            self.assertEqual(df_w.loc[0, "precip_past_3h_mm"], 2.0)
            # Hour 2: 3h sum = 2.0 + 2.0 + 2.0 = 6.0
            self.assertEqual(df_w.loc[2, "precip_past_3h_mm"], 6.0)
            # Hour 5: max gust in past 6 hours should be 10 + 5 = 15.0
            self.assertEqual(df_w.loc[5, "max_wind_gust_past_6h_ms"], 15.0)


    def test_string_standardization_null_handling(self):
        """Verify that nulls, empty strings, and stringified null artifacts do not become literal 'NAN'/'NONE'."""
        test_inputs = pd.Series([None, np.nan, "", "   ", "nan", "None", "brooklyn", "Noise - residential"])

        res_upper = standardize_string_series(test_inputs, "UNSPECIFIED", case="upper")
        self.assertEqual(res_upper.iloc[0], "UNSPECIFIED")
        self.assertEqual(res_upper.iloc[1], "UNSPECIFIED")
        self.assertEqual(res_upper.iloc[2], "UNSPECIFIED")
        self.assertEqual(res_upper.iloc[3], "UNSPECIFIED")
        self.assertEqual(res_upper.iloc[4], "UNSPECIFIED")
        self.assertEqual(res_upper.iloc[5], "UNSPECIFIED")
        self.assertEqual(res_upper.iloc[6], "BROOKLYN")

        res_title = standardize_string_series(test_inputs, "Unknown", case="title")
        self.assertEqual(res_title.iloc[0], "Unknown")
        self.assertEqual(res_title.iloc[4], "Unknown")
        self.assertEqual(res_title.iloc[7], "Noise - Residential")


if __name__ == "__main__":
    unittest.main()
