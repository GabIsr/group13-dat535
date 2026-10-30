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
from src.silver.clean_311 import clean_and_validate_311, to_utc_datetime, standardize_string_series, haversine_distance_km, spark_haversine_distance_km
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

            row1 = df_clean[df_clean["unique_key"] == 1001].iloc[0]
            self.assertEqual(row1["spatial_resolution_level"], "EXACT_COORDINATES")
            self.assertEqual(row1["grid_id"], "GRID_40.8_-74.0")
            self.assertEqual(row1["clean_incident_zip"], "10001")

            row2 = df_clean[df_clean["unique_key"] == 1002].iloc[0]
            self.assertEqual(row2["spatial_resolution_level"], "ZIP_APPROXIMATION")
            self.assertTrue(row2["grid_id"].startswith("GRID_"))
            self.assertEqual(row2["clean_incident_zip"], "10001")

            row3 = df_clean[df_clean["unique_key"] == 1003].iloc[0]
            self.assertEqual(row3["spatial_resolution_level"], "BOROUGH_APPROXIMATION")
            self.assertEqual(row3["grid_id"], BOROUGH_CENTROIDS["BROOKLYN"][2])
            self.assertEqual(row3["clean_incident_zip"], "00000")

            # Verify spatial distance metadata and schema types
            self.assertEqual(str(df_clean["unique_key"].dtype), "Int64")
            self.assertTrue(pd.api.types.is_float_dtype(df_clean["latitude"]))
            self.assertTrue(pd.api.types.is_float_dtype(df_clean["longitude"]))
            self.assertTrue(pd.api.types.is_float_dtype(df_clean["grid_distance_km"]))
            self.assertTrue(row1["grid_distance_km"] > 0.0)
            self.assertTrue(row2["grid_distance_km"] > 0.0)
            self.assertTrue(row3["grid_distance_km"] > 0.0)
            self.assertTrue(res["avg_grid_distance_km"] > 0.0)

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

            row_closed = df_res[df_res["unique_key"] == 2001].iloc[0]
            self.assertTrue(row_closed["is_ticket_closed"])
            self.assertEqual(row_closed["ticket_status_category"], "CLOSED")
            self.assertEqual(row_closed["resolution_duration_hours"], 1.5)
            self.assertTrue(pd.isna(row_closed["open_ticket_age_days"]))

            row_open = df_res[df_res["unique_key"] == 2002].iloc[0]
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


    def test_invalid_unique_key_quarantine(self):
        """Verify that non-numeric or missing unique_keys are safely routed to quarantine without crashing."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            mock_records = [
                {
                    "unique_key": "INVALID_TEXT_KEY",
                    "created_date": "2026-10-18T10:00:00.000",
                    "closed_date": "2026-10-18T11:30:00.000",
                    "complaint_type": "Noise",
                    "borough": "MANHATTAN",
                    "latitude": "40.7500",
                    "longitude": "-73.9900",
                },
                {
                    "unique_key": "99999",
                    "created_date": "2026-10-18T10:00:00.000",
                    "closed_date": "2026-10-18T11:30:00.000",
                    "complaint_type": "Noise",
                    "borough": "MANHATTAN",
                    "latitude": "40.7500",
                    "longitude": "-73.9900",
                },
            ]
            bronze_in = tmp_path / "bronze"
            bronze_in.mkdir()
            pd.DataFrame(mock_records).to_parquet(bronze_in / "part_0000.parquet", index=False)

            silver_out = tmp_path / "silver"
            quar_out = tmp_path / "quarantine"

            res = clean_and_validate_311(bronze_in, silver_out, quar_out)
            self.assertEqual(res["input_rows"], 2)
            self.assertEqual(res["quarantined_rows"], 1)
            self.assertEqual(res["valid_rows_accepted"], 1)

            df_quar = pd.read_parquet(quar_out / "quarantined_311.parquet")
            self.assertEqual(df_quar.iloc[0]["quarantine_reason"], "INVALID_OR_NON_NUMERIC_UNIQUE_KEY")




    def test_weather_deduplication(self):
        """Verify that duplicate (grid_id, timestamp_utc) records in Bronze weather are deduplicated, keeping latest ingestion."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            mock_records = [
                # Older duplicate ingestion
                {
                    "grid_id": "GRID_40.7_-74.0",
                    "time": "2026-10-18T10:00",
                    "temperature_2m": 12.0,
                    "precipitation": 0.0,
                    "surface_pressure": 1013.0,
                    "wind_speed_10m": 5.0,
                    "wind_gusts_10m": 10.0,
                    "_ingested_at_utc": "2026-10-18T10:00:00Z",
                },
                # Newer duplicate ingestion (should overwrite/be kept)
                {
                    "grid_id": "GRID_40.7_-74.0",
                    "time": "2026-10-18T10:00",
                    "temperature_2m": 18.0,
                    "precipitation": 1.0,
                    "surface_pressure": 1013.0,
                    "wind_speed_10m": 5.0,
                    "wind_gusts_10m": 12.0,
                    "_ingested_at_utc": "2026-10-18T12:00:00Z",
                },
                # Distinct hour
                {
                    "grid_id": "GRID_40.7_-74.0",
                    "time": "2026-10-18T11:00",
                    "temperature_2m": 19.0,
                    "precipitation": 2.0,
                    "surface_pressure": 1013.0,
                    "wind_speed_10m": 6.0,
                    "wind_gusts_10m": 15.0,
                    "_ingested_at_utc": "2026-10-18T12:00:00Z",
                },
            ]
            bronze_in = tmp_path / "bronze_weather"
            bronze_in.mkdir()
            pd.DataFrame(mock_records).to_parquet(bronze_in / "part_0000.parquet", index=False)

            silver_out = tmp_path / "silver_weather"
            res = clean_and_validate_weather(bronze_in, silver_out)

            self.assertEqual(res["input_rows"], 3)
            self.assertEqual(res["duplicate_rows_dropped"], 1)
            self.assertEqual(res["valid_rows"], 2)

            clean_files = list(silver_out.glob("**/*.parquet"))
            df_w = pd.concat([pd.read_parquet(f) for f in clean_files]).sort_values("timestamp_utc").reset_index(drop=True)
            self.assertEqual(len(df_w), 2)
            # Verify latest ingestion kept (temperature_2m == 18.0)
            row_10 = df_w[df_w["timestamp_utc"] == pd.Timestamp("2026-10-18 10:00:00", tz="UTC")].iloc[0]
            self.assertEqual(row_10["temperature_2m"], 18.0)
            self.assertEqual(row_10["precip_past_3h_mm"], 1.0)
            row_11 = df_w[df_w["timestamp_utc"] == pd.Timestamp("2026-10-18 11:00:00", tz="UTC")].iloc[0]
            self.assertEqual(row_11["precip_past_3h_mm"], 3.0)

    def test_cartesian_join_explosion_prevention(self):
        """Verify that join_silver_datasets detects and rejects 1-to-many Cartesian multiplication."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            s_311 = tmp_path / "silver_311"
            s_weather = tmp_path / "silver_weather"
            s_joined = tmp_path / "silver_joined"
            s_311.mkdir()
            s_weather.mkdir()

            mock_311 = [
                {
                    "unique_key": 99001,
                    "created_at_utc": pd.Timestamp("2026-10-18 10:15:00", tz="UTC"),
                    "weather_join_hour_utc": pd.Timestamp("2026-10-18 10:00:00", tz="UTC"),
                    "grid_id": "GRID_40.7_-74.0",
                    "complaint_type": "Noise",
                }
            ]
            pd.DataFrame(mock_311).to_parquet(s_311 / "part_0000.parquet", index=False)

            # Intentionally duplicate weather dimension row
            mock_weather = [
                {
                    "grid_id": "GRID_40.7_-74.0",
                    "timestamp_utc": pd.Timestamp("2026-10-18 10:00:00", tz="UTC"),
                    "temperature_2m": 15.0,
                },
                {
                    "grid_id": "GRID_40.7_-74.0",
                    "timestamp_utc": pd.Timestamp("2026-10-18 10:00:00", tz="UTC"),
                    "temperature_2m": 15.0,
                },
            ]
            pd.DataFrame(mock_weather).to_parquet(s_weather / "part_0000.parquet", index=False)

            with self.assertRaises(ValueError) as ctx:
                join_silver_datasets(s_311, s_weather, s_joined)

            self.assertIn("Cartesian join explosion detected", str(ctx.exception))



    def test_sensor_anomaly_ledger_logging_and_quality_notes(self):
        """Verify atmospheric physical law enforcement, masking to NaN, and ledgering in sensor_anomaly_ledger."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            bronze_in = tmp_path / 'bronze_weather'
            silver_out = tmp_path / 'silver_weather'
            ledger_file = tmp_path / 'quality' / 'sensor_anomaly_ledger.parquet'
            bronze_in.mkdir()

            mock_weather = [
                # Row 1: Dew point physically exceeds ambient temperature (22°C > 18°C)
                {
                    'grid_id': 'GRID_40.7_-74.0',
                    'time': '2026-10-18T10:00',
                    'temperature_2m': 18.0,
                    'dew_point_2m': 22.0,  # Physically impossible (supersaturation)
                    'precipitation': 0.0,
                    'relative_humidity_2m': 85,
                    'wind_speed_10m': 5.0,
                    'wind_gusts_10m': 10.0,
                    'surface_pressure': 1013.0,
                    'weather_code': 0,
                },
                # Row 2: Wind gust is slower than sustained wind speed (3.0 m/s < 8.0 m/s)
                {
                    'grid_id': 'GRID_40.7_-74.0',
                    'time': '2026-10-18T11:00',
                    'temperature_2m': 19.0,
                    'dew_point_2m': 12.0,
                    'precipitation': 0.0,
                    'relative_humidity_2m': 115,  # Physically impossible (>100%)
                    'wind_speed_10m': 8.0,
                    'wind_gusts_10m': 3.0,  # Aerodynamically impossible
                    'surface_pressure': 1013.0,
                    'weather_code': 0,
                },
            ]
            pd.DataFrame(mock_weather).to_parquet(bronze_in / 'part_0000.parquet', index=False)

            res = clean_and_validate_weather(
                bronze_weather_dir=bronze_in,
                silver_weather_dir=silver_out,
                anomaly_ledger_path=ledger_file,
            )

            self.assertEqual(res['valid_rows'], 2)
            self.assertEqual(res['sensor_anomalies_logged'], 3)  # dew_point, humidity, wind_gust

            # Verify ledger file exists and contains the 3 anomalies
            self.assertTrue(ledger_file.exists())
            df_ledger = pd.read_parquet(ledger_file)
            self.assertEqual(len(df_ledger), 3)
            self.assertIn('DEW_POINT_EXCEEDS_TEMPERATURE', df_ledger['rule_violated'].values)
            self.assertIn('GUST_LESS_THAN_SUSTAINED_SPEED', df_ledger['rule_violated'].values)
            self.assertIn('RELATIVE_HUMIDITY_OUT_OF_BOUNDS', df_ledger['rule_violated'].values)

            # Verify Silver dataset has corrupt sensors masked to NaN and audit notes attached
            clean_files = list(silver_out.glob('**/*.parquet'))
            df_w = pd.concat([pd.read_parquet(f) for f in clean_files]).sort_values('timestamp_utc').reset_index(drop=True)

            # Row 1 check: dew point is masked to NaN
            self.assertTrue(np.isnan(df_w.loc[0, 'dew_point_2m']))
            self.assertEqual(df_w.loc[0, 'temperature_2m'], 18.0)
            self.assertIn('DEW_POINT_EXCEEDS_TEMPERATURE', str(df_w.loc[0, 'quality_audit_notes']))

            # Row 2 check: wind gust and humidity are masked
            self.assertTrue(np.isnan(df_w.loc[1, 'wind_gusts_10m']))
            self.assertTrue(pd.isna(df_w.loc[1, 'relative_humidity_2m']))
            self.assertIn('GUST_LESS_THAN_SUSTAINED_SPEED', str(df_w.loc[1, 'quality_audit_notes']))

    def test_unique_key_positive_integer_and_epoch_quarantine(self):
        """Verify that unique_key accepts any positive integer (7 or 8 digits) and quarantines <= 0 or pre-2003 dates."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            bronze_in = tmp_path / 'bronze_311'
            silver_out = tmp_path / 'silver_311'
            quarantine_out = tmp_path / 'quarantine'
            bronze_in.mkdir()

            mock_311 = [
                # Row 1: Valid 7-digit historical key (2004 era)
                {
                    'unique_key': '4705298',
                    'created_date': '2026-10-18T10:00:00.000',
                    'closed_date': '2026-10-18T12:00:00.000',
                    'agency': 'NYPD',
                    'complaint_type': 'Noise',
                    'borough': 'MANHATTAN',
                    'incident_zip': '10001',
                    'status': 'CLOSED',
                },
                # Row 2: Valid 8-digit modern key
                {
                    'unique_key': '70880898',
                    'created_date': '2026-10-18T11:00:00.000',
                    'closed_date': '2026-10-18T11:00:00.000',  # Instant closure (0s)
                    'agency': 'DSNY',
                    'complaint_type': 'Sanitation',
                    'borough': 'BROOKLYN',
                    'incident_zip': '11201',
                    'status': 'CLOSED',
                },
                # Row 3: Invalid non-positive unique_key (0) -> should quarantine
                {
                    'unique_key': '0',
                    'created_date': '2026-10-18T12:00:00.000',
                    'agency': 'DOT',
                    'complaint_type': 'Street Condition',
                    'borough': 'QUEENS',
                    'status': 'OPEN',
                },
                # Row 4: Pre-2003 date (1999 Unix artifact) -> should quarantine
                {
                    'unique_key': '10000001',
                    'created_date': '1999-05-12T08:00:00.000',
                    'agency': 'DOT',
                    'complaint_type': 'Street Condition',
                    'borough': 'QUEENS',
                    'status': 'OPEN',
                },
            ]
            pd.DataFrame(mock_311).to_parquet(bronze_in / 'part_0000.parquet', index=False)

            res = clean_and_validate_311(
                bronze_311_dir=bronze_in,
                silver_311_dir=silver_out,
                quarantine_dir=quarantine_out,
            )

            self.assertEqual(res['valid_rows_accepted'], 2)
            self.assertEqual(res['quarantined_rows'], 2)
            self.assertEqual(res['instant_closure_count'], 1)

            # Check quarantine file reasons
            q_file = quarantine_out / 'quarantined_311.parquet'
            self.assertTrue(q_file.exists())
            df_q = pd.read_parquet(q_file)
            reasons = df_q['quarantine_reason'].tolist()
            self.assertIn('INVALID_OR_NON_NUMERIC_UNIQUE_KEY', reasons)
            self.assertIn('PRE_311_EPOCH_DATE', reasons)

            # Check clean Silver records
            clean_files = list(silver_out.glob('**/*.parquet'))
            df_clean = pd.concat([pd.read_parquet(f) for f in clean_files])
            self.assertIn(4705298, df_clean['unique_key'].tolist())
            self.assertIn(70880898, df_clean['unique_key'].tolist())
            row_instant = df_clean[df_clean['unique_key'] == 70880898].iloc[0]
            self.assertTrue(row_instant['is_instant_closure'])
            self.assertIn('INSTANT_CLOSURE_0SEC', str(row_instant['quality_audit_notes']))



    def test_future_closed_date_and_multi_reason_quarantine_and_reverse_status_conflict(self):
        """Verify future closed_date quarantine, multi-reason quarantine string, and reverse status conflict tagging."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            bronze_in = tmp_path / "bronze_311"
            silver_out = tmp_path / "silver_311"
            quarantine_out = tmp_path / "quarantine"
            bronze_in.mkdir()

            mock_311 = [
                # Row 1: Valid ticket with reverse status conflict (status == CLOSED, but closed_date is None)
                {
                    "unique_key": "50001",
                    "created_date": "2026-10-18T10:00:00.000",
                    "closed_date": None,
                    "agency": "NYPD",
                    "complaint_type": "Noise",
                    "borough": "MANHATTAN",
                    "incident_zip": "10001",
                    "status": "CLOSED",
                },
                # Row 2: Future closed_date (year 2099) -> should quarantine as FUTURE_CLOSED_DATE
                {
                    "unique_key": "50002",
                    "created_date": "2026-10-18T10:00:00.000",
                    "closed_date": "2099-01-01T10:00:00.000",
                    "agency": "DOT",
                    "complaint_type": "Pothole",
                    "borough": "QUEENS",
                    "incident_zip": "11101",
                    "status": "CLOSED",
                },
                # Row 3: Multiple quarantine violations (invalid key <= 0 AND pre-2003 epoch date)
                {
                    "unique_key": "-5",
                    "created_date": "1995-01-01T00:00:00.000",
                    "closed_date": None,
                    "agency": "DSNY",
                    "complaint_type": "Sanitation",
                    "borough": "BROOKLYN",
                    "incident_zip": "11201",
                    "status": "OPEN",
                },
            ]
            pd.DataFrame(mock_311).to_parquet(bronze_in / "part_0000.parquet", index=False)

            res = clean_and_validate_311(bronze_in, silver_out, quarantine_out)
            self.assertEqual(res["valid_rows_accepted"], 1)
            self.assertEqual(res["quarantined_rows"], 2)
            self.assertEqual(res["status_conflict_count"], 1)

            # Verify reverse status conflict in Silver
            clean_files = list(silver_out.glob("**/*.parquet"))
            df_clean = pd.concat([pd.read_parquet(f) for f in clean_files])
            row_valid = df_clean.iloc[0]
            self.assertTrue(row_valid["is_status_conflict"])
            self.assertIn("STATUS_CONFLICT(status=CLOSED_without_closed_date)", str(row_valid["quality_audit_notes"]))

            # Verify quarantine reasons (including multi-reason string)
            df_q = pd.read_parquet(quarantine_out / "quarantined_311.parquet")
            q_reasons = df_q["quarantine_reason"].tolist()
            self.assertIn("FUTURE_CLOSED_DATE", q_reasons)
            self.assertIn("INVALID_OR_NON_NUMERIC_UNIQUE_KEY; PRE_311_EPOCH_DATE", q_reasons)

    def test_unkeyed_weather_quarantine(self):
        """Verify that weather rows with missing grid_id/time or invalid timestamp are routed to weather quarantine."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            bronze_in = tmp_path / "bronze_weather"
            silver_out = tmp_path / "silver_weather"
            ledger_file = tmp_path / "quality" / "sensor_anomaly_ledger.parquet"
            quarantine_out = tmp_path / "quarantine_weather"
            bronze_in.mkdir()

            mock_weather = [
                # Valid row
                {
                    "grid_id": "GRID_40.7_-74.0",
                    "time": "2026-10-18T10:00",
                    "temperature_2m": 15.0,
                    "precipitation": 0.0,
                    "wind_speed_10m": 4.0,
                    "wind_gusts_10m": 7.0,
                    "surface_pressure": 1013.0,
                },
                # Missing grid_id
                {
                    "grid_id": None,
                    "time": "2026-10-18T11:00",
                    "temperature_2m": 16.0,
                    "precipitation": 0.0,
                    "wind_speed_10m": 4.0,
                    "wind_gusts_10m": 7.0,
                    "surface_pressure": 1013.0,
                },
                # Unparseable timestamp
                {
                    "grid_id": "GRID_40.7_-74.0",
                    "time": "NOT_A_TIMESTAMP",
                    "temperature_2m": 17.0,
                    "precipitation": 0.0,
                    "wind_speed_10m": 4.0,
                    "wind_gusts_10m": 7.0,
                    "surface_pressure": 1013.0,
                },
            ]
            pd.DataFrame(mock_weather).to_parquet(bronze_in / "part_0000.parquet", index=False)

            res = clean_and_validate_weather(
                bronze_weather_dir=bronze_in,
                silver_weather_dir=silver_out,
                anomaly_ledger_path=ledger_file,
                quarantine_dir=quarantine_out,
            )

            self.assertEqual(res["valid_rows"], 1)
            self.assertEqual(res["quarantined_rows"], 2)

            q_file = quarantine_out / "quarantined_weather.parquet"
            self.assertTrue(q_file.exists())
            df_q = pd.read_parquet(q_file)
            self.assertEqual(len(df_q), 2)
            self.assertIn("MISSING_GRID_ID", df_q["quarantine_reason"].tolist())
            self.assertIn("INVALID_TIMESTAMP_FORMAT", df_q["quarantine_reason"].tolist())



    def test_haversine_distance_vectorization(self):
        """Verify Great-Circle Haversine distance calculations, index independence, and bounds handling."""
        # 1. Identical coordinates -> distance must be 0.0
        lat1 = pd.Series([40.7500, 40.8000, np.nan])
        lon1 = pd.Series([-73.9900, -74.0000, -73.9900])
        lat2 = pd.Series([40.7500, 40.8000, 40.7500])
        lon2 = pd.Series([-73.9900, -73.9000, np.nan])

        dist = haversine_distance_km(lat1, lon1, lat2, lon2)
        # Point 1: same coordinates -> 0.0 km
        self.assertEqual(dist.iloc[0], 0.0)
        # Point 2: (40.8000, -74.0000) to (40.8000, -73.9000) -> approx 8.44 km
        self.assertAlmostEqual(dist.iloc[1], 8.44, delta=0.2)
        # Point 3: coordinate with NaN -> distance is NaN
        self.assertTrue(pd.isna(dist.iloc[2]))

        # 2. Mismatched index test: Series with different index labels must evaluate positionally without NaN corruption
        s_lat1 = pd.Series([40.7500, 40.8000], index=[100, 200])
        s_lon1 = pd.Series([-73.9900, -74.0000], index=[100, 200])
        s_lat2 = pd.Series([40.7500, 40.8000], index=[0, 1])
        s_lon2 = pd.Series([-73.9900, -73.9000], index=[0, 1])
        mismatch_dist = haversine_distance_km(s_lat1, s_lon1, s_lat2, s_lon2)
        self.assertEqual(list(mismatch_dist.index), [100, 200])
        self.assertEqual(mismatch_dist.loc[100], 0.0)
        self.assertAlmostEqual(mismatch_dist.loc[200], 8.44, delta=0.2)

        # 3. Unequal length handling defensively returns NaN Series instead of raising ValueError
        mismatched_len_dist = haversine_distance_km(
            pd.Series([40.75]),
            pd.Series([-73.99]),
            pd.Series([40.75, 40.80]),
            pd.Series([-73.99, -74.00]),
        )
        self.assertEqual(len(mismatched_len_dist), 1)
        self.assertTrue(pd.isna(mismatched_len_dist.iloc[0]))

        # 4. Out-of-bounds coordinate handling -> NaN
        bad_coords = haversine_distance_km(
            pd.Series([999.0, 40.7500]),
            pd.Series([-73.9900, -300.0]),
            pd.Series([40.7500, 40.7500]),
            pd.Series([-73.9900, -73.9900]),
        )
        self.assertTrue(pd.isna(bad_coords.iloc[0]))
        self.assertTrue(pd.isna(bad_coords.iloc[1]))

    def test_spark_haversine_distance_vectorization(self):
        """Verify PySpark Great-Circle Haversine distance calculations, null handling, and physical bounds."""
        try:
            from pyspark.sql.functions import col
            from src.utils.spark import get_spark_session
            spark = get_spark_session("test_spark_haversine")
        except Exception:
            self.skipTest("PySpark / Java environment not available")

        test_data = [
            (40.7500, -73.9900, 40.7500, -73.9900),
            (40.8000, -74.0000, 40.8000, -73.9000),
            (None, -73.9900, 40.7500, None),
            (999.0, -73.9900, 40.7500, -73.9900),  # Latitude > 90 out of bounds -> None
            (40.7500, -250.0, 40.7500, -73.9900),  # Longitude < -180 out of bounds -> None
        ]
        df = spark.createDataFrame(test_data, ["lat1", "lon1", "lat2", "lon2"])
        df_res = (
            df.withColumn("dist", spark_haversine_distance_km(col("lat1"), col("lon1"), col("lat2"), col("lon2")))
            .collect()
        )

        self.assertEqual(df_res[0]["dist"], 0.0)
        self.assertAlmostEqual(df_res[1]["dist"], 8.44, delta=0.2)
        self.assertIsNone(df_res[2]["dist"])
        self.assertIsNone(df_res[3]["dist"])
        self.assertIsNone(df_res[4]["dist"])

    def test_borough_validation(self):
        """Verify strict 5-borough normalization and soft quarantine/tagging of unstandardized boroughs."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            bronze_in = tmp_path / "bronze"
            silver_out = tmp_path / "silver"
            quarantine_out = tmp_path / "quarantine"
            bronze_in.mkdir()

            raw_data = pd.DataFrame({
                "unique_key": [1001, 1002, 1003, 1004],
                "created_date": ["2023-05-01 12:00:00"] * 4,
                "borough": ["MANHATTAN", "manhattan", "Unspecified", "ATLANTIS"],
                "latitude": [40.75, 40.75, 40.75, 40.75],
                "longitude": [-73.98, -73.98, -73.98, -73.98],
                "_batch_id": ["batch_1"] * 4,
            })
            raw_data.to_parquet(bronze_in / "test.parquet")

            clean_and_validate_311(bronze_311_dir=bronze_in, silver_311_dir=silver_out, quarantine_dir=quarantine_out)
            clean_files = list(silver_out.glob("**/*.parquet"))
            self.assertTrue(len(clean_files) > 0)
            df_res = pd.concat([pd.read_parquet(f) for f in clean_files])

            self.assertEqual(df_res.loc[df_res["unique_key"] == 1001, "borough"].iloc[0], "MANHATTAN")
            self.assertEqual(df_res.loc[df_res["unique_key"] == 1002, "borough"].iloc[0], "MANHATTAN")
            self.assertEqual(df_res.loc[df_res["unique_key"] == 1003, "borough"].iloc[0], "UNSPECIFIED")
            self.assertTrue(df_res.loc[df_res["unique_key"] == 1001, "is_valid_borough"].iloc[0])
            self.assertTrue(df_res.loc[df_res["unique_key"] == 1003, "is_valid_borough"].iloc[0])

            # Atlantis should be standardized to UNSPECIFIED and flagged is_valid_borough = False
            row4 = df_res.loc[df_res["unique_key"] == 1004].iloc[0]
            self.assertEqual(row4["borough"], "UNSPECIFIED")
            self.assertFalse(row4["is_valid_borough"])
            self.assertIn("UNRECOGNIZED_BOROUGH(ATLANTIS)", row4["quality_audit_notes"])

    def test_agency_syntactic_and_registry_validation(self):
        """Verify syntactic sanitization and zero-loss registry soft tagging of agency acronyms."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            bronze_in = tmp_path / "bronze"
            silver_out = tmp_path / "silver"
            quarantine_out = tmp_path / "quarantine"
            bronze_in.mkdir()

            raw_data = pd.DataFrame({
                "unique_key": [2001, 2002, 2003, 2004],
                "created_date": ["2023-05-01 12:00:00"] * 4,
                "agency": ["NYPD", "311", "99999", "NEWAGY"],
                "latitude": [40.75, 40.75, 40.75, 40.75],
                "longitude": [-73.98, -73.98, -73.98, -73.98],
                "_batch_id": ["batch_1"] * 4,
            })
            raw_data.to_parquet(bronze_in / "test.parquet")

            clean_and_validate_311(bronze_311_dir=bronze_in, silver_311_dir=silver_out, quarantine_dir=quarantine_out)
            clean_files = list(silver_out.glob("**/*.parquet"))
            self.assertTrue(len(clean_files) > 0)
            df_res = pd.concat([pd.read_parquet(f) for f in clean_files])

            # NYPD: known, valid
            row1 = df_res.loc[df_res["unique_key"] == 2001].iloc[0]
            self.assertEqual(row1["agency"], "NYPD")
            self.assertTrue(row1["is_known_agency"])

            # 311: valid numeric exception, known
            row2 = df_res.loc[df_res["unique_key"] == 2002].iloc[0]
            self.assertEqual(row2["agency"], "311")
            self.assertTrue(row2["is_known_agency"])

            # 99999: pure numeric malformed agency -> sanitized to UNKNOWN
            row3 = df_res.loc[df_res["unique_key"] == 2003].iloc[0]
            self.assertEqual(row3["agency"], "UNKNOWN")
            self.assertIn("MALFORMED_AGENCY(99999)", row3["quality_audit_notes"])

            # NEWAGY: syntactically valid but unknown in registry -> kept as NEWAGY, is_known_agency = False
            row4 = df_res.loc[df_res["unique_key"] == 2004].iloc[0]
            self.assertEqual(row4["agency"], "NEWAGY")
            self.assertFalse(row4["is_known_agency"])
            self.assertIn("UNREGISTERED_AGENCY(NEWAGY)", row4["quality_audit_notes"])

    def test_channel_type_and_status_validation(self):
        """Verify open_data_channel_type domain constraint and status normalization."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            bronze_in = tmp_path / "bronze"
            silver_out = tmp_path / "silver"
            quarantine_out = tmp_path / "quarantine"
            bronze_in.mkdir()

            raw_data = pd.DataFrame({
                "unique_key": [3001, 3002],
                "created_date": ["2023-05-01 12:00:00"] * 2,
                "open_data_channel_type": ["PHONE", "TELEPATHY"],
                "status": ["CLOSED", "FLYING"],
                "closed_date": ["2023-05-01 14:00:00", None],
                "latitude": [40.75, 40.75],
                "longitude": [-73.98, -73.98],
                "_batch_id": ["batch_1"] * 2,
            })
            raw_data.to_parquet(bronze_in / "test.parquet")

            clean_and_validate_311(bronze_311_dir=bronze_in, silver_311_dir=silver_out, quarantine_dir=quarantine_out)
            clean_files = list(silver_out.glob("**/*.parquet"))
            self.assertTrue(len(clean_files) > 0)
            df_res = pd.concat([pd.read_parquet(f) for f in clean_files])

            row1 = df_res.loc[df_res["unique_key"] == 3001].iloc[0]
            self.assertEqual(row1["open_data_channel_type"], "PHONE")
            self.assertEqual(row1["status"], "CLOSED")

            row2 = df_res.loc[df_res["unique_key"] == 3002].iloc[0]
            self.assertEqual(row2["open_data_channel_type"], "UNKNOWN")
            self.assertEqual(row2["status"], "FLYING")
            self.assertIn("UNRECOGNIZED_CHANNEL(TELEPATHY)", row2["quality_audit_notes"])
            self.assertIn("UNRECOGNIZED_STATUS(FLYING)", row2["quality_audit_notes"])

    def test_metadata_lineage_quarantine(self):
        """Verify that Bronze records with corrupted lineage metadata (_batch_id missing/empty) are quarantined."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            bronze_in = tmp_path / "bronze"
            silver_out = tmp_path / "silver"
            quarantine_out = tmp_path / "quarantine"
            bronze_in.mkdir()

            raw_data = pd.DataFrame({
                "unique_key": [4001, 4002],
                "created_date": ["2023-05-01 12:00:00"] * 2,
                "latitude": [40.75, 40.75],
                "longitude": [-73.98, -73.98],
                "_batch_id": ["valid_batch", ""],
            })
            raw_data.to_parquet(bronze_in / "test.parquet")

            res = clean_and_validate_311(bronze_311_dir=bronze_in, silver_311_dir=silver_out, quarantine_dir=quarantine_out)
            self.assertEqual(res["quarantined_rows"], 1)
            self.assertEqual(res["valid_rows_accepted"], 1)

            df_q = pd.read_parquet(quarantine_out / "quarantined_311.parquet")
            self.assertEqual(len(df_q), 1)
            self.assertEqual(df_q.iloc[0]["unique_key"], 4002)
            self.assertIn("CORRUPTED_INGESTION_METADATA", df_q.iloc[0]["quarantine_reason"])


    def test_corrupted_sensor_masking_before_rolling_window_and_time_gaps(self):
        """
        Verify that corrupted sensor readings are masked BEFORE rolling window computations,
        and verify that chronological time-based rangeBetween handles temporal gaps cleanly.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            mock_weather = [
                # Hour 0: Valid 2.0 mm
                {
                    "grid_id": "GRID_40.7_-74.0",
                    "time": "2026-10-18T10:00",
                    "temperature_2m": 15.0,
                    "precipitation": 2.0,
                    "surface_pressure": 1013.0,
                    "wind_speed_10m": 5.0,
                    "wind_gusts_10m": 10.0,
                },
                # Hour 1: Corrupted sentinel precipitation (-999.0 mm)
                {
                    "grid_id": "GRID_40.7_-74.0",
                    "time": "2026-10-18T11:00",
                    "temperature_2m": 15.0,
                    "precipitation": -999.0,
                    "surface_pressure": 1013.0,
                    "wind_speed_10m": 5.0,
                    "wind_gusts_10m": 10.0,
                },
                # Hour 2: Valid 3.0 mm (Window past 3h should sum 2.0 + (masked NaN) + 3.0 = 5.0, NOT 2 - 999 + 3 = -994!)
                {
                    "grid_id": "GRID_40.7_-74.0",
                    "time": "2026-10-18T12:00",
                    "temperature_2m": 15.0,
                    "precipitation": 3.0,
                    "surface_pressure": 1013.0,
                    "wind_speed_10m": 5.0,
                    "wind_gusts_10m": 10.0,
                },
                # Hour 10: Valid 4.0 mm, but occurs after an 8-hour gap!
                # Because window is time-based rangeBetween(-2 * 3600, 0), it should ONLY include Hour 10 (4.0 mm),
                # NOT previous rows across the gap!
                {
                    "grid_id": "GRID_40.7_-74.0",
                    "time": "2026-10-18T20:00",
                    "temperature_2m": 15.0,
                    "precipitation": 4.0,
                    "surface_pressure": 1013.0,
                    "wind_speed_10m": 5.0,
                    "wind_gusts_10m": 10.0,
                },
            ]
            bronze_in = tmp_path / "bronze_weather"
            silver_out = tmp_path / "silver_weather"
            ledger_file = tmp_path / "quality" / "sensor_anomaly_ledger.parquet"
            bronze_in.mkdir()
            pd.DataFrame(mock_weather).to_parquet(bronze_in / "part_0000.parquet", index=False)

            res = clean_and_validate_weather(
                bronze_weather_dir=bronze_in,
                silver_weather_dir=silver_out,
                anomaly_ledger_path=ledger_file,
            )
            self.assertEqual(res["valid_rows"], 4)

            clean_files = list(silver_out.glob("**/*.parquet"))
            df_w = pd.concat([pd.read_parquet(f) for f in clean_files]).sort_values("timestamp_utc").reset_index(drop=True)

            # Hour 1 raw precipitation was masked to NaN
            self.assertTrue(pd.isna(df_w.loc[1, "precipitation"]))

            # Hour 2 3-hour sum must be 2.0 + 3.0 = 5.0 (corrupt -999.0 was excluded)
            self.assertEqual(df_w.loc[2, "precip_past_3h_mm"], 5.0)

            # Hour 10 (after 8-hour gap) must ONLY sum Hour 10 (4.0 mm), ignoring Hour 2 from 8 hours ago
            self.assertEqual(df_w.loc[3, "precip_past_3h_mm"], 4.0)

            # Ledger must contain the negative precipitation violation
            df_ledger = pd.read_parquet(ledger_file)
            self.assertIn("PRECIPITATION_OUT_OF_BOUNDS", df_ledger["rule_violated"].values)

    def test_multiple_unkeyed_records_all_quarantined_without_cdc_drop(self):
        """
        Verify that multiple unkeyed/invalid records are ALL routed to quarantine,
        and not collapsed/dropped as duplicates of null by CDC deduplication.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            mock_records = [
                # Unkeyed row 1
                {
                    "unique_key": None,
                    "created_date": "2026-10-18T10:00:00.000",
                    "complaint_type": "Noise",
                    "borough": "MANHATTAN",
                    "_batch_id": "b1",
                },
                # Unkeyed row 2 (different complaint)
                {
                    "unique_key": None,
                    "created_date": "2026-10-18T10:05:00.000",
                    "complaint_type": "Illegal Parking",
                    "borough": "BROOKLYN",
                    "_batch_id": "b1",
                },
                # Invalid text key row 3
                {
                    "unique_key": "CORRUPT_KEY",
                    "created_date": "2026-10-18T10:10:00.000",
                    "complaint_type": "Pothole",
                    "borough": "QUEENS",
                    "_batch_id": "b1",
                },
                # Valid row 4
                {
                    "unique_key": "55555",
                    "created_date": "2026-10-18T10:15:00.000",
                    "complaint_type": "Heat/Hot Water",
                    "borough": "BRONX",
                    "latitude": "40.8500",
                    "longitude": "-73.9000",
                    "_batch_id": "b1",
                },
            ]
            bronze_in = tmp_path / "bronze"
            silver_out = tmp_path / "silver"
            quar_out = tmp_path / "quarantine"
            bronze_in.mkdir()
            pd.DataFrame(mock_records).to_parquet(bronze_in / "part_0000.parquet", index=False)

            res = clean_and_validate_311(bronze_in, silver_out, quar_out)
            self.assertEqual(res["input_rows"], 4)
            # Crucial: ALL 3 invalid records must be in quarantine! None dropped by CDC dedup!
            self.assertEqual(res["quarantined_rows"], 3)
            self.assertEqual(res["valid_rows_accepted"], 1)

            df_quar = pd.read_parquet(quar_out / "quarantined_311.parquet")
            self.assertEqual(len(df_quar), 3)


if __name__ == "__main__":
    unittest.main()
