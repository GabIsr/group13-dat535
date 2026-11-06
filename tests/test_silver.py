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
from src.silver.spatial_cleaning import (
    canonicalize_street_name_expr,
    normalize_bbl_and_house_numbers,
    standardize_cross_streets,
    normalize_highway_corridors,
    canonicalize_fallback_addresses,
    normalize_all_spatial_features,
)
from src.silver.incident_clustering import (
    generate_hierarchical_location_keys,
    cluster_citizen_call_bursts,
)
from src.utils.spark import get_spark_session
from pyspark.sql.functions import col, lit, to_timestamp


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
            self.assertIn(str(df_clean["unique_key"].dtype), ["Int64", "int64"])
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
            df_m311 = pd.DataFrame(mock_311)
            df_m311["created_at_utc"] = df_m311["created_at_utc"].astype("datetime64[us, UTC]")
            df_m311["weather_join_hour_utc"] = df_m311["weather_join_hour_utc"].astype("datetime64[us, UTC]")
            df_m311.to_parquet(s_311 / "part_0000.parquet", index=False)

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
            df_mw = pd.DataFrame(mock_weather)
            df_mw["timestamp_utc"] = df_mw["timestamp_utc"].astype("datetime64[us, UTC]")
            df_mw.to_parquet(s_weather / "part_0000.parquet", index=False)

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

    def test_cdc_dedup_prioritizes_source_updated_at_over_ingestion_time(self):
        """
        Verify that CDC deduplication prioritizes source_updated_at over _ingested_at_utc.
        Simulates an out-of-order backfill: a 2025 backfill (OPEN) ingested LATER at 12:00
        must NOT overwrite a 2026 batch (CLOSED) that was ingested EARLIER at 10:00!
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            bronze_in = tmp_path / "bronze"
            silver_out = tmp_path / "silver"
            quar_out = tmp_path / "quarantine"
            bronze_in.mkdir()

            mock_records = [
                # Record 1: From 2026 batch (ingested at 10:00, but updated on Socrata on 2026-01-15: CLOSED)
                {
                    "unique_key": "999999",
                    "created_date": "2025-12-20T10:00:00.000",
                    "closed_date": "2026-01-15T12:00:00.000",
                    "status": "CLOSED",
                    "agency": "NYPD",
                    "complaint_type": "Noise",
                    "borough": "MANHATTAN",
                    "latitude": "40.7500",
                    "longitude": "-73.9900",
                    "source_updated_at": "2026-01-15T12:00:00.000Z",
                    "source_created_at": "2025-12-20T15:05:00.000Z",
                    "_ingested_at_utc": "2026-10-30T10:00:00Z",
                    "_batch_id": "b_2026",
                },
                # Record 2: From historical 2025 backfill (ingested at 12:00, but Socrata update was 2025-12-20: OPEN)
                {
                    "unique_key": "999999",
                    "created_date": "2025-12-20T10:00:00.000",
                    "closed_date": None,
                    "status": "OPEN",
                    "agency": "NYPD",
                    "complaint_type": "Noise",
                    "borough": "MANHATTAN",
                    "latitude": "40.7500",
                    "longitude": "-73.9900",
                    "source_updated_at": "2025-12-20T10:00:00.000Z",
                    "source_created_at": "2025-12-20T15:05:00.000Z",
                    "_ingested_at_utc": "2026-10-30T12:00:00Z",
                    "_batch_id": "b_2025",
                },
            ]
            pd.DataFrame(mock_records).to_parquet(bronze_in / "part_0000.parquet", index=False)

            res = clean_and_validate_311(bronze_in, silver_out, quar_out)
            self.assertEqual(res["input_rows"], 2)
            self.assertEqual(res["duplicate_rows_removed"], 1)
            self.assertEqual(res["valid_rows_accepted"], 1)

            clean_files = list(silver_out.glob("**/*.parquet"))
            df_res = pd.concat([pd.read_parquet(f) for f in clean_files])
            winner = df_res.iloc[0]

            # The 2026 update MUST win because source_updated_at was 2026-01-15!
            self.assertEqual(winner["status"], "CLOSED")
            self.assertEqual(str(winner["_batch_id"]), "b_2026")
            self.assertIn("source_intake_lag_hours", df_res.columns)
            self.assertAlmostEqual(winner["source_intake_lag_hours"], 0.08, delta=0.05)

    def test_weather_soil_and_cloud_cover_bounds_and_flags(self):
        """
        Verify validation of extended meteorological variables:
        soil_temperature_0_to_7cm, soil_moisture_0_to_7cm, and cloud_cover.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            bronze_in = tmp_path / "bronze_weather"
            silver_out = tmp_path / "silver_weather"
            ledger_file = tmp_path / "quality" / "sensor_anomaly_ledger.parquet"
            bronze_in.mkdir()

            mock_weather = [
                # Row 1: Valid sub-zero soil temp -> is_ground_frozen, saturated moisture -> is_soil_saturated, overcast
                {
                    "grid_id": "GRID_40.7_-74.0",
                    "time": "2026-10-18T10:00",
                    "temperature_2m": -3.0,
                    "precipitation": 2.0,
                    "surface_pressure": 1013.0,
                    "wind_speed_10m": 5.0,
                    "wind_gusts_10m": 10.0,
                    "soil_temperature_0_to_7cm": -1.5,
                    "soil_moisture_0_to_7cm": 0.45,
                    "cloud_cover": 95,
                },
                # Row 2: Corrupted soil temp (999.0) and corrupted soil moisture (-5.0) -> masked to NaN
                {
                    "grid_id": "GRID_40.7_-74.0",
                    "time": "2026-10-18T11:00",
                    "temperature_2m": 15.0,
                    "precipitation": 0.0,
                    "surface_pressure": 1013.0,
                    "wind_speed_10m": 5.0,
                    "wind_gusts_10m": 10.0,
                    "soil_temperature_0_to_7cm": 999.0,  # Corrupt
                    "soil_moisture_0_to_7cm": -5.0,     # Corrupt
                    "cloud_cover": 20,
                },
            ]
            pd.DataFrame(mock_weather).to_parquet(bronze_in / "part_0000.parquet", index=False)

            res = clean_and_validate_weather(
                bronze_weather_dir=bronze_in,
                silver_weather_dir=silver_out,
                anomaly_ledger_path=ledger_file,
            )
            self.assertEqual(res["valid_rows"], 2)
            self.assertEqual(res["sensor_anomalies_logged"], 2)

            clean_files = list(silver_out.glob("**/*.parquet"))
            df_w = pd.concat([pd.read_parquet(f) for f in clean_files]).sort_values("timestamp_utc").reset_index(drop=True)

            # Row 1 checks:
            self.assertTrue(df_w.loc[0, "is_ground_frozen"])
            self.assertTrue(df_w.loc[0, "is_soil_saturated"])
            self.assertTrue(df_w.loc[0, "is_overcast"])

            # Row 2 checks (corrupt sensors masked):
            self.assertTrue(np.isnan(df_w.loc[1, "soil_temperature_0_to_7cm"]))
            self.assertTrue(np.isnan(df_w.loc[1, "soil_moisture_0_to_7cm"]))
            self.assertFalse(df_w.loc[1, "is_ground_frozen"])
            self.assertFalse(df_w.loc[1, "is_soil_saturated"])

            # Anomaly ledger checks:
            df_ledger = pd.read_parquet(ledger_file)
            self.assertIn("SOIL_TEMP_OUT_OF_BOUNDS", df_ledger["rule_violated"].values)
            self.assertIn("SOIL_MOISTURE_OUT_OF_BOUNDS", df_ledger["rule_violated"].values)



    def test_negative_duration_sub18s_quarantined(self):
        """
        Verify that sub-18s negative durations (e.g. closed 5s before creation) are caught
        by raw epoch seconds check and routed to quarantine, rather than rounding to 0.00h
        and falsely masquerading as instant closures.
        """
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            bronze_in = tmp_path / "bronze_311"
            silver_out = tmp_path / "silver_311"
            quarantine_out = tmp_path / "quarantine"
            bronze_in.mkdir()

            mock_data = [
                {
                    "unique_key": 70990001,
                    "created_date": "2026-10-18T12:00:05.000",
                    "closed_date": "2026-10-18T12:00:00.000",  # 5 seconds BEFORE created!
                    "agency": "NYPD",
                    "complaint_type": "Noise",
                    "status": "Closed",
                    "borough": "MANHATTAN",
                    "_batch_id": "test_batch_001",
                },
                {
                    "unique_key": 70990002,
                    "created_date": "2026-10-18T12:00:00.000",
                    "closed_date": "2026-10-18T12:00:00.000",  # Exact 0-second instant closure
                    "agency": "NYPD",
                    "complaint_type": "Noise",
                    "status": "Closed",
                    "borough": "MANHATTAN",
                    "_batch_id": "test_batch_001",
                },
            ]
            pd.DataFrame(mock_data).to_parquet(bronze_in / "part_0000.parquet", index=False)

            res = clean_and_validate_311(bronze_in, silver_out, quarantine_out)

            # Row 1 must be quarantined! Row 2 is valid instant closure.
            self.assertEqual(res["quarantined_rows"], 1)
            self.assertEqual(res["valid_rows_accepted"], 1)

            df_q = pd.read_parquet(quarantine_out / "quarantined_311.parquet")
            self.assertEqual(df_q.iloc[0]["unique_key"], 70990001)
            self.assertIn("NEGATIVE_RESOLUTION_DURATION", df_q.iloc[0]["quarantine_reason"])

            df_clean = pd.read_parquet(silver_out)
            self.assertEqual(df_clean.iloc[0]["unique_key"], 70990002)
            self.assertTrue(df_clean.iloc[0]["is_instant_closure"])
            self.assertIn("created_hour_utc", df_clean.columns)
            self.assertIn("created_hour_local", df_clean.columns)

    def test_spatial_cleaning_bbl_and_house_numbers(self):
        """Verify BBL normalization and house number extraction from addresses."""
        spark = get_spark_session("test_spatial_bbl")
        mock_data = [
            (1, "1022150225", "100-02 QUEENS BLVD"),
            (2, "3001234567.0", "350 5 AVENUE"),
            (3, "123", "12A ELM STREET"),
            (4, "invalid_bbl", "BROADWAY"),
            (5, None, None),
        ]
        df = spark.createDataFrame(mock_data, ["unique_key", "bbl", "incident_address"])
        df_clean = normalize_bbl_and_house_numbers(df)
        pdf = df_clean.toPandas().sort_values("unique_key").reset_index(drop=True)

        self.assertEqual(pdf.loc[0, "clean_bbl"], "1022150225")
        self.assertEqual(pdf.loc[0, "house_num_extracted"], "100-02")

        self.assertEqual(pdf.loc[1, "clean_bbl"], "3001234567")
        self.assertEqual(pdf.loc[1, "house_num_extracted"], "350")

        self.assertTrue(pd.isna(pdf.loc[2, "clean_bbl"]))
        self.assertEqual(pdf.loc[2, "house_num_extracted"], "12A")

        self.assertTrue(pd.isna(pdf.loc[3, "clean_bbl"]))
        self.assertTrue(pd.isna(pdf.loc[3, "house_num_extracted"]))

        self.assertTrue(pd.isna(pdf.loc[4, "clean_bbl"]))
        self.assertTrue(pd.isna(pdf.loc[4, "house_num_extracted"]))

    def test_spatial_cleaning_cross_streets_lexicographical_invariance(self):
        """Verify cross streets canonicalization and order-invariant matching."""
        spark = get_spark_session("test_cross_streets")
        mock_data = [
            (1, "BROADWAY", "WEST 42 STREET"),
            (2, "W 42 ST", "BROADWAY"),
        ]
        df = spark.createDataFrame(mock_data, ["unique_key", "cross_street_1", "cross_street_2"])
        df_clean = standardize_cross_streets(df)
        pdf = df_clean.toPandas().sort_values("unique_key").reset_index(drop=True)

        # Both permutations must yield the exact same ordered street pair
        self.assertEqual(pdf.loc[0, "clean_cross_street_a"], "BROADWAY")
        self.assertEqual(pdf.loc[0, "clean_cross_street_b"], "W 42 ST")
        self.assertEqual(pdf.loc[1, "clean_cross_street_a"], "BROADWAY")
        self.assertEqual(pdf.loc[1, "clean_cross_street_b"], "W 42 ST")
        self.assertTrue(pdf.loc[0, "has_valid_cross_streets"])
        self.assertTrue(pdf.loc[1, "has_valid_cross_streets"])

    def test_spatial_cleaning_highway_defensive_filter(self):
        """Verify highway validation requires both name and valid segment."""
        spark = get_spark_session("test_highway")
        mock_data = [
            (1, "BQE", "10-20", "NORTHBOUND"),
            (2, "FDR DRIVE", "UNSPECIFIED", "SOUTHBOUND"),
            (3, None, "10-20", "NB"),
        ]
        df = spark.createDataFrame(mock_data, ["unique_key", "bridge_highway_name", "bridge_highway_segment", "bridge_highway_direction"])
        df_clean = normalize_highway_corridors(df)
        pdf = df_clean.toPandas().sort_values("unique_key").reset_index(drop=True)

        self.assertTrue(pdf.loc[0, "has_valid_highway"])
        self.assertEqual(pdf.loc[0, "clean_highway_direction"], "NB")

        self.assertFalse(pdf.loc[1, "has_valid_highway"])
        self.assertFalse(pdf.loc[2, "has_valid_highway"])

    def test_hierarchical_spatial_routing_precedence(self):
        """Verify the 6 spatial routes and unlocatable fallback."""
        spark = get_spark_session("test_routing")
        mock_data = [
            # 1. Building
            (1, "ADDRESS", "1022150225", "100", "MANHATTAN", None, None, None, None, None, None, None, None, None),
            # 2. Highway
            (2, "HIGHWAY", None, None, "BROOKLYN", None, None, "BQE", "SEG_A", "NB", None, None, None, None),
            # 3. Intersection
            (3, "INTERSECTION", None, None, "MANHATTAN", "BROADWAY", "W 42 ST", None, None, None, None, None, None, None),
            # 4. Blockface
            (4, "BLOCKFACE", None, None, "QUEENS", "1 AVE", "2 AVE", None, None, None, None, None, "MAIN ST", None),
            # 5. Public Place
            (5, "PLACE", None, None, "MANHATTAN", None, None, None, None, None, "CENTRAL PARK", None, None, None),
            # 6. Unlocatable
            (6, "UNKNOWN", None, None, "UNSPECIFIED", None, None, None, None, None, None, None, None, None),
        ]
        cols = [
            "unique_key", "address_type", "clean_bbl", "house_num_extracted", "borough",
            "clean_cross_street_a", "clean_cross_street_b", "clean_highway_name",
            "clean_highway_segment", "clean_highway_direction", "clean_landmark",
            "clean_park_facility_name", "clean_street_name", "clean_incident_address"
        ]
        from pyspark.sql.types import StructType, StructField, LongType, StringType
        schema = StructType([StructField("unique_key", LongType(), True)] + [StructField(c, StringType(), True) for c in cols[1:]])
        df = spark.createDataFrame(mock_data, schema=schema)
        df = df.withColumn("has_valid_cross_streets", col("clean_cross_street_a").isNotNull() & col("clean_cross_street_b").isNotNull())
        df = df.withColumn("has_valid_highway", col("clean_highway_name").isNotNull() & col("clean_highway_segment").isNotNull())
        df = df.withColumn("clean_incident_zip", lit(None).cast("string"))

        df_routed = generate_hierarchical_location_keys(df)
        pdf = df_routed.toPandas().sort_values("unique_key").reset_index(drop=True)

        self.assertEqual(pdf.loc[0, "loc_routing_type"], "1. Buildings (BBL + House#)")
        self.assertEqual(pdf.loc[0, "loc_key"], "BBL_1022150225_HN_100")

        self.assertEqual(pdf.loc[1, "loc_routing_type"], "2. Highway Corridors")
        self.assertEqual(pdf.loc[1, "loc_key"], "HWY_BQE_NB_SEG_A")

        self.assertEqual(pdf.loc[2, "loc_routing_type"], "3. Intersections (Cross Streets)")
        self.assertEqual(pdf.loc[2, "loc_key"], "INT_BROADWAY_X_W 42 ST_MANHATTAN")

        self.assertEqual(pdf.loc[3, "loc_routing_type"], "4. Blockfaces (Road Segments)")
        self.assertEqual(pdf.loc[3, "loc_key"], "BLK_MAIN ST_1 AVE_X_2 AVE_QUEENS")

        self.assertEqual(pdf.loc[4, "loc_routing_type"], "5. Public Places & Parks")
        self.assertEqual(pdf.loc[4, "loc_key"], "LND_CENTRAL PARK_MANHATTAN")

        self.assertEqual(pdf.loc[5, "loc_routing_type"], "UNLOCATABLE")
        self.assertTrue(pd.isna(pdf.loc[5, "loc_key"]))

    def test_ticket_1_anchored_adaptive_burst_clustering(self):
        """Verify Ticket-1 adaptive windowing max(burst_window_hours, ticket_1_closed_date)."""
        from pyspark.sql.functions import to_timestamp
        spark = get_spark_session("test_clustering")
        mock_data = [
            # Incident 1 at LOC_A: Ticket 101 closed in 2h -> Cutoff is 10:00 + 6h = 16:00
            (101, "LOC_A", "1. Buildings", "Noise", "2026-05-01 10:00:00", "2026-05-01 12:00:00"),
            # Ticket 102 arrives at 13:00 <= 16:00 -> duplicate of 101!
            (102, "LOC_A", "1. Buildings", "Noise", "2026-05-01 13:00:00", None),
            # Incident 2 (Wave 2) at LOC_A: arrives at 17:00 > 16:00. Closed next day at 10:00 -> Cutoff is next day 10:00!
            (103, "LOC_A", "1. Buildings", "Noise", "2026-05-01 17:00:00", "2026-05-02 10:00:00"),
            # Ticket 104 arrives at 23:00 <= next day 10:00 -> duplicate of 103!
            (104, "LOC_A", "1. Buildings", "Noise", "2026-05-01 23:00:00", None),
            # Incident 3 (Wave 3) at LOC_A: arrives at next day 12:00 > next day 10:00 -> new Wave!
            (105, "LOC_A", "1. Buildings", "Noise", "2026-05-02 12:00:00", None),
            # Unlocatable record: receives isolated cluster without skew
            (999, None, "UNLOCATABLE", "Noise", "2026-05-01 11:00:00", None),
        ]
        cols = ["unique_key", "loc_key", "loc_routing_type", "complaint_type", "created_at_utc", "closed_at_utc"]
        df = spark.createDataFrame(mock_data, cols)
        df = df.withColumn("created_at_utc", to_timestamp("created_at_utc")).withColumn("closed_at_utc", to_timestamp("closed_at_utc"))

        df_clustered = cluster_citizen_call_bursts(df, burst_window_hours=6.0, num_partitions=2)
        pdf = df_clustered.toPandas().sort_values("unique_key").reset_index(drop=True)

        # Ticket 101: Anchor of Incident 1
        self.assertFalse(pdf.loc[pdf["unique_key"] == 101, "is_burst_duplicate"].iloc[0])
        self.assertEqual(pdf.loc[pdf["unique_key"] == 101, "burst_sequence_num"].iloc[0], 1)
        self.assertEqual(pdf.loc[pdf["unique_key"] == 101, "cluster_size"].iloc[0], 2)
        c101_id = pdf.loc[pdf["unique_key"] == 101, "incident_cluster_id"].iloc[0]

        # Ticket 102: Duplicate of 101
        self.assertTrue(pdf.loc[pdf["unique_key"] == 102, "is_burst_duplicate"].iloc[0])
        self.assertEqual(pdf.loc[pdf["unique_key"] == 102, "burst_sequence_num"].iloc[0], 2)
        self.assertEqual(pdf.loc[pdf["unique_key"] == 102, "incident_cluster_id"].iloc[0], c101_id)

        # Ticket 103: Anchor of Incident 2 (Wave 2)
        self.assertFalse(pdf.loc[pdf["unique_key"] == 103, "is_burst_duplicate"].iloc[0])
        self.assertEqual(pdf.loc[pdf["unique_key"] == 103, "burst_sequence_num"].iloc[0], 1)
        self.assertEqual(pdf.loc[pdf["unique_key"] == 103, "cluster_size"].iloc[0], 2)
        c103_id = pdf.loc[pdf["unique_key"] == 103, "incident_cluster_id"].iloc[0]
        self.assertNotEqual(c101_id, c103_id)

        # Ticket 104: Duplicate of 103
        self.assertTrue(pdf.loc[pdf["unique_key"] == 104, "is_burst_duplicate"].iloc[0])
        self.assertEqual(pdf.loc[pdf["unique_key"] == 104, "burst_sequence_num"].iloc[0], 2)
        self.assertEqual(pdf.loc[pdf["unique_key"] == 104, "incident_cluster_id"].iloc[0], c103_id)

        # Ticket 105: Anchor of Incident 3 (Wave 3)
        self.assertFalse(pdf.loc[pdf["unique_key"] == 105, "is_burst_duplicate"].iloc[0])
        self.assertEqual(pdf.loc[pdf["unique_key"] == 105, "burst_sequence_num"].iloc[0], 1)
        self.assertEqual(pdf.loc[pdf["unique_key"] == 105, "cluster_size"].iloc[0], 1)

        # Unlocatable record (999)
        self.assertFalse(pdf.loc[pdf["unique_key"] == 999, "is_burst_duplicate"].iloc[0])
        self.assertEqual(pdf.loc[pdf["unique_key"] == 999, "cluster_size"].iloc[0], 1)
        self.assertEqual(pdf.loc[pdf["unique_key"] == 999, "loc_routing_type"].iloc[0], "UNLOCATABLE")
        self.assertEqual(pdf.loc[pdf["unique_key"] == 999, "incident_cluster_id"].iloc[0], "INC_UNLOC_999")

    def test_spatial_cleaning_queens_hyphenated_address_and_house_number(self):
        """Verify Queens hyphenated house numbers (e.g. 100-02) are preserved in both house_num and address."""
        spark = get_spark_session("test_queens_hyphen")
        mock_data = [
            (1, "100-02 QUEENS BLVD"),
            (2, "100 - 02 QUEENS BLVD"),
            (3, "14-19 31ST AVE"),
            (4, "350 5TH AVENUE"),
        ]
        df = spark.createDataFrame(mock_data, ["unique_key", "incident_address"])
        df = normalize_bbl_and_house_numbers(df)
        df = canonicalize_fallback_addresses(df)
        pdf = df.toPandas().sort_values("unique_key").reset_index(drop=True)

        # Row 1: 100-02 Queens Blvd
        self.assertEqual(pdf.loc[0, "house_num_extracted"], "100-02")
        self.assertEqual(pdf.loc[0, "clean_incident_address"], "100-02 QUEENS BLVD")
        self.assertTrue(pdf.loc[0, "has_valid_house_number"])
        self.assertTrue(pdf.loc[0, "has_valid_fallback_address"])

        # Row 2: 100 - 02 Queens Blvd normalizes to 100-02
        self.assertEqual(pdf.loc[1, "house_num_extracted"], "100-02")
        self.assertEqual(pdf.loc[1, "clean_incident_address"], "100-02 QUEENS BLVD")

        # Row 3: 14-19 31ST AVE
        self.assertEqual(pdf.loc[2, "house_num_extracted"], "14-19")
        self.assertEqual(pdf.loc[2, "clean_incident_address"], "14-19 31ST AVE")

        # Row 4: 350 5TH AVE
        self.assertEqual(pdf.loc[3, "house_num_extracted"], "350")
        self.assertEqual(pdf.loc[3, "clean_incident_address"], "350 5TH AVE")

    def test_spatial_cleaning_directional_normalization(self):
        """Verify leading directional prefixes are normalized while protecting iconic street names like WEST END AVE."""
        spark = get_spark_session("test_directionals")
        mock_data = [
            (1, "NORTH BROADWAY"),
            (2, "WEST 42ND STREET"),
            (3, "WEST END AVENUE"),
            (4, "W END AVE"),
            (5, "EAST END AVENUE"),
            (6, "350 WEST 42ND STREET"),
            (7, "350 WEST END AVENUE"),
            (8, "SOUTH STREET"),
            (9, "UNKNOWN"),
        ]
        df = spark.createDataFrame(mock_data, ["unique_key", "incident_address"])
        df = canonicalize_fallback_addresses(df)
        pdf = df.toPandas().sort_values("unique_key").reset_index(drop=True)

        self.assertEqual(pdf.loc[0, "clean_incident_address"], "N BROADWAY")
        self.assertEqual(pdf.loc[1, "clean_incident_address"], "W 42ND ST")
        self.assertEqual(pdf.loc[2, "clean_incident_address"], "WEST END AVE")
        self.assertEqual(pdf.loc[3, "clean_incident_address"], "WEST END AVE")  # Normalizes W END to WEST END
        self.assertEqual(pdf.loc[4, "clean_incident_address"], "EAST END AVE")
        self.assertEqual(pdf.loc[5, "clean_incident_address"], "350 W 42ND ST")
        self.assertEqual(pdf.loc[6, "clean_incident_address"], "350 WEST END AVE")
        self.assertEqual(pdf.loc[7, "clean_incident_address"], "S ST")
        self.assertTrue(pd.isna(pdf.loc[8, "clean_incident_address"]))

    def test_spatial_cleaning_cross_street_placeholder_fallback(self):
        """Verify invalid placeholders (UNKNOWN, NAN, NONE) do NOT swallow valid fallback cross streets."""
        spark = get_spark_session("test_cross_street_fallbacks")
        mock_data = [
            # Primary street 1 has UNKNOWN, fallback has BROADWAY -> should pick BROADWAY!
            (1, "UNKNOWN", "BROADWAY", "NAN", "W 42 ST"),
            # Primary has valid street, fallback has something else -> primary wins
            (2, "5 AVE", "BROADWAY", "42 ST", "W 42 ST"),
            # Both primary and fallback are invalid placeholders -> should be None
            (3, "NULL", "UNSPECIFIED", "NAN", "UNKNOWN"),
        ]
        cols = ["unique_key", "intersection_street_1", "cross_street_1", "intersection_street_2", "cross_street_2"]
        df = spark.createDataFrame(mock_data, cols)
        df = standardize_cross_streets(df)
        pdf = df.toPandas().sort_values("unique_key").reset_index(drop=True)

        # Row 1: correctly recovered BROADWAY and W 42 ST
        self.assertTrue(pdf.loc[0, "has_valid_cross_streets"])
        self.assertEqual(pdf.loc[0, "clean_cross_street_a"], "BROADWAY")
        self.assertEqual(pdf.loc[0, "clean_cross_street_b"], "W 42 ST")

        # Row 2: primary streets 5 AVE and 42 ST
        self.assertTrue(pdf.loc[1, "has_valid_cross_streets"])
        self.assertEqual(pdf.loc[1, "clean_cross_street_a"], "42 ST")
        self.assertEqual(pdf.loc[1, "clean_cross_street_b"], "5 AVE")

        # Row 3: both invalid -> has_valid_cross_streets is False
        self.assertFalse(pdf.loc[2, "has_valid_cross_streets"])
        self.assertTrue(pd.isna(pdf.loc[2, "clean_cross_street_a"]))
        self.assertTrue(pd.isna(pdf.loc[2, "clean_cross_street_b"]))

    def test_spatial_cleaning_bbl_and_quality_indicators(self):
        """Verify BBL validation edge cases (trailing .0, scientific notation, length) and quality indicators."""
        spark = get_spark_session("test_bbl_indicators")
        mock_data = [
            (1, "1008400001", "350 5TH AVE"),       # Valid BBL
            (2, "1008400001.0", "350 5TH AVE"),     # Trailing .0 float conversion
            (3, "1.0084E9", "350 5TH AVE"),         # Scientific notation (invalid)
            (4, "12345", "350 5TH AVE"),            # Short BBL (invalid)
            (5, "6001234567", "350 5TH AVE"),       # Invalid borough 6
            (6, None, None),                         # Nulls
        ]
        df = spark.createDataFrame(mock_data, ["unique_key", "bbl", "incident_address"])
        df = normalize_bbl_and_house_numbers(df)
        df = canonicalize_fallback_addresses(df)
        pdf = df.toPandas().sort_values("unique_key").reset_index(drop=True)

        self.assertEqual(pdf.loc[0, "clean_bbl"], "1008400001")
        self.assertTrue(pdf.loc[0, "has_valid_bbl"])
        self.assertTrue(pdf.loc[0, "has_valid_house_number"])

        self.assertEqual(pdf.loc[1, "clean_bbl"], "1008400001")
        self.assertTrue(pdf.loc[1, "has_valid_bbl"])

        self.assertTrue(pd.isna(pdf.loc[2, "clean_bbl"]))
        self.assertFalse(pdf.loc[2, "has_valid_bbl"])

        self.assertTrue(pd.isna(pdf.loc[3, "clean_bbl"]))
        self.assertFalse(pdf.loc[3, "has_valid_bbl"])

        self.assertTrue(pd.isna(pdf.loc[4, "clean_bbl"]))
        self.assertFalse(pdf.loc[4, "has_valid_bbl"])

        self.assertTrue(pd.isna(pdf.loc[5, "clean_bbl"]))
        self.assertFalse(pdf.loc[5, "has_valid_bbl"])
        self.assertFalse(pdf.loc[5, "has_valid_house_number"])
        self.assertFalse(pdf.loc[5, "has_valid_fallback_address"])

    def test_spatial_cleaning_highway_segment_formatting(self):
        """Verify highway segment format normalization and defensive presence requirement."""
        spark = get_spark_session("test_highway_formatting")
        mock_data = [
            (1, "BQE", "SEGMENT-01", "NORTHBOUND"),
            (2, "BQE", "Segment  01", "NB"),
            (3, "FDR DRIVE", "UNKNOWN", "SOUTH"),
            (4, None, "SEGMENT 01", "NB"),
        ]
        df = spark.createDataFrame(mock_data, ["unique_key", "bridge_highway_name", "bridge_highway_segment", "bridge_highway_direction"])
        df = normalize_highway_corridors(df)
        pdf = df.toPandas().sort_values("unique_key").reset_index(drop=True)

        # Both SEGMENT-01 and Segment 01 normalize to SEGMENT 01
        self.assertEqual(pdf.loc[0, "clean_highway_segment"], "SEGMENT 01")
        self.assertEqual(pdf.loc[1, "clean_highway_segment"], "SEGMENT 01")
        self.assertTrue(pdf.loc[0, "has_valid_highway"])
        self.assertTrue(pdf.loc[1, "has_valid_highway"])

        # Row 3: UNKNOWN segment is dropped -> has_valid_highway is False
        self.assertFalse(pdf.loc[2, "has_valid_highway"])
        self.assertTrue(pd.isna(pdf.loc[2, "clean_highway_segment"]))

        # Row 4: Missing highway name -> has_valid_highway is False
        self.assertFalse(pdf.loc[3, "has_valid_highway"])


if __name__ == "__main__":
    unittest.main()
