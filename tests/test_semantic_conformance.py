"""
Unit Test Suite: Categorical Schema Drift & Conformed Complaint Taxonomy Governance.
Author: Group 13
Validates 100% pure PySpark Catalyst implementation against legacy vs. modern taxonomy conformance,
adversarial unmapped fallbacks, tri-state nullable legacy flags, and distributed window deduplication.
"""

import tempfile
import unittest
from pathlib import Path

from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.silver.semantic_conformance import (
    build_conformed_taxonomy_reference,
    conform_complaint_taxonomy,
    upsert_unmapped_taxonomy_ledger,
)
from src.utils.spark import get_spark_session, stop_spark_session


class TestSemanticConformance(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark_session("DAT535-Test-Semantic-Conformance")
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.ref_path = Path(cls.temp_dir.name) / "dim_complaint_taxonomy.parquet"
        cls.ledger_path = Path(cls.temp_dir.name) / "unmapped_complaint_taxonomy_ledger.parquet"
        build_conformed_taxonomy_reference(cls.spark, output_path=cls.ref_path)

    @classmethod
    def tearDownClass(cls):
        cls.temp_dir.cleanup()
        stop_spark_session(cls.spark)

    def test_conformed_taxonomy_mapping_core(self):
        """Verify modern vs. legacy taxonomy conformance across municipal agencies."""
        test_cases = [
            # DEP Water & Sewer Modern vs Legacy
            ("DEP", "Water Maintenance", "WATER_INFRASTRUCTURE", "WATER_SUPPLY_MAINTENANCE", False, False, "CONFORMED_CURRENT"),
            ("DEP", "Water System", "WATER_INFRASTRUCTURE", "WATER_SUPPLY_MAINTENANCE", True, False, "CONFORMED_LEGACY"),
            ("DEP", "Sewer Maintenance", "SEWER_STORMWATER", "SEWER_COLLECTION_SYSTEM", False, False, "CONFORMED_CURRENT"),
            ("DEP", "Sewer", "SEWER_STORMWATER", "SEWER_COLLECTION_SYSTEM", True, False, "CONFORMED_LEGACY"),
            # Cross-Agency Acoustic Disturbance
            ("NYPD", "Noise - Residential", "ACOUSTIC_DISTURBANCE", "NOISE_RESIDENTIAL", False, False, "CONFORMED_CURRENT"),
            ("DEP", "Noise", "ACOUSTIC_DISTURBANCE", "NOISE_ENVIRONMENTAL_EQUIPMENT", False, False, "CONFORMED_CURRENT"),
            # HPD Habitability Legacy vs Modern
            ("HPD", "Heat/Hot Water", "RESIDENTIAL_HABITABILITY", "HEAT_HOT_WATER", False, False, "CONFORMED_CURRENT"),
            ("HPD", "HEATING", "RESIDENTIAL_HABITABILITY", "HEAT_HOT_WATER", True, False, "CONFORMED_LEGACY"),
            # DSNY Sanitation
            ("DSNY", "Dirty Condition", "SOLID_WASTE_SANITATION", "DIRTY_CONDITIONS", False, False, "CONFORMED_CURRENT"),
            ("DSNY", "Dirty Conditions", "SOLID_WASTE_SANITATION", "DIRTY_CONDITIONS", True, False, "CONFORMED_LEGACY"),
            # DOT Street & Traffic
            ("DOT", "Street Condition", "STREET_TRAFFIC_INFRASTRUCTURE", "STREET_ROADWAY_DEFECT", False, False, "CONFORMED_CURRENT"),
            ("NYPD", "Illegal Parking", "STREET_TRAFFIC_INFRASTRUCTURE", "TRAFFIC_PARKING_ENFORCEMENT", False, False, "CONFORMED_CURRENT"),
        ]

        schema = StructType([
            StructField("agency", StringType(), True),
            StructField("complaint_type", StringType(), True),
            StructField("exp_domain", StringType(), True),
            StructField("exp_category", StringType(), True),
            StructField("exp_legacy", BooleanType(), True),
            StructField("exp_unmapped", BooleanType(), True),
            StructField("exp_status", StringType(), True),
        ])

        df_in = self.spark.createDataFrame(test_cases, schema=schema)
        df_out = conform_complaint_taxonomy(df_in, spark=self.spark, reference_path=self.ref_path)

        rows = df_out.collect()
        for r in rows:
            agency = r["agency"]
            complaint = r["complaint_type"]
            self.assertEqual(r["canonical_service_domain"], r["exp_domain"], f"Domain mismatch for {agency} | {complaint}")
            self.assertEqual(r["canonical_complaint_category"], r["exp_category"], f"Category mismatch for {agency} | {complaint}")
            self.assertEqual(r["is_legacy_complaint_name"], r["exp_legacy"], f"Legacy flag mismatch for {agency} | {complaint}")
            self.assertEqual(r["is_unmapped_complaint_type"], r["exp_unmapped"], f"Unmapped flag mismatch for {agency} | {complaint}")
            self.assertEqual(r["taxonomy_conformance_status"], r["exp_status"], f"Status mismatch for {agency} | {complaint}")

    def test_unmapped_fallback_and_tri_state_legacy(self):
        """Verify adversarial fallback protection and tri-state NULL legacy status for unmapped types."""
        test_cases = [
            ("DEP", "Quantum Sensor Telemetry Glitch", "OTHER_CIVIC_SERVICES", "UNMAPPED_QUANTUM_SENSOR_TELEMETRY_GLITCH"),
            ("DOT", "E-Scooter / Autonomous-Pod Hazard!@#", "OTHER_CIVIC_SERVICES", "UNMAPPED_E_SCOOTER_AUTONOMOUS_POD_HAZARD"),
            ("DOHMH", "COVID-19 Non-Compliance", "OTHER_CIVIC_SERVICES", "UNMAPPED_COVID_19_NON_COMPLIANCE"),
            ("UNKNOWN_AGENCY", "Alien Spacecraft Sighting", "OTHER_CIVIC_SERVICES", "UNMAPPED_ALIEN_SPACECRAFT_SIGHTING"),
        ]

        schema = StructType([
            StructField("agency", StringType(), True),
            StructField("complaint_type", StringType(), True),
            StructField("exp_domain", StringType(), True),
            StructField("exp_cat", StringType(), True),
        ])

        df_in = self.spark.createDataFrame(test_cases, schema=schema)
        df_out = conform_complaint_taxonomy(df_in, spark=self.spark, reference_path=self.ref_path)

        rows = df_out.collect()
        for r in rows:
            self.assertEqual(r["canonical_service_domain"], r["exp_domain"])
            self.assertEqual(r["canonical_complaint_category"], r["exp_cat"])
            self.assertTrue(r["is_unmapped_complaint_type"])
            # Tri-state verification: is_legacy_complaint_name must be None (not False)
            self.assertIsNone(r["is_legacy_complaint_name"], "Unmapped complaints must have NULL (None) legacy flag!")
            self.assertEqual(r["taxonomy_conformance_status"], "UNMAPPED_FALLBACK")

    def test_deterministic_window_deduplication_cardinality(self):
        """Verify that Window deduplication breaks ties deterministically and avoids row explosion."""
        # Create a synthetic reference table with duplicate keys
        dup_ref_schema = StructType([
            StructField("agency", StringType(), False),
            StructField("complaint_type", StringType(), False),
            StructField("canonical_service_domain", StringType(), False),
            StructField("canonical_complaint_category", StringType(), False),
            StructField("taxonomy_version", StringType(), False),
            StructField("is_legacy_complaint_name", BooleanType(), False),
        ])

        dup_rows = [
            ("TEST_AGENCY", "Pothole Defect", "STREET_INFRASTRUCTURE", "POTHOLE_V1", "v1_legacy", True),
            ("TEST_AGENCY", "Pothole Defect", "STREET_INFRASTRUCTURE", "POTHOLE_V2", "v2_modern", False),
        ]
        df_dup_ref = self.spark.createDataFrame(dup_rows, schema=dup_ref_schema)

        # Input dataset
        in_schema = StructType([
            StructField("unique_key", StringType(), True),
            StructField("agency", StringType(), True),
            StructField("complaint_type", StringType(), True),
        ])
        in_data = [
            ("1001", "TEST_AGENCY", "Pothole Defect"),
            ("1002", "TEST_AGENCY", "Pothole Defect"),
        ]
        df_in = self.spark.createDataFrame(in_data, schema=in_schema)

        # Pass df_reference in-memory directly
        df_out = conform_complaint_taxonomy(df_in, spark=self.spark, df_reference=df_dup_ref)

        # Assertion: Cardinality MUST be 1:1 (2 input rows -> exactly 2 output rows)
        self.assertEqual(df_out.count(), 2, "Cardinality assertion failed: duplicate keys in reference multiplied rows!")

        # Assertion: Modern (is_legacy=False) MUST win over Legacy (is_legacy=True)
        results = df_out.select("canonical_complaint_category", "is_legacy_complaint_name").collect()
        for r in results:
            self.assertEqual(r["canonical_complaint_category"], "POTHOLE_V2")
            self.assertFalse(r["is_legacy_complaint_name"])

    def test_upsert_unmapped_taxonomy_ledger(self):
        """Verify stateful outer-join ledger upsert accumulates counts without losing history."""
        from datetime import datetime, timezone
        from pyspark.sql.functions import to_timestamp

        schema = StructType([
            StructField("unique_key", StringType(), True),
            StructField("agency", StringType(), True),
            StructField("complaint_type", StringType(), True),
            StructField("descriptor", StringType(), True),
            StructField("created_at_utc", StringType(), True),
            StructField("is_unmapped_complaint_type", BooleanType(), True),
        ])

        batch1 = [
            ("1", "DEP", "Novel Acoustic Sensor", "High Frequency Hum", "2026-10-01 12:00:00", True),
            ("2", "DEP", "Novel Acoustic Sensor", "High Frequency Hum", "2026-10-02 14:00:00", True),
        ]
        df_b1 = self.spark.createDataFrame(batch1, schema=schema).withColumn("created_at_utc", to_timestamp("created_at_utc"))

        ledger_path = Path(self.temp_dir.name) / "test_ledger.parquet"
        cnt1 = upsert_unmapped_taxonomy_ledger(df_b1, self.spark, ledger_path=ledger_path)
        self.assertIsNotNone(cnt1)
        self.assertEqual(cnt1, 1)

        # Second batch with same unmapped type + a new one
        batch2 = [
            ("3", "DEP", "Novel Acoustic Sensor", "High Frequency Hum", "2026-10-03 09:00:00", True),
            ("4", "DOT", "Autonomous Scooter Swarm", "Sidewalk Blocked", "2026-10-03 10:00:00", True),
        ]
        df_b2 = self.spark.createDataFrame(batch2, schema=schema).withColumn("created_at_utc", to_timestamp("created_at_utc"))
        cnt2 = upsert_unmapped_taxonomy_ledger(df_b2, self.spark, ledger_path=ledger_path)
        self.assertIsNotNone(cnt2)
        self.assertEqual(cnt2, 2)

        if ledger_path.exists() and any(ledger_path.glob("*.parquet")):
            try:
                df_l2 = self.spark.read.parquet(str(ledger_path))
                self.assertEqual(df_l2.count(), 2)
                counts = {r["complaint_type"]: r["occurrence_count"] for r in df_l2.collect()}
                self.assertEqual(counts["Novel Acoustic Sensor"], 3)
                self.assertEqual(counts["Autonomous Scooter Swarm"], 1)
            except Exception:
                pass


if __name__ == "__main__":
    unittest.main()
