"""
Unit Test Suite: Resolution Text Mining, Semantic Outcome Classification & Phantom Closure Auditing.
Author: Group 13
Validates 100% pure PySpark Catalyst implementation against the 50-case Golden Benchmark,
adversarial negation traps, bare-access vs qualified access, and duration sanity bounds.
"""

import unittest
from pathlib import Path

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.silver.resolution_text_mining import (
    clean_resolution_description_column,
    classify_resolution_outcomes,
    upsert_unclassified_resolutions_ledger,
)
from src.utils.spark import get_spark_session, stop_spark_session


class TestResolutionTextMining(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark_session("DAT535-Test-Resolution-Mining")

    @classmethod
    def tearDownClass(cls):
        stop_spark_session(cls.spark)

    def test_50_case_golden_benchmark(self):
        """Verify 100% precision and recall on the 50-case hand-labeled Golden Benchmark."""
        golden_benchmark = [
            # ACTION_TAKEN_REMEDIED
            ("Department of Transportation repaired the pothole and smoothed the asphalt surface.", True, "ACTION_TAKEN_REMEDIED"),
            ("Sanitation crew cleaned the illegal dumping site and removed debris.", True, "ACTION_TAKEN_REMEDIED"),
            ("DEP technicians serviced the fire hydrant and replaced the leaking cap.", True, "ACTION_TAKEN_REMEDIED"),
            ("HPD inspected and the boiler condition was corrected by landlord under supervision.", True, "ACTION_TAKEN_REMEDIED"),
            ("Officer gained access to the basement and repaired the leaking gas valve.", True, "ACTION_TAKEN_REMEDIED"),
            ("DEP repaired catch basin grate.", True, "ACTION_TAKEN_REMEDIED"),
            ("Notice of violation and summons was issued to the commercial establishment.", True, "ACTION_TAKEN_REMEDIED"),
            ("NYPD arrested the perpetrator for disorderly conduct at the scene.", True, "ACTION_TAKEN_REMEDIED"),
            ("Work completed by field technician. Water pressure restored.", True, "ACTION_TAKEN_REMEDIED"),
            ("Sanitation serviced the overflowing corner litter basket.", True, "ACTION_TAKEN_REMEDIED"),

            # ADMINISTRATIVE_DISMISSAL
            ("The condition was resolved prior to agency inspection. No action needed.", True, "ADMINISTRATIVE_DISMISSAL"),
            ("Inspection determined that police action was not necessary at this time.", True, "ADMINISTRATIVE_DISMISSAL"),
            ("Property is outside NYC municipal jurisdiction. Matter closed.", True, "ADMINISTRATIVE_DISMISSAL"),
            ("Investigation revealed the complaint is unfounded and fictitious.", True, "ADMINISTRATIVE_DISMISSAL"),
            ("No action was taken because NYC Parks determined the issue is out of its jurisdiction.", True, "ADMINISTRATIVE_DISMISSAL"),
            ("Heat was not required at the time of the inspection under housing maintenance code.", True, "ADMINISTRATIVE_DISMISSAL"),
            ("Summons not issued due to lack of corroborating evidence.", True, "ADMINISTRATIVE_DISMISSAL"),
            ("Violation not corrected by tenant, but city agency lacks enforcement authority.", True, "ADMINISTRATIVE_DISMISSAL"),
            ("Unable to fix issue because property owner refused city jurisdiction.", True, "ADMINISTRATIVE_DISMISSAL"),
            ("No city agency action required as private utility is responsible.", True, "ADMINISTRATIVE_DISMISSAL"),

            # DUPLICATE_CONSOLIDATED
            ("This complaint is a duplicate of a previously reported building-wide outage.", True, "DUPLICATE_CONSOLIDATED"),
            ("Already reported under master work order 128491. Closed administratively.", True, "DUPLICATE_CONSOLIDATED"),
            ("Duplicate complaint. Refer to complaint #883910 for active status.", True, "DUPLICATE_CONSOLIDATED"),
            ("Condition has already been reported by another tenant. Merged with master record.", True, "DUPLICATE_CONSOLIDATED"),
            ("Already closed under duplicate tracking ticket.", True, "DUPLICATE_CONSOLIDATED"),

            # DEFERRED_PENDING_ACTION
            ("Inspection performed; complaint referred to Department of Buildings for structural audit.", True, "DEFERRED_PENDING_ACTION"),
            ("Transferred to Department of Transportation for scheduled asphalt resurfacing.", True, "DEFERRED_PENDING_ACTION"),
            ("Catch basin scheduled for future maintenance during spring capital works.", True, "DEFERRED_PENDING_ACTION"),
            ("Forwarded to New York State DEC for environmental review.", True, "DEFERRED_PENDING_ACTION"),
            ("Will be inspected during regular daytime business hours next quarter.", True, "DEFERRED_PENDING_ACTION"),

            # ACCESS_DENIED_UNABLE_TO_LOCATE
            ("The Department was not able to gain access to the apartment to conduct inspection.", True, "ACCESS_DENIED_UNABLE_TO_LOCATE"),
            ("Inspector could not gain access after two separate attempts.", True, "ACCESS_DENIED_UNABLE_TO_LOCATE"),
            ("Premises was locked and no one was home at time of inspector arrival.", True, "ACCESS_DENIED_UNABLE_TO_LOCATE"),
            ("Officer attempted to inspect but couldn't locate condition on block.", True, "ACCESS_DENIED_UNABLE_TO_LOCATE"),
            ("Access was denied by building superintendent.", True, "ACCESS_DENIED_UNABLE_TO_LOCATE"),
            ("The reported vehicle cannot be located at the given coordinates.", True, "ACCESS_DENIED_UNABLE_TO_LOCATE"),
            ("Unable to access the rooms where the alleged condition exists.", True, "ACCESS_DENIED_UNABLE_TO_LOCATE"),

            # NO_VIOLATION_FOUND
            ("Officer inspected the location and found no violation of city noise ordinances.", True, "NO_VIOLATION_FOUND"),
            ("DEP investigated this complaint. No violation was found.", True, "NO_VIOLATION_FOUND"),
            ("Inspector observed the premises and found compliant with health code.", True, "NO_VIOLATION_FOUND"),
            ("DEP didn't observe a violation of the NYC Air Code during field survey.", True, "NO_VIOLATION_FOUND"),
            ("Did not observe any evidence of rodent infestation upon physical inspection.", True, "NO_VIOLATION_FOUND"),
            ("No condition observed at time of arrival; sound level meters registered below legal threshold.", True, "NO_VIOLATION_FOUND"),

            # UNSTATED_OR_EMPTY
            ("", True, "UNSTATED_OR_EMPTY"),
            ("   ", True, "UNSTATED_OR_EMPTY"),
            (None, True, "UNSTATED_OR_EMPTY"),

            # OPEN_TICKET
            ("Inspector assigned; inspection pending in queue.", False, "OPEN_TICKET"),
            ("Work order dispatched to field crew.", False, "OPEN_TICKET"),

            # UNCLASSIFIED_OR_GENERIC
            ("Random alphanumeric code XYZ-992-ALPHA logged by automatic IoT gateway.", True, "UNCLASSIFIED_OR_GENERIC"),
            ("Awaiting further municipal guidance regarding pilot sensor telemetry.", True, "UNCLASSIFIED_OR_GENERIC"),
        ]

        schema = StructType([
            StructField("resolution_description", StringType(), True),
            StructField("is_ticket_closed", BooleanType(), True),
            StructField("ground_truth", StringType(), True),
        ])

        df_input = self.spark.createDataFrame(golden_benchmark, schema=schema)
        df_classified = classify_resolution_outcomes(df_input)

        results = df_classified.select("resolution_description", "ground_truth", "resolution_outcome_category").collect()
        for r in results:
            self.assertEqual(
                r["ground_truth"],
                r["resolution_outcome_category"],
                f"Classification mismatch for text={r['resolution_description']!r}: "
                f"expected {r['ground_truth']}, got {r['resolution_outcome_category']}"
            )

    def test_negation_traps_and_anti_patterns(self):
        """Verify that negative words take strict precedence over action keywords."""
        test_cases = [
            ("Summons not issued due to lack of evidence.", "ADMINISTRATIVE_DISMISSAL", False),
            ("Violation not corrected by tenant; landlord out of town.", "ADMINISTRATIVE_DISMISSAL", False),
            ("Unable to fix issue because of private property restrictions.", "ADMINISTRATIVE_DISMISSAL", False),
            ("No violations were issued after thorough inspection.", "NO_VIOLATION_FOUND", True),
            ("Observed no evidence of rodent activity.", "NO_VIOLATION_FOUND", True),
            ("Pothole was repaired and asphalt smoothed.", "ACTION_TAKEN_REMEDIED", True),
        ]

        schema = StructType([
            StructField("resolution_description", StringType(), True),
            StructField("expected_category", StringType(), True),
            StructField("expected_field_verified", BooleanType(), True),
        ])

        data = [(t[0], t[1], t[2]) for t in test_cases]
        from pyspark.sql.functions import lit
        df_input = self.spark.createDataFrame(data, schema=schema).withColumn("is_ticket_closed", lit(True))

        df_classified = classify_resolution_outcomes(df_input)
        rows = df_classified.select("resolution_description", "expected_category", "resolution_outcome_category", "expected_field_verified", "is_field_verified").collect()

        for r in rows:
            self.assertEqual(r["expected_category"], r["resolution_outcome_category"])
            self.assertEqual(r["expected_field_verified"], r["is_field_verified"])

    def test_duration_bounds_and_clock_skew_filtering(self):
        """Verify that physical duration sanity bounds [0.0, 720.0] filter anomalies to NULL."""
        from datetime import datetime, timezone
        from pyspark.sql.functions import lit, to_timestamp

        schema = StructType([
            StructField("resolution_description", StringType(), True),
            StructField("is_ticket_closed", BooleanType(), True),
            StructField("created_at_utc", StringType(), True),
            StructField("closed_at_utc", StringType(), True),
            StructField("test_case", StringType(), True),
        ])

        # Test cases: normal (24h), negative clock skew (-2h), multi-year artifact (1500h)
        data = [
            ("Pothole was repaired.", True, "2026-10-01 10:00:00", "2026-10-02 10:00:00", "valid_24h"),
            ("Pothole was repaired.", True, "2026-10-01 10:00:00", "2026-10-01 08:00:00", "negative_clock_skew"),
            ("Pothole was repaired.", True, "2026-01-01 10:00:00", "2026-10-01 10:00:00", "multi_month_artifact"),
            ("Duplicate complaint.", True, "2026-10-01 10:00:00", "2026-10-02 10:00:00", "phantom_not_verified"),
        ]

        df_input = (
            self.spark.createDataFrame(data, schema=schema)
            .withColumn("created_at_utc", to_timestamp("created_at_utc"))
            .withColumn("closed_at_utc", to_timestamp("closed_at_utc"))
        )

        df_classified = classify_resolution_outcomes(df_input)
        results = {r["test_case"]: r for r in df_classified.collect()}

        # 1. Valid 24h should have field_remediation_hours == 24.0
        self.assertEqual(results["valid_24h"]["field_remediation_hours"], 24.0)
        self.assertTrue(results["valid_24h"]["is_field_verified"])
        self.assertFalse(results["valid_24h"]["is_phantom_closure"])

        # 2. Negative clock skew should be filtered to NULL
        self.assertIsNone(results["negative_clock_skew"]["field_remediation_hours"])
        self.assertTrue(results["negative_clock_skew"]["is_field_verified"])

        # 3. Multi-month artifact (> 720h) should be filtered to NULL
        self.assertIsNone(results["multi_month_artifact"]["field_remediation_hours"])
        self.assertTrue(results["multi_month_artifact"]["is_field_verified"])

        # 4. Phantom closure should not have field remediation hours
        self.assertIsNone(results["phantom_not_verified"]["field_remediation_hours"])
        self.assertFalse(results["phantom_not_verified"]["is_field_verified"])
        self.assertTrue(results["phantom_not_verified"]["is_phantom_closure"])

    def test_bare_access_vs_qualified_impediment(self):
        """Verify bare 'access' allows action taken, while qualified impediment triggers access denied."""
        from pyspark.sql.functions import lit

        schema = StructType([
            StructField("resolution_description", StringType(), True),
            StructField("expected_category", StringType(), True),
        ])

        cases = [
            ("Officer gained access and repaired the leaking gas valve.", "ACTION_TAKEN_REMEDIED"),
            ("Unable to gain access to the apartment.", "ACCESS_DENIED_UNABLE_TO_LOCATE"),
            ("Inspector could not gain access after 3 attempts.", "ACCESS_DENIED_UNABLE_TO_LOCATE"),
            ("Access was denied by the tenant.", "ACCESS_DENIED_UNABLE_TO_LOCATE"),
            ("Building locked and no one was home.", "ACCESS_DENIED_UNABLE_TO_LOCATE"),
            ("Unable to locate vehicle at the reported location.", "ACCESS_DENIED_UNABLE_TO_LOCATE"),
        ]

        df_in = self.spark.createDataFrame(cases, schema=schema).withColumn("is_ticket_closed", lit(True))
        df_out = classify_resolution_outcomes(df_in)

        for row in df_out.collect():
            self.assertEqual(
                row["expected_category"],
                row["resolution_outcome_category"],
                f"Failed for description: {row['resolution_description']!r}"
            )

    def test_open_ticket_and_empty_text_null_safety(self):
        """Verify that open tickets and empty descriptions never throw exceptions or trigger false actions."""
        from pyspark.sql.functions import lit

        schema = StructType([
            StructField("resolution_description", StringType(), True),
            StructField("is_ticket_closed", BooleanType(), True),
            StructField("expected_category", StringType(), True),
            StructField("expected_phantom", BooleanType(), True),
        ])

        cases = [
            ("Work in progress by maintenance team.", False, "OPEN_TICKET", False),
            (None, False, "OPEN_TICKET", False),
            ("", True, "UNSTATED_OR_EMPTY", False),
            ("   ", True, "UNSTATED_OR_EMPTY", False),
            (None, True, "UNSTATED_OR_EMPTY", False),
        ]

        df_in = self.spark.createDataFrame(cases, schema=schema)
        df_out = classify_resolution_outcomes(df_in)

        for row in df_out.collect():
            self.assertEqual(row["expected_category"], row["resolution_outcome_category"])
            self.assertEqual(row["expected_phantom"], row["is_phantom_closure"])
            self.assertFalse(row["is_field_verified"])
            self.assertIsNone(row["field_remediation_hours"])


if __name__ == "__main__":
    unittest.main()
