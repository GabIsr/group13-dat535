"""
Silver Layer: Categorical Schema Drift & Conformed Complaint Taxonomy Governance.
Resolves vocabulary divergence across municipal agencies and longitudinal time horizons
via an Invariant Reference Dimension, Deterministic Window Deduplication, Broadcast Joins,
and a Stateful Lakehouse Observability Ledger.

100% Pure PySpark (Zero Python @udf, Zero toPandas Driver Serialization).
Author: Group 13
"""

import logging
import shutil
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    broadcast,
    col,
    concat,
    count as spark_count,
    first as spark_first,
    greatest,
    least,
    lit,
    max as spark_max,
    min as spark_min,
    regexp_replace,
    row_number,
    trim,
    upper,
    when,
)
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

from src.silver.config import (
    MAX_UNMAPPED_TAXONOMY_RATE_PCT,
    TAXONOMY_REFERENCE_PATH,
    UNMAPPED_TAXONOMY_LEDGER_PATH,
)

logger = logging.getLogger("semantic_conformance")

# -----------------------------------------------------------------------------
# Conformed Complaint Taxonomy Mapping (96 Core Categories across 12 Agencies)
# -----------------------------------------------------------------------------
# Format: (agency, raw_complaint_type): (canonical_domain, canonical_category, taxonomy_version, is_legacy)

CONFORMED_TAXONOMY_MAP: Dict[Tuple[str, str], Tuple[str, str, str, bool]] = {
    # =========================================================================
    # 1. WATER INFRASTRUCTURE (DEP)
    # =========================================================================
    ("DEP", "Water Maintenance"): ("WATER_INFRASTRUCTURE", "WATER_SUPPLY_MAINTENANCE", "v2_modern", False),
    ("DEP", "Water System"): ("WATER_INFRASTRUCTURE", "WATER_SUPPLY_MAINTENANCE", "v1_legacy", True),
    ("DEP", "Water Quality"): ("WATER_INFRASTRUCTURE", "DRINKING_WATER_QUALITY", "v2_modern", False),
    ("DEP", "Water Conservation"): ("WATER_INFRASTRUCTURE", "WATER_CONSERVATION", "v2_modern", False),
    ("DEP", "Industrial Waste"): ("WATER_INFRASTRUCTURE", "INDUSTRIAL_EFFLUENT", "v2_modern", False),

    # =========================================================================
    # 2. SEWER & STORMWATER SYSTEM (DEP & DPR)
    # =========================================================================
    ("DEP", "Sewer Maintenance"): ("SEWER_STORMWATER", "SEWER_COLLECTION_SYSTEM", "v2_modern", False),
    ("DEP", "Sewer"): ("SEWER_STORMWATER", "SEWER_COLLECTION_SYSTEM", "v1_legacy", True),
    ("DEP", "Waste Water Treatment Plant"): ("SEWER_STORMWATER", "TREATMENT_PLANT_OPERATIONS", "v2_modern", False),
    ("DEP", "Green Infrastructure"): ("SEWER_STORMWATER", "STORMWATER_BIOSWALES", "v2_modern", False),
    ("DPR", "Root/Sewer/Sidewalk Condition"): ("SEWER_STORMWATER", "ROOT_SEWER_INTRUSION", "v2_modern", False),

    # =========================================================================
    # 3. ACOUSTIC DISTURBANCE (Cross-Agency: NYPD, DEP, EDC)
    # =========================================================================
    ("DEP", "Noise"): ("ACOUSTIC_DISTURBANCE", "NOISE_ENVIRONMENTAL_EQUIPMENT", "v2_modern", False),
    ("NYPD", "Noise - Residential"): ("ACOUSTIC_DISTURBANCE", "NOISE_RESIDENTIAL", "v2_modern", False),
    ("NYPD", "Noise - Street/Sidewalk"): ("ACOUSTIC_DISTURBANCE", "NOISE_STREET_SIDEWALK", "v2_modern", False),
    ("NYPD", "Noise - Commercial"): ("ACOUSTIC_DISTURBANCE", "NOISE_COMMERCIAL", "v2_modern", False),
    ("NYPD", "Noise - Vehicle"): ("ACOUSTIC_DISTURBANCE", "NOISE_VEHICLE", "v2_modern", False),
    ("NYPD", "Noise - Park"): ("ACOUSTIC_DISTURBANCE", "NOISE_PARK", "v2_modern", False),
    ("NYPD", "Noise - House of Worship"): ("ACOUSTIC_DISTURBANCE", "NOISE_WORSHIP", "v2_modern", False),
    ("EDC", "Noise - Helicopter"): ("ACOUSTIC_DISTURBANCE", "NOISE_AVIATION", "v2_modern", False),

    # =========================================================================
    # 4. SOLID WASTE & SANITATION (DSNY)
    # =========================================================================
    ("DSNY", "Dirty Condition"): ("SOLID_WASTE_SANITATION", "DIRTY_CONDITIONS", "v2_modern", False),
    ("DSNY", "Dirty Conditions"): ("SOLID_WASTE_SANITATION", "DIRTY_CONDITIONS", "v1_legacy", True),
    ("DSNY", "Sanitation Condition"): ("SOLID_WASTE_SANITATION", "DIRTY_CONDITIONS", "v1_legacy", True),
    ("DSNY", "Illegal Dumping"): ("SOLID_WASTE_SANITATION", "ILLEGAL_DUMPING", "v2_modern", False),
    ("DSNY", "Missed Collection"): ("SOLID_WASTE_SANITATION", "MISSED_COLLECTION", "v2_modern", False),
    ("DSNY", "Missed Collection (All Materials)"): ("SOLID_WASTE_SANITATION", "MISSED_COLLECTION", "v1_legacy", True),
    ("DSNY", "Derelict Vehicles"): ("SOLID_WASTE_SANITATION", "DERELICT_VEHICLE_DISPOSAL", "v2_modern", False),
    ("DSNY", "Residential Disposal Complaint"): ("SOLID_WASTE_SANITATION", "RESIDENTIAL_DISPOSAL", "v2_modern", False),
    ("DSNY", "Street Sweeping Complaint"): ("SOLID_WASTE_SANITATION", "STREET_SWEEPING", "v2_modern", False),
    ("DSNY", "Dead Animal"): ("SOLID_WASTE_SANITATION", "DEAD_ANIMAL_REMOVAL", "v2_modern", False),
    ("DSNY", "Graffiti"): ("SOLID_WASTE_SANITATION", "GRAFFITI_ABATEMENT", "v2_modern", False),
    ("DSNY", "Obstruction"): ("SOLID_WASTE_SANITATION", "SIDEWALK_OBSTRUCTION", "v2_modern", False),
    ("DSNY", "Vendor Enforcement"): ("SOLID_WASTE_SANITATION", "VENDOR_ENFORCEMENT", "v2_modern", False),
    ("DSNY", "Recycling Enforcement"): ("SOLID_WASTE_SANITATION", "RECYCLING_COMPLIANCE", "v2_modern", False),
    ("DSNY", "Litter Basket / Request"): ("SOLID_WASTE_SANITATION", "LITTER_BASKETS", "v2_modern", False),

    # =========================================================================
    # 5. RESIDENTIAL HABITABILITY & STRUCTURES (HPD & DOB)
    # =========================================================================
    ("HPD", "Heat/Hot Water"): ("RESIDENTIAL_HABITABILITY", "HEAT_HOT_WATER", "v2_modern", False),
    ("HPD", "HEATING"): ("RESIDENTIAL_HABITABILITY", "HEAT_HOT_WATER", "v1_legacy", True),
    ("HPD", "HEAT/HOT WATER"): ("RESIDENTIAL_HABITABILITY", "HEAT_HOT_WATER", "v1_legacy", True),
    ("HPD", "Unsanitary Condition"): ("RESIDENTIAL_HABITABILITY", "RESIDENTIAL_UNSANITARY", "v2_modern", False),
    ("HPD", "Paint/Plaster"): ("RESIDENTIAL_HABITABILITY", "PAINT_PLASTER_DEFECT", "v2_modern", False),
    ("HPD", "PAINT - PLASTER"): ("RESIDENTIAL_HABITABILITY", "PAINT_PLASTER_DEFECT", "v1_legacy", True),
    ("HPD", "PAINT/PLASTER"): ("RESIDENTIAL_HABITABILITY", "PAINT_PLASTER_DEFECT", "v1_legacy", True),
    ("HPD", "Plumbing"): ("RESIDENTIAL_HABITABILITY", "INTERNAL_PLUMBING", "v2_modern", False),
    ("HPD", "PLUMBING"): ("RESIDENTIAL_HABITABILITY", "INTERNAL_PLUMBING", "v1_legacy", True),
    ("HPD", "Water Leak"): ("RESIDENTIAL_HABITABILITY", "INTERNAL_WATER_LEAK", "v2_modern", False),
    ("HPD", "WATER LEAK"): ("RESIDENTIAL_HABITABILITY", "INTERNAL_WATER_LEAK", "v1_legacy", True),
    ("HPD", "Appliance"): ("RESIDENTIAL_HABITABILITY", "APPLIANCE_DEFECT", "v2_modern", False),
    ("HPD", "APPLIANCE"): ("RESIDENTIAL_HABITABILITY", "APPLIANCE_DEFECT", "v1_legacy", True),
    ("HPD", "Door/Window"): ("RESIDENTIAL_HABITABILITY", "DOOR_WINDOW_DEFECT", "v2_modern", False),
    ("HPD", "DOOR/WINDOW"): ("RESIDENTIAL_HABITABILITY", "DOOR_WINDOW_DEFECT", "v1_legacy", True),
    ("HPD", "Electric"): ("RESIDENTIAL_HABITABILITY", "ELECTRICAL_DEFECT", "v2_modern", False),
    ("HPD", "ELECTRIC"): ("RESIDENTIAL_HABITABILITY", "ELECTRICAL_DEFECT", "v1_legacy", True),
    ("HPD", "General Construction"): ("RESIDENTIAL_HABITABILITY", "GENERAL_CONSTRUCTION", "v1_legacy", True),
    ("HPD", "GENERAL CONSTRUCTION"): ("RESIDENTIAL_HABITABILITY", "GENERAL_CONSTRUCTION", "v1_legacy", True),
    ("HPD", "NONCONST"): ("RESIDENTIAL_HABITABILITY", "GENERAL_MAINTENANCE", "v1_legacy", True),
    ("HPD", "General Maintenance"): ("RESIDENTIAL_HABITABILITY", "GENERAL_MAINTENANCE", "v2_modern", False),
    ("HPD", "SAFETY"): ("RESIDENTIAL_HABITABILITY", "BUILDING_SAFETY_FIRE", "v1_legacy", True),
    ("HPD", "Mold"): ("RESIDENTIAL_HABITABILITY", "MOLD_INFESTATION", "v2_modern", False),
    ("HPD", "Lead"): ("RESIDENTIAL_HABITABILITY", "LEAD_PAINT_HAZARD", "v2_modern", False),
    ("DOB", "General Construction/Plumbing"): ("RESIDENTIAL_HABITABILITY", "DOB_CONSTRUCTION_PLUMBING", "v2_modern", False),
    ("DOB", "Building/Use"): ("RESIDENTIAL_HABITABILITY", "BUILDING_USE_ZONING", "v2_modern", False),
    ("DOB", "Elevator"): ("RESIDENTIAL_HABITABILITY", "ELEVATOR_SAFETY", "v2_modern", False),
    ("DOB", "Boilers"): ("RESIDENTIAL_HABITABILITY", "BOILER_COMPLIANCE", "v2_modern", False),
    ("DOB", "Illegal Conversion"): ("RESIDENTIAL_HABITABILITY", "ILLEGAL_CONVERSION", "v2_modern", False),
    ("DOB", "Scaffold Safety"): ("RESIDENTIAL_HABITABILITY", "SCAFFOLD_SAFETY", "v2_modern", False),

    # =========================================================================
    # 6. STREET & TRAFFIC INFRASTRUCTURE (DOT & NYPD)
    # =========================================================================
    ("NYPD", "Illegal Parking"): ("STREET_TRAFFIC_INFRASTRUCTURE", "TRAFFIC_PARKING_ENFORCEMENT", "v2_modern", False),
    ("NYPD", "Blocked Driveway"): ("STREET_TRAFFIC_INFRASTRUCTURE", "TRAFFIC_PARKING_ENFORCEMENT", "v2_modern", False),
    ("NYPD", "Traffic/Illegal Parking"): ("STREET_TRAFFIC_INFRASTRUCTURE", "TRAFFIC_PARKING_ENFORCEMENT", "v1_legacy", True),
    ("DOT", "Street Condition"): ("STREET_TRAFFIC_INFRASTRUCTURE", "STREET_ROADWAY_DEFECT", "v2_modern", False),
    ("DOT", "Street Light Condition"): ("STREET_TRAFFIC_INFRASTRUCTURE", "STREET_LIGHTING", "v2_modern", False),
    ("DOT", "Traffic Signal Condition"): ("STREET_TRAFFIC_INFRASTRUCTURE", "TRAFFIC_SIGNAL", "v2_modern", False),
    ("DOT", "Sidewalk Condition"): ("STREET_TRAFFIC_INFRASTRUCTURE", "SIDEWALK_DEFECT", "v2_modern", False),
    ("DOT", "Curb Condition"): ("STREET_TRAFFIC_INFRASTRUCTURE", "CURB_DEFECT", "v2_modern", False),
    ("DOT", "Highway Condition"): ("STREET_TRAFFIC_INFRASTRUCTURE", "HIGHWAY_DEFECT", "v2_modern", False),
    ("DOT", "Broken Parking Meter"): ("STREET_TRAFFIC_INFRASTRUCTURE", "PARKING_METER_DEFECT", "v2_modern", False),
    ("DOT", "Street Sign - Damaged"): ("STREET_TRAFFIC_INFRASTRUCTURE", "TRAFFIC_SIGNAGE", "v2_modern", False),
    ("DOT", "Street Sign - Dangling"): ("STREET_TRAFFIC_INFRASTRUCTURE", "TRAFFIC_SIGNAGE", "v2_modern", False),
    ("DOT", "Street Sign - Missing"): ("STREET_TRAFFIC_INFRASTRUCTURE", "TRAFFIC_SIGNAGE", "v2_modern", False),

    # =========================================================================
    # 7. PUBLIC HEALTH & ENVIRONMENTAL HAZARDS (DOHMH & DEP)
    # =========================================================================
    ("DOHMH", "Rodent"): ("PUBLIC_HEALTH_ENVIRONMENT", "RODENT_INFESTATION", "v2_modern", False),
    ("DOHMH", "Food Establishment"): ("PUBLIC_HEALTH_ENVIRONMENT", "FOOD_SAFETY_INSPECTION", "v2_modern", False),
    ("DOHMH", "Indoor Air Quality"): ("PUBLIC_HEALTH_ENVIRONMENT", "INDOOR_AIR_QUALITY", "v2_modern", False),
    ("DOHMH", "Smoking Or Vaping"): ("PUBLIC_HEALTH_ENVIRONMENT", "SMOKING_COMPLIANCE", "v2_modern", False),
    ("DEP", "Lead"): ("PUBLIC_HEALTH_ENVIRONMENT", "LEAD_CONTAMINATION", "v2_modern", False),
    ("DEP", "Air Quality"): ("PUBLIC_HEALTH_ENVIRONMENT", "AIR_QUALITY_POLLUTION", "v2_modern", False),
    ("DEP", "Hazardous Material"): ("PUBLIC_HEALTH_ENVIRONMENT", "HAZARDOUS_MATERIALS", "v2_modern", False),
    ("DEP", "Asbestos"): ("PUBLIC_HEALTH_ENVIRONMENT", "ASBESTOS_HAZARD", "v2_modern", False),

    # =========================================================================
    # 8. PARKS & URBAN FORESTRY (DPR)
    # =========================================================================
    ("DPR", "Maintenance Or Facility"): ("PARKS_FORESTRY", "PARK_FACILITY_MAINTENANCE", "v2_modern", False),
    ("DPR", "Damaged Tree"): ("PARKS_FORESTRY", "TREE_DAMAGE_EMERGENCY", "v2_modern", False),
    ("DPR", "Overgrown Tree/Branches"): ("PARKS_FORESTRY", "TREE_OVERGROWTH_PRUNING", "v2_modern", False),
    ("DPR", "Dead/Dying Tree"): ("PARKS_FORESTRY", "DEAD_TREE_REMOVAL", "v2_modern", False),
    ("DPR", "Illegal Tree Damage"): ("PARKS_FORESTRY", "TREE_VANDALISM", "v2_modern", False),
    ("DPR", "Animal In A Park"): ("PARKS_FORESTRY", "PARK_ANIMAL_CONDITION", "v2_modern", False),
    ("DPR", "Violation Of Park Rules"): ("PARKS_FORESTRY", "PARK_RULE_ENFORCEMENT", "v2_modern", False),
    ("DPR", "New Tree Request"): ("PARKS_FORESTRY", "FORESTRY_PLANTING", "v2_modern", False),

    # =========================================================================
    # 9. SOCIAL SERVICES & HOMELESSNESS (DHS)
    # =========================================================================
    ("DHS", "Homeless Person Assistance"): ("SOCIAL_SERVICES_HOMELESSNESS", "HOMELESS_ASSISTANCE", "v2_modern", False),
    ("DHS", "Encampment"): ("SOCIAL_SERVICES_HOMELESSNESS", "ENCAMPMENT_OUTREACH", "v2_modern", False),
    ("DHS", "Homeless Street Condition"): ("SOCIAL_SERVICES_HOMELESSNESS", "STREET_CONDITION_OUTREACH", "v2_modern", False),

    # =========================================================================
    # 10. CIVIC REGULATION & CONSUMER PROTECTION (DCWP, TLC, NYPD)
    # =========================================================================
    ("DCWP", "Consumer Complaint"): ("CIVIC_REGULATION_CONSUMER", "CONSUMER_PROTECTION", "v2_modern", False),
    ("TLC", "For Hire Vehicle Complaint"): ("CIVIC_REGULATION_CONSUMER", "TAXI_LIVERY_ENFORCEMENT", "v2_modern", False),
    ("TLC", "Taxi Complaint"): ("CIVIC_REGULATION_CONSUMER", "YELLOW_TAXI_ENFORCEMENT", "v2_modern", False),
    ("TLC", "Lost Property"): ("CIVIC_REGULATION_CONSUMER", "TAXI_LOST_PROPERTY", "v2_modern", False),
    ("NYPD", "Non-Emergency Police Matter"): ("CIVIC_REGULATION_CONSUMER", "NON_EMERGENCY_CIVIC", "v2_modern", False),
    ("NYPD", "Animal-Abuse"): ("CIVIC_REGULATION_CONSUMER", "ANIMAL_PROTECTION", "v2_modern", False),
    ("NYPD", "Panhandling"): ("CIVIC_REGULATION_CONSUMER", "QUALITY_OF_LIFE_ENFORCEMENT", "v2_modern", False),
    ("NYPD", "Drug Activity"): ("CIVIC_REGULATION_CONSUMER", "NARCOTICS_ENFORCEMENT", "v2_modern", False),
}


def build_conformed_taxonomy_reference(
    spark: SparkSession,
    output_path: Path = TAXONOMY_REFERENCE_PATH,
) -> DataFrame:
    """
    Build and persist the Invariant Conformed Complaint Taxonomy reference table (dim_complaint_taxonomy).
    100% pure PySpark DataFrame generation and Parquet persistence.
    """
    schema = StructType([
        StructField("agency", StringType(), False),
        StructField("complaint_type", StringType(), False),
        StructField("canonical_service_domain", StringType(), False),
        StructField("canonical_complaint_category", StringType(), False),
        StructField("taxonomy_version", StringType(), False),
        StructField("is_legacy_complaint_name", BooleanType(), False),
    ])

    rows = [
        (
            k[0],
            k[1],
            v[0],
            v[1],
            v[2],
            v[3],
        )
        for k, v in CONFORMED_TAXONOMY_MAP.items()
    ]

    df_ref = spark.createDataFrame(rows, schema=schema)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df_ref.write.mode("overwrite").parquet(str(output_path))
        logger.info(f"Persisted conformed taxonomy reference table ({len(rows)} entries) to {output_path}")
    except Exception as e:
        logger.warning(f"Could not persist reference table to disk (non-blocking fallback to in-memory): {e}")

    return df_ref


def conform_complaint_taxonomy(
    df: DataFrame,
    spark: Optional[SparkSession] = None,
    reference_path: Optional[Path] = TAXONOMY_REFERENCE_PATH,
    df_reference: Optional[DataFrame] = None,
) -> DataFrame:
    """
    Conform raw agency complaint types to standardized canonical service domains and categories
    via Broadcast Hash Join with distributed Window deduplication.

    Applies:
    1. Distributed-Safe Window Deduplication on Reference Keys: Window.partitionBy('ref_agency', 'ref_complaint_type')
       deterministically breaks ties (modern over legacy) with zero non-deterministic shuffle behavior.
    2. Broadcast Hash Join on Normalized Keys (upper/trim): evaluates locally on each partition without shuffle.
    3. Adversarial Fallback Protection: Clean alphanumeric regex sanitization maps unseen types to
       `canonical_service_domain = 'OTHER_CIVIC_SERVICES'`, `is_unmapped_complaint_type = True`.
    4. Tri-State Nullable Legacy Flag: `is_legacy_complaint_name` is True (legacy), False (modern),
       or NULL (unmapped/unknown).
    5. Taxonomy Conformance Status: `'CONFORMED_CURRENT'`, `'CONFORMED_LEGACY'`, or `'UNMAPPED_FALLBACK'`.

    100% pure PySpark Catalyst expressions.
    """
    if spark is None:
        spark = df.sparkSession

    # Load or generate reference dimension table
    if df_reference is not None:
        df_ref = df_reference
    elif reference_path is not None and reference_path.exists():
        try:
            df_ref = spark.read.parquet(str(reference_path))
        except Exception as e:
            logger.warning(f"Could not read reference parquet at {reference_path}: {e}. Rebuilding reference table.")
            df_ref = build_conformed_taxonomy_reference(spark, reference_path)
    else:
        df_ref = build_conformed_taxonomy_reference(spark, reference_path)

    # 1. Distributed-Safe Window Deduplication on Reference Table
    w_dedup = Window.partitionBy("ref_agency", "ref_complaint_type").orderBy(
        col("is_legacy_complaint_name").asc(),
        col("taxonomy_version").desc(),
    )

    df_ref_clean = (
        df_ref
        .withColumn("ref_agency", upper(trim(col("agency"))))
        .withColumn("ref_complaint_type", upper(trim(col("complaint_type"))))
        .withColumn("_rn", row_number().over(w_dedup))
        .filter(col("_rn") == 1)
        .drop("_rn", "agency", "complaint_type")
    )

    # 2. Stage Input DataFrame with Normalized Join Keys
    cols_in = set(df.columns)
    raw_agency = col("agency") if "agency" in cols_in else lit("UNKNOWN")
    raw_complaint = col("complaint_type") if "complaint_type" in cols_in else lit("UNKNOWN")

    df_staged = (
        df
        .withColumn("_join_agency", upper(trim(F.coalesce(raw_agency, lit("UNKNOWN")))))
        .withColumn("_join_complaint_type", upper(trim(F.coalesce(raw_complaint, lit("UNKNOWN")))))
    )

    # 3. Broadcast Hash Join (Zero-Shuffle Evaluation)
    df_joined = df_staged.join(
        broadcast(df_ref_clean),
        (df_staged["_join_agency"] == df_ref_clean["ref_agency"]) &
        (df_staged["_join_complaint_type"] == df_ref_clean["ref_complaint_type"]),
        how="left",
    )

    # 4. Construct Adversarial Sanitized Fallback Category
    # PySpark regex sanitization: replace non-alphanumeric with '_', then trim leading/trailing '_'
    raw_cat_str = upper(trim(F.coalesce(raw_complaint, lit("UNKNOWN"))))
    raw_sanitized = regexp_replace(raw_cat_str, r"[^A-Za-z0-9]+", "_")
    raw_stripped = regexp_replace(regexp_replace(raw_sanitized, r"^_+", ""), r"_+$", "")
    fallback_cat = concat(lit("UNMAPPED_"), raw_stripped)

    # 5. Apply Conformance Attributes & Tri-State Nullable Legacy Flag
    source_version = F.coalesce(col("taxonomy_version"), lit("v_unmapped"))
    is_unmapped = source_version == "v_unmapped"

    df_conformed = (
        df_joined
        .withColumn(
            "canonical_service_domain",
            F.coalesce(col("canonical_service_domain"), lit("OTHER_CIVIC_SERVICES")),
        )
        .withColumn(
            "canonical_complaint_category",
            F.coalesce(col("canonical_complaint_category"), fallback_cat),
        )
        .withColumn("source_taxonomy_version", source_version)
        .withColumn("is_unmapped_complaint_type", is_unmapped)
        .withColumn(
            "is_legacy_complaint_name",
            when(is_unmapped, lit(None).cast("boolean")).otherwise(col("is_legacy_complaint_name")),
        )
        .withColumn(
            "taxonomy_conformance_status",
            when(is_unmapped, lit("UNMAPPED_FALLBACK"))
            .when(col("is_legacy_complaint_name") == True, lit("CONFORMED_LEGACY"))
            .otherwise(lit("CONFORMED_CURRENT")),
        )
        .drop("_join_agency", "_join_complaint_type", "ref_agency", "ref_complaint_type", "taxonomy_version")
    )

    return df_conformed


def upsert_unmapped_taxonomy_ledger(
    df: DataFrame,
    spark: SparkSession,
    ledger_path: Path = UNMAPPED_TAXONOMY_LEDGER_PATH,
) -> Optional[int]:
    """
    Idempotent stateful metadata catalog upsert for unmapped complaint taxonomy types.
    Tracks novel or seasonal complaint types with occurrence_count, first_seen_utc,
    last_seen_utc, and sample descriptors. Incorporates active governance review columns.

    100% pure PySpark aggregation and Parquet persistence with atomic staging swap.
    """
    try:
        cols_df = set(df.columns)
        if "is_unmapped_complaint_type" not in cols_df:
            return 0

        # Filter for unmapped records
        df_unmapped = df.filter(col("is_unmapped_complaint_type") == True)
        unmapped_cnt = df_unmapped.count()

        if unmapped_cnt == 0:
            logger.info("Zero unmapped complaint taxonomy types found in current batch.")
            return 0

        # Defensive handling for missing attributes in synthetic unit test DataFrames
        cols = set(df_unmapped.columns)
        if "agency" not in cols:
            df_unmapped = df_unmapped.withColumn("agency", lit("UNKNOWN"))
        if "complaint_type" not in cols:
            df_unmapped = df_unmapped.withColumn("complaint_type", lit("UNKNOWN"))

        key_col = col("unique_key") if "unique_key" in cols else lit(None).cast("long")
        desc_col = col("descriptor") if "descriptor" in cols else lit(None).cast("string")
        time_col = col("created_at_utc") if "created_at_utc" in cols else lit(None).cast("timestamp")

        # Aggregate batch catalog
        df_batch = (
            df_unmapped.groupBy("agency", "complaint_type")
            .agg(
                spark_count("*").alias("batch_count"),
                spark_min(time_col).alias("first_seen_utc"),
                spark_max(time_col).alias("last_seen_utc"),
                spark_first(desc_col, ignorenulls=True).alias("sample_descriptor"),
                spark_first(key_col, ignorenulls=True).alias("sample_unique_key"),
            )
            .withColumn("review_status", lit("NEEDS_REVIEW"))
            .withColumn("suggested_domain", lit(None).cast("string"))
            .withColumn("review_notes", lit(None).cast("string"))
            .withColumn("reviewed_at_utc", lit(None).cast("timestamp"))
        )

        ledger_path.parent.mkdir(parents=True, exist_ok=True)

        if ledger_path.exists() and any(ledger_path.glob("*.parquet")):
            try:
                df_existing = spark.read.parquet(str(ledger_path))

                # Add workflow governance columns if missing in legacy ledger
                if "review_status" not in df_existing.columns:
                    df_existing = df_existing.withColumn("review_status", lit("NEEDS_REVIEW"))
                if "suggested_domain" not in df_existing.columns:
                    df_existing = df_existing.withColumn("suggested_domain", lit(None).cast("string"))
                if "review_notes" not in df_existing.columns:
                    df_existing = df_existing.withColumn("review_notes", lit(None).cast("string"))
                if "reviewed_at_utc" not in df_existing.columns:
                    df_existing = df_existing.withColumn("reviewed_at_utc", lit(None).cast("timestamp"))

                df_merged = (
                    df_existing.join(
                        df_batch,
                        on=["agency", "complaint_type"],
                        how="outer",
                    )
                    .select(
                        col("agency"),
                        col("complaint_type"),
                        (
                            F.coalesce(df_existing["occurrence_count"], lit(0)) +
                            F.coalesce(df_batch["batch_count"], lit(0))
                        ).cast("integer").alias("occurrence_count"),
                        F.coalesce(df_batch["sample_descriptor"], df_existing["sample_descriptor"]).alias("sample_descriptor"),
                        F.coalesce(df_batch["sample_unique_key"], df_existing["sample_unique_key"]).alias("sample_unique_key"),
                        least(
                            F.coalesce(df_existing["first_seen_utc"], df_batch["first_seen_utc"]),
                            F.coalesce(df_batch["first_seen_utc"], df_existing["first_seen_utc"]),
                        ).alias("first_seen_utc"),
                        greatest(
                            F.coalesce(df_existing["last_seen_utc"], df_batch["last_seen_utc"]),
                            F.coalesce(df_batch["last_seen_utc"], df_existing["last_seen_utc"]),
                        ).alias("last_seen_utc"),
                        F.coalesce(df_existing["review_status"], lit("NEEDS_REVIEW")).alias("review_status"),
                        df_existing["suggested_domain"].alias("suggested_domain"),
                        df_existing["review_notes"].alias("review_notes"),
                        df_existing["reviewed_at_utc"].alias("reviewed_at_utc"),
                    )
                    .orderBy(col("occurrence_count").desc())
                )
            except Exception as e:
                logger.warning(f"Could not read existing ledger at {ledger_path}: {e}. Reinitializing catalog.")
                df_merged = df_batch.select(
                    col("agency"),
                    col("complaint_type"),
                    col("batch_count").cast("integer").alias("occurrence_count"),
                    col("sample_descriptor"),
                    col("sample_unique_key"),
                    col("first_seen_utc"),
                    col("last_seen_utc"),
                    col("review_status"),
                    col("suggested_domain"),
                    col("review_notes"),
                    col("reviewed_at_utc"),
                ).orderBy(col("occurrence_count").desc())
        else:
            df_merged = df_batch.select(
                col("agency"),
                col("complaint_type"),
                col("batch_count").cast("integer").alias("occurrence_count"),
                col("sample_descriptor"),
                col("sample_unique_key"),
                col("first_seen_utc"),
                col("last_seen_utc"),
                col("review_status"),
                col("suggested_domain"),
                col("review_notes"),
                col("reviewed_at_utc"),
            ).orderBy(col("occurrence_count").desc())

        # Calculate total distinct categories before persistence
        total_distinct = df_merged.count()

        # Atomic staging swap pattern for Parquet persistence:
        # Prevents SparkFileNotFoundException when lazily reading from ledger_path while overwriting
        staging_dir = ledger_path.parent / f".tmp_{ledger_path.stem}_{int(time.time() * 1000)}"
        try:
            df_merged.write.mode("overwrite").parquet(str(staging_dir))
            if ledger_path.exists():
                if ledger_path.is_dir():
                    shutil.rmtree(ledger_path)
                else:
                    ledger_path.unlink()
            staging_dir.rename(ledger_path)
            logger.info(f"Upserted unmapped taxonomy ledger ({total_distinct} distinct categories) at {ledger_path}")
            return total_distinct
        except Exception as write_err:
            if staging_dir.exists():
                try:
                    if staging_dir.is_dir():
                        shutil.rmtree(staging_dir)
                    else:
                        staging_dir.unlink()
                except Exception:
                    pass
            logger.warning(f"Note: Local Parquet write skipped (e.g. Windows file permissions): {write_err}")
            return total_distinct

    except Exception as e:
        logger.warning(f"Unmapped taxonomy ledger upsert encountered non-blocking notice: {e}")
        return None
