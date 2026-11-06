"""
Silver Layer: Spatial Feature Cleaning and Street Canonicalization.
Provides pure, modular PySpark functions to standardize BBLs, house numbers,
street suffixes, directional prefixes, highway corridors, and cross-street intersections.
Author: Group 13
"""

from typing import Union
from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import (
    col,
    concat,
    greatest,
    least,
    lit,
    regexp_extract,
    regexp_replace,
    trim,
    upper,
    when,
)

# Standard invalid string placeholders treated as null
INVALID_PLACEHOLDERS = ["", "NAN", "NONE", "NULL", "UNSPECIFIED", "UNKNOWN", "N/A"]


def _is_valid_street_expr(col_expr: Column) -> Column:
    """Check if a street column expression contains a non-empty, non-placeholder string."""
    s = upper(trim(col_expr.cast("string")))
    return col_expr.isNotNull() & ~s.isin(INVALID_PLACEHOLDERS)


def canonicalize_street_name_expr(col_or_name: Union[str, Column]) -> Column:
    """
    Construct a PySpark Column expression that canonicalizes a street name or address string:
    1. Trims, converts to uppercase.
    2. Preserves Queens hyphenated house numbers (e.g., '100-02', '14-19') while standardizing spacing.
    3. Removes non-semantic punctuation (periods, commas, quotes, slashes, hash signs).
    4. Replaces isolated hyphens while leaving digit-hyphen-digit numbers intact.
    5. Preserves distinct street names containing directionals (e.g. 'WEST END AVE', 'EAST END AVE').
    6. Canonicalizes leading directional prefixes (NORTH -> N, SOUTH -> S, EAST -> E, WEST -> W).
    7. Canonicalizes standard NYC street suffixes (AVENUE -> AVE, STREET -> ST, etc.).
    8. Collapses redundant whitespace and maps invalid placeholders to SQL NULL.
    """
    c = col(col_or_name) if isinstance(col_or_name, str) else col_or_name
    c = upper(trim(c.cast("string")))

    # 1. Normalize spaces around hyphens in Queens-style house numbers: '100 - 02' -> '100-02'
    c = regexp_replace(c, r"(\d+)\s*-\s*(\d+)", "$1-$2")

    # 2. Replace punctuation with spaces (excluding hyphens for now)
    c = regexp_replace(c, r"[.,\/#;\"\']", " ")

    # 3. Replace hyphens UNLESS they are between two digits (e.g. preserve '100-02', strip 'CROSS-ISLAND')
    c = regexp_replace(c, r"(?<!\d)-|-(?!\d)", " ")

    # 4. Protect specific iconic NYC street names containing directionals:
    # 'WEST END' / 'W END' -> 'WEST END', 'EAST END' / 'E END' -> 'EAST END'
    c = regexp_replace(c, r"\b(W|WEST)\s+END\b", "WEST END")
    c = regexp_replace(c, r"\b(E|EAST)\s+END\b", "EAST END")

    # 5. Canonicalize leading directional prefixes (at start of string or immediately after house number)
    # E.g. 'NORTH BROADWAY' -> 'N BROADWAY', '350 WEST 42ND ST' -> '350 W 42ND ST'
    c = regexp_replace(c, r"^NORTH\b", "N")
    c = regexp_replace(c, r"^SOUTH\b", "S")
    c = regexp_replace(c, r"^EAST\b(?!\s+END\b)", "E")
    c = regexp_replace(c, r"^WEST\b(?!\s+END\b)", "W")

    c = regexp_replace(c, r"^(\d+[\w\-]*\s+)NORTH\b", "$1N")
    c = regexp_replace(c, r"^(\d+[\w\-]*\s+)SOUTH\b", "$1S")
    c = regexp_replace(c, r"^(\d+[\w\-]*\s+)EAST\b(?!\s+END\b)", "$1E")
    c = regexp_replace(c, r"^(\d+[\w\-]*\s+)WEST\b(?!\s+END\b)", "$1W")

    # 6. Canonicalize standard NYC street suffix variants
    c = regexp_replace(c, r"\b(AVENUE|AV)\b", "AVE")
    c = regexp_replace(c, r"\b(STREET|STR)\b", "ST")
    c = regexp_replace(c, r"\b(BOULEVARD|BOUL)\b", "BLVD")
    c = regexp_replace(c, r"\b(ROAD)\b", "RD")
    c = regexp_replace(c, r"\b(DRIVE)\b", "DR")
    c = regexp_replace(c, r"\b(PLACE)\b", "PL")
    c = regexp_replace(c, r"\b(COURT)\b", "CT")
    c = regexp_replace(c, r"\b(TERRACE)\b", "TER")
    c = regexp_replace(c, r"\b(LANE)\b", "LN")
    c = regexp_replace(c, r"\b(PARKWAY)\b", "PKWY")
    c = regexp_replace(c, r"\b(EXPRESSWAY)\b", "EXPWY")
    c = regexp_replace(c, r"\b(HIGHWAY)\b", "HWY")
    c = regexp_replace(c, r"\b(TURNPIKE)\b", "TPKE")

    # 7. Collapse multiple spaces into one space and trim
    c = regexp_replace(c, r"\s+", " ")
    c = trim(c)

    return when(c.isin(INVALID_PLACEHOLDERS), lit(None).cast("string")).otherwise(c)


def normalize_bbl_and_house_numbers(df: DataFrame) -> DataFrame:
    """
    Standardize BBL (Borough-Block-Lot) tax lot identifiers and extract house numbers.
    - Validates BBL to strict 10-digit format (Borough digit 1-5, Block 5 digits, Lot 4 digits).
    - Regex-extracts the leading house number from incident_address (e.g., '100-02', '350', '12A').
    - Emits boolean quality indicators: has_valid_bbl, has_valid_house_number.
    """
    cols = set(df.columns)

    if "bbl" in cols:
        # Strip trailing .0 (from float conversions) and whitespace
        bbl_raw = regexp_replace(trim(col("bbl").cast("string")), r"\.0$", "")
        valid_bbl_expr = when(bbl_raw.rlike(r"^[1-5]\d{9}$"), bbl_raw).otherwise(lit(None).cast("string"))
        df = df.withColumn("clean_bbl", valid_bbl_expr)
    else:
        df = df.withColumn("clean_bbl", lit(None).cast("string"))

    if "incident_address" in cols:
        addr_str = upper(trim(col("incident_address").cast("string")))
        # Standardize spacing around hyphens: '100 - 02' -> '100-02'
        addr_str = regexp_replace(addr_str, r"(\d+)\s*-\s*(\d+)", "$1-$2")
        extracted_hn = regexp_extract(addr_str, r"^(\d+[\w\-]*)", 1)
        valid_hn_expr = when(
            extracted_hn.isNotNull() & (extracted_hn != "") & ~extracted_hn.isin(INVALID_PLACEHOLDERS),
            extracted_hn,
        ).otherwise(lit(None).cast("string"))
        df = df.withColumn("house_num_extracted", valid_hn_expr)
    else:
        df = df.withColumn("house_num_extracted", lit(None).cast("string"))

    # Explicit Boolean Data-Quality Indicators
    df = df.withColumn("has_valid_bbl", col("clean_bbl").isNotNull())
    df = df.withColumn("has_valid_house_number", col("house_num_extracted").isNotNull())

    return df


def standardize_cross_streets(df: DataFrame) -> DataFrame:
    """
    Canonicalize intersection and cross-street pairs.
    - Validates and filters candidate street values (rejecting UNKNOWN, NAN, NONE, etc.) before fallback selection.
    - Falls back from intersection_street_1/2 to cross_street_1/2 when primary is missing or invalid.
    - Standardizes street names, directionals, and suffixes via canonicalize_street_name_expr.
    - Enforces lexicographical order: street_a = least(s1, s2), street_b = greatest(s1, s2).
    - Ensures 'BROADWAY & W 42 ST' matches 'W 42 ST & BROADWAY' identically.
    - Avoids temporary column collision by evaluating expressions directly in Catalyst.
    """
    cols = set(df.columns)

    # 1. Resolve candidate for street 1 (filter out placeholders before fallback)
    if "intersection_street_1" in cols and "cross_street_1" in cols:
        s1_raw = when(
            _is_valid_street_expr(col("intersection_street_1")),
            col("intersection_street_1"),
        ).when(
            _is_valid_street_expr(col("cross_street_1")),
            col("cross_street_1"),
        ).otherwise(lit(None).cast("string"))
    elif "intersection_street_1" in cols:
        s1_raw = when(
            _is_valid_street_expr(col("intersection_street_1")),
            col("intersection_street_1"),
        ).otherwise(lit(None).cast("string"))
    elif "cross_street_1" in cols:
        s1_raw = when(
            _is_valid_street_expr(col("cross_street_1")),
            col("cross_street_1"),
        ).otherwise(lit(None).cast("string"))
    else:
        s1_raw = lit(None).cast("string")

    # 2. Resolve candidate for street 2 (filter out placeholders before fallback)
    if "intersection_street_2" in cols and "cross_street_2" in cols:
        s2_raw = when(
            _is_valid_street_expr(col("intersection_street_2")),
            col("intersection_street_2"),
        ).when(
            _is_valid_street_expr(col("cross_street_2")),
            col("cross_street_2"),
        ).otherwise(lit(None).cast("string"))
    elif "intersection_street_2" in cols:
        s2_raw = when(
            _is_valid_street_expr(col("intersection_street_2")),
            col("intersection_street_2"),
        ).otherwise(lit(None).cast("string"))
    elif "cross_street_2" in cols:
        s2_raw = when(
            _is_valid_street_expr(col("cross_street_2")),
            col("cross_street_2"),
        ).otherwise(lit(None).cast("string"))
    else:
        s2_raw = lit(None).cast("string")

    # 3. Canonicalize candidate street expressions directly (Zero temporary column collision)
    s1_clean = canonicalize_street_name_expr(s1_raw)
    s2_clean = canonicalize_street_name_expr(s2_raw)

    # 4. Canonicalize raw street_name (used for Blockface route)
    if "street_name" in cols:
        df = df.withColumn("clean_street_name", canonicalize_street_name_expr("street_name"))
    else:
        df = df.withColumn("clean_street_name", lit(None).cast("string"))

    # 5. Lexicographical sorting: street_a = least, street_b = greatest
    valid_intersection = (
        s1_clean.isNotNull()
        & s2_clean.isNotNull()
        & (s1_clean != s2_clean)
    )

    df = df.withColumn(
        "clean_cross_street_a",
        when(valid_intersection, least(s1_clean, s2_clean)).otherwise(lit(None).cast("string")),
    )
    df = df.withColumn(
        "clean_cross_street_b",
        when(valid_intersection, greatest(s1_clean, s2_clean)).otherwise(lit(None).cast("string")),
    )
    df = df.withColumn("has_valid_cross_streets", valid_intersection)

    return df


def normalize_highway_corridors(df: DataFrame) -> DataFrame:
    """
    Standardize highway corridor attributes (bridge_highway_name, bridge_highway_segment, bridge_highway_direction).
    Defensive gate: requires both highway name and verified highway segment.
    Prevents assigning records with empty segments to overly broad corridor keys.
    """
    cols = set(df.columns)

    if "bridge_highway_name" in cols:
        hwy_name = canonicalize_street_name_expr("bridge_highway_name")
    else:
        hwy_name = lit(None).cast("string")

    if "bridge_highway_segment" in cols:
        seg_str = upper(trim(col("bridge_highway_segment").cast("string")))
        # Normalize segment formatting: replace hyphens/slashes with space, collapse whitespace
        seg_str = regexp_replace(seg_str, r"[.,\-\/]", " ")
        seg_str = regexp_replace(seg_str, r"\s+", " ")
        seg_clean = when(
            seg_str.isin(INVALID_PLACEHOLDERS),
            lit(None).cast("string"),
        ).otherwise(trim(seg_str))
    else:
        seg_clean = lit(None).cast("string")

    if "bridge_highway_direction" in cols:
        dir_str = upper(trim(col("bridge_highway_direction").cast("string")))
        # Canonicalize common directionals
        dir_clean = (
            when(dir_str.isin(["NORTHBOUND", "NORTH", "NB", "N/B", "N"]), lit("NB"))
            .when(dir_str.isin(["SOUTHBOUND", "SOUTH", "SB", "S/B", "S"]), lit("SB"))
            .when(dir_str.isin(["EASTBOUND", "EAST", "EB", "E/B", "E"]), lit("EB"))
            .when(dir_str.isin(["WESTBOUND", "WEST", "WB", "W/B", "W"]), lit("WB"))
            .when(dir_str.isin(INVALID_PLACEHOLDERS), lit(None).cast("string"))
            .otherwise(dir_str)
        )
    else:
        dir_clean = lit(None).cast("string")

    df = df.withColumn("clean_highway_name", hwy_name)
    df = df.withColumn("clean_highway_segment", seg_clean)
    df = df.withColumn("clean_highway_direction", dir_clean)

    # Valid highway corridor requires both name AND segment
    has_valid_hwy = col("clean_highway_name").isNotNull() & col("clean_highway_segment").isNotNull()
    df = df.withColumn("has_valid_highway", has_valid_hwy)

    return df


def canonicalize_fallback_addresses(df: DataFrame) -> DataFrame:
    """
    Standardize incident_address for address-fallback routing when BBL is unavailable.
    Applies suffix and directional canonicalization while preserving Queens hyphenated house numbers.
    Emits boolean indicator: has_valid_fallback_address.
    """
    cols = set(df.columns)

    if "incident_address" in cols:
        df = df.withColumn("clean_incident_address", canonicalize_street_name_expr("incident_address"))
    else:
        df = df.withColumn("clean_incident_address", lit(None).cast("string"))

    if "landmark" in cols:
        df = df.withColumn("clean_landmark", canonicalize_street_name_expr("landmark"))
    else:
        df = df.withColumn("clean_landmark", lit(None).cast("string"))

    if "park_facility_name" in cols:
        df = df.withColumn("clean_park_facility_name", canonicalize_street_name_expr("park_facility_name"))
    else:
        df = df.withColumn("clean_park_facility_name", lit(None).cast("string"))

    # Explicit Boolean Indicator
    df = df.withColumn("has_valid_fallback_address", col("clean_incident_address").isNotNull())

    return df


def normalize_all_spatial_features(df: DataFrame) -> DataFrame:
    """
    Pipeline orchestrator that applies all prerequisite spatial cleaning functions in sequence.
    """
    df = normalize_bbl_and_house_numbers(df)
    df = standardize_cross_streets(df)
    df = normalize_highway_corridors(df)
    df = canonicalize_fallback_addresses(df)
    return df
