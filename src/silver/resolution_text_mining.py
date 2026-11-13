"""
Silver Layer: Resolution Text Mining, Semantic Outcome Classification & Phantom Closure Auditing.
Exposes administrative dismissals masquerading as physical remediation ("Phantom Closures")
via deterministic, distributed PySpark Catalyst regular expressions.

100% Pure PySpark (Zero Python @udf, Zero toPandas Driver Serialization).
Author: Group 13
"""

import logging
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    count as spark_count,
    first as spark_first,
    greatest,
    least,
    lit,
    max as spark_max,
    min as spark_min,
    regexp_replace,
    round as spark_round,
    trim,
    upper,
    when,
)

from src.silver.config import (
    MAX_FIELD_REMEDIATION_HOURS,
    MIN_FIELD_REMEDIATION_HOURS,
    UNCLASSIFIED_RESOLUTIONS_LEDGER_PATH,
)

logger = logging.getLogger("resolution_text_mining")

# -----------------------------------------------------------------------------
# Hardened V2 Regular Expression Dictionaries (Evaluated on Clean Uppercase Text)
# -----------------------------------------------------------------------------

# 1. Duplicate & Consolidation (Supremacy: avoids double-counting work orders)
REGEX_DUPLICATE = (
    r"\b(?:DUPLICATE|PREVIOUSLY REPORTED|ALREADY (?:BEEN )?REPORTED|ALREADY CLOSED|"
    r"REFER TO COMPLAINT|MASTER RECORD)\b"
)

# 2. Operational Handoff & Deferral (Ticket exited agency jurisdiction/perimeter)
REGEX_DEFERRED = (
    r"\b(?:REFERRED TO|TRANSFERRED TO|WILL BE INSPECTED|SCHEDULED FOR (?:FUTURE )?MAINTENANCE|"
    r"PENDING FUTURE|FORWARDED TO)\b"
)

# 3. Physical Access Denial (Requires explicit impediment qualifier; bare 'ACCESS' forbidden)
REGEX_ACCESS_DENIED = (
    r"\b(?:(?:UNABLE|FAILED|COULD\s*NOT|COULDN'?T|NOT ABLE)\s+(?:TO\s+)?(?:GAIN\s+)?ACCESS|"
    r"DENIED ACCESS|NO ACCESS (?:COULD|WAS)|ACCESS (?:WAS )?DENIED|NO ONE (?:WAS )?HOME|"
    r"PREMISES (?:WAS )?LOCKED|CANNOT BE LOCATED|UNABLE TO LOCATE|COULDN'?T LOCATE)\b"
)

# 4. Explicit Negations of Action / Uncorrected Traps (Must catch BEFORE affirmative action)
REGEX_NEGATED_ACTION = (
    r"\b(?:(?:SUMMONS|VIOLATION(?:S)?|TICKET(?:S)?)\s+(?:WAS\s+|WERE\s+)?NOT\s+ISSUED|NOT\s+(?:BEEN\s+)?ISSUED|"
    r"NOT\s+CORRECTED|NOT\s+REPAIRED|NOT\s+RESOLVED|NOT\s+REMEDIED|UNABLE\s+TO\s+FIX|"
    r"FAILED\s+TO\s+COMPLY|NO\s+ACTION\s+(?:WAS\s+)?TAKEN|ACTION\s+WAS\s+NOT\s+NECESSARY|"
    r"NOT\s+WITHIN\s+JURISDICTION|OUTSIDE\s+(?:NYC\s+)?(?:MUNICIPAL\s+)?JURISDICTION|"
    r"NOT\s+RESPONSIBLE|RESOLVED\s+PRIOR|UNFOUNDED|NO\s+CITY\s+AGENCY\s+ACTION|"
    r"DIDN'?T\s+OBSERVE|DID\s+NOT\s+OBSERVE|NOT\s+REQUIRED)\b"
)

# 5. Administrative Dismissal (General lack of jurisdiction, unfounded, condition cleared before arrival)
REGEX_ADMIN_DISMISSAL = (
    r"\b(?:NO\s+ACTION|NOT\s+WITHIN\s+JURISDICTION|OUTSIDE\s+(?:NYC\s+)?(?:MUNICIPAL\s+)?JURISDICTION|"
    r"NOT\s+RESPONSIBLE|RESOLVED\s+PRIOR|UNFOUNDED|NO\s+CITY\s+AGENCY\s+ACTION|ACTION\s+(?:WAS\s+)?NOT\s+NECESSARY|"
    r"OUT\s+OF\s+JURISDICTION|NOT\s+REQUIRED|AVAILABLE\s+ON\s+(?:THE\s+)?(?:DEPARTMENT|WEBSITE)|"
    r"SEE\s+NOTES|TEMP(?:ORARY)?\s+CONDITION|CONTACTED\s+THE\s+MANAGING\s+AGENT|"
    r"DID\s+NOT\s+ACCEPT\s+ASSISTANCE|POLICE\s+ACTION\s+WAS\s+NOT\s+NECESSARY)\b"
)

# 6. Negative Inspection / Code Compliance (Official inspection verified code compliance)
REGEX_NO_VIOLATION = (
    r"\b(?:DIDN'?T\s+OBSERVE|DID\s+NOT\s+OBSERVE|NO\s+(?:[A-Z]+\s+)?VIOLATION(?:S)?|NOT\s+OBSERVED|"
    r"OBSERVED\s+NO\s+(?:EVIDENCE|[A-Z]+\s+VIOLATION)|NO\s+EVIDENCE|NOT\s+FOUND|NOT\s+IN\s+VIOLATION|"
    r"NO\s+CONDITION(?:S)?\s+OBSERVED|FOUND\s+COMPLIANT|FOUND\s+NO\s+CONDITION|DID\s+NOT\s+VIOLATE|"
    r"NO\s+VIOLATION\s+WAS\s+WARRANTED|CONDITION\s+WAS\s+NOT\s+FOUND)\b"
)

# 7. Affirmative Action Remediation (Requires non-negated physical or enforcement action)
REGEX_ACTION_TAKEN = (
    r"\b(?:(?:SUMMONS|VIOLATION|TICKET)\s+(?:WAS\s+)?ISSUED|ISSUED\s+(?:A\s+)?(?:SUMMONS|VIOLATION|TICKET)|"
    r"REPAIRED|CORRECTED|CLEANED|REMOVED|SERVICED|SATISFIED|REMEDIED|ARREST(?:ED)?|WORK\s+COMPLETED|"
    r"(?:TOOK|TAKEN)\s+ACTION(?:\s+TO\s+(?:FIX|REMEDY))?|ACTION\s+WAS\s+TAKEN|FIX(?:ED|ING)?|"
    r"CLEANED\s+(?:THE\s+)?LOCATION|COMPLETED\s+THE\s+REQUESTED\s+WORK\s+ORDER|"
    r"REPAIRED\s+THE\s+POTHOLE|REMEDY\s+THE\s+CONDITION)\b"
)


def clean_resolution_description_column(raw_col=None):
    """
    Construct a PySpark Catalyst expression that cleans mojibake formatting,
    collapses multiple whitespaces, and trims the resolution description string.
    100% pure PySpark SQL expression.
    """
    if raw_col is None:
        raw_col = col("resolution_description")
    coalesced = F.coalesce(raw_col, lit(""))
    clean_step1 = regexp_replace(coalesced, r"Ã‚Â°", "°")
    clean_step2 = regexp_replace(clean_step1, r"â€™", "'")
    clean_step3 = regexp_replace(clean_step2, r"â€œ|â€\x9d|â€", '"')
    clean_step4 = regexp_replace(clean_step3, r"\s+", " ")
    return trim(clean_step4)


def classify_resolution_outcomes(df: DataFrame) -> DataFrame:
    """
    Classify 311 resolution outcomes into 8 mutually exclusive categories
    using strict sequential precedence and negation-safe regular expressions.

    Categories:
    - OPEN_TICKET: Ticket is currently open or unclosed.
    - UNSTATED_OR_EMPTY: Closed ticket lacking resolution narrative (data governance defect).
    - DUPLICATE_CONSOLIDATED: Ticket consolidated into master work order / duplicate report.
    - DEFERRED_PENDING_ACTION: Referred or forwarded to another agency/future maintenance.
    - ACCESS_DENIED_UNABLE_TO_LOCATE: Agency was unable to gain physical access or locate condition.
    - ADMINISTRATIVE_DISMISSAL: Unfounded, outside municipal jurisdiction, or no action needed.
    - NO_VIOLATION_FOUND: Official field inspection confirmed conditions complied with city code.
    - ACTION_TAKEN_REMEDIED: Physical repair, remediation, summons issued, or arrest made.
    - UNCLASSIFIED_OR_GENERIC: Novel boilerplate or unmapped text (routed to observability ledger).

    Also attaches:
    - is_field_verified (Boolean): True iff physical remediation or verified compliance occurred.
    - is_phantom_closure (Boolean): True iff closed ticket was resolved without verified field remediation.
    - field_remediation_hours (Double): Duration in hours, bounded by [0.0, 720.0] hours.
    """
    cols = set(df.columns)
    raw_desc = col("resolution_description") if "resolution_description" in cols else lit(None).cast("string")

    # 1. Clean resolution text
    clean_text = clean_resolution_description_column(raw_desc)
    clean_upper = upper(clean_text)

    # 2. Closure condition
    if "is_ticket_closed" in cols:
        is_closed = col("is_ticket_closed") == True
    elif "closed_at_utc" in cols:
        is_closed = col("closed_at_utc").isNotNull()
    else:
        is_closed = lit(False)

    # 3. Chained Catalyst When Expressions (Strict Top-Down Precedence)
    category_expr = (
        when(~is_closed, lit("OPEN_TICKET"))
        .when(clean_text == "", lit("UNSTATED_OR_EMPTY"))
        .when(clean_upper.rlike(REGEX_DUPLICATE), lit("DUPLICATE_CONSOLIDATED"))
        .when(clean_upper.rlike(REGEX_DEFERRED), lit("DEFERRED_PENDING_ACTION"))
        .when(clean_upper.rlike(REGEX_ACCESS_DENIED), lit("ACCESS_DENIED_UNABLE_TO_LOCATE"))
        # Negation Traps: Must evaluate before affirmative action
        .when(
            clean_upper.rlike(REGEX_NEGATED_ACTION),
            when(clean_upper.rlike(REGEX_NO_VIOLATION), lit("NO_VIOLATION_FOUND"))
            .otherwise(lit("ADMINISTRATIVE_DISMISSAL"))
        )
        .when(clean_upper.rlike(REGEX_ADMIN_DISMISSAL), lit("ADMINISTRATIVE_DISMISSAL"))
        .when(clean_upper.rlike(REGEX_NO_VIOLATION), lit("NO_VIOLATION_FOUND"))
        .when(clean_upper.rlike(REGEX_ACTION_TAKEN), lit("ACTION_TAKEN_REMEDIED"))
        .otherwise(lit("UNCLASSIFIED_OR_GENERIC"))
    )

    df_classified = df.withColumn("clean_resolution_text", clean_text).withColumn(
        "resolution_outcome_category", category_expr
    )

    # 4. Attach Field Verification, Affirmative Phantom Closure, and Unclassified Flags
    # Strict Affirmative Semantic Identification (Guaranteed Statistical Purity):
    # Only explicit administrative non-remedial dismissals count as phantom closures.
    PHANTOM_CATEGORIES = [
        "ADMINISTRATIVE_DISMISSAL",
        "DUPLICATE_CONSOLIDATED",
        "DEFERRED_PENDING_ACTION",
        "ACCESS_DENIED_UNABLE_TO_LOCATE",
    ]
    VERIFIED_CATEGORIES = [
        "ACTION_TAKEN_REMEDIED",
        "NO_VIOLATION_FOUND",
    ]

    df_classified = (
        df_classified
        .withColumn(
            "is_field_verified",
            col("resolution_outcome_category").isin(VERIFIED_CATEGORIES)
        )
        .withColumn(
            "is_phantom_closure",
            is_closed & col("resolution_outcome_category").isin(PHANTOM_CATEGORIES)
        )
        .withColumn(
            "is_unclassified_resolution",
            is_closed & (col("resolution_outcome_category") == "UNCLASSIFIED_OR_GENERIC")
        )
    )

    # 5. Attach Bounded Field Remediation Duration (filtering clock-skew anomalies)
    if "created_at_utc" in cols and "closed_at_utc" in cols:
        raw_hours = (col("closed_at_utc").cast("long") - col("created_at_utc").cast("long")) / 3600.0
        df_classified = df_classified.withColumn(
            "field_remediation_hours",
            when(
                col("is_field_verified") &
                (raw_hours >= MIN_FIELD_REMEDIATION_HOURS) &
                (raw_hours <= MAX_FIELD_REMEDIATION_HOURS),
                spark_round(raw_hours, 2)
            ).otherwise(lit(None).cast("double"))
        )
    elif "resolution_duration_hours" in cols:
        raw_hours = col("resolution_duration_hours")
        df_classified = df_classified.withColumn(
            "field_remediation_hours",
            when(
                col("is_field_verified") &
                (raw_hours >= MIN_FIELD_REMEDIATION_HOURS) &
                (raw_hours <= MAX_FIELD_REMEDIATION_HOURS),
                spark_round(raw_hours, 2)
            ).otherwise(lit(None).cast("double"))
        )
    else:
        df_classified = df_classified.withColumn("field_remediation_hours", lit(None).cast("double"))

    return df_classified


def upsert_unclassified_resolutions_ledger(
    df: DataFrame,
    spark: SparkSession,
    ledger_path: Path = UNCLASSIFIED_RESOLUTIONS_LEDGER_PATH,
) -> Optional[int]:
    """
    Idempotent stateful template catalog upsert for unclassified resolution descriptions.
    Tracks novel boilerplates with first_seen_utc, last_seen_utc, and occurrence_count.
    Pure PySpark aggregation.
    """
    try:
        # Filter for unclassified non-empty closed templates
        df_unclass = df.filter(
            (col("is_ticket_closed") == True) &
            (col("resolution_outcome_category") == "UNCLASSIFIED_OR_GENERIC") &
            (col("clean_resolution_text") != "")
        )

        unclass_cnt = df_unclass.count()
        if unclass_cnt == 0:
            logger.info("Zero unclassified resolution templates found in current batch.")
            return 0

        # Aggregate batch catalog
        df_batch_catalog = (
            df_unclass.groupBy("agency", "clean_resolution_text")
            .agg(
                spark_count("unique_key").alias("batch_count"),
                spark_min("created_at_utc").alias("first_seen_utc"),
                spark_max("created_at_utc").alias("last_seen_utc"),
                spark_first("unique_key").alias("sample_unique_key"),
            )
            .withColumn("review_status", lit("NEEDS_REVIEW"))
            .withColumn("suggested_category", lit(None).cast("string"))
            .withColumn("review_notes", lit(None).cast("string"))
            .withColumn("reviewed_at_utc", lit(None).cast("timestamp"))
        )

        ledger_path.parent.mkdir(parents=True, exist_ok=True)

        if ledger_path.exists():
            try:
                df_existing = spark.read.parquet(str(ledger_path))

                # Handle legacy schema migrations if necessary
                if "first_created_utc" in df_existing.columns and "first_seen_utc" not in df_existing.columns:
                    df_existing = (
                        df_existing
                        .withColumnRenamed("first_created_utc", "first_seen_utc")
                        .withColumnRenamed("last_created_utc", "last_seen_utc")
                    )

                # Add workflow governance columns if missing in historical ledger
                if "review_status" not in df_existing.columns:
                    df_existing = df_existing.withColumn("review_status", lit("NEEDS_REVIEW"))
                if "suggested_category" not in df_existing.columns:
                    df_existing = df_existing.withColumn("suggested_category", lit(None).cast("string"))
                if "review_notes" not in df_existing.columns:
                    df_existing = df_existing.withColumn("review_notes", lit(None).cast("string"))
                if "reviewed_at_utc" not in df_existing.columns:
                    df_existing = df_existing.withColumn("reviewed_at_utc", lit(None).cast("timestamp"))

                df_merged = (
                    df_existing.join(
                        df_batch_catalog,
                        on=["agency", "clean_resolution_text"],
                        how="outer",
                    )
                    .select(
                        col("agency"),
                        col("clean_resolution_text"),
                        (
                            F.coalesce(df_existing["occurrence_count"], lit(0)) +
                            F.coalesce(df_batch_catalog["batch_count"], lit(0))
                        ).cast("integer").alias("occurrence_count"),
                        F.coalesce(
                            df_batch_catalog["sample_unique_key"],
                            df_existing["sample_unique_key"]
                        ).alias("sample_unique_key"),
                        least(
                            F.coalesce(df_existing["first_seen_utc"], df_batch_catalog["first_seen_utc"]),
                            F.coalesce(df_batch_catalog["first_seen_utc"], df_existing["first_seen_utc"]),
                        ).alias("first_seen_utc"),
                        greatest(
                            F.coalesce(df_existing["last_seen_utc"], df_batch_catalog["last_seen_utc"]),
                            F.coalesce(df_batch_catalog["last_seen_utc"], df_existing["last_seen_utc"]),
                        ).alias("last_seen_utc"),
                        # Active Governance: preserve engineer review_status or default to NEEDS_REVIEW
                        F.coalesce(df_existing["review_status"], lit("NEEDS_REVIEW")).alias("review_status"),
                        df_existing["suggested_category"].alias("suggested_category"),
                        df_existing["review_notes"].alias("review_notes"),
                        df_existing["reviewed_at_utc"].alias("reviewed_at_utc"),
                    )
                    .orderBy(col("occurrence_count").desc())
                )
            except Exception as e:
                logger.warning(f"Could not read existing ledger at {ledger_path}: {e}. Reinitializing catalog.")
                df_merged = df_batch_catalog.select(
                    col("agency"),
                    col("clean_resolution_text"),
                    col("batch_count").cast("integer").alias("occurrence_count"),
                    col("sample_unique_key"),
                    col("first_seen_utc"),
                    col("last_seen_utc"),
                    col("review_status"),
                    col("suggested_category"),
                    col("review_notes"),
                    col("reviewed_at_utc"),
                ).orderBy(col("occurrence_count").desc())
        else:
            df_merged = df_batch_catalog.select(
                col("agency"),
                col("clean_resolution_text"),
                col("batch_count").cast("integer").alias("occurrence_count"),
                col("sample_unique_key"),
                col("first_seen_utc"),
                col("last_seen_utc"),
                col("review_status"),
                col("suggested_category"),
                col("review_notes"),
                col("reviewed_at_utc"),
            ).orderBy(col("occurrence_count").desc())

        # Write merged catalog to Parquet
        try:
            df_merged.write.mode("overwrite").parquet(str(ledger_path))
            total_templates = df_merged.count()
            logger.info(f"Upserted template observability ledger ({total_templates} distinct templates) at {ledger_path}")
            return total_templates
        except Exception as write_err:
            logger.warning(f"Note: Local Parquet write skipped (e.g. Windows file permissions): {write_err}")
            return unclass_cnt

    except Exception as e:
        logger.warning(f"Template observability ledger upsert encountered non-blocking notice: {e}")
        return None
