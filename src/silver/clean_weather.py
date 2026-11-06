"""
Silver Layer: Open-Meteo ERA5-Land Weather Cleaning, Physical Bound Validation,
Atmospheric Law Verification, and Sensor Anomaly Ledgering with Apache Spark.
Filters corrupt/sentinel records, applies physical sanity bounds, tags extreme weather events,
masks corrupted fields to NULL without row loss, builds chronological rolling window metrics via PySpark Window functions,
and maintains a complete forensic Audit Ledger.
Author: Group 13
"""

import functools
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    array,
    array_compact,
    coalesce,
    col,
    concat,
    concat_ws,
    date_format,
    isnan,
    lit,
    max as spark_max,
    min as spark_min,
    round as spark_round,
    row_number,
    sum as spark_sum,
    to_timestamp,
    trim,
    when,
)
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.storagelevel import StorageLevel

from src.silver.config import (
    BRONZE_DIR,
    PROJECT_ROOT,
    QUARANTINE_DIR,
    SENSOR_ANOMALY_LEDGER_PATH,
    SILVER_DIR,
    VALID_WMO_WEATHER_CODES,
    WEATHER_EXTREME_THRESHOLDS,
    WEATHER_HARD_LIMITS,
)
from src.utils.spark import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("clean_weather")

LEDGER_SCHEMA = StructType([
    StructField("timestamp_utc", TimestampType(), True),
    StructField("grid_id", StringType(), True),
    StructField("field_name", StringType(), True),
    StructField("raw_value", DoubleType(), True),
    StructField("rule_violated", StringType(), True),
    StructField("reason", StringType(), True),
    StructField("action_taken", StringType(), True),
    StructField("masked_at_utc", StringType(), True),
])


def quarantine_unkeyed_weather(
    df_raw: DataFrame,
    processed_at_utc: str,
    quarantine_dir: Path,
) -> Tuple[DataFrame, int]:
    """
    Evaluate row-level primary keys (grid_id, time), parse UTC timestamps,
    and route unkeyed or corrupt observations to the quarantine table via pure PySpark.
    """
    cols = df_raw.columns

    cond_missing_grid = (
        col("grid_id").isNull()
        | trim(col("grid_id")).isin(["", "nan", "None", "NONE", "NAN", "null", "NULL"])
        if "grid_id" in cols
        else lit(True)
    )
    cond_missing_time = (
        col("time").isNull()
        | trim(col("time")).isin(["", "nan", "None", "NONE", "NAN", "null", "NULL"])
        if "time" in cols
        else lit(True)
    )
    parsed_ts = to_timestamp(col("time")) if "time" in cols else lit(None)
    cond_invalid_ts = (~cond_missing_time) & parsed_ts.isNull()
    cond_corrupt_lineage = (
        (col("_batch_id").isNull() | trim(col("_batch_id")).isin(["", "nan", "None", "<NA>"]))
        if "_batch_id" in cols
        else lit(False)
    )

    quarantine_mask = cond_missing_grid | cond_missing_time | cond_invalid_ts | cond_corrupt_lineage

    quarantine_reasons = array_compact(
        array(
            when(cond_missing_grid, lit("MISSING_GRID_ID")),
            when(cond_missing_time, lit("MISSING_TIME_KEY")),
            when(cond_invalid_ts, lit("INVALID_TIMESTAMP_FORMAT")),
            when(cond_corrupt_lineage, lit("CORRUPTED_INGESTION_METADATA")),
        )
    )

    df_evaluated = (
        df_raw.withColumn("_quarantine_reason", concat_ws("; ", quarantine_reasons))
        .withColumn("_is_quarantined", quarantine_mask)
        .withColumn("timestamp_utc", parsed_ts)
    )

    df_evaluated.persist(StorageLevel.MEMORY_AND_DISK)

    df_quarantined = df_evaluated.filter(col("_is_quarantined") == True)
    quarantined_weather_rows = df_quarantined.count()

    if quarantined_weather_rows > 0:
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        q_path = quarantine_dir / "quarantined_weather.parquet"
        save_q = (
            df_quarantined.withColumn("quarantine_reason", col("_quarantine_reason"))
            .withColumn("_quarantined_at_utc", lit(processed_at_utc))
            .drop("_is_quarantined", "_quarantine_reason")
        )
        save_q.coalesce(1).write.mode("overwrite").parquet(str(q_path))
        logger.warning(f"Quarantined {quarantined_weather_rows:,} unkeyed/unparseable weather rows to {q_path}")

    df_valid = df_evaluated.filter(col("_is_quarantined") == False).drop("_is_quarantined", "_quarantine_reason")
    df_evaluated.unpersist()

    return df_valid, quarantined_weather_rows


def deduplicate_weather(df_valid: DataFrame, cols: List[str]) -> Tuple[DataFrame, int]:
    """
    Deduplicate valid weather records on (grid_id, timestamp_utc), retaining the latest ingestion.
    """
    before_dedup_count = df_valid.count()
    w_dedup = Window.partitionBy("grid_id", "timestamp_utc").orderBy(
        col("_ingested_at_utc").desc() if "_ingested_at_utc" in cols else col("timestamp_utc").desc()
    )
    df_clean_spark = df_valid.withColumn("_rn", row_number().over(w_dedup)).filter(col("_rn") == 1).drop("_rn")
    df_clean_spark.persist(StorageLevel.MEMORY_AND_DISK)

    valid_count = df_clean_spark.count()
    duplicate_rows_dropped = before_dedup_count - valid_count

    if duplicate_rows_dropped > 0:
        logger.info(
            f"Deduplicated Bronze weather records: dropped {duplicate_rows_dropped:,} "
            f"duplicate observations on (grid_id, timestamp_utc)."
        )

    return df_clean_spark, duplicate_rows_dropped


def evaluate_sensor_rules_and_mask(
    df: DataFrame,
    processed_at_utc: str,
    anomaly_ledger_path: Path,
) -> Tuple[DataFrame, int, int]:
    """
    Evaluate atmospheric physical laws and sanity bounds across all meteorological sensor columns.
    Masks corrupted values to NULL (avoiding row loss) and logs forensic anomalies to the Audit Ledger.
    100% pure PySpark implementation.
    """
    cols = df.columns
    rules = []

    # A. Temperature Physical Bounds [-35°C, 45°C]
    if "temperature_2m" in cols:
        cond_temp = col("temperature_2m").isNotNull() & ~col("temperature_2m").between(
            WEATHER_HARD_LIMITS["temp_min"], WEATHER_HARD_LIMITS["temp_max"]
        )
        reason_temp = concat(lit("Value "), col("temperature_2m"), lit("°C exceeds NYC physical climate range [-35°C, 45°C]"))
        rules.append(("temperature_2m", "TEMPERATURE_OUT_OF_BOUNDS", cond_temp, reason_temp))

    # B. Apparent Temperature Bounds [-45°C, 55°C]
    if "apparent_temperature" in cols:
        cond_app = col("apparent_temperature").isNotNull() & ~col("apparent_temperature").between(
            WEATHER_HARD_LIMITS["apparent_temp_min"], WEATHER_HARD_LIMITS["apparent_temp_max"]
        )
        reason_app = concat(lit("Value "), col("apparent_temperature"), lit("°C exceeds heat index / wind chill range [-45°C, 55°C]"))
        rules.append(("apparent_temperature", "APPARENT_TEMPERATURE_OUT_OF_BOUNDS", cond_app, reason_app))

    # C. Atmospheric Thermodynamic Law: Dew Point <= Temperature
    if "dew_point_2m" in cols and "temperature_2m" in cols:
        cond_dew = (
            col("dew_point_2m").isNotNull()
            & col("temperature_2m").isNotNull()
            & (col("dew_point_2m") > (col("temperature_2m") + 0.1))
        )
        reason_dew = concat(
            lit("Dew point "),
            col("dew_point_2m"),
            lit("°C physically exceeds ambient temperature (temp="),
            col("temperature_2m"),
            lit("°C)"),
        )
        rules.append(("dew_point_2m", "DEW_POINT_EXCEEDS_TEMPERATURE", cond_dew, reason_dew))

    # D. Relative Humidity Bounds [0%, 100%]
    if "relative_humidity_2m" in cols:
        cond_rh = col("relative_humidity_2m").isNotNull() & ~col("relative_humidity_2m").between(
            WEATHER_HARD_LIMITS["humidity_min"], WEATHER_HARD_LIMITS["humidity_max"]
        )
        reason_rh = concat(lit("Humidity "), col("relative_humidity_2m"), lit("% must strictly be between 0% and 100%"))
        rules.append(("relative_humidity_2m", "RELATIVE_HUMIDITY_OUT_OF_BOUNDS", cond_rh, reason_rh))

    # E. Precipitation Bounds [0.0, 150.0 mm/hr]
    if "precipitation" in cols:
        cond_precip = col("precipitation").isNotNull() & (
            (col("precipitation") < WEATHER_HARD_LIMITS["precip_min"])
            | (col("precipitation") > WEATHER_HARD_LIMITS["precip_max"])
        )
        reason_precip = concat(
            lit("Precipitation "), col("precipitation"), lit(" mm/hr outside realistic physical limits [0, 150]")
        )
        rules.append(("precipitation", "PRECIPITATION_OUT_OF_BOUNDS", cond_precip, reason_precip))

    # F. Rain Bounds (rain >= 0 and rain <= precipitation)
    if "rain" in cols and "precipitation" in cols:
        cond_rain = col("rain").isNotNull() & (
            (col("rain") < 0.0)
            | (col("precipitation").isNotNull() & (col("rain") > (col("precipitation") + 0.05)))
        )
        reason_rain = concat(
            lit("Rain component "), col("rain"), lit(" mm exceeds total precipitation (precip="), col("precipitation"), lit(" mm)")
        )
        rules.append(("rain", "RAIN_EXCEEDS_TOTAL_PRECIP", cond_rain, reason_rain))

    # G. Snowfall and Snow Depth
    if "snowfall" in cols:
        cond_snow = col("snowfall").isNotNull() & (col("snowfall") < 0.0)
        reason_snow = concat(lit("Snowfall "), col("snowfall"), lit(" cm cannot be negative"))
        rules.append(("snowfall", "NEGATIVE_SNOWFALL", cond_snow, reason_snow))

    if "snow_depth" in cols:
        cond_depth = col("snow_depth").isNotNull() & (col("snow_depth") < 0.0)
        reason_depth = concat(lit("Snow depth "), col("snow_depth"), lit(" m cannot be negative"))
        rules.append(("snow_depth", "NEGATIVE_SNOW_DEPTH", cond_depth, reason_depth))

    # H. Sustained Wind Speed [0.0, 65.0 m/s]
    if "wind_speed_10m" in cols:
        cond_wind = col("wind_speed_10m").isNotNull() & ~col("wind_speed_10m").between(
            WEATHER_HARD_LIMITS["wind_speed_min"], WEATHER_HARD_LIMITS["wind_speed_max"]
        )
        reason_wind = concat(
            lit("Sustained wind speed "), col("wind_speed_10m"), lit(" m/s outside physical sanity bounds [0, 65]")
        )
        rules.append(("wind_speed_10m", "WIND_SPEED_OUT_OF_BOUNDS", cond_wind, reason_wind))

    # I. Aerodynamic Law: Wind Gust >= Sustained Wind Speed
    if "wind_gusts_10m" in cols and "wind_speed_10m" in cols:
        cond_gust = (
            col("wind_gusts_10m").isNotNull()
            & col("wind_speed_10m").isNotNull()
            & (col("wind_gusts_10m") < (col("wind_speed_10m") - 0.01))
        )
        reason_gust = concat(
            lit("Peak gust "),
            col("wind_gusts_10m"),
            lit(" m/s physically cannot be slower than sustained speed (sustained="),
            col("wind_speed_10m"),
            lit(" m/s)"),
        )
        rules.append(("wind_gusts_10m", "GUST_LESS_THAN_SUSTAINED_SPEED", cond_gust, reason_gust))

    # J. Wind Direction [0°, 360°]
    if "wind_direction_10m" in cols:
        cond_wdir = col("wind_direction_10m").isNotNull() & ~col("wind_direction_10m").between(
            WEATHER_HARD_LIMITS["wind_dir_min"], WEATHER_HARD_LIMITS["wind_dir_max"]
        )
        reason_wdir = concat(lit("Compass wind direction "), col("wind_direction_10m"), lit("° outside [0, 360]"))
        rules.append(("wind_direction_10m", "WIND_DIRECTION_OUT_OF_BOUNDS", cond_wdir, reason_wdir))

    # K. Surface Barometric Pressure [870, 1085 hPa]
    if "surface_pressure" in cols:
        cond_press = col("surface_pressure").isNotNull() & ~col("surface_pressure").between(
            WEATHER_HARD_LIMITS["pressure_min"], WEATHER_HARD_LIMITS["pressure_max"]
        )
        reason_press = concat(
            lit("Surface pressure "), col("surface_pressure"), lit(" hPa outside terrestrial extremes [870, 1085]")
        )
        rules.append(("surface_pressure", "SURFACE_PRESSURE_OUT_OF_BOUNDS", cond_press, reason_press))

    # L. World Meteorological Organization (WMO) Code Table 4677 Validity
    if "weather_code" in cols:
        cond_wmo = col("weather_code").isNotNull() & ~col("weather_code").isin(VALID_WMO_WEATHER_CODES)
        reason_wmo = concat(lit("Weather code "), col("weather_code"), lit(" not found in WMO Code Table 4677 standard"))
        rules.append(("weather_code", "INVALID_WMO_WEATHER_CODE", cond_wmo, reason_wmo))

    # M. Soil Temperature Bounds [-30°C, 50°C]
    if "soil_temperature_0_to_7cm" in cols:
        cond_soil_temp = col("soil_temperature_0_to_7cm").isNotNull() & ~col("soil_temperature_0_to_7cm").between(
            WEATHER_HARD_LIMITS["soil_temp_min"], WEATHER_HARD_LIMITS["soil_temp_max"]
        )
        reason_soil_temp = concat(
            lit("Soil temperature "), col("soil_temperature_0_to_7cm"), lit("°C outside physical limits [-30°C, 50°C]")
        )
        rules.append(("soil_temperature_0_to_7cm", "SOIL_TEMP_OUT_OF_BOUNDS", cond_soil_temp, reason_soil_temp))

    # N. Soil Moisture Bounds [0.0, 1.0 m3/m3]
    if "soil_moisture_0_to_7cm" in cols:
        cond_soil_moist = col("soil_moisture_0_to_7cm").isNotNull() & ~col("soil_moisture_0_to_7cm").between(
            WEATHER_HARD_LIMITS["soil_moist_min"], WEATHER_HARD_LIMITS["soil_moist_max"]
        )
        reason_soil_moist = concat(
            lit("Volumetric soil moisture "),
            col("soil_moisture_0_to_7cm"),
            lit(" m3/m3 outside saturation range [0, 1]"),
        )
        rules.append(("soil_moisture_0_to_7cm", "SOIL_MOISTURE_OUT_OF_BOUNDS", cond_soil_moist, reason_soil_moist))

    # O. Cloud Cover Bounds [0%, 100%]
    if "cloud_cover" in cols:
        cond_cloud = col("cloud_cover").isNotNull() & ~col("cloud_cover").between(
            WEATHER_HARD_LIMITS["cloud_cover_min"], WEATHER_HARD_LIMITS["cloud_cover_max"]
        )
        reason_cloud = concat(lit("Cloud cover "), col("cloud_cover"), lit("% outside range [0, 100]"))
        rules.append(("cloud_cover", "CLOUD_COVER_OUT_OF_BOUNDS", cond_cloud, reason_cloud))

    # 1. Build Sensor Anomaly Ledger via PySpark
    anomaly_dfs = []
    for target_col, rule_name, cond, reason in rules:
        df_viol = df.filter(cond).select(
            col("timestamp_utc"),
            col("grid_id"),
            lit(target_col).alias("field_name"),
            col(target_col).cast("double").alias("raw_value"),
            lit(rule_name).alias("rule_violated"),
            reason.alias("reason"),
            lit("MASKED_TO_NAN").alias("action_taken"),
            lit(processed_at_utc).alias("masked_at_utc"),
        )
        anomaly_dfs.append(df_viol)

    combined_ledger_spark = functools.reduce(lambda a, b: a.unionByName(b), anomaly_dfs) if anomaly_dfs else None

    # Write Sensor Anomaly Audit Ledger Parquet via pure PySpark
    anomaly_ledger_path.parent.mkdir(parents=True, exist_ok=True)
    if combined_ledger_spark is not None:
        combined_ledger_spark.persist(StorageLevel.MEMORY_AND_DISK)
        num_anomalies = combined_ledger_spark.count()
    else:
        num_anomalies = 0

    if num_anomalies > 0:
        combined_ledger_spark.coalesce(1).write.mode("overwrite").parquet(str(anomaly_ledger_path))
        combined_ledger_spark.unpersist()
        logger.warning(
            f"Sensor Anomaly Audit: Logged {num_anomalies:,} corrupt sensor readings to {anomaly_ledger_path}"
        )
    else:
        empty_ledger = df.sparkSession.createDataFrame([], schema=LEDGER_SCHEMA)
        empty_ledger.coalesce(1).write.mode("overwrite").parquet(str(anomaly_ledger_path))

    # 2. Attach Row-Level Observability Audit Notes BEFORE masking target_col
    df_masked = df
    audit_note_exprs = []
    for target_col, rule_name, cond, _ in rules:
        note_expr = when(cond, concat(lit(f"MASKED {target_col}="), col(target_col).cast("string"), lit(f" ({rule_name})")))
        audit_note_exprs.append(note_expr)

    notes_array = array_compact(array(*audit_note_exprs))
    audit_notes_str = concat_ws("; ", notes_array)
    df_masked = df_masked.withColumn(
        "quality_audit_notes",
        when(trim(audit_notes_str) != "", audit_notes_str).otherwise(lit(None)),
    )
    df_masked = df_masked.withColumn("has_corrupted_sensor_fields", col("quality_audit_notes").isNotNull())

    # Now mask violated sensor readings to SQL NULL (Catalyst skips NULL in window sums/min/max)
    for target_col, rule_name, cond, _ in rules:
        df_masked = df_masked.withColumn(target_col, when(cond, lit(None)).otherwise(col(target_col)))

    corrupted_fields_rows = df_masked.filter(col("has_corrupted_sensor_fields") == True).count()

    return df_masked, num_anomalies, corrupted_fields_rows


def compute_rolling_weather_windows(df: DataFrame) -> DataFrame:
    """
    Compute chronological rolling window aggregations (3h, 6h, 24h) via PySpark Window functions.
    Operates on masked sensor values so corrupted readings do not pollute rolling metrics.
    """
    # Sanitize floating-point NaNs to SQL NULL so Catalyst window aggregations skip missing values:
    for field in df.schema.fields:
        if isinstance(field.dataType, (DoubleType, FloatType)):
            df = df.withColumn(field.name, when(isnan(col(field.name)), lit(None)).otherwise(col(field.name)))

    df_window = df.withColumn("_ts_secs", col("timestamp_utc").cast("long"))

    w_grid_3 = Window.partitionBy("grid_id").orderBy("_ts_secs").rangeBetween(-2 * 3600, 0)
    w_grid_6 = Window.partitionBy("grid_id").orderBy("_ts_secs").rangeBetween(-5 * 3600, 0)
    w_grid_24 = Window.partitionBy("grid_id").orderBy("_ts_secs").rangeBetween(-23 * 3600, 0)

    if "precipitation" in df_window.columns:
        df_window = (
            df_window.withColumn("precip_past_3h_mm", spark_round(spark_sum("precipitation").over(w_grid_3), 2))
            .withColumn("precip_past_6h_mm", spark_round(spark_sum("precipitation").over(w_grid_6), 2))
            .withColumn("precip_past_24h_mm", spark_round(spark_sum("precipitation").over(w_grid_24), 2))
        )
    else:
        df_window = (
            df_window.withColumn("precip_past_3h_mm", lit(None).cast("double"))
            .withColumn("precip_past_6h_mm", lit(None).cast("double"))
            .withColumn("precip_past_24h_mm", lit(None).cast("double"))
        )

    if "wind_gusts_10m" in df_window.columns:
        df_window = df_window.withColumn(
            "max_wind_gust_past_6h_ms", spark_round(spark_max("wind_gusts_10m").over(w_grid_6), 2)
        )
    else:
        df_window = df_window.withColumn("max_wind_gust_past_6h_ms", lit(None).cast("double"))

    if "temperature_2m" in df_window.columns:
        df_window = (
            df_window.withColumn("min_temp_past_24h_c", spark_round(spark_min("temperature_2m").over(w_grid_24), 2))
            .withColumn("max_temp_past_24h_c", spark_round(spark_max("temperature_2m").over(w_grid_24), 2))
        )
    else:
        df_window = (
            df_window.withColumn("min_temp_past_24h_c", lit(None).cast("double"))
            .withColumn("max_temp_past_24h_c", lit(None).cast("double"))
        )

    return df_window.drop("_ts_secs")


def enrich_weather_attributes(df: DataFrame, processed_at_utc: str) -> DataFrame:
    """
    Standardize physical metrics and derive soft quality tags / extreme weather flags in PySpark.
    """
    cond_cold = col("temperature_2m") < WEATHER_EXTREME_THRESHOLDS["temp_extreme_cold"]
    cond_heat = col("temperature_2m") > WEATHER_EXTREME_THRESHOLDS["temp_extreme_heat"]
    cond_torrential = col("precipitation") >= WEATHER_EXTREME_THRESHOLDS["precip_torrential_hr"]
    cond_wind = (
        col("wind_gusts_10m") >= WEATHER_EXTREME_THRESHOLDS["wind_gust_severe"]
        if "wind_gusts_10m" in df.columns
        else lit(False)
    )

    df_enriched = df.withColumn(
        "is_extreme_weather",
        coalesce(cond_cold | cond_heat | cond_torrential | cond_wind, lit(False)),
    )
    df_enriched = df_enriched.withColumn("is_freezing", coalesce(col("temperature_2m") <= 0.0, lit(False)))
    df_enriched = df_enriched.withColumn("is_raining", coalesce(col("precipitation") > 0.0, lit(False)))

    if "soil_temperature_0_to_7cm" in df.columns:
        df_enriched = df_enriched.withColumn(
            "is_ground_frozen", coalesce(col("soil_temperature_0_to_7cm") < 0.0, lit(False))
        )
    else:
        df_enriched = df_enriched.withColumn("is_ground_frozen", lit(False))

    if "soil_moisture_0_to_7cm" in df.columns:
        df_enriched = df_enriched.withColumn(
            "is_soil_saturated", coalesce(col("soil_moisture_0_to_7cm") >= 0.40, lit(False))
        )
    else:
        df_enriched = df_enriched.withColumn("is_soil_saturated", lit(False))

    if "cloud_cover" in df.columns:
        df_enriched = df_enriched.withColumn("is_overcast", coalesce(col("cloud_cover") >= 80.0, lit(False)))
    else:
        df_enriched = df_enriched.withColumn("is_overcast", lit(False))

    for int_col in ["weather_code", "relative_humidity_2m", "wind_direction_10m", "cloud_cover"]:
        if int_col in df_enriched.columns:
            df_enriched = df_enriched.withColumn(int_col, col(int_col).cast("short"))

    df_enriched = df_enriched.withColumn("_silver_processed_at_utc", to_timestamp(lit(processed_at_utc)))
    df_enriched = df_enriched.withColumn("year", date_format(col("timestamp_utc"), "yyyy"))

    return df_enriched


def write_silver_weather(df_silver: DataFrame, silver_weather_dir: Path) -> int:
    """
    Write partitioned Parquet dataset to disk structured by grid_id and year.
    Uses pure PySpark with directory partitioning, keeping grid_id and year in Parquet schema.
    """
    silver_weather_dir.mkdir(parents=True, exist_ok=True)
    partitions = df_silver.select("grid_id", "year").distinct().collect()
    written_files = 0
    for row in partitions:
        g_id = row["grid_id"]
        yr = row["year"]
        part_dir = silver_weather_dir / f"grid_id={g_id}" / f"year={yr}"
        part_dir.mkdir(parents=True, exist_ok=True)
        (
            df_silver.filter((col("grid_id") == g_id) & (col("year") == yr))
            .coalesce(1)
            .write.mode("overwrite")
            .parquet(str(part_dir))
        )
        written_files += 1

    return written_files


def clean_and_validate_weather(
    bronze_weather_dir: Path = BRONZE_DIR / "weather",
    silver_weather_dir: Path = SILVER_DIR / "weather",
    anomaly_ledger_path: Path = SENSOR_ANOMALY_LEDGER_PATH,
    quarantine_dir: Path = QUARANTINE_DIR / "weather",
) -> Dict[str, Any]:
    """
    100% pure PySpark Silver Weather pipeline:
    1. Primary key quarantine & temporal standardization (PySpark).
    2. Deduplication on (grid_id, timestamp_utc) via PySpark Window.
    3. Atmospheric physical bounds verification & sensor masking (PySpark).
    4. Chronological rolling window features (PySpark Window functions).
    5. Meteorological standard attributes & extreme weather tagging (PySpark).
    6. Native PySpark partitioned Parquet output.
    """
    start_time = time.time()
    processed_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    bronze_files = sorted(bronze_weather_dir.glob("**/*.parquet"))
    if not bronze_files:
        logger.warning(f"No Bronze weather files found in {bronze_weather_dir}")
        return {"status": "skipped", "reason": "no_input_files"}

    spark = get_spark_session(app_name="DAT535-Silver-Clean-Weather")
    spark.conf.set("spark.sql.ansi.enabled", "false")
    logger.info(f"Loading {len(bronze_files)} Bronze weather partition files into PySpark...")

    df_raw = spark.read.parquet(*[str(f) for f in bronze_files])
    input_rows = df_raw.count()
    logger.info(f"Loaded {input_rows:,} raw weather observations from Bronze.")

    # 1. Unkeyed & Corrupted Primary Key Quarantine
    df_valid, quarantined_weather_rows = quarantine_unkeyed_weather(
        df_raw=df_raw,
        processed_at_utc=processed_at_utc,
        quarantine_dir=quarantine_dir,
    )

    valid_count = df_valid.count()
    if valid_count == 0:
        if not anomaly_ledger_path.exists():
            empty_ledger = spark.createDataFrame([], schema=LEDGER_SCHEMA)
            empty_ledger.coalesce(1).write.mode("overwrite").parquet(str(anomaly_ledger_path))

        elapsed = time.time() - start_time
        logger.info(
            f"Completed Silver Weather processing with PySpark: 0 valid rows | "
            f"0 partition files written | 0 sensor anomalies ledgered | {elapsed:.2f}s"
        )
        return {
            "dataset": "silver_weather",
            "input_rows": input_rows,
            "valid_rows": 0,
            "dropped_unkeyed_rows": quarantined_weather_rows,
            "quarantined_rows": quarantined_weather_rows,
            "duplicate_rows_dropped": 0,
            "corrupted_fields_masked": 0,
            "sensor_anomalies_logged": 0,
            "extreme_weather_tagged_rows": 0,
            "distinct_grid_cells": 0,
            "partitions_written": 0,
            "duration_seconds": round(elapsed, 2),
            "status": "success",
        }

    # 2. Deduplication
    df_dedup, duplicate_rows_dropped = deduplicate_weather(df_valid, df_raw.columns)

    # 3. Sensor Rules, Physical Bounds & Anomaly Ledger
    df_masked, num_anomalies, corrupted_fields_rows = evaluate_sensor_rules_and_mask(
        df=df_dedup,
        processed_at_utc=processed_at_utc,
        anomaly_ledger_path=anomaly_ledger_path,
    )

    # 4. Cumulative & Lagged Weather Features (Chronological PySpark Rolling Windows)
    df_windowed = compute_rolling_weather_windows(df_masked)

    # 5. Enrichment & Standardized Derived Flags
    df_silver = enrich_weather_attributes(df_windowed, processed_at_utc)
    df_silver.persist(StorageLevel.MEMORY_AND_DISK)

    extreme_rows_count = df_silver.filter(col("is_extreme_weather") == True).count()
    distinct_grids = df_silver.select("grid_id").distinct().count()
    total_valid = df_silver.count()

    # 6. Partitioned Parquet Write via Pure PySpark
    written_files = write_silver_weather(df_silver, silver_weather_dir)

    df_dedup.unpersist()
    df_silver.unpersist()

    elapsed = time.time() - start_time
    logger.info(
        f"Completed Silver Weather processing with PySpark: {total_valid:,} valid rows | "
        f"{written_files} partition files written | {num_anomalies} sensor anomalies ledgered | {elapsed:.2f}s"
    )

    return {
        "dataset": "silver_weather",
        "input_rows": input_rows,
        "valid_rows": total_valid,
        "dropped_unkeyed_rows": quarantined_weather_rows,
        "quarantined_rows": quarantined_weather_rows,
        "duplicate_rows_dropped": duplicate_rows_dropped,
        "corrupted_fields_masked": corrupted_fields_rows,
        "sensor_anomalies_logged": num_anomalies,
        "extreme_weather_tagged_rows": extreme_rows_count,
        "distinct_grid_cells": distinct_grids,
        "partitions_written": written_files,
        "duration_seconds": round(elapsed, 2),
        "status": "success",
    }
