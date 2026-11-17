# Architectural Specification: Pure Raw Bronze Preservation & Silver Consolidation

**Project:** DAT535 Data Engineering – Spring / Autumn 2026  
**Author:** Group 13  
**Status:** Approved Architectural Decision Record (ADR)  
**Target Files:** `src/ingestion/ingest_311.py`, `src/ingestion/ingest_weather.py`, `src/utils/manifest.py`, `src/silver/clean_311.py`, `src/silver/clean_weather.py`, `src/silver/join_silver.py`  

---

## 1. Executive Summary & Problem Context

During our architecture review, faculty feedback highlighted a critical medallion compliance requirement:
> *"In Bronze, you should preserve all data and fields exactly as delivered by the original source—do not rename fields, do not cast types, do not enrich records, and do not append row-level metadata to the source schema."*

Our initial implementation followed an **"Enriched Bronze"** model (common in certain commercial cloud platforms), where technical lineage attributes (`_batch_id`, `_ingested_at_utc`), field renamings (`:created_at` $\rightarrow$ `source_created_at`), and synthetic spatial identifiers (`grid_id`, `borough_hint`) were injected directly into the raw records during Bronze conversion.

This specification documents the refactoring to a **"Pure Raw Store" (Immutable Raw Zone)** architecture:
1. **Pristine Bronze Schema:** Bronze Parquet files become a 1:1, untampered replica of the upstream API responses.
2. **Externalized Lineage:** All run metadata, batch identifiers, cryptographic SHA-256 hashes, and date bounds are tracked externally in Tier-1 JSON manifests.
3. **Consolidated Silver Transformations:** All field renamings, timezone conversions (EST $\rightarrow$ UTC), type castings, synthetic spatial mappings, and lineage injections are consolidated exclusively into the Silver layer.
4. **Elimination of Upstream Web Artifacts:** Socrata web portal columns (`:created_at`) are dropped entirely from the ingestion query, anchoring all analytics exclusively to verified municipal agency timestamps (`created_date`, `closed_date`, `resolution_action_updated_date`).

---

## 2. Architectural Comparison: Before vs. After

```text
========================================================================================
PREVIOUS ARCHITECTURE (Enriched Bronze)
========================================================================================
[Upstream APIs]
       |
       v
[Landing Layer]  --> Exact raw payload (raw_part_*.json.gz)
       |
       v
[Bronze Layer]   --> Injected _batch_id, _ingested_at_utc, _source_file
 (MUTATED)       --> Renamed :created_at to source_created_at & cast to timestamp
                 --> Injected synthetic grid_id and borough_hint into weather
                 --> Partitioned by derived business dates (year=YYYY/month=MM)
       |
       v
[Silver Layer]   --> Recast timestamps again (duplicate work)
                 --> Checked grid_id again (caused Spark "COLUMN_ALREADY_EXISTS" warning)
                 --> Cleaned and joined analytical facts

========================================================================================
REFACTORED ARCHITECTURE (Pure Raw Store & Conformed Silver)
========================================================================================
[Upstream APIs]
       |
       v
[Landing Layer]  --> Exact raw payload (raw_part_*.json.gz)
       |
       v
[Bronze Layer]   --> 1:1 UNTOUCHED source schema (no renames, no type casts, no extra cols)
 (PRISTINE)      --> Partitioned strictly by batch folder (batch_id=...)
       |             |
       |             +--> Tracked in External Tier-1 JSON Manifests (data/metadata/manifests/)
       v
[Silver Layer]   --> 1. Injects lineage (_source_file, _batch_id) via input_file_name()
 (CONFORMED)     --> 2. Standardizes schema & parses UTC timestamps (EST -> UTC once)
                 --> 3. Performs spatial mesh assignment (lat/lon -> grid_id, borough)
                 --> 4. Deduplicates & filters anomalies (quarantine)
                 --> 5. Enriches Fact Join with ERA5 hourly weather observations
```

---

## 3. Dataset 1: NYC 311 Service Requests Refactoring

### 3.1 What is Removed vs. What is Moved

| Pipeline Component | Previous State (Before) | Refactored State (After) | Rationale / Academic Rule |
| :--- | :--- | :--- | :--- |
| **API `$select` Query** | `select_clause="*, :created_at"` | `select_clause="*"` | Socrata's colon fields (`:created_at`) are web portal upload timestamps, not municipal events. Dropped completely. |
| **Field Renaming** | `:created_at` $\rightarrow$ `source_created_at` | **REMOVED** | Field renaming in Bronze violates raw preservation. Since `:created_at` is dropped, no renaming is required. |
| **Type Casting** | `:created_at` parsed & cast to `TimestampType` in Bronze | **REMOVED from Bronze** | Bronze preserves native string/numeric types. All timestamp parsing belongs strictly in Silver. |
| **Injected Columns** | Added `_batch_id`, `_ingested_at_utc`, `_source_file`, `_source_name`, `_schema_version`, `_bronze_status` | **REMOVED from Bronze rows** $\rightarrow$ **MOVED to Silver** | Bronze row schema remains 100% identical to Socrata. Silver attaches lineage upon reading Bronze files. |
| **Bronze Partitioning** | Partitioned by `year=YYYY/month=MM` derived from `created_date` | Partitioned by batch directory: `batch_id=311_YYYYMMDD_XXX/` | Business date derivation in Bronze causes issues with unparseable dates. Partitioning by batch preserves file-level lineage without altering row data. |
| **Silver Ingestion** | Relied on `_batch_id` column present in Bronze rows | Silver reads Bronze and derives `_source_file` and `_batch_id` via PySpark `input_file_name()` | Maintains full data lineage in Silver without polluting the raw Bronze schema. |
| **Timezone Normalization** | Parsed in Silver via `to_utc_timestamp(..., "America/New_York")` | **KEPT exclusively in Silver** | Silver remains the single authoritative place where local Eastern Time is standardized to UTC. |

### 3.2 Elimination of Duplicate Work in 311 Processing
* **Before:** `:created_at` was cast to timestamp in `ingest_311.py`, and then re-parsed with `to_timestamp(col("source_created_at"))` in `clean_311.py`.
* **After:** `:created_at` is completely removed. Silver only parses `created_date`, `closed_date`, and `resolution_action_updated_date` once, improving Bronze ingestion throughput and removing schema evolution vulnerabilities.

---

## 4. Dataset 2: Open-Meteo Weather Refactoring

### 4.1 What is Removed vs. What is Moved

| Pipeline Component | Previous State (Before) | Refactored State (After) | Rationale / Academic Rule |
| :--- | :--- | :--- | :--- |
| **Synthetic `grid_id`** | Injected `"GRID_40.5_-74.3"` into Bronze Parquet rows | **REMOVED from Bronze** $\rightarrow$ **MOVED to Silver** | `grid_id` is an internal project coordinate mesh identifier; it does not exist in Open-Meteo. Spatial mesh mapping belongs in Silver. |
| **Synthetic `borough_hint`** | Injected `"MANHATTAN"`, `"BROOKLYN"` into Bronze Parquet rows | **REMOVED from Bronze** $\rightarrow$ **MOVED to Silver** | Borough assignment is a municipal business classification. Open-Meteo is a global physical model. |
| **Coordinate Injection** | Merged root `grid_lat`, `grid_lon`, `elevation` into each hourly row in Bronze | Kept as native API output table (`latitude`, `longitude`, `elevation`, `time`, hourly variables) | Preserves original API fields without synthetic column aliases. |
| **Timestamp Coercion** | PyArrow `coerce_timestamps="us"` forced in Bronze write | **REMOVED from Bronze** | Raw string timestamps (`"2026-10-01T00:00"`) are preserved in Bronze. Silver parses them into UTC timestamps. |
| **Bronze Partitioning** | `grid_id=GRID_XX_YY/year=YYYY` | `batch_id=weather_YYYYMMDD_XXX/` | Prevents partitioning by synthetic fields and eliminates PySpark's `COLUMN_ALREADY_EXISTS: grid_id` warning. |
| **Silver Spatial Assignment**| Expected `grid_id` already present in Bronze weather | Silver assigns `grid_id` and `borough_hint` in Step 2 by matching `(latitude, longitude)` against `ERA5_NYC_GRID_POINTS` | Silver explicitly owns spatial enrichment and coordinate discretization. |

---

## 5. Traceability & Lineage Without Mutating Bronze Rows

A core requirement of data governance is knowing: *Which batch ingested this row, when did it arrive, and what was the source file?*

By shifting to the Pure Raw Store pattern, lineage is established through **File Provenance & Catalog Registries**:

### 5.1 External Tier-1 JSON Manifests
Every batch produces an immutable manifest document in `data/metadata/manifests/` (e.g. `nyc_311_311_20261116_001.json`). The manifest records:
* `batch_id`, `dataset_name`, `source_name`, `source_url`
* `retrieved_at_utc`, `source_data_start`, `source_data_end`
* `row_count`, `byte_size`, execution duration, and Git commit hash (`code_version`)
* `schema_fingerprint`: Cryptographic SHA-256 hash of the pure source schema
* `files`: A complete list of all written files, their relative paths, row counts, byte sizes, and individual file SHA-256 checksums

### 5.2 How Silver Recovers Lineage via `input_file_name()`
When Silver reads the pristine Bronze directory, it dynamically attaches the audit trail using PySpark's built-in file metadata functions:

```python
# 1. Read pristine Bronze Parquet files
df_bronze = spark.read.parquet("data/bronze/nyc_311/")

# 2. Extract origin file path dynamically
df_silver_staged = df_bronze.withColumn("_source_file", input_file_name())

# 3. Extract batch_id from the directory structure or filename via regex
# Pattern matches batch_id=311_YYYYMMDD_XXX or 311_YYYYMMDD_XXX_part-*.parquet
df_silver_staged = df_silver_staged.withColumn(
    "_batch_id", 
    regexp_extract(col("_source_file"), r"(311_\d{8}_\d{3})", 1)
)
```

**Key Benefit:** Bronze remains 100% compliant with raw preservation standards, while Silver gains full row-level lineage tracking.

---

## 6. Silver Layer Execution Order of Operations

To prevent downstream schema errors or broken deduplication, Silver implements transformations in strict chronological order:

```text
SILVER 311 PIPELINE ORDER OF OPERATIONS:
-----------------------------------------------------------------------------------------
Step 1: Bronze Scan & Lineage Injection
        - Read pristine Bronze Parquet.
        - Attach `_source_file` and `_batch_id` via `input_file_name()`.
Step 2: Boundary Quarantine
        - Evaluate missing `created_date`, corrupted `unique_key`, or invalid lineage.
        - Route corrupted rows to `quarantined_311.parquet`.
Step 3: Timestamp Normalization & Calendar Derivations
        - Parse `created_date` and `closed_date` from America/New_York -> UTC.
        - Derive `resolution_duration_hours`, `created_hour_local`, `day_of_week`, `year`, `month`.
Step 4: Spatial Fallback & Grid Matching
        - Resolve GPS coordinates to 19 ERA5-Land grid cells (`grid_id`).
Step 5: Resolution Text Mining
        - Classify `resolution_description` into confirmed fix vs. unconfirmed/phantom closures.
Step 6: CDC Deduplication
        - Partition by `unique_key` and order by latest municipal update date, then `_batch_id`.
Step 7: Hive-Partitioned Write
        - Write conformed Silver records partitioned by `year=YYYY/month=MM`.

SILVER WEATHER PIPELINE ORDER OF OPERATIONS:
-----------------------------------------------------------------------------------------
Step 1: Bronze Scan & Lineage Injection
        - Read pristine Bronze Weather Parquet.
        - Attach `_source_file` and `_batch_id`.
Step 2: Spatial Mesh & Borough Assignment
        - Map `(latitude, longitude)` -> `grid_id` and `borough_hint`.
Step 3: Unkeyed Quarantine & Boundary Audit
        - Verify primary keys `(grid_id, time)` and audit physical sensor bounds.
Step 4: Timestamp Normalization
        - Parse ISO `time` string -> `timestamp_utc`.
Step 5: Deduplication
        - Deduplicate on `(grid_id, timestamp_utc)`.
Step 6: Rolling Meteorological Aggregations
        - Compute 3h/6h/24h rolling precipitation and temperature extremes via PySpark Arrow.
Step 7: Hive-Partitioned Write
        - Write conformed Silver weather partitioned by `year=YYYY/month=MM`.
```

---

## 7. Implementation Roadmap & Impact Analysis

### 7.1 Affected Files Matrix

| File Path | Role in Architecture | Planned Refactoring Actions | Estimated Line Delta |
| :--- | :--- | :--- | :--- |
| `src/ingestion/ingest_311.py` | Bronze Ingestion | Drop `:created_at`, remove rename/cast, remove row-level `_` metadata injection, write to batch folder. | ~35 lines removed |
| `src/ingestion/ingest_weather.py` | Bronze Ingestion | Remove `grid_id`, `borough_hint`, remove row-level `_` metadata, write to batch folder. | ~45 lines removed |
| `src/utils/manifest.py` | Metadata Engine | Ensure manifest schema fingerprint captures pure source schema; add batch lookup helper. | ~25 lines added |
| `src/silver/clean_311.py` | Silver Cleaning | Inject `_batch_id` from `input_file_name()` in Step 1; remove `source_created_at` & lag calculation. | ~25 lines modified |
| `src/silver/clean_weather.py` | Silver Cleaning | Inject `_batch_id` from `input_file_name()`; assign `grid_id` and `borough_hint` in Step 2. | ~40 lines modified |
| `src/silver/join_silver.py` | Silver Fact Join | Remove `"source_created_at"` from column projection list. | ~2 lines modified |
| `tests/test_ingestion.py` | Ingestion Unit Tests| Assert that Bronze has pure schema (no `_batch_id` inside Parquet rows). | ~20 lines modified |
| `tests/test_silver.py` | Silver Unit Tests | Test that Silver correctly extracts `_batch_id` from file path and assigns `grid_id`. | ~30 lines modified |
| `tests/test_pipeline_e2e.py` | Integration Tests | Align mock Bronze data with pristine schema. | ~15 lines modified |

* **Total Files Impacted:** 9 files (6 core pipeline files + 3 test suites).  
* **Net Code Reduction:** ~120 lines of redundant transformation logic eliminated from Bronze.

---

## 8. Verification & Acceptance Criteria

1. **Bronze Schema Purity Check:**
   * Running `spark.read.parquet("data/bronze/nyc311/...")` returns only columns present in the official NYC Open Data dictionary. Zero columns starting with `_` or synthetic identifiers.
   * Running `spark.read.parquet("data/bronze/weather/...")` returns only Open-Meteo fields (`latitude`, `longitude`, `elevation`, `time`, hourly metrics). No `grid_id` or `borough_hint`.
2. **Silver Lineage Preservation:**
   * Output Silver tables (`data/silver/fact_311_service_requests/`) successfully contain `_batch_id` and `_source_file` populated via `input_file_name()`.
3. **Auditability & Traceability:**
   * Every Bronze Parquet file matches an exact SHA-256 hash in a corresponding `data/metadata/manifests/*.json` file.
4. **Idempotent Reruns:**
   * Running `python run_silver.py --stage all --mode backfill --start-date 2026-10-01 --end-date 2026-10-31` correctly reads all Bronze batches spanning October, cleans the data, and populates the Silver fact partition without warning or failure.
5. **Zero Spark Catalyst Warnings:**
   * The warning `DataSource: [COLUMN_ALREADY_EXISTS] The column grid_id already exists` must no longer appear during Silver runs.
