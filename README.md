# NYC 311 & Weather Medallion Lakehouse Platform
### DAT535: Data-Driven Decision Making | University of Stavanger (UiS)
**Course Project — Group 13**

[![Python 3.10+](https://img.shields.io/badge/python-3.10%20%7C%203.11-blue.svg)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.x-E25A1C.svg)](https://spark.apache.org/)
[![Architecture](https://img.shields.io/badge/Architecture-Medallion%20Lakehouse-success.svg)](#architecture-overview)
[![License](https://img.shields.io/badge/License-Academic%20Evaluation-lightgrey.svg)]()

An enterprise-grade **Medallion Lakehouse data pipeline** engineered with **PySpark 3.5.x**, processing and coupling multi-modal municipal event streams (NYC 311 Service Requests) with high-resolution reanalysis weather data (ECMWF ERA5-Land via Open-Meteo). 

Designed to operate under real-world municipal data anomalies, the platform implements **CDC rolling watermarks**, **spatial-temporal incident deduplication**, **unstructured resolution text mining**, **categorical schema drift conformance**, and **automated quarantine isolation**.

---

## 📑 Table of Contents
1. [Project Statement & Analytical Scope](#-project-statement--analytical-scope)
2. [Medallion Architecture Overview](#-medallion-architecture-overview)
3. [Key Engineering Innovations](#-key-engineering-innovations)
4. [Data Marts in Gold Layer](#-data-marts-in-gold-layer)
5. [Repository Structure](#-repository-structure)
6. [Prerequisites & Installation](#-prerequisites--installation)
7. [Running the Pipeline](#-running-the-pipeline)
8. [Interactive Operational Dashboard](#-interactive-operational-dashboard)
9. [Automated Test Suite & Verification](#-automated-test-suite--verification)
10. [CI/CD & Scheduled Operations](#-cicd--scheduled-operations)

---

## 🎯 Project Statement & Analytical Scope

* **What it builds**: An end-to-end distributed data engineering pipeline that continuously ingests, cleans, harmonizes, and aggregates millions of raw municipal 311 citizen complaints and hourly meteorological observations into trusted analytical data marts.
* **Problem Addressed**: City agencies (DEP, DOT, NYPD, DSNY) struggle with duplicate call surges during severe weather, unstandardized complaint taxonomies, and misleading operational response SLAs caused by administrative dismissals.
* **Intended Outcome**: Provides city executives, emergency management (OEM), and infrastructure engineers with high-fidelity service demand elasticity metrics, neighborhood climate vulnerability rankings, and true field remediation SLAs.
* **Intentionally Outside Scope**: Sub-second event streaming (Apache Flink / Kafka) and black-box machine learning model training. The platform strictly prioritizes verifiable, deterministic distributed Lakehouse processing.

---

## 🏗️ Medallion Architecture Overview

```
                                      [Upstream Sources]
                             NYC 311 API          Open-Meteo ERA5
                                  │                      │
                                  ▼                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  LANDING LAYER                                                               │
│  • Raw compressed API payloads (.json.gz)                                    │
│  • Exact source auditability preserved                                       │
└──────────────────────────────────────┬───────────────────────────────────────┘
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER (Ingest & Preserve)                                            │
│  • Append-only Parquet storage partitioned by year/month                     │
│  • Row-level lineage: _batch_id, _ingested_at_utc, _source_file              │
│  • Manifest metadata tracking watermarks & SHA-256 schema fingerprints       │
└──────────────────────────────────────┬───────────────────────────────────────┘
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER (Clean, Conformed & Enriched)                                  │
│  • Deterministic Window Deduplication (source_updated_at DESC)               │
│  • Spatial Imputation via US Census Bureau ZCTA Centroid Gazetteers          │
│  • Spatiotemporal Incident Clustering (±15 min, ≤150m panic call deflation)  │
│  • Unstructured Resolution Text Mining (Field Verified vs. Phantom Closures) │
│  • 10 Invariant Canonical Service Domains (dim_complaint_taxonomy)           │
│  • Quarantine Isolation: Out-of-bounds sensor spikes & malformed records     │
└──────────────────────────────────────┬───────────────────────────────────────┘
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER (Curate & Serve)                                                 │
│  • Mart 1: Daily Spatiotemporal Service Demand (Weather Elasticity)          │
│  • Mart 2: Agency Resolution & SLA Performance (Phantom Audit)               │
│  • Mart 3: ZIP-Code Climate Vulnerability & Spatial Uncertainty Scorecard     │
│  • Gold Data Quality Ledger & Zero-Row-Loss Reconciliation Verification      │
└──────────────────────────────────────┬───────────────────────────────────────┘
                                       ▼
                     [Streamlit Operational Intelligence Platform]
```

---

## ⚡ Key Engineering Innovations

### 1. Spatiotemporal Call Burst Clustering & Panic Deflation
During severe rainstorms or flash floods, hundreds of citizens report the same fallen tree or clogged catch basin. The Silver engine partitions records across a **$\pm 15$-minute and $\le 150$-meter spatiotemporal window** using PySpark window functions, tagging primary records vs. duplicate panic calls (`is_incident_lead` vs. `is_duplicate_burst`). This deflates call center noise by **$30\%\text{--}65\%$** during extreme weather without losing ticket auditability.

### 2. Unstructured Resolution Text Mining & "Phantom Closure" Audit
Municipal agencies often boast resolution times under 3 hours. By mining unstructured resolution descriptions with compiled regex rules, the pipeline separates **True Field Remediation** (pothole paved, sewer unclogged) from **Administrative Phantom Closures** (*"Officer arrived and found no condition"*, *"Referred to external website"*).
* Result: Recalibrates reported SLAs from an artificial $2.4\text{ hours}$ to the true $28\text{--}56\text{ hours}$ required for on-site physical repairs.

### 3. Conformed Semantic Taxonomy & Schema Drift Resilience
NYC 311 categorizes requests using over 250 volatile strings that evolve over time (e.g., DEP renaming `Water System` $\to$ `Water Maintenance`). 
* The pipeline implements an invariant dimension (`dim_complaint_taxonomy.parquet`) mapping requests across 12 agencies into **10 Invariant Municipal Service Domains** (`WATER_INFRASTRUCTURE`, `SEWER_STORMWATER`, `ACOUSTIC_DISTURBANCE`, etc.).
* Unmapped or newly emerging categories are routed safely to `OTHER_CIVIC_SERVICES` and logged in real time to `unmapped_complaint_taxonomy_ledger.parquet`.

### 4. Catalyst Optimization & Memory Safety (Zero Driver OOMs)
The PySpark implementation strictly adheres to distributed Catalyst optimization principles:
* **Zero Python UDFs**: $100\%$ native Spark SQL Catalyst expressions.
* **Zero `.toPandas()` Collection on Master Data**: Heavy transforms execute in distributed partitions.
* **Broadcast Hash Joins**: Small reference tables (Census centroids, weather grid hints, complaint taxonomy) use `broadcast()` joins, eliminating cluster-wide shuffle exchanges.

---

## 📊 Data Marts in Gold Layer

| Data Mart | Granularity | Key Metrics / Dimensions | Primary Consumer |
| :--- | :--- | :--- | :--- |
| **`daily_service_demand.parquet`** | `date × grid_id × borough × complaint_type` | Total calls, distinct physical incidents, redundant burst calls, precipitation (mm), temperature (°C), peak wind gust (m/s), extreme weather indicator. | **NYC Emergency Management (OEM) & 311 Dispatch** |
| **`agency_resolution_performance.parquet`** | `reporting_date × borough × agency` | Daily tickets created/closed, active backlog count, long-open backlog ($>7$ days), phantom closure rate ($\%$), median & P90 field remediation hours. | **Mayoral Office of Operations & City Council** |
| **`zip_infrastructure_vulnerability.parquet`** | `incident_zip × borough × complaint_type` | Storm Sensitivity Multiplier (wet-to-dry complaint ratio), spatial distance to weather sensor, exact GPS $\%$ vs. centroid-imputed $\%$. | **DEP Capital Infrastructure & Urban Resilience** |

---

## 📁 Repository Structure

```text
group13-dat535/
├── .github/workflows/          # CI/CD and automated pipeline execution
│   ├── dat535-dev.yml          # Automated test gate and dev pipeline execution
│   ├── dat535-prod.yml         # Production deployment pipeline
│   └── pipeline.yml            # Scheduled daily incremental run (14:00 UTC)
├── data/
│   ├── bronze/                 # Partitioned raw Parquet storage
│   ├── gold/marts/             # Production analytical data marts
│   ├── metadata/quality/       # Data observability ledgers & sensor quarantines
│   └── reference/              # Census centroids & official borough GeoJSON
├── docs/                       # Technical dossiers, OOM analysis & CDC design
├── notebooks/                  # Methodological proofs & exploratory analysis
│   ├── 09_landing_and_bronze_showcase.ipynb
│   ├── 10_silver_layer_showcase.ipynb
│   ├── 12_gold_layer_business_marts_showcase.ipynb
│   ├── 14_sub_resolution_text_robustness_audit.ipynb
│   └── 15_categorical_schema_drift_and_semantic_conformance_proof.ipynb
├── src/
│   ├── ingestion/              # Socrata 311 & Open-Meteo ERA5 REST ingestion
│   ├── silver/                 # Silver cleaning, joins, burst clustering & taxonomy
│   ├── gold/                   # Gold data mart builders (PySpark)
│   └── utils/                  # Manifests, CDC watermarks, and Spark session builders
├── tests/                      # Automated unit, mock, and integration test suite
├── app.py                      # Interactive Streamlit operational intelligence platform
├── requirements.txt            # Python dependencies
├── run_project.py              # Unified CLI master entry point
├── run_ingestion.py            # Ingestion orchestration CLI
├── run_silver.py               # Silver transformation orchestration CLI
└── run_gold.py                 # Gold mart build CLI
```

---

## ⚙️ Prerequisites & Installation

### Requirements
* **Python**: 3.10 or 3.11
* **Java Runtime**: Java 8, 11, or 17 (Required by PySpark)
* **PySpark**: 3.5.x

### Step-by-Step Setup
1. **Clone the repository**:
   ```bash
   git clone https://github.com/<your-org>/group13-dat535.git
   cd group13-dat535
   ```

2. **Create and activate a virtual environment**:
   ```bash
   # Using venv:
   python -m venv .venv
   source .venv/bin/activate       # On Linux/macOS
   .venv\Scripts\Activate.ps1     # On Windows PowerShell

   # Or using Conda:
   conda create -n dat535 python=3.11 -y
   conda activate dat535
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables**:
   ```bash
   cp .env.example .env
   ```
   *(Optional)* Edit `.env` to supply your Socrata App Token (`SOCRATA_APP_TOKEN`) for high-volume API queries.

---

## 🚀 Running the Pipeline

The project includes a master orchestrator (`run_project.py`) with environment separation (`--env dev` vs. `--env prod`):

### 1. Run Complete Medallion Pipeline (Landing $\to$ Bronze $\to$ Silver $\to$ Gold)
```bash
python run_project.py all --env dev
```

### 2. Run Individual Layers
```bash
# Ingest raw data to Bronze:
python run_ingestion.py --dataset all --mode incremental --env dev

# Process Silver layer (cleaning, burst deduplication, spatial joins):
python run_silver.py --stage all --env dev

# Build Gold analytical data marts:
python run_gold.py --env dev
```

### 3. Historical Backfill Mode
```bash
python run_ingestion.py --dataset all --mode backfill --start-date 2026-07-01 --end-date 2026-09-30 --env dev
```

---

## 🖥️ Interactive Operational Dashboard

The project includes an executive-grade **Streamlit Decision Support Platform** (`app.py`) providing visual analytics across the 3 Gold marts:
* **Dual-Axis Hydrological Timeline**: Correlating daily calls against rainfall in mm.
* **Non-Linear Tipping Point Analysis**: Identifying the $15\text{ mm}$ drainage threshold.
* **The Accountability Divide**: Contrasting official reported SLAs against true verified remediation SLAs.
* **NYC Climate Fragility Map**: Rendering official NYC Borough GIS boundaries (GeoJSON) with neighborhood vulnerability pins.
* **Lakehouse Observability Ledger**: Live monitoring of sensor anomalies and schema drift events.

Launch the dashboard locally:
```bash
streamlit run app.py
```
*(Access in your browser at `http://localhost:8501`)*

---

## 🧪 Automated Test Suite & Verification

The project includes comprehensive test coverage across all layers using Python `unittest` and `pytest`:
* **API Ingestion & Mocking**: Tests timeouts, rate limits (HTTP 429), and watermark progression (`tests/test_ingestion.py`).
* **Silver Cleaning & Deduplication**: Validates Window rank ordering, centroid imputation, and quarantine isolation (`tests/test_silver.py`).
* **Text Mining & Regex Verification**: Asserts phantom closure identification accuracy (`tests/test_resolution_mining.py`).
* **Gold Zero-Loss Reconciliation**: Guarantees that Gold mart aggregations match Silver totals with zero dropped records (`tests/test_gold.py`).

Execute all tests:
```bash
# Via the master CLI:
python run_project.py test

# Or directly via unittest:
python -m unittest discover -s tests
```

---

## 🔄 CI/CD & Scheduled Operations

Continuous integration and automated scheduling are implemented using **GitHub Actions**:
* **Dev Quality Gate (`.github/workflows/dat535-dev.yml`)**: Triggers on pull requests to `dev`, executing PySpark runtime verification, unit tests, and pipeline quality gates.
* **Production Deployment (`.github/workflows/dat535-prod.yml`)**: Executes on release merges to `main`.
* **Daily Scheduled Incremental CDC (`.github/workflows/pipeline.yml`)**:
  * Runs daily at **14:00 UTC**.
  * **ECMWF Publication Lag Handling**: Open-Meteo ERA5-Land reanalysis is published with a 5-day delay ($D-5$) by 12:00 UTC. The 14:00 UTC schedule provides a 2-hour buffer, ensuring complete weather reanalysis joins before computing Gold vulnerability scorecards.

---

## 👥 Contributors & Academic Defense

* **Course**: DAT535: Data-Driven Decision Making
* **Institution**: University of Stavanger (UiS)
* **Team**: Group 13
* **Academic Evaluation Term**: Fall 2026
