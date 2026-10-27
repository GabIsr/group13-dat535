"""
Configuration and constants for the Medallion Gold Layer.
Defines storage paths, thresholds for SLA definitions, and business rule constants.
Author: Group 13
"""

from pathlib import Path

# Base Paths (dynamically resolved to project root)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
GOLD_MARTS_DIR = GOLD_DIR / "marts"
GOLD_METADATA_DIR = GOLD_DIR / "metadata"

# Ensure essential Gold directories exist
for d in [GOLD_DIR, GOLD_MARTS_DIR, GOLD_METADATA_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# Silver Source Paths
SILVER_311_DIR = SILVER_DIR / "nyc311"
SILVER_WEATHER_DIR = SILVER_DIR / "weather"
SILVER_JOINED_DIR = SILVER_DIR / "311_weather_joined"

# Business Thresholds
LONG_OPEN_BACKLOG_DAYS = 7.0  # Threshold in days to consider a ticket "long-open" backlog
STORM_PRECIP_THRESHOLD_MM = 15.0  # Daily rainfall threshold defining a storm day
STORM_WIND_THRESHOLD_MS = 17.0  # Daily peak wind gust threshold defining severe wind
