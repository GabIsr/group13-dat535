"""
Configuration, validation rules, and spatial reference lookups for the Silver Layer.
Author: Group 13
"""

from pathlib import Path
from typing import Dict, Tuple

# Base Paths (relative to workspace root)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
QUARANTINE_DIR = SILVER_DIR / "quarantine"
REFERENCE_DIR = DATA_DIR / "reference"
METADATA_DIR = DATA_DIR / "metadata"
QUALITY_DIR = METADATA_DIR / "quality"

# Ensure essential silver directories exist
for d in [SILVER_DIR, QUARANTINE_DIR, REFERENCE_DIR, QUALITY_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# Geographic Bounds for NYC Region (Filtering invalid 0.0 or out-of-bounds coordinates)
NYC_LAT_MIN = 40.40
NYC_LAT_MAX = 41.05
NYC_LON_MIN = -74.30
NYC_LON_MAX = -73.65

# Borough Centroids for Level 3 Fallback (lat, lon, grid_id)
BOROUGH_CENTROIDS: Dict[str, Tuple[float, float, str]] = {
    "MANHATTAN": (40.7831, -73.9712, "GRID_40.8_-74.0"),
    "BRONX": (40.8448, -73.8648, "GRID_40.8_-73.9"),
    "BROOKLYN": (40.6782, -73.9442, "GRID_40.7_-73.9"),
    "QUEENS": (40.7282, -73.7949, "GRID_40.7_-73.8"),
    "STATEN ISLAND": (40.5795, -74.1502, "GRID_40.6_-74.2"),
}

# Weather Physical Hard Limits (Physical impossibility / corrupt sentinels)
WEATHER_HARD_LIMITS = {
    "temp_min": -90.0,      # Colder than Vostok station (-89.2°C)
    "temp_max": 60.0,       # Hotter than Death Valley (56.7°C)
    "precip_min": 0.0,
    "wind_min": 0.0,
    "pressure_min": 850.0,  # Lowest sea-level pressure in hurricanes
    "pressure_max": 1090.0, # Highest sea-level pressure recorded
}

# Weather Soft Quality Thresholds (Tag as extreme weather, DO NOT discard)
WEATHER_EXTREME_THRESHOLDS = {
    "temp_extreme_cold": -25.0,  # NYC all-time record low is -26.1°C
    "temp_extreme_heat": 40.0,   # NYC all-time record high is 41.1°C
    "precip_torrential_hr": 25.0,# 25 mm/hr (1 inch/hr flash flood rate)
    "wind_gust_severe": 80.0,    # 80 km/h severe storm threshold
}

# Standard Reference Table Path
ZIP_CENTROIDS_PATH = REFERENCE_DIR / "nyc_zip_centroids.parquet"
