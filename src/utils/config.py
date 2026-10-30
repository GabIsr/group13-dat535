"""
Configuration and constants for DAT535 Ingestion and Bronze Layer.
Author: Group 13
"""

from pathlib import Path

# Base Paths (relative to workspace root)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
LANDING_DIR = DATA_DIR / "landing"
BRONZE_DIR = DATA_DIR / "bronze"
METADATA_DIR = DATA_DIR / "metadata"
MANIFESTS_DIR = METADATA_DIR / "manifests"

# Ensure essential directories can be created
for d in [LANDING_DIR, BRONZE_DIR, MANIFESTS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# Datasets
DATASET_311 = "nyc_311"
DATASET_WEATHER = "weather_era5"

# Upstream Source Configuration
SOCRATA_311_ENDPOINT = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
SOCRATA_DATASET_ID = "erm2-nwe9"
SOURCE_NAME_311 = "nyc_open_data"

OPEN_METEO_ARCHIVE_ENDPOINT = "https://archive-api.open-meteo.com/v1/archive"
OPEN_METEO_DATASET_ID = "era5_land"
SOURCE_NAME_WEATHER = "open_meteo"

SCHEMA_VERSION = "v1.0"

# Open-Meteo Hourly Meteorological Variables
WEATHER_HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "apparent_temperature",
    "precipitation",
    "rain",
    "snowfall",
    "snow_depth",
    "weather_code",
    "surface_pressure",
    "wind_speed_10m",
    "wind_gusts_10m",
    "wind_direction_10m",
    "soil_temperature_0_to_7cm",
    "soil_moisture_0_to_7cm",
    "cloud_cover",
]

# 19 Native ERA5-Land Grid Cells Covering New York City (0.1 deg / ~9 km resolution)
# Centers rounded to 0.1 degree; verified against NYC 311 spatial bounds
ERA5_NYC_GRID_POINTS = [
    {"grid_lat": 40.5, "grid_lon": -74.3, "grid_id": "GRID_40.5_-74.3", "borough_hint": "STATEN ISLAND"},
    {"grid_lat": 40.5, "grid_lon": -74.2, "grid_id": "GRID_40.5_-74.2", "borough_hint": "STATEN ISLAND"},
    {"grid_lat": 40.5, "grid_lon": -74.1, "grid_id": "GRID_40.5_-74.1", "borough_hint": "STATEN ISLAND"},
    {"grid_lat": 40.6, "grid_lon": -74.2, "grid_id": "GRID_40.6_-74.2", "borough_hint": "STATEN ISLAND"},
    {"grid_lat": 40.6, "grid_lon": -74.1, "grid_id": "GRID_40.6_-74.1", "borough_hint": "STATEN ISLAND"},
    {"grid_lat": 40.6, "grid_lon": -74.0, "grid_id": "GRID_40.6_-74.0", "borough_hint": "BROOKLYN"},
    {"grid_lat": 40.6, "grid_lon": -73.9, "grid_id": "GRID_40.6_-73.9", "borough_hint": "BROOKLYN"},
    {"grid_lat": 40.6, "grid_lon": -73.8, "grid_id": "GRID_40.6_-73.8", "borough_hint": "QUEENS"},
    {"grid_lat": 40.6, "grid_lon": -73.7, "grid_id": "GRID_40.6_-73.7", "borough_hint": "QUEENS"},
    {"grid_lat": 40.7, "grid_lon": -74.0, "grid_id": "GRID_40.7_-74.0", "borough_hint": "BROOKLYN"},
    {"grid_lat": 40.7, "grid_lon": -73.9, "grid_id": "GRID_40.7_-73.9", "borough_hint": "BROOKLYN"},
    {"grid_lat": 40.7, "grid_lon": -73.8, "grid_id": "GRID_40.7_-73.8", "borough_hint": "QUEENS"},
    {"grid_lat": 40.7, "grid_lon": -73.7, "grid_id": "GRID_40.7_-73.7", "borough_hint": "QUEENS"},
    {"grid_lat": 40.8, "grid_lon": -74.0, "grid_id": "GRID_40.8_-74.0", "borough_hint": "MANHATTAN"},
    {"grid_lat": 40.8, "grid_lon": -73.9, "grid_id": "GRID_40.8_-73.9", "borough_hint": "BRONX"},
    {"grid_lat": 40.8, "grid_lon": -73.8, "grid_id": "GRID_40.8_-73.8", "borough_hint": "QUEENS"},
    {"grid_lat": 40.8, "grid_lon": -73.7, "grid_id": "GRID_40.8_-73.7", "borough_hint": "QUEENS"},
    {"grid_lat": 40.9, "grid_lon": -73.9, "grid_id": "GRID_40.9_-73.9", "borough_hint": "BRONX"},
    {"grid_lat": 40.9, "grid_lon": -73.8, "grid_id": "GRID_40.9_-73.8", "borough_hint": "BRONX"},
]
