"""
Configuration, validation rules, legal boundaries, and spatial reference lookups for the Silver Layer.
Author: Group 13
"""

from pathlib import Path
from typing import Dict, Set, Tuple

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

# Sensor Anomaly Ledger Path (Data Observability & Audit Trail)
SENSOR_ANOMALY_LEDGER_PATH = QUALITY_DIR / "sensor_anomaly_ledger.parquet"

# Geographic Bounds for NYC Region (Filtering invalid 0.0 or out-of-bounds coordinates)
NYC_LAT_MIN = 40.40
NYC_LAT_MAX = 41.05
NYC_LON_MIN = -74.30
NYC_LON_MAX = -73.65

# NY State Plane Coordinate System (Long Island Zone, EPSG:2263, units: US Survey Feet)
# Derived from NYC Department of Information Technology and Telecommunications (DoITT) GIS specifications
NYC_STATE_PLANE_BOUNDS = {
    "x_min": 900000.0,
    "x_max": 1075000.0,
    "y_min": 110000.0,
    "y_max": 285000.0,
}

# Borough Centroids for Level 3 Fallback (lat, lon, grid_id)
BOROUGH_CENTROIDS: Dict[str, Tuple[float, float, str]] = {
    "MANHATTAN": (40.7831, -73.9712, "GRID_40.8_-74.0"),
    "BRONX": (40.8448, -73.8648, "GRID_40.8_-73.9"),
    "BROOKLYN": (40.6782, -73.9442, "GRID_40.7_-73.9"),
    "QUEENS": (40.7282, -73.7949, "GRID_40.7_-73.8"),
    "STATEN ISLAND": (40.5795, -74.1502, "GRID_40.6_-74.2"),
}

# -------------------------------------------------------------------------
# NYC 311 Institutional & Legal Boundaries
# -------------------------------------------------------------------------

# NYC 311 launch date: March 9, 2003 (Mayor Michael Bloomberg administration)
# Any complaint timestamp prior to March 2003 represents a corrupted epoch/default artifact.
NYC_311_LAUNCH_DATE = "2003-03-01T00:00:00Z"

# NYC 5 Boroughs (Legal administrative divisions of the City of New York)
# Source: NYC Charter Title 1, Chapter 1 §2; 2025 NYC 311 Data Dictionary
VALID_NYC_BOROUGHS: Set[str] = {
    "MANHATTAN",
    "BRONX",
    "BROOKLYN",
    "QUEENS",
    "STATEN ISLAND",
    "UNSPECIFIED",
}

# NYC Council Districts: Established by New York City Charter, Chapter 2, Section 22
# "There shall be a council which shall consist of the public advocate and fifty-one council members."
# Range is strictly 1 to 51 (1-10: Manhattan, 11-18: Bronx, 19-32: Queens, 33-48: Brooklyn, 49-51: Staten Island).
NYC_COUNCIL_DISTRICT_MIN = 1
NYC_COUNCIL_DISTRICT_MAX = 51

# Official Municipal Agencies handling NYC 311 Service Requests
# Sources: NYC Charter Title 14-25, NYC Open Data 311 Data Dictionary (Updated 2025, erm2-nwe9)
KNOWN_NYC_AGENCIES: Set[str] = {
    "311",    # NYC 311 Customer Service Center
    "3-1-1",  # NYC 311 Customer Service Center Alternative Notation
    "ACS",    # Administration for Children's Services
    "AJC",    # Administrative Justice Coordinator
    "CHALL",  # City Hall
    "DCA",    # Department of Consumer Affairs (Historical)
    "DCWP",   # Department of Consumer and Worker Protection (formerly DCA)
    "DCP",    # Department of City Planning
    "DEP",    # Department of Environmental Protection
    "DFTA",   # Department for the Aging
    "DHS",    # Department of Homeless Services
    "DOB",    # Department of Buildings
    "DOE",    # Department of Education
    "DOF",    # Department of Finance
    "DOHMH",  # Department of Health and Mental Hygiene
    "DOITT",  # Department of Information Technology & Telecommunications (Historical)
    "OTI",    # Office of Technology and Innovation (formerly DOITT)
    "DOP",    # Department of Probation
    "DOT",    # Department of Transportation
    "DPR",    # Department of Parks and Recreation
    "DSNY",   # Department of Sanitation
    "DYCD",   # Department of Youth and Community Development
    "EDC",    # Economic Development Corporation
    "FDNY",   # Fire Department of New York
    "HPD",    # Department of Housing Preservation and Development
    "HRA",    # Human Resources Administration
    "MOIA",   # Mayor's Office of Immigrant Affairs
    "NYCEM",  # NYC Emergency Management
    "OEM",    # Office of Emergency Management (Predecessor to NYCEM)
    "NYCHA",  # New York City Housing Authority
    "NYPD",   # Police Department
    "OATH",   # Office of Administrative Trials and Hearings
    "OPS",    # Mayor's Office of Operations
    "TLC",    # Taxi and Limousine Commission
    "DCAS",   # Department of Citywide Administrative Services
    "OOS",    # Out of Scope / External Multi-Jurisdiction
}

# NYC 311 Submission Channels
# Source: 2025 NYC 311 Data Dictionary & NYC Open Data Socrata API specifications
VALID_OPEN_DATA_CHANNELS: Set[str] = {
    "PHONE",
    "ONLINE",
    "MOBILE",
    "OTHER",
    "UNKNOWN",
}

# Known 311 Ticket Statuses
# Source: 2025 NYC 311 Data Dictionary (Assigned, Canceled, Closed, Pending, Started, Open)
KNOWN_311_STATUSES: Set[str] = {
    "ASSIGNED",
    "CANCELED",
    "CLOSED",
    "IN PROGRESS",
    "OPEN",
    "PENDING",
    "STARTED",
    "UNSPECIFIED",
    "UNKNOWN",
}

# NYC Metropolitan 3-digit ZIP prefixes
# 100-104: Manhattan & Bronx; 110-114, 116: Queens; 112: Brooklyn; 103: Staten Island; plus adjacent NY/NJ metro
NYC_METRO_ZIP_PREFIXES: Set[str] = {
    "100", "101", "102", "103", "104",
    "110", "111", "112", "113", "114", "116",
    # Adjacent Tri-State Commuter Metro (Suburbs & NJ border)
    "070", "071", "072", "073", "074", "075", "076", "077",
    "085", "087", "088", "089",
    "105", "106", "107", "108", "109", "115", "117", "118", "119",
}

# -------------------------------------------------------------------------
# Meteorological Physical Bounds & Atmospheric Laws (WMO Standards)
# -------------------------------------------------------------------------

# World Meteorological Organization (WMO) Code Table 4677: Present Weather
# Standard discrete codes (0-99). Values outside this set represent API corruption.
VALID_WMO_WEATHER_CODES: Set[int] = {
    0, 1, 2, 3, 45, 48, 51, 53, 55, 56, 57,
    61, 63, 65, 66, 67, 71, 73, 75, 77, 80, 81, 82, 85, 86, 95, 96, 99,
}

# NYC Climate & Meteorological Physical Hard Limits
# Historical Records: NOAA NCEI Central Park Station (USW00094728)
# All-time record low: -15°F (-26.1°C) on Feb 9, 1934
# All-time record high: 106°F (41.1°C) on July 9, 1936
WEATHER_HARD_LIMITS = {
    "temp_min": -35.0,      # Absolute minimum with 9°C margin beyond Central Park all-time low (-26.1°C)
    "temp_max": 45.0,       # Absolute maximum with 4°C margin beyond Central Park all-time high (41.1°C)
    "apparent_temp_min": -45.0,  # Extreme winter wind chill floor
    "apparent_temp_max": 55.0,   # Extreme summer heat index ceiling
    "precip_min": 0.0,
    "precip_max": 150.0,    # World record hourly rainfall ceiling; values >150 mm/hr indicate sensor failure
    "wind_speed_min": 0.0,
    "wind_speed_max": 65.0, # Sustained wind ceiling (Category 5 hurricane threshold is ~70 m/s)
    "pressure_min": 870.0,  # Lowest sea-level pressure recorded in global tropical cyclones (Typhoon Tip: 870 hPa)
    "pressure_max": 1085.0, # Highest sea-level pressure recorded in continental anticyclones (Siberia: 1084 hPa)
    "humidity_min": 0,      # Relative humidity percentage floor
    "humidity_max": 100,    # Relative humidity percentage ceiling
    "wind_dir_min": 0,      # Compass degrees
    "wind_dir_max": 360,
    "soil_temp_min": -30.0, # Ground frost / soil temperature floor
    "soil_temp_max": 50.0,  # Summer surface soil heat ceiling
    "soil_moist_min": 0.0,  # Volumetric soil water content m3/m3 floor
    "soil_moist_max": 1.0,  # Full soil saturation ceiling (1.0 = 100% water by volume)
    "cloud_cover_min": 0,   # Sky cloud cover percentage floor
    "cloud_cover_max": 100, # Sky cloud cover percentage ceiling
}

# Weather Soft Quality Thresholds (Tag as extreme weather, DO NOT discard)
WEATHER_EXTREME_THRESHOLDS = {
    "temp_extreme_cold": -25.0,  # NYC all-time record low is -26.1°C
    "temp_extreme_heat": 40.0,   # NYC all-time record high is 41.1°C
    "precip_torrential_hr": 25.0,# 25 mm/hr (1 inch/hr flash flood rate)
    "wind_gust_severe": 22.2,    # 22.2 m/s (80 km/h severe storm threshold)
}

# Standard Reference Table Path
ZIP_CENTROIDS_PATH = REFERENCE_DIR / "nyc_zip_centroids.parquet"
