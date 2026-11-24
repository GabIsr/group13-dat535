"""
Reference Data Utility: Build NYC ZIP Centroids Reference Parquet.
Derives nyc_zip_centroids.parquet from the official U.S. Census Bureau National Gazetteer (2026_Gaz_zcta_national.txt).
Author: Group 13
"""

import argparse
import logging
from pathlib import Path
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("build_reference_zip_centroids")

# Default relative paths within repository
DEFAULT_RAW_GAZETTEER = Path("data/reference/2026_Gaz_zcta_national.txt")
DEFAULT_OUTPUT_PARQUET = Path("data/reference/nyc_zip_centroids.parquet")


def build_nyc_zip_centroids(
    raw_path: Path = DEFAULT_RAW_GAZETTEER,
    output_path: Path = DEFAULT_OUTPUT_PARQUET,
) -> pd.DataFrame:
    """
    Parse national Gazetteer, filter for NYC ZIP codes (prefixes 100-116),
    calculate nearest ERA5 0.1-degree weather grid IDs, and serialize to Parquet.
    """
    if not raw_path.exists():
        raise FileNotFoundError(f"Raw Census Gazetteer not found at: {raw_path}")

    logger.info(f"Reading raw national Census Gazetteer from: {raw_path}")
    df_raw = pd.read_csv(raw_path, sep="|", dtype=str)
    df_raw.columns = [c.strip() for c in df_raw.columns]

    # Clean primary key and coordinates
    df_raw["GEOID"] = df_raw["GEOID"].str.strip()
    df_raw["INTPTLAT"] = df_raw["INTPTLAT"].str.strip().astype(float)
    df_raw["INTPTLONG"] = df_raw["INTPTLONG"].str.strip().astype(float)

    # Filter for NYC postal prefixes (100 to 116)
    nyc_prefixes = tuple(str(p) for p in range(100, 117))
    df_nyc = df_raw[df_raw["GEOID"].str.startswith(nyc_prefixes)].copy()
    df_nyc = df_nyc.sort_values("GEOID").reset_index(drop=True)
    logger.info(f"Filtered {len(df_nyc)} NYC metropolitan ZIP codes from {len(df_raw)} national entries.")

    # Construct reference table schema
    out = pd.DataFrame()
    out["zip5"] = df_nyc["GEOID"]
    out["centroid_lat"] = df_nyc["INTPTLAT"]
    out["centroid_lon"] = df_nyc["INTPTLONG"]
    out["grid_lat"] = out["centroid_lat"].round(1)
    out["grid_lon"] = out["centroid_lon"].round(1)
    out["grid_id"] = (
        "GRID_"
        + out["grid_lat"].map("{:.1f}".format)
        + "_"
        + out["grid_lon"].map("{:.1f}".format)
    )
    out["source"] = "US_CENSUS_BUREAU_GAZETTEER_2020"

    # Ensure output directory exists and save parquet
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(output_path, index=False, engine="pyarrow", compression="snappy")
    logger.info(f"Successfully generated {len(out)} reference records to: {output_path}")

    return out


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build NYC ZIP Centroids Reference Parquet")
    parser.add_argument("--raw", type=Path, default=DEFAULT_RAW_GAZETTEER, help="Path to raw Gazetteer txt")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUTPUT_PARQUET, help="Path to output parquet")
    args = parser.parse_args()

    build_nyc_zip_centroids(raw_path=args.raw, output_path=args.out)
