import geopandas as gpd
import pandas as pd
import sqlite3
from shapely.geometry import Point

# Configuration
DB_PATH = "db/subway.db"
GIS_PATH = "data/01_raw/06_map/AL_D010_11_20260104/AL_D010_11_20260104.shp"
OUTPUT_PATH = "output/station_catchment_stats.csv"

# Columns in Shapefile (Inferred from inspection)
# A9: Usage (e.g., '단독주택', '공동주택')
# A18: Area (N)
# A26: Households (N) - Sum this
# A27: Families (N) - Maybe sum this too? Let's check logic. Usually households is mostly relevant.
# We will sum A26 + A27 for "Total Households" just in case, or keep them separate.
# Based on common sense, A26 (Households) is likely the primary metric for residential units.


def main():
    print("1. Loading Stations from DB...")
    conn = sqlite3.connect(DB_PATH)
    query = """
    SELECT 
        sr.station_id,
        s.station_name_kr as station_name,
        l.line_name,
        sr.lat,
        sr.lon
    FROM Station_Routes sr
    JOIN Stations s ON sr.station_id = s.station_id
    JOIN Lines l ON sr.line_id = l.line_id
    WHERE sr.lat IS NOT NULL AND sr.lon IS NOT NULL
    """
    stations_df = pd.read_sql(query, conn)
    conn.close()

    print(f"   Loaded {len(stations_df)} stations.")

    # Convert to GeoDataFrame (WGS84)
    geometry = [Point(xy) for xy in zip(stations_df.lon, stations_df.lat)]
    stations_gdf = gpd.GeoDataFrame(stations_df, geometry=geometry, crs="EPSG:4326")

    # Project to EPSG:5186 (Korea Central Belt 2010 - Matches GIS Data)
    print("2. Projecting Stations to EPSG:5186...")
    stations_gdf = stations_gdf.to_crs("EPSG:5186")

    # Create 500m Buffers
    print("3. Creating 500m Buffers...")
    stations_gdf["geometry"] = stations_gdf.geometry.buffer(500)

    # Load GIS Data
    print("4. Loading Building GIS Data (This may take a moment)...")
    # Read only necessary columns to save memory
    # A9: Usage, A18: Area, A26: Households, A27: Families
    buildings_gdf = gpd.read_file(
        GIS_PATH, encoding="cp949", include_fields=["A9", "A18", "A26", "A27"]
    )

    # Ensure CRS matches (Should be EPSG:5186, but it might be undefined in file, so we set or reproject)
    if buildings_gdf.crs is None:
        print("   Buildings CRS is missing. Setting to EPSG:5186.")
        buildings_gdf.set_crs("EPSG:5186", inplace=True)
    elif buildings_gdf.crs != "EPSG:5186":
        print(f"   Buildings CRS is {buildings_gdf.crs}. Reprojecting to EPSG:5186.")
        buildings_gdf = buildings_gdf.to_crs("EPSG:5186")

    # Spatial Join
    print("5. Performing Spatial Join (Stations <-> Buildings)...")
    # op='intersects' (default) or 'within'. 'intersects' includes buildings partially in buffer.
    joined_gdf = gpd.sjoin(
        buildings_gdf, stations_gdf, how="inner", predicate="intersects"
    )

    print(f"   Matches found: {len(joined_gdf)}")

    # Aggregate
    print("6. Aggregating Statistics...")
    # Group by Station and Usage
    stats = (
        joined_gdf.groupby(["station_id", "station_name", "line_name", "A9"])
        .agg(
            {
                "A18": "sum",  # Total Area
                "A26": "sum",  # Total Households
                "A27": "sum",  # Total Families
            }
        )
        .reset_index()
    )

    stats.rename(
        columns={
            "A9": "usage_type",
            "A18": "total_area",
            "A26": "total_households",
            "A27": "total_families",
        },
        inplace=True,
    )

    # Save Results
    print(f"7. Saving results to {OUTPUT_PATH}...")
    stats.to_csv(
        OUTPUT_PATH, index=False, encoding="utf-8-sig"
    )  # utf-8-sig for Korean Excel compatibility
    print("Done!")


if __name__ == "__main__":
    main()
