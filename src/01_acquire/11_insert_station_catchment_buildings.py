import geopandas as gpd
import pandas as pd
import sqlite3
from shapely.geometry import Point
from huggingface_hub import snapshot_download
from pathlib import Path

# Configuration
DB_PATH = "db/subway.db"
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

    # Download GIS data from Hugging Face
    print("4. Downloading & Loading Building GIS Data (This may take a moment)...")
    try:
        snapshot_path = snapshot_download(
            repo_id="alrq/subway",
            repo_type="dataset",
            allow_patterns=["*AL_D010_11_20260104*"],
        )
        # Find the .shp file recursively
        gis_files = list(Path(snapshot_path).rglob("*.shp"))
        if not gis_files:
            print("Error: No .shp file found in downloaded snapshot.")
            return

        gis_path = str(gis_files[0])
        print(f"   Using GIS file: {gis_path}")

        buildings_gdf = gpd.read_file(gis_path, encoding="cp949")
    except Exception as e:
        print(f"Error loading GIS data: {e}")
        return

    # Filter columns
    target_cols = [
        "A9",
        "A11",
        "A13",
        "A16",
        "A18",
        "A24",
        "A25",
        "A26",
        "A27",
        "geometry",
    ]
    # Check if columns exist
    missing_cols = [c for c in target_cols if c not in buildings_gdf.columns]
    if missing_cols:
        print(f"Warning: Missing columns in GIS data: {missing_cols}")

    # Keep only available target columns
    available_cols = [c for c in target_cols if c in buildings_gdf.columns]
    buildings_gdf = buildings_gdf[available_cols]

    # Ensure CRS matches
    if buildings_gdf.crs is None:
        print("   Buildings CRS is missing. Setting to EPSG:5186.")
        buildings_gdf.set_crs("EPSG:5186", inplace=True)
    elif buildings_gdf.crs != "EPSG:5186":
        print(f"   Buildings CRS is {buildings_gdf.crs}. Reprojecting to EPSG:5186.")
        buildings_gdf = buildings_gdf.to_crs("EPSG:5186")

    # Spatial Join
    print("5. Performing Spatial Join (Stations <-> Buildings)...")
    joined_gdf = gpd.sjoin(
        buildings_gdf, stations_gdf, how="inner", predicate="intersects"
    )

    print(f"   Matches found: {len(joined_gdf)}")

    # Prepare Data for DB Insertion
    print("6. Preparing data for database insertion...")

    # Rename for DB
    db_data = joined_gdf.rename(
        columns={
            "station_id": "station_id",
            "A24": "building_name",
            "A25": "building_detail_name",
            "A9": "usage_type",
            "A11": "structure_type",
            "A13": "approval_date",
            "A16": "height",
            "A18": "floor_area",
            "A26": "households",
            "A27": "families",
        }
    )

    # Select columns matching the table schema
    db_cols = [
        "station_id",
        "building_name",
        "building_detail_name",
        "usage_type",
        "structure_type",
        "approval_date",
        "height",
        "floor_area",
        "households",
        "families",
    ]

    # Fill NaNs with appropriate values (None for objects, 0 for numeric if needed, but SQL handles NULL)
    # Actually pandas NaN -> SQL NULL is automatic with to_sql usually,
    # but let's be explicit if needed.

    insert_df = db_data[db_cols].copy()

    # Create Table if not exists (Double check)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Station_Catchment_Buildings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        station_id INTEGER NOT NULL,
        building_name TEXT,       -- A24
        building_detail_name TEXT,-- A25
        usage_type TEXT,          -- A9
        structure_type TEXT,      -- A11
        approval_date TEXT,       -- A13
        height REAL,              -- A16
        floor_area REAL,          -- A18 (연면적)
        households INTEGER,       -- A26 (세대수)
        families INTEGER,         -- A27 (가구수)
        FOREIGN KEY (station_id) REFERENCES Stations(station_id) ON DELETE CASCADE
    );
    """)
    conn.commit()

    # Clear existing data?
    # Maybe we should clear data to avoid duplicates if re-run.
    print("   Clearing existing catchment building data...")
    cursor.execute("DELETE FROM Station_Catchment_Buildings")
    conn.commit()

    print(f"   Inserting {len(insert_df)} rows into Station_Catchment_Buildings...")
    insert_df.to_sql(
        "Station_Catchment_Buildings", conn, if_exists="append", index=False
    )
    conn.close()

    # Aggregate for CSV (Legacy support / Summary)
    print("7. Aggregating Statistics for Summary CSV...")
    stats = (
        joined_gdf.groupby(["station_id", "station_name", "line_name", "A9"])
        .agg(
            {
                "A18": "sum",
                "A26": "sum",
                "A27": "sum",
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
    print(f"8. Saving summary to {OUTPUT_PATH}...")
    stats.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
    print("Done!")


if __name__ == "__main__":
    main()
