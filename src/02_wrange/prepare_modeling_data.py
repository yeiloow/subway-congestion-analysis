import pandas as pd
import sqlite3
import os
import re

# Define paths
DB_PATH = "db/subway.db"
OUTPUT_DIR = "data/02_processed"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "model_dataset.csv")


def normalize_dong_name(dong_name):
    """
    Normalizes Hangjeong-dong names to match Beopjeong-dong names.
    Strategy: Remove numeric suffixes (e.g., 'Garak 1-dong' -> 'Garak-dong').
    """
    if pd.isna(dong_name):
        return None
    # Remove digits before '동'
    normalized = re.sub(r"[\d\.]+", "", dong_name)
    # Restore 'dong' suffix logic handled by regex above implicitly if '1동' -> '동'
    # Actually checking previous logic: re.sub(r"[\d\.]+", "", "가락1동") -> "가락동" Correct.
    # re.sub(r"[\d\.]+", "", "종로1.2.3.4가동") -> "종로가동". Correct.

    # Simple heuristic used in plan: strip numeric suffixes.
    normalized = re.sub(r"(\d+)(동)", r"\2", dong_name)
    if "가" in dong_name and "동" in dong_name:
        pass  # Leave complex cases like Jongno alone for now? Or apply same logic?
        # "종로1.2.3.4가동" -> "종로1.2.3.4가동" with this regex (no match for \d+동)
        # But "창신1동" -> "창신동"

    normalized = re.sub(r"(\d+)(동)", r"\2", dong_name)
    return normalized


def load_data(conn):
    print("Loading data from database...")
    query = """
    SELECT sr.*, s.station_name_kr 
    FROM Station_Routes sr
    JOIN Stations s ON sr.station_id = s.station_id
    """
    stations = pd.read_sql(query, conn)
    congestion = pd.read_sql("SELECT * FROM Station_Congestion", conn)
    revenue = pd.read_sql("SELECT * FROM Dong_Estimated_Revenue", conn)
    floating = pd.read_sql("SELECT * FROM Dong_Floating_Population", conn)
    return stations, congestion, revenue, floating


def process_data():
    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with sqlite3.connect(DB_PATH) as conn:
        stations, congestion, revenue, floating = load_data(conn)

    print(
        f"Loaded: Stations({len(stations)}), Congestion({len(congestion)}), Revenue({len(revenue)}), Floating({len(floating)})"
    )

    # 1. Prepare Revenue Data
    revenue["normalized_dong"] = revenue["admin_dong_name"].apply(normalize_dong_name)
    revenue_agg = (
        revenue.groupby(["quarter_code", "normalized_dong"])["month_sales_amt"]
        .sum()
        .reset_index()
    )
    revenue_agg.rename(
        columns={"month_sales_amt": "total_estimated_revenue"}, inplace=True
    )

    # 2. Prepare Floating Population Data
    floating["normalized_dong"] = floating["admin_dong_name"].apply(normalize_dong_name)
    floating_agg = (
        floating.groupby(["quarter_code", "normalized_dong"])["total_floating_pop"]
        .sum()
        .reset_index()
    )

    # 3. Prepare Congestion Data
    congestion_with_loc = pd.merge(
        congestion,
        stations[
            ["station_number", "administrative_dong", "line_id", "station_name_kr"]
        ],
        on="station_number",
        how="left",
    )
    congestion_with_loc = congestion_with_loc.dropna(subset=["administrative_dong"])
    congestion_with_loc["normalized_dong"] = congestion_with_loc["administrative_dong"]

    # 4. Merge All
    # Merge Congestion + Revenue
    merged_df = pd.merge(
        congestion_with_loc,
        revenue_agg,
        on=["quarter_code", "normalized_dong"],
        how="inner",
    )

    # Merge Floating Pop
    merged_df = pd.merge(
        merged_df, floating_agg, on=["quarter_code", "normalized_dong"], how="left"
    )

    print(f"Merged Dataset Size: {len(merged_df)}")

    # Handle Missing Floating Data
    merged_df.dropna(subset=["total_floating_pop"], inplace=True)
    print(f"Final Size after dropping missing floating pop: {len(merged_df)}")

    # Save
    print(f"Saving to {OUTPUT_FILE}...")
    merged_df.to_csv(OUTPUT_FILE, index=False)
    print("Done.")


if __name__ == "__main__":
    process_data()
