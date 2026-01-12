import pandas as pd
import sqlite3
import logging
from src.utils.db_util import get_connection
from src.utils.config import OUTPUT_DIR, LOG_FORMAT, LOG_LEVEL

# Configure Logging
# logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)


def run_insert_floating_population():
    # File paths
    input_file = OUTPUT_DIR / "서울시_상권분석서비스_길단위인구_행정동_2019_2025.csv"

    # Check if input file exists
    if not input_file.exists():
        logger.error(f"Error: Input file '{input_file}' not found.")
        return

    # Load data
    logger.info("Loading data...")
    try:
        df = pd.read_csv(input_file, encoding="utf-8")
    except Exception as e:
        logger.error(f"Error reading CSV: {e}")
        return

    # Filter for years 2023, 2024, 2025
    # quarter_code format is YYYYQ (e.g., 20231)
    # Convert to string to easily check prefix
    df["기준_년분기_코드"] = df["기준_년분기_코드"].astype(str)

    target_years = ["2023", "2024", "2025"]
    mask = df["기준_년분기_코드"].str[:4].isin(target_years)
    filtered_df = df[mask].copy()

    logger.info(f"Filtered {len(filtered_df)} rows for years {target_years}.")

    # Column mapping (CSV header -> Table column)
    column_mapping = {
        "기준_년분기_코드": "quarter_code",
        "행정동_코드": "admin_dong_code",
        "행정동_코드_명": "admin_dong_name",
        "총_유동인구_수": "total_floating_pop",
        "남성_유동인구_수": "male_floating_pop",
        "여성_유동인구_수": "female_floating_pop",
        "연령대_10_유동인구_수": "age_10_floating_pop",
        "연령대_20_유동인구_수": "age_20_floating_pop",
        "연령대_30_유동인구_수": "age_30_floating_pop",
        "연령대_40_유동인구_수": "age_40_floating_pop",
        "연령대_50_유동인구_수": "age_50_floating_pop",
        "연령대_60_이상_유동인구_수": "age_60_over_floating_pop",
        "시간대_00_06_유동인구_수": "time_00_06_floating_pop",
        "시간대_06_11_유동인구_수": "time_06_11_floating_pop",
        "시간대_11_14_유동인구_수": "time_11_14_floating_pop",
        "시간대_14_17_유동인구_수": "time_14_17_floating_pop",
        "시간대_17_21_유동인구_수": "time_17_21_floating_pop",
        "시간대_21_24_유동인구_수": "time_21_24_floating_pop",
        "월요일_유동인구_수": "mon_floating_pop",
        "화요일_유동인구_수": "tue_floating_pop",
        "수요일_유동인구_수": "wed_floating_pop",
        "목요일_유동인구_수": "thu_floating_pop",
        "금요일_유동인구_수": "fri_floating_pop",
        "토요일_유동인구_수": "sat_floating_pop",
        "일요일_유동인구_수": "sun_floating_pop",
    }

    # Rename columns
    filtered_df = filtered_df.rename(columns=column_mapping)

    # Remove duplicates
    duplicate_count = filtered_df.duplicated(
        subset=["quarter_code", "admin_dong_code"]
    ).sum()
    if duplicate_count > 0:
        logger.warning(
            f"Found {duplicate_count} duplicate rows. Dropping duplicates (keeping first)."
        )
        filtered_df = filtered_df.drop_duplicates(
            subset=["quarter_code", "admin_dong_code"], keep="first"
        )

    # Select only the columns that exist in the mapping
    columns_to_keep = list(column_mapping.values())
    final_df = filtered_df[columns_to_keep]

    # Connect to DB
    logger.info("Connecting to database...")
    try:
        conn = get_connection()
    except Exception as e:
        logger.error(f"Failed to connect to DB: {e}")
        return

    try:
        # Insert data
        logger.info("Inserting data into Dong_Floating_Population...")
        final_df.to_sql(
            "Dong_Floating_Population", conn, if_exists="append", index=False
        )
        logger.info("Data insertion completed successfully.")

    except sqlite3.Error as e:
        logger.error(f"An error occurred: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
    run_insert_floating_population()
