from src.utils.config import DATA_DIR, LOG_FORMAT, LOG_LEVEL
import pandas as pd
import logging
from src.utils.db_util import get_connection

# Configure Logging
logger = logging.getLogger(__name__)


def run_insert_impact_analysis():
    # File paths
    input_file = DATA_DIR / "final_impact_analysis_optionA_total_보완점 포함.csv"

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

    # Column mapping (CSV header -> Table column)
    column_mapping = {
        "날짜": "base_date",
        "호선": "line_name",
        "역명": "station_name",
        "역명_정규화": "station_name_normalized",
        "요일": "day_of_week",
        "카테고리": "category",
        "승차": "boarding_count",
        "하차": "alighting_count",
        "승하차합계": "total_count",
        "평균_승차": "avg_boarding_count",
        "평균_하차": "avg_alighting_count",
        "평균_승하차합계": "avg_total_count",
        "상승률_%": "increase_rate",
        "상승여부": "increase_status",
    }

    # Verify if all columns exist
    missing_cols = [col for col in column_mapping.keys() if col not in df.columns]
    if missing_cols:
        logger.error(f"Missing columns in CSV: {missing_cols}")
        logger.info(f"Available columns: {df.columns.tolist()}")
        return

    # Rename columns
    df = df.rename(columns=column_mapping)

    # Remove duplicates
    # UNIQUE(base_date, line_name, station_name, category)
    subset_cols = ["base_date", "line_name", "station_name", "category"]
    duplicate_count = df.duplicated(subset=subset_cols).sum()
    if duplicate_count > 0:
        logger.warning(
            f"Found {duplicate_count} duplicate rows. Dropping duplicates (keeping first)."
        )
        df = df.drop_duplicates(subset=subset_cols, keep="first")

    # Select columns to keep
    columns_to_keep = list(column_mapping.values())
    final_df = df[columns_to_keep]

    # Connect to DB
    logger.info("Connecting to database...")
    try:
        conn = get_connection()
    except Exception as e:
        logger.error(f"Failed to connect to DB: {e}")
        return

    try:
        # Insert data
        logger.info("Inserting data into Impact_Analysis_OptionA...")
        final_df.to_sql(
            "Impact_Analysis_OptionA", conn, if_exists="append", index=False
        )
        logger.info(f"Successfully inserted {len(final_df)} rows.")

    except Exception as e:
        logger.error(f"An error occurred during insertion: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
    run_insert_impact_analysis()
