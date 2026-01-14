import pandas as pd
import logging
from src.utils.db_util import get_connection
from src.utils.config import DATA_DIR, LOG_FORMAT, LOG_LEVEL

logger = logging.getLogger(__name__)


def run_insert_subway_timetable():
    file_path = DATA_DIR / "서울교통공사_서울 도시철도 열차운행시각표_20250704.csv"

    if not file_path.exists():
        logger.error(f"File not found: {file_path}")
        return

    logger.info(f"Processing {file_path}...")

    try:
        conn = get_connection()
    except Exception as e:
        logger.error(f"Failed to connect to DB: {e}")
        return

    try:
        # Load dataset
        # Using utf-8 as determined by inspection
        df = pd.read_csv(file_path, encoding="utf-8")

        # Rename columns to match schema
        column_mapping = {
            "고유번호": "source_id",
            "호선": "line_id",
            "역사코드": "station_code",
            "역사명": "station_name",
            "주중주말": "day_type",
            "방향": "direction",
            "급행여부": "is_express",
            "열차코드": "train_number",
            "열차도착시간": "arrival_time",
            "열차출발시간": "departure_time",
            "출발역": "origin_station",
            "도착역": "destination_station",
        }

        # Check for missing columns
        missing_cols = [col for col in column_mapping.keys() if col not in df.columns]
        if missing_cols:
            logger.error(f"Missing columns in CSV: {missing_cols}")
            return

        df_renamed = df.rename(columns=column_mapping)

        # Select only relevant columns
        final_cols = list(column_mapping.values())
        final_df = df_renamed[final_cols]

        # Drop duplicates based on unique constraint
        subset_cols = [
            "line_id",
            "station_code",
            "day_type",
            "direction",
            "train_number",
        ]
        initial_len = len(final_df)
        final_df = final_df.drop_duplicates(subset=subset_cols)
        if len(final_df) < initial_len:
            logger.warning(f"Dropped {initial_len - len(final_df)} duplicate rows.")

        # Handle data adjustments if necessary
        # e.g., ensure is_express is integer (0, 1) - it seemed so in inspection

        # Clear existing data in the table to avoid duplication on re-run
        conn.execute("DELETE FROM Subway_Timetable")
        conn.commit()
        logger.info("Cleared existing data in Subway_Timetable.")

        # Insert data
        final_df.to_sql("Subway_Timetable", conn, if_exists="append", index=False)
        logger.info(
            f"Successfully inserted {len(final_df)} rows into Subway_Timetable."
        )

    except Exception as e:
        logger.error(f"Error processing/inserting data: {e}")
    finally:
        conn.close()
        logger.info("Done.")


if __name__ == "__main__":
    logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
    run_insert_subway_timetable()
