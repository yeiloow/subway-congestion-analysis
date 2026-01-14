import pandas as pd
import glob
import logging
from src.utils.db_util import get_connection
from src.utils.config import DATA_DIR, LOG_FORMAT, LOG_LEVEL

# Configure Logging
logger = logging.getLogger(__name__)


def run_insert_daily_passengers():
    try:
        conn = get_connection()
    except Exception as e:
        logger.error(e)
        return

    # Define file path
    import unicodedata
    from huggingface_hub import hf_hub_download

    # Folder "01_raw/지하철혼잡도" is NFD
    _folder = unicodedata.normalize("NFD", "01_raw/지하철혼잡도")
    # Filename is likely NFC
    _filename = unicodedata.normalize(
        "NFC", "서울시_역별_승하차_인원_정보_2023_2025.csv"
    )

    repo_id = "alrq/subway"

    try:
        logger.info(f"Downloading {_filename}...")
        file_path = hf_hub_download(
            repo_id=repo_id, filename=f"{_folder}/{_filename}", repo_type="dataset"
        )
        files = [file_path]
    except Exception as e:
        logger.error(f"Error downloading {_filename}: {e}")
        return

    try:
        for file_path in sorted(files):
            logger.info(f"Processing {file_path}...")

            try:
                df = pd.read_csv(file_path)
            except Exception as e:
                logger.error(f"Error reading {file_path}: {e}")
                continue

            # Check expected columns
            expected_cols = [
                "사용일자",
                "노선명",
                "역명",
                "승차총승객수",
                "하차총승객수",
                "등록일자",
            ]
            missing_cols = [col for col in expected_cols if col not in df.columns]
            if missing_cols:
                logger.warning(f"Skipping {file_path}: Missing columns {missing_cols}")
                continue

            # Rename columns
            column_mapping = {
                "사용일자": "usage_date",
                "노선명": "line_name",
                "역명": "station_name",
                "승차총승객수": "boarding_count",
                "하차총승객수": "alighting_count",
                "등록일자": "registration_date",
            }
            df_renamed = df.rename(columns=column_mapping)

            # Ensure data types
            df_renamed["usage_date"] = df_renamed["usage_date"].astype(str)
            df_renamed["line_name"] = df_renamed["line_name"].astype(str)
            df_renamed["station_name"] = df_renamed["station_name"].astype(str)
            # registration_date could be int or str in CSV, ensure str
            if "registration_date" in df_renamed.columns:
                df_renamed["registration_date"] = df_renamed["registration_date"].apply(
                    lambda x: str(x) if pd.notnull(x) else None
                )

            # Select columns to insert
            final_df = df_renamed[list(column_mapping.values())]

            # Clear existing data?
            # Since this seems to be a bulk load, and we have UNIQUE constraints,
            # we might want to truncate the table or use REPLACE.
            # Decision: DELETE all data before inserting to ensure a clean slate for this specific table,
            # assuming this script is the sole source of truth and the CSV contains a full history or we want to replace it.
            # However, if multiple files exist, we should be careful.
            # The prompt implies one file '2023_2025.csv'.
            # I will clear the table once before processing files if I can be sure.
            # Safest is 'append' and ignore duplicates or let it fail, but 'sqlite3' doesn't support 'INSERT OR IGNORE' via pandas to_sql easily without chunking or method override.
            # I will delete all rows from Station_Daily_Passengers first.

            cursor = conn.cursor()
            cursor.execute("DELETE FROM Station_Daily_Passengers")
            conn.commit()
            logger.info("Cleared Station_Daily_Passengers table.")

            # Insert data
            final_df.to_sql(
                "Station_Daily_Passengers", conn, if_exists="append", index=False
            )
            logger.info(f"Successfully inserted {len(final_df)} rows from {file_path}.")

    except Exception as e:
        logger.error(f"Error inserting data: {e}")
    finally:
        conn.close()
        logger.info("Done.")


if __name__ == "__main__":
    logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
    run_insert_daily_passengers()
