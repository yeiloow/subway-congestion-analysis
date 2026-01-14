import pandas as pd
import sqlite3
import os
import re
import glob
import logging
from src.utils.db_util import get_connection
from src.utils.config import DATA_DIR, LOG_FORMAT, LOG_LEVEL

# Configure Logging
# logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)


def get_quarter_code(filename):
    """
    Extracts quarter code from filename (e.g., ..._20231231.csv -> 20234).
    Assumes filename ends with YYYYMMDD.csv
    """
    match = re.search(r"(\d{4})(\d{2})(\d{2})\.csv$", filename)
    if not match:
        return None

    year = match.group(1)
    month = int(match.group(2))

    if 1 <= month <= 3:
        quarter = 1
    elif 4 <= month <= 6:
        quarter = 2
    elif 7 <= month <= 9:
        quarter = 3
    elif 10 <= month <= 12:
        quarter = 4
    else:
        return None

    return f"{year}{quarter}"


def process_and_insert(file_path, conn):
    logger.info(f"Processing {file_path}...")

    filename = os.path.basename(file_path)
    quarter_code = get_quarter_code(filename)

    if not quarter_code:
        logger.warning(f"Skipping {filename}: Could not determine quarter code.")
        return

    # Load dataset
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")
        return

    # Define time columns
    cols = [
        "5시30분",
        "6시00분",
        "6시30분",
        "7시00분",
        "7시30분",
        "8시00분",
        "8시30분",
        "9시00분",
        "9시30분",
        "10시00분",
        "10시30분",
        "11시00분",
        "11시30분",
        "12시00분",
        "12시30분",
        "13시00분",
        "13시30분",
        "14시00분",
        "14시30분",
        "15시00분",
        "15시30분",
        "16시00분",
        "16시30분",
        "17시00분",
        "17시30분",
        "18시00분",
        "18시30분",
        "19시00분",
        "19시30분",
        "20시00분",
        "20시30분",
        "21시00분",
        "21시30분",
        "22시00분",
        "22시30분",
        "23시00분",
        "23시30분",
        "00시00분",
        "00시30분",
    ]

    # Rename time columns to integers 0..38
    # Only map columns that exist in the dataframe
    existing_cols = [c for c in cols if c in df.columns]
    time_mapping = {time: i for i, time in enumerate(cols) if time in existing_cols}
    df_renamed = df.rename(columns=time_mapping)

    # Melt the dataframe
    # Keep identifying columns
    id_vars = ["연번", "요일구분", "호선", "역번호", "출발역", "상하구분"]
    # Check if all id_vars exist
    missing_ids = [col for col in id_vars if col not in df_renamed.columns]
    if missing_ids:
        logger.warning(f"Skipping {filename}: Missing columns {missing_ids}")
        return

    value_vars = list(time_mapping.values())

    melted_df = df_renamed.melt(
        id_vars=id_vars,
        value_vars=value_vars,
        var_name="time_slot",
        value_name="congestion_level",
    )

    # Filter out missing congestion levels (if any)
    melted_df = melted_df.dropna(subset=["congestion_level"])

    # Transform columns to match schema

    # 1. Map '요일구분' to day_of_week
    day_map = {
        "평일": 0,
        "토요일": 1,
        "일요일": 2,
        "공휴일": 2,
    }  # Added public holiday as Sunday equivalent
    melted_df["day_of_week"] = melted_df["요일구분"].map(day_map)
    # Filter out unmapped days if any
    melted_df = melted_df.dropna(subset=["day_of_week"])
    melted_df["day_of_week"] = melted_df["day_of_week"].astype(int)

    # 2. Map '상하구분' to is_upline
    upline_map = {"상선": 0, "내선": 0, "하선": 1, "외선": 1}
    melted_df["is_upline"] = melted_df["상하구분"].map(upline_map)
    # Filter out unmapped directions if any
    melted_df = melted_df.dropna(subset=["is_upline"])
    melted_df["is_upline"] = melted_df["is_upline"].astype(int)

    # 3. Ensure station_code is string
    melted_df["station_code"] = melted_df["역번호"].astype(str)

    # 4. Add quarter_code
    melted_df["quarter_code"] = quarter_code

    # Select final columns
    final_df = melted_df[
        [
            "quarter_code",
            "station_code",
            "day_of_week",
            "is_upline",
            "time_slot",
            "congestion_level",
        ]
    ]

    # Fix wrong number
    # 까치산역 260 -> 200
    final_df.loc[final_df["station_code"] == "260", "station_code"] = "200"

    # Drop non existing stations
    final_df = final_df[
        ~final_df["station_code"].isin(["260", "9001", "9002", "9003", "9005", "9006"])
    ]
    final_df.dropna(subset=["station_code"], inplace=True)

    # Clean non-numeric congestion levels (just in case)
    final_df["congestion_level"] = pd.to_numeric(
        final_df["congestion_level"], errors="coerce"
    )
    final_df.dropna(subset=["congestion_level"], inplace=True)

    # VALIDATION: Check if all station_codes exist in Station_Routes
    valid_stations_df = pd.read_sql("SELECT station_code FROM Station_Routes", conn)
    valid_stations = set(valid_stations_df["station_code"].astype(str))

    # Filter final_df to only include valid stations
    final_df = final_df[final_df["station_code"].isin(valid_stations)]

    try:
        # Insert into Station_Congestion
        final_df.to_sql("Station_Congestion", conn, if_exists="append", index=False)
        logger.info(f"Successfully inserted {len(final_df)} rows for {quarter_code}.")
    except sqlite3.IntegrityError as e:
        logger.error(f"Integrity Error inserting data for {quarter_code}: {e}")
        # Could implement fallback logic here
        pass
    except Exception as e:
        logger.error(f"Error inserting data: {e}")


def run_insert_congestion():
    try:
        conn = get_connection()
    except Exception as e:
        logger.error(e)
        return

    # Define files to process
    import unicodedata
    from huggingface_hub import hf_hub_download

    # Folder "01_raw/지하철혼잡도" is NFD
    _folder = unicodedata.normalize("NFC", "01_raw/지하철혼잡도")

    target_dates = ["20231231", "20240331", "20240630", "20241231", "20250331"]

    files = []
    repo_id = "alrq/subway"

    for date_str in target_dates:
        filename = f"서울교통공사_지하철혼잡도정보_{date_str}.csv"
        # Filename is likely NFC
        _filename = unicodedata.normalize("NFC", filename)

        try:
            logger.info(f"Downloading {_filename}...")
            file_path = hf_hub_download(
                repo_id=repo_id, filename=f"{_folder}/{_filename}", repo_type="dataset"
            )
            files.append(file_path)
        except Exception as e:
            logger.error(f"Error downloading {_filename}: {e}")

    if not files:
        logger.warning("No files downloaded.")
        return

    conn.execute("PRAGMA foreign_keys = ON")

    try:
        for file_path in sorted(files):
            # Clean up existing data for that quarter to allow re-runs
            filename = os.path.basename(file_path)
            quarter_code = get_quarter_code(filename)
            if quarter_code:
                cursor = conn.cursor()
                cursor.execute(
                    "DELETE FROM Station_Congestion WHERE quarter_code = ?",
                    (quarter_code,),
                )
                conn.commit()
                logger.info(f"Cleared existing data for {quarter_code}")

            process_and_insert(file_path, conn)

    finally:
        conn.close()
        logger.info("Done.")


if __name__ == "__main__":
    logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
    run_insert_congestion()
