from src.utils.config import DATA_DIR
import pandas as pd
import sqlite3
import logging
from src.utils.db_util import get_connection
from src.utils.config import LOG_FORMAT, LOG_LEVEL

# Configure Logging
# logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)


def run_insert_estimated_revenue():
    import unicodedata
    from huggingface_hub import hf_hub_download
    import os

    # Note: Folder "01_raw/상권분석서비스" is NFD.
    _folder = unicodedata.normalize("NFD", "01_raw/상권분석서비스")
    # Filename is likely NFC
    _filename = unicodedata.normalize(
        "NFC", "서울시_상권분석서비스_추정매출_행정동_2023_2025.csv"
    )

    input_file = hf_hub_download(
        repo_id="alrq/subway",
        filename=f"{_folder}/{_filename}",
        repo_type="dataset",
    )

    # Check if input file exists
    if not os.path.exists(input_file):
        logger.error(f"Error: Input file '{input_file}' not found.")
        return

    # Load data
    logger.info("Loading data...")
    # The file might be large, so we can use chunks if needed, but for now let's try reading it all.
    try:
        df = pd.read_csv(input_file, encoding="utf-8")
    except Exception as e:
        logger.error(f"Error reading CSV: {e}")
        return

    # Filter for years 2023, 2024, 2025
    # quarter_code format is YYYYQ (e.g., 20231)
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
        "서비스_업종_코드": "service_type_code",
        "서비스_업종_코드_명": "service_type_name",
        "당월_매출_금액": "month_sales_amt",
        "당월_매출_건수": "month_sales_cnt",
        "주중_매출_금액": "weekday_sales_amt",
        "주말_매출_금액": "weekend_sales_amt",
        "월요일_매출_금액": "mon_sales_amt",
        "화요일_매출_금액": "tue_sales_amt",
        "수요일_매출_금액": "wed_sales_amt",
        "목요일_매출_금액": "thu_sales_amt",
        "금요일_매출_금액": "fri_sales_amt",
        "토요일_매출_금액": "sat_sales_amt",
        "일요일_매출_금액": "sun_sales_amt",
        "시간대_00~06_매출_금액": "time_00_06_sales_amt",
        "시간대_06~11_매출_금액": "time_06_11_sales_amt",
        "시간대_11~14_매출_금액": "time_11_14_sales_amt",
        "시간대_14~17_매출_금액": "time_14_17_sales_amt",
        "시간대_17~21_매출_금액": "time_17_21_sales_amt",
        "시간대_21~24_매출_금액": "time_21_24_sales_amt",
        "남성_매출_금액": "male_sales_amt",
        "여성_매출_금액": "female_sales_amt",
        "연령대_10_매출_금액": "age_10_sales_amt",
        "연령대_20_매출_금액": "age_20_sales_amt",
        "연령대_30_매출_금액": "age_30_sales_amt",
        "연령대_40_매출_금액": "age_40_sales_amt",
        "연령대_50_매출_금액": "age_50_sales_amt",
        "연령대_60_이상_매출_금액": "age_60_over_sales_amt",
        "주중_매출_건수": "weekday_sales_cnt",
        "주말_매출_건수": "weekend_sales_cnt",
        "월요일_매출_건수": "mon_sales_cnt",
        "화요일_매출_건수": "tue_sales_cnt",
        "수요일_매출_건수": "wed_sales_cnt",
        "목요일_매출_건수": "thu_sales_cnt",
        "금요일_매출_건수": "fri_sales_cnt",
        "토요일_매출_건수": "sat_sales_cnt",
        "일요일_매출_건수": "sun_sales_cnt",
        "시간대_00~06_매출_건수": "time_00_06_sales_cnt",
        "시간대_06~11_매출_건수": "time_06_11_sales_cnt",
        "시간대_11~14_매출_건수": "time_11_14_sales_cnt",
        "시간대_14~17_매출_건수": "time_14_17_sales_cnt",
        "시간대_17~21_매출_건수": "time_17_21_sales_cnt",
        "시간대_21~24_매출_건수": "time_21_24_sales_cnt",
        "남성_매출_건수": "male_sales_cnt",
        "여성_매출_건수": "female_sales_cnt",
        "연령대_10_매출_건수": "age_10_sales_cnt",
        "연령대_20_매출_건수": "age_20_sales_cnt",
        "연령대_30_매출_건수": "age_30_sales_cnt",
        "연령대_40_매출_건수": "age_40_sales_cnt",
        "연령대_50_매출_건수": "age_50_sales_cnt",
        "연령대_60_이상_매출_건수": "age_60_over_sales_cnt",
    }

    # Rename columns
    filtered_df = filtered_df.rename(columns=column_mapping)

    # Remove duplicates
    # UNIQUE(quarter_code, admin_dong_code, service_type_code)
    duplicate_count = filtered_df.duplicated(
        subset=["quarter_code", "admin_dong_code", "service_type_code"]
    ).sum()
    if duplicate_count > 0:
        logger.warning(
            f"Found {duplicate_count} duplicate rows. Dropping duplicates (keeping first)."
        )
        filtered_df = filtered_df.drop_duplicates(
            subset=["quarter_code", "admin_dong_code", "service_type_code"],
            keep="first",
        )

    # Select columns to keep
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
        logger.info("Inserting data into Dong_Estimated_Revenue...")
        final_df.to_sql("Dong_Estimated_Revenue", conn, if_exists="append", index=False)
        logger.info("Data insertion completed successfully.")

    except sqlite3.Error as e:
        logger.error(f"An error occurred: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
    run_insert_estimated_revenue()
