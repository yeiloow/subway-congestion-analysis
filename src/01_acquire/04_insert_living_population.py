import pandas as pd
import sqlite3
import logging
from src.utils.db_util import get_connection
from src.utils.config import DATA_DIR, LOG_FORMAT, LOG_LEVEL

# Configure Logging
# logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

# Base Data Directory for raw files
RAW_DATA_DIR = DATA_DIR / "01_raw"

# Mapping for Local People columns (Korean -> English)
LOCAL_COL_MAP = {
    "총생활인구수": "local_total_living_pop",
    "남자0세부터9세생활인구수": "local_male_age_0_9_pop",
    "남자10세부터14세생활인구수": "local_male_age_10_14_pop",
    "남자15세부터19세생활인구수": "local_male_age_15_19_pop",
    "남자20세부터24세생활인구수": "local_male_age_20_24_pop",
    "남자25세부터29세생활인구수": "local_male_age_25_29_pop",
    "남자30세부터34세생활인구수": "local_male_age_30_34_pop",
    "남자35세부터39세생활인구수": "local_male_age_35_39_pop",
    "남자40세부터44세생활인구수": "local_male_age_40_44_pop",
    "남자45세부터49세생활인구수": "local_male_age_45_49_pop",
    "남자50세부터54세생활인구수": "local_male_age_50_54_pop",
    "남자55세부터59세생활인구수": "local_male_age_55_59_pop",
    "남자60세부터64세생활인구수": "local_male_age_60_64_pop",
    "남자65세부터69세생활인구수": "local_male_age_65_69_pop",
    "남자70세이상생활인구수": "local_male_age_70_over_pop",
    "여자0세부터9세생활인구수": "local_female_age_0_9_pop",
    "여자10세부터14세생활인구수": "local_female_age_10_14_pop",
    "여자15세부터19세생활인구수": "local_female_age_15_19_pop",
    "여자20세부터24세생활인구수": "local_female_age_20_24_pop",
    "여자25세부터29세생활인구수": "local_female_age_25_29_pop",
    "여자30세부터34세생활인구수": "local_female_age_30_34_pop",
    "여자35세부터39세생활인구수": "local_female_age_35_39_pop",
    "여자40세부터44세생활인구수": "local_female_age_40_44_pop",
    "여자45세부터49세생활인구수": "local_female_age_45_49_pop",
    "여자50세부터54세생활인구수": "local_female_age_50_54_pop",
    "여자55세부터59세생활인구수": "local_female_age_55_59_pop",
    "여자60세부터64세생활인구수": "local_female_age_60_64_pop",
    "여자65세부터69세생활인구수": "local_female_age_65_69_pop",
    "여자70세이상생활인구수": "local_female_age_70_over_pop",
}

# Mapping for Long-Term Foreigner columns
LONG_TERM_COL_MAP = {
    "중국인체류인구수": "long_term_chinese_stay_pop",
    "중국외외국인체류인구수": "long_term_non_chinese_stay_pop",
}

# Mapping for Short-Term Foreigner columns
SHORT_TERM_COL_MAP = {
    "중국인체류인구수": "short_term_chinese_stay_pop",
    "중국외외국인체류인구수": "short_term_non_chinese_stay_pop",
}


def get_file_path(category, year, month):
    # category: '03_local', '04_long_foreigner', '05_temp_foreigner'
    # filename pattern: LOCAL_PEOPLE_DONG_YYYYMM.csv, LONG_FOREIGNER_DONG_YYYYMM.csv, TEMP_FOREIGNER_DONG_YYYYMM.csv

    if category == "03_local":
        filename = f"LOCAL_PEOPLE_DONG_{year}{month}.csv"
    elif category == "04_long_foreigner":
        filename = f"LONG_FOREIGNER_DONG_{year}{month}.csv"
    elif category == "05_temp_foreigner":
        filename = f"TEMP_FOREIGNER_DONG_{year}{month}.csv"
    else:
        return None

    path = RAW_DATA_DIR / category / filename
    if path.exists():
        return path
    return None


def read_csv_safe(path, dtype, index_col=False):
    encodings = ["utf-8", "cp949", "euc-kr"]
    for enc in encodings:
        try:
            return pd.read_csv(path, dtype=dtype, index_col=index_col, encoding=enc)
        except UnicodeDecodeError:
            continue
        except Exception as e:
            logger.error(f"Error reading {path} with {enc}: {e}")
            raise e
    raise ValueError(f"Cwld not read {path} with any of the attempted encodings.")


def process_quarter(year, quarter_end_date):
    # quarter_end_date: 'MMDD' string, e.g., '0331'
    month = quarter_end_date[:2]
    target_date = f"{year}{quarter_end_date}"

    logger.info(f"Processing {year}-{month} (Target Date: {target_date})...")

    # Paths
    local_path = get_file_path("03_local", year, month)
    long_path = get_file_path("04_long_foreigner", year, month)
    short_path = get_file_path("05_temp_foreigner", year, month)

    if not (local_path and long_path and short_path):
        logger.warning(f"  Missing files for {year}-{month}. Skipping.")
        return None

    # Read and Filter
    try:
        # Local
        df_local_raw = read_csv_safe(local_path, dtype=str, index_col=False)
        # Rename first 3 cols to sanitize
        cols = df_local_raw.columns.tolist()
        if len(cols) >= 3:
            df_local_raw.rename(
                columns={
                    cols[0]: "기준일ID",
                    cols[1]: "시간대구분",
                    cols[2]: "행정동코드",
                },
                inplace=True,
            )

        df_local = df_local_raw[df_local_raw["기준일ID"] == target_date].copy()

        # Long-term
        df_long_raw = read_csv_safe(long_path, dtype=str, index_col=False)
        cols = df_long_raw.columns.tolist()
        if len(cols) >= 3:
            df_long_raw.rename(
                columns={
                    cols[0]: "기준일ID",
                    cols[1]: "시간대구분",
                    cols[2]: "행정동코드",
                },
                inplace=True,
            )

        df_long = df_long_raw[df_long_raw["기준일ID"] == target_date].copy()

        # Short-term
        df_short_raw = read_csv_safe(short_path, dtype=str, index_col=False)
        cols = df_short_raw.columns.tolist()
        if len(cols) >= 3:
            df_short_raw.rename(
                columns={
                    cols[0]: "기준일ID",
                    cols[1]: "시간대구분",
                    cols[2]: "행정동코드",
                },
                inplace=True,
            )

        df_short = df_short_raw[df_short_raw["기준일ID"] == target_date].copy()

        if df_local.empty:
            logger.warning(
                f"  Local data empty for {target_date}. Available dates: {df_local_raw['기준일ID'].unique()[:5]}"
            )
            return None
        if df_long.empty:
            logger.warning(
                f"  Long-term data empty for {target_date}. Available dates: {df_long_raw['기준일ID'].unique()[:5]}"
            )
            return None
        if df_short.empty:
            logger.warning(
                f"  Short-term data empty for {target_date}. Available dates: {df_short_raw['기준일ID'].unique()[:5]}"
            )
            return None

    except Exception as e:
        logger.error(f"  Error reading files: {e}")
        return None

    # Rename columns
    df_local.rename(columns=LOCAL_COL_MAP, inplace=True)
    df_long.rename(columns=LONG_TERM_COL_MAP, inplace=True)
    df_short.rename(columns=SHORT_TERM_COL_MAP, inplace=True)

    # Keep only necessary columns + join keys
    local_cols = ["기준일ID", "시간대구분", "행정동코드"] + list(LOCAL_COL_MAP.values())
    long_cols = ["기준일ID", "시간대구분", "행정동코드"] + list(
        LONG_TERM_COL_MAP.values()
    )
    short_cols = ["기준일ID", "시간대구분", "행정동코드"] + list(
        SHORT_TERM_COL_MAP.values()
    )

    # Intersection with existing columns (just in case some CSVs have extra cols)
    df_local = df_local[df_local.columns.intersection(local_cols)]
    df_long = df_long[df_long.columns.intersection(long_cols)]
    df_short = df_short[df_short.columns.intersection(short_cols)]

    # Merge
    merge_keys = ["기준일ID", "시간대구분", "행정동코드"]

    merged_df = pd.merge(df_local, df_long, on=merge_keys, how="inner")
    merged_df = pd.merge(merged_df, df_short, on=merge_keys, how="inner")

    # Rename keys to DB schema
    merged_df.rename(
        columns={
            "기준일ID": "base_date",
            "시간대구분": "time_slot",
            "행정동코드": "admin_dong_code",
        },
        inplace=True,
    )

    return merged_df


def run_insert_living_population():
    years = ["2023", "2024", "2025"]
    quarters_end = ["0331", "0630", "0930", "1231"]

    try:
        conn = get_connection()
    except Exception as e:
        logger.error(f"Failed to connect to DB: {e}")
        return

    total_inserted = 0

    try:
        for year in years:
            for q_end in quarters_end:
                df = process_quarter(year, q_end)
                if df is not None and not df.empty:
                    logger.info(f"  Inserting {len(df)} rows for {year}{q_end}...")

                    try:
                        df.to_sql(
                            "Dong_Living_Population",
                            conn,
                            if_exists="append",
                            index=False,
                        )
                        total_inserted += len(df)
                        logger.info("  Done.")
                    except sqlite3.Error as e:
                        logger.error(f"  Database Error: {e}")
                    except Exception as e:
                        logger.error(f"  Error inserting: {e}")
        conn.commit()
    finally:
        conn.close()

    logger.info(f"Total rows inserted: {total_inserted}")


if __name__ == "__main__":
    logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
    run_insert_living_population()
