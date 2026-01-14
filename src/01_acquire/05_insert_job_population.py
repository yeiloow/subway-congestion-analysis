import csv
import logging
from src.utils.db_util import get_connection
from src.utils.config import OUTPUT_DIR, LOG_FORMAT, LOG_LEVEL
import unicodedata
from huggingface_hub import hf_hub_download
import os

# Configure Logging
# logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)


# Define file path
# Note: Folder "01_raw/상권분석서비스" is NFD.
_folder = unicodedata.normalize("NFD", "01_raw/상권분석서비스")
# Filename is likely NFC (based on repr) but using NFD for folder is critical.
_filename = unicodedata.normalize(
    "NFC", "서울시_상권분석서비스_직장인구_행정동_2023_2025.csv"
)

DONG_POP_CSV_PATH = hf_hub_download(
    repo_id="alrq/subway",
    filename=f"{_folder}/{_filename}",
    repo_type="dataset",
)


def populate_dong_workplace_pop(conn):
    logger.info("Populating Dong_Workplace_Population...")
    try:
        if not os.path.exists(DONG_POP_CSV_PATH):
            logger.error(f"File not found: {DONG_POP_CSV_PATH}")
            return

        with open(DONG_POP_CSV_PATH, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            _ = next(reader)  # Skip header

            query = """
                INSERT OR IGNORE INTO Dong_Workplace_Population (
                    quarter_code, admin_dong_code, admin_dong_name, total_pop, male_pop, female_pop,
                    age_10_pop, age_20_pop, age_30_pop, age_40_pop, age_50_pop, age_60_over_pop,
                    male_age_10_pop, male_age_20_pop, male_age_30_pop, male_age_40_pop, male_age_50_pop, male_age_60_over_pop,
                    female_age_10_pop, female_age_20_pop, female_age_30_pop, female_age_40_pop, female_age_50_pop, female_age_60_over_pop
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            count = 0
            for row in reader:
                if len(row) < 24:
                    continue

                cleaned_row = []
                for i, val in enumerate(row):
                    val = val.strip()
                    if i >= 3:  # Numeric columns start from index 3
                        try:
                            if not val:
                                cleaned_row.append(0)
                            else:
                                cleaned_row.append(int(float(val)))
                        except ValueError:
                            cleaned_row.append(None)
                    else:
                        cleaned_row.append(val)

                conn.execute(query, cleaned_row)
                count += 1

        logger.info(f"Inserted {count} rows into Dong_Workplace_Population.")

    except Exception as e:
        logger.error(f"Error populating Dong_Workplace_Population: {e}")


def run_insert_job_population():
    try:
        conn = get_connection()
    except Exception as e:
        logger.error(e)
        return

    try:
        populate_dong_workplace_pop(conn)
        conn.commit()
    except Exception as e:
        logger.error(f"Error during insertion: {e}")
    finally:
        conn.close()

    logger.info("Job population data insertion completed.")


if __name__ == "__main__":
    logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
    run_insert_job_population()
