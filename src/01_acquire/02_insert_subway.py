import csv
import logging
from src.utils.db_util import get_connection
from src.utils.admin_dong import get_admin_dong
from src.utils.config import DATA_DIR, OUTPUT_DIR, LOG_FORMAT, LOG_LEVEL
from dotenv import load_dotenv
import os
import time

# Configure Logging
# logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

# Define file paths
import unicodedata
from huggingface_hub import hf_hub_download

# Define file paths
# Note: Filenames in HF repo have mixed normalizations.
# Folder "01_raw/지하철역" is NFD.
_folder = unicodedata.normalize("NFD", "01_raw/지하철역")

# File 1: "서울교통공사..." is NFD.
_base_name = unicodedata.normalize("NFD", "서울교통공사_역주소_전화번호_20250318.csv")
BASE_CSV_PATH = hf_hub_download(
    repo_id="alrq/subway",
    filename=f"{_folder}/{_base_name}",
    repo_type="dataset",
)

# File 2: "역사마스터정보.csv" is NFC.
_ref_name = unicodedata.normalize("NFC", "역사마스터정보.csv")
REF_CSV_PATH = hf_hub_download(
    repo_id="alrq/subway",
    filename=f"{_folder}/{_ref_name}",
    repo_type="dataset",
)

load_dotenv()
KAKAO_API_KEY = os.getenv("KAKAO_API_KEY")


def load_reference_data():
    """
    Load reference data (lat, lon) from the master file.
    Returns a dict: key=(line_name, station_code_str), value=(lat, lon)
    Also creates a secondary lookup: key=(line_name, station_name), value=(lat, lon)
    """
    ref_by_code = {}
    ref_by_name = {}

    if not os.path.exists(REF_CSV_PATH):
        logger.warning(f"Reference file not found: {REF_CSV_PATH}")
        return {}, {}

    with open(REF_CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row or len(row) < 5:
                continue

            # Format: Line, Name, Code, Lat, Lon
            line = row[0].strip()
            name = row[1].strip()
            code_raw = row[2].strip()
            lat = row[3].strip()
            lon = row[4].strip()

            try:
                code = str(int(float(code_raw)))
            except (ValueError, TypeError):
                code = code_raw

            try:
                lat_val = float(lat)
                lon_val = float(lon)
            except ValueError:
                lat_val = None
                lon_val = None

            ref_by_code[(line, code)] = (lat_val, lon_val)
            ref_by_name[(line, name)] = (lat_val, lon_val)

    return ref_by_code, ref_by_name


def get_or_create_station(conn, station_name):
    cur = conn.cursor()
    cur.execute(
        "SELECT station_id FROM Stations WHERE station_name_kr = ?", (station_name,)
    )
    row = cur.fetchone()
    if row:
        return row[0]
    else:
        cur.execute(
            "INSERT INTO Stations (station_name_kr) VALUES (?)", (station_name,)
        )
        return cur.lastrowid


def get_or_create_line(conn, line_name):
    cur = conn.cursor()
    cur.execute("SELECT line_id FROM Lines WHERE line_name = ?", (line_name,))
    row = cur.fetchone()
    if row:
        return row[0]
    else:
        cur.execute("INSERT INTO Lines (line_name) VALUES (?)", (line_name,))
        return cur.lastrowid


def run_insert_subway():
    try:
        conn = get_connection()
    except Exception as e:
        logger.error(e)
        return

    # No longer re-initializing DB here. Rely on 01_init_db.py

    ref_by_code, ref_by_name = load_reference_data()

    logger.info("Start processing subway stations...")

    if not KAKAO_API_KEY:
        logger.error("KAKAO_API_KEY is missing. Please check your .env file.")
        return

    if not os.path.exists(BASE_CSV_PATH):
        logger.error(f"Base CSV path not found: {BASE_CSV_PATH}")
        return

    with open(BASE_CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            line_num = row["호선"].strip()
            line_name = f"{line_num}호선"  # Normalize '1' -> '1호선'

            station_name = row["역명"].strip().split("(")[0]
            station_code = row["역번호"].strip()
            road_addr = row["도로명주소"].strip()

            coords = ref_by_code.get((line_name, station_code))
            if not coords:
                coords = ref_by_name.get((line_name, station_name))

            if coords:
                lat, lon = coords
            else:
                lat, lon = None, None

            # dong = extract_dong(jibeon_addr)
            dong = get_admin_dong(road_addr, KAKAO_API_KEY)

            # API rate limiting
            time.sleep(0.1)

            try:
                s_id = get_or_create_station(conn, station_name)
                l_id = get_or_create_line(conn, line_name)

                conn.execute(
                    """
                    INSERT OR REPLACE INTO Station_Routes 
                    (station_id, line_id, station_code, road_address, admin_dong_name, admin_dong_code, lat, lon)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (s_id, l_id, station_code, road_addr, dong[0], dong[1], lat, lon),
                )

            except Exception as e:
                logger.error(f"Error processing {line_name} {station_name}: {e}")

    conn.commit()
    conn.close()
    logger.info("Database population completed.")


if __name__ == "__main__":
    logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
    run_insert_subway()
