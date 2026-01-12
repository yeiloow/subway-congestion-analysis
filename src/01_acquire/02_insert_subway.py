import csv
import logging
from src.utils.db_util import get_connection
from src.utils.config import DATA_DIR, OUTPUT_DIR, LOG_FORMAT, LOG_LEVEL

# Configure Logging
# logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

# Define file paths
BASE_CSV_PATH = (
    DATA_DIR / "01_raw/01_subway_info/서울교통공사_역주소 및 전화번호_20250318.csv"
)
REF_CSV_PATH = DATA_DIR / "01_raw/01_subway_info/역사마스터정보.csv"
DONG_POP_CSV_PATH = (
    DATA_DIR / "01_raw/07_openapi/서울시_상권분석서비스_직장인구_행정동_2023_2025.csv"
)


def load_reference_data():
    """
    Load reference data (lat, lon) from the master file.
    Returns a dict: key=(line_name, station_code_str), value=(lat, lon)
    Also creates a secondary lookup: key=(line_name, station_name), value=(lat, lon)
    """
    ref_by_code = {}
    ref_by_name = {}

    if not REF_CSV_PATH.exists():
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


def extract_dong(jibeon_address):
    """
    Extract administrative dong from Jibeon address.
    Heuristic:
    - Seoul: 3rd word (Seoul, Gu, Dong)
    - Gyeonggi: 4th word (Gyeonggi, City, Gu, Dong) or 3rd if no Gu
    """
    if not jibeon_address:
        return None

    parts = jibeon_address.split()
    if len(parts) < 3:
        return None

    # Check for Gyeonggi-do
    if parts[0].startswith("경기"):
        if len(parts) > 3:
            if parts[2].endswith("구"):
                return parts[3]
            else:
                return parts[2]
        return parts[2]

    # Check for Seoul/Incheon
    if parts[0].startswith("서울") or parts[0].startswith("인천"):
        return parts[2]

    return parts[2]


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


def populate_dong_workplace_pop(conn):
    logger.info("Populating Dong_Workplace_Population...")
    try:
        if not DONG_POP_CSV_PATH.exists():
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

    if not BASE_CSV_PATH.exists():
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
            jibeon_addr = row["지번주소"].strip()

            coords = ref_by_code.get((line_name, station_code))
            if not coords:
                coords = ref_by_name.get((line_name, station_name))

            if coords:
                lat, lon = coords
            else:
                lat, lon = None, None

            dong = extract_dong(jibeon_addr)

            try:
                s_id = get_or_create_station(conn, station_name)
                l_id = get_or_create_line(conn, line_name)

                conn.execute(
                    """
                    INSERT OR REPLACE INTO Station_Routes 
                    (station_id, line_id, station_number, road_address, administrative_dong, lat, lon)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                    (s_id, l_id, station_code, road_addr, dong, lat, lon),
                )

            except Exception as e:
                logger.error(f"Error processing {line_name} {station_name}: {e}")

    populate_dong_workplace_pop(conn)

    conn.commit()
    conn.close()
    logger.info("Database population completed.")


if __name__ == "__main__":
    logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
    run_insert_subway()
