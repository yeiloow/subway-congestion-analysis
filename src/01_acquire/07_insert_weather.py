import os
import sys
import logging
import pandas as pd

# Add project root to sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.utils.db_util import get_weather_engine
from src.utils.config import LOG_FORMAT, LOG_LEVEL

# Configure Logging
logger = logging.getLogger(__name__)


def insert_daily_temperature(engine):
    file_path = "data/daily_min_max_temp_202301_202512.csv"
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return

    logger.info(f"Loading {file_path}...")
    df = pd.read_csv(file_path)

    # Rename columns to match DB schema
    df = df.rename(columns={"date": "base_date"})

    # Ensure base_date is string
    df["base_date"] = df["base_date"].astype(str)

    logger.info(f"Inserting {len(df)} rows into Daily_Temperature...")
    try:
        df.to_sql("Daily_Temperature", engine, if_exists="append", index=False)
        logger.info("Successfully inserted Daily_Temperature data.")
    except Exception as e:
        logger.error(f"Error inserting Daily_Temperature: {e}")


def insert_hourly_weather(engine):
    file_path = "data/day_weather_202301_202512.csv"
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return

    logger.info(f"Loading {file_path}...")
    df = pd.read_csv(file_path)

    # Rename columns to match DB schema
    df = df.rename(columns={"date": "base_date"})

    # Ensure base_date is string
    df["base_date"] = df["base_date"].astype(str)

    logger.info(f"Inserting {len(df)} rows into Hourly_Weather...")
    try:
        df.to_sql("Hourly_Weather", engine, if_exists="append", index=False)
        logger.info("Successfully inserted Hourly_Weather data.")
    except Exception as e:
        logger.error(f"Error inserting Hourly_Weather: {e}")


def run_insert_weather():
    engine = get_weather_engine()
    insert_daily_temperature(engine)
    insert_hourly_weather(engine)


if __name__ == "__main__":
    logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
    run_insert_weather()
