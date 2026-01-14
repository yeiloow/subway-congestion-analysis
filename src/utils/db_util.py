import logging
import sqlite3
from sqlalchemy import create_engine
from src.utils.config import DB_URL, DB_PATH

logger = logging.getLogger(__name__)


def get_engine():
    """
    지하철 데이터베이스를 위한 SQLAlchemy 엔진을 생성하고 반환합니다.
    """
    try:
        engine = create_engine(DB_URL)
        return engine
    except Exception as e:
        logger.error(f"Failed to create engine: {e}")
        raise


def get_connection():
    """
    raw sqlite3 연결을 반환합니다.
    전체 ORM이 필요하지 않은 스크립트나 raw SQL이 선호될 수 있는 대량 삽입(bulk insert)에 유용합니다.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database at {DB_PATH}: {e}")
        raise


def get_weather_engine():
    """
    날씨 데이터베이스를 위한 SQLAlchemy 엔진을 생성하고 반환합니다.
    """
    try:
        from src.utils.config import WEATHER_DB_URL

        engine = create_engine(WEATHER_DB_URL)
        return engine
    except Exception as e:
        logger.error(f"Failed to create weather engine: {e}")
        raise


def get_weather_connection():
    """
    날씨 데이터베이스를 위한 raw sqlite3 연결을 반환합니다.
    """
    try:
        from src.utils.config import WEATHER_DB_PATH

        conn = sqlite3.connect(WEATHER_DB_PATH)
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to weather database at {WEATHER_DB_PATH}: {e}")
        raise
