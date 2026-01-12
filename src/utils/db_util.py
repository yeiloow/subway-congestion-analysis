import logging
import sqlite3
from sqlalchemy import create_engine
from src.utils.config import DB_URL, DB_PATH

logger = logging.getLogger(__name__)


def get_engine():
    """
    Creates and returns a SQLAlchemy engine for the subway database.
    """
    try:
        engine = create_engine(DB_URL)
        return engine
    except Exception as e:
        logger.error(f"Failed to create engine: {e}")
        raise


def get_connection():
    """
    Returns a raw sqlite3 connection.
    Useful for scripts that don't need the full ORM or for bulk inserts where raw SQL might be preferred.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database at {DB_PATH}: {e}")
        raise
