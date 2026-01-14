import os
import logging
from src.utils.db_util import get_engine, get_weather_engine
from src.utils.config import DB_PATH, WEATHER_DB_PATH, LOG_FORMAT, LOG_LEVEL

# Configure Logging
# logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)


def init_database(schema_path, engine, db_path_for_log):
    """
    지정된 SQL 스키마 파일을 사용하여 데이터베이스를 초기화합니다.
    SQLAlchemy를 사용하여 DB 연결을 관리합니다.
    """
    try:
        # 1. SQL 파일 읽기
        if not os.path.exists(schema_path):
            raise FileNotFoundError(f"스키마 파일을 찾을 수 없습니다: {schema_path}")

        with open(schema_path, "r", encoding="utf-8") as f:
            sql_script = f.read()

        # 3. DB 연결 및 스크립트 실행
        with engine.connect() as conn:
            # SQLAlchemy의 execute()는 기본적으로 다중 구문을 지원하지 않을 수 있음 (드라이버 의존적)
            # SQLite의 경우 raw connection을 통해 executescript를 사용하는 것이 가장 안전함
            connection = conn.connection
            cursor = connection.cursor()
            cursor.executescript(sql_script)

            # executescript는 보통 즉시 적용되지만, 명시적으로 커밋을 호출함.
            connection.commit()

        logger.info(
            f"성공: '{db_path_for_log}' 데이터베이스가 생성되고 초기화되었습니다."
        )

    except Exception as e:
        logger.error(f"알 수 없는 에러: {e}")


def run_init_db(schema_name="db/schema.sql"):
    # 디렉토리 존재 확인 및 생성 (DB_DIR handled in config, but good to ensure)
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    engine = get_engine()
    init_database(schema_name, engine, DB_PATH)


def run_init_weather_db(schema_name="db/weather_schema.sql"):
    # 디렉토리 존재 확인 및 생성
    os.makedirs(os.path.dirname(WEATHER_DB_PATH), exist_ok=True)
    engine = get_weather_engine()
    init_database(schema_name, engine, WEATHER_DB_PATH)


if __name__ == "__main__":
    # Configure Logging only when run directly
    logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)

    # 설정: DB 파일명과 SQL 파일 경로
    # config.py의 DB_PATH를 사용하므로 여기서는 스키마 경로만 지정하면 됨.
    # 스키마 파일은 보통 db/schema.sql에 위치함.

    # Assuming the script execution context, finding schema.sql relative to config or project root is safer.
    # recreating the path logic here relative to this script for now, but ideally config should have SCHEMA_PATH

    # Lets assume we run from root as per README
    SCHEMA_NAME = "db/schema.sql"
    WEATHER_SCHEMA_NAME = "db/weather_schema.sql"

    run_init_db(SCHEMA_NAME)
    run_init_weather_db(WEATHER_SCHEMA_NAME)
