import logging
from src.utils.db_util import get_connection
from src.utils.config import LOG_FORMAT, LOG_LEVEL

# Configure Logging
# logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)


def insert_admin_dong_mapping():
    """
    Dong_Workplace_Population 테이블에서 고유한 admin_dong_code와 admin_dong_name을 추출하여
    Admin_Dong_Mapping 테이블에 삽입합니다.
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        # 1. 기존 데이터 조회 (Dong_Workplace_Population이 가장 신뢰할 수 있는 소스라고 가정)
        # 다른 테이블(Dong_Floating_Population 등)에서도 가져올 수 있지만 일단 하나만 사용
        cursor.execute(
            """
            SELECT DISTINCT admin_dong_code, admin_dong_name
            FROM Dong_Workplace_Population
            WHERE admin_dong_code IS NOT NULL AND admin_dong_name IS NOT NULL
            """
        )
        rows = cursor.fetchall()
        logger.info(
            f"Dong_Workplace_Population에서 {len(rows)}개의 고유 행정동 정보를 찾았습니다."
        )

        # 2. Admin_Dong_Mapping 테이블에 삽입 (IGNORE로 중복 무시)
        count = 0
        for code, name in rows:
            cursor.execute(
                """
                INSERT OR IGNORE INTO Admin_Dong_Mapping (admin_dong_code, admin_dong_name)
                VALUES (?, ?)
                """,
                (code, name),
            )
            count += 1

        conn.commit()
        logger.info(f"Admin_Dong_Mapping 테이블에 {count}개의 데이터가 처리되었습니다.")

    except Exception as e:
        logger.error(f"Admin_Dong_Mapping 입력 중 오류 발생: {e}")
        conn.rollback()
    finally:
        conn.close()


def run_insert_admin_dong_mapping():
    insert_admin_dong_mapping()


if __name__ == "__main__":
    logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
    run_insert_admin_dong_mapping()
