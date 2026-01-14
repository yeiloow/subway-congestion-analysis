import logging
import time
from src.utils.db_util import get_connection
from src.utils.admin_dong import get_admin_dong
from src.utils.config import LOG_FORMAT, LOG_LEVEL
from dotenv import load_dotenv
import os

load_dotenv()

logger = logging.getLogger(__name__)

KAKAO_API_KEY = os.getenv("KAKAO_API_KEY")


def update_all_admin_dong():
    """
    Station_Routes 테이블의 모든 레코드에 대해
    road_address를 기반으로 행정동을 조회하여 administrative_dong을 업데이트합니다.
    """
    if not KAKAO_API_KEY:
        logger.error("KAKAO_API_KEY가 설정되지 않았습니다. .env 파일을 확인하세요.")
        return

    conn = get_connection()
    cursor = conn.cursor()

    # 모든 Station_Routes 조회
    cursor.execute(
        """
        SELECT route_id, station_code, road_address, admin_dong_name
        FROM Station_Routes
        """
    )
    rows = cursor.fetchall()

    logger.info(f"총 {len(rows)}개의 역 정보를 업데이트합니다.")

    updated_count = 0
    failed_count = 0

    for route_id, station_code, road_address, current_dong in rows:
        if not road_address:
            logger.warning(f"[{station_code}] 도로명주소가 없어 건너뜁니다.")
            failed_count += 1
            continue

        try:
            new_dong, dong_code = get_admin_dong(road_address, KAKAO_API_KEY)

            if new_dong in ["주소 확인 불가", "행정동 정보 없음"]:
                logger.warning(f"[{station_code}] {road_address} -> {new_dong}")
                failed_count += 1
                continue

            # TODO: dong_code (행정동 코드)를 DB에 저장하려면 컬럼 추가 필요
            # 현재는 행정동 명칭만 업데이트

            # 행정동 명칭과 코드를 모두 업데이트
            cursor.execute(
                """
                UPDATE Station_Routes
                SET admin_dong_name = ?, admin_dong_code = ?
                WHERE route_id = ?
                """,
                (new_dong, dong_code, route_id),
            )

            logger.info(f"[{station_code}] {current_dong} -> {new_dong}")
            updated_count += 1

            # API 호출 제한을 피하기 위한 딜레이
            time.sleep(0.1)

        except Exception as e:
            logger.error(f"[{station_code}] 업데이트 실패: {e}")
            failed_count += 1

    conn.commit()
    conn.close()

    logger.info(f"업데이트 완료: 성공 {updated_count}개, 실패 {failed_count}개")


if __name__ == "__main__":
    logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
    update_all_admin_dong()
