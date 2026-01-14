import logging
import time
import os
import sys
import importlib.util

# Add project root to sys.path to allow importing src.utils
# This file is in src/01_acquire/, so root is ../..
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.utils.config import LOG_LEVEL, LOG_FORMAT

# Configure Logging Globally
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)


def import_module_from_path(module_name, file_path):
    """
    Dynamically import a module from a file path.
    This is necessary because the directory and file names start with numbers,
    which are not valid Python identifiers for standard imports.
    """
    try:
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        if spec is None:
            logger.error(f"{file_path}에서 {module_name}의 spec을 로드할 수 없습니다")
            return None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module
    except Exception as e:
        logger.error(
            f"{file_path}에서 {module_name}을(를) 가져오는 데 실패했습니다: {e}"
        )
        return None


def main():
    start_time = time.time()
    logger.info("데이터베이스 생성 및 데이터 적재 프로세스를 시작합니다...")

    # Get current directory
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Define steps

    steps = [
        ("01_init_db.py", "run_init_db", "1단계: 데이터베이스 초기화"),
        (
            "01_init_db.py",
            "run_init_weather_db",
            "1-1단계: 날씨 데이터베이스 초기화",
        ),
        (
            "02_insert_subway.py",
            "run_insert_subway",
            "2단계: 지하철 정보 추가",
        ),
        (
            "06_insert_subway_timetable.py",
            "run_insert_subway_timetable",
            "4단계: 지하철 시간표 추가",
        ),
        (
            "03_insert_congestion.py",
            "run_insert_congestion",
            "5단계: 혼잡도 데이터 추가",
        ),
        (
            "03_insert_job_population.py",
            "run_insert_job_population",
            "6단계: 직장인구 데이터 추가",
        ),
        (
            "04_insert_floating_population.py",
            "run_insert_floating_population",
            "7단계: 유동인구 데이터 추가",
        ),
        (
            "04_insert_living_population.py",
            "run_insert_living_population",
            "8단계: 생활인구 데이터 추가",
        ),
        (
            "05_insert_estimated_revenue.py",
            "run_insert_estimated_revenue",
            "9단계: 추정 매출 데이터 추가",
        ),
        (
            "07_insert_weather.py",
            "run_insert_weather",
            "10단계: 날씨 데이터 추가",
        ),
        (
            "09_insert_impact_analysis.py",
            "run_insert_impact_analysis",
            "11단계: 영향 분석 데이터 추가",
        ),
        (
            "10_insert_admin_dong_mapping.py",
            "run_insert_admin_dong_mapping",
            "12단계: 행정동 매핑 데이터 추가",
        ),
        (
            "11_insert_station_catchment_buildings.py",
            "main",
            "13단계: 역세권 계산",
        ),
    ]

    for filename, func_name, step_desc in steps:
        logger.info(f">>> {step_desc}")
        file_path = os.path.join(current_dir, filename)

        module_name = filename.replace(".py", "")
        module = import_module_from_path(module_name, file_path)

        if module and hasattr(module, func_name):
            try:
                func = getattr(module, func_name)
                func()
            except Exception as e:
                logger.error(f"{filename}의 {func_name} 실행 중 오류 발생: {e}")
                # Decide whether to continue or stop. For now, we continue but mark failure.
        else:
            logger.error(f"{filename}에서 '{func_name}' 함수를 찾을 수 없습니다")

    elapsed_time = time.time() - start_time
    logger.info(
        f"모든 단계가 완료되었습니다 (오류는 로그를 확인하세요). 소요 시간: {elapsed_time:.2f}초"
    )


if __name__ == "__main__":
    main()
