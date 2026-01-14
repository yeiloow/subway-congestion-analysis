import os
from src.utils.config import DB_PATH


def clean_db():
    # DB 연결이 닫혀있는지 확인 후 삭제
    if os.path.exists(DB_PATH):
        try:
            os.remove(DB_PATH)
            print(f"데이터베이스 파일이 삭제되었습니다: {DB_PATH}")
        except PermissionError:
            print(
                f"파일을 삭제할 수 없습니다. 다른 프로그램에서 사용 중일 수 있습니다: {DB_PATH}"
            )
    else:
        print(f"삭제할 데이터베이스 파일이 없습니다: {DB_PATH}")


if __name__ == "__main__":
    clean_db()
