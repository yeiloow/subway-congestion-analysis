import os

db_path = "db/subway.db"

# DB 연결이 닫혀있는지 확인 후 삭제
if os.path.exists(db_path):
    os.remove(db_path)
    print("데이터베이스 파일이 삭제되었습니다.")
