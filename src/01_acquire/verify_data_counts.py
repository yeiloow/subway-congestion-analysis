import sys
import os
from sqlalchemy import create_engine, text

# Add project root to sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.utils.config import DB_URL, WEATHER_DB_URL


def get_table_counts(db_url, db_label):
    """
    Connects to the database and prints row counts for all tables.
    """
    print(f"\n[{db_label} 검증 시작] ({db_url})")

    try:
        engine = create_engine(db_url)
        with engine.connect() as conn:
            # SQLite specific query to get table names
            query = text(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
            )
            tables = conn.execute(query).fetchall()

            if not tables:
                print(f"⚠️  {db_label}: 생성된 테이블이 없습니다.")
                return

            all_valid = True
            for table_name_tuple in tables:
                table_name = table_name_tuple[0]
                count_query = text(f"SELECT COUNT(*) FROM {table_name}")
                count = conn.execute(count_query).scalar()

                if count > 0:
                    print(f"✅ {table_name}: {count} rows")
                else:
                    print(f"❌ {table_name}: 0 rows (데이터 없음)")
                    all_valid = False

            if all_valid:
                print(f"✨ {db_label} 모든 테이블에 데이터가 존재합니다.")
            else:
                print(f"⚠️  {db_label} 일부 테이블에 데이터가 없습니다.")

    except Exception as e:
        print(f"❌ {db_label} 검증 중 오류 발생: {e}")


def main():
    # Verify Main Subway DB
    get_table_counts(DB_URL, "Main Subway DB")

    # Verify Weather DB
    get_table_counts(WEATHER_DB_URL, "Weather DB")


if __name__ == "__main__":
    main()
