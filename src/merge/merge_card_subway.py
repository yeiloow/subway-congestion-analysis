import pandas as pd
import glob
import os


def read_subway_files(folder_path="./"):
    """
    지정된 폴더에서 CARD_SUBWAY_MONTH_XXXX 패턴의 CSV 파일을 읽어
    하나의 데이터프레임으로 통합하는 함수
    """

    # 1. 파일 목록 가져오기 (CARD_SUBWAY_MONTH_로 시작하는 모든 csv 파일)
    # 파일명이 예: CARD_SUBWAY_MONTH_2015.csv 라고 가정
    file_pattern = os.path.join(folder_path, "boarding", "CARD_SUBWAY_MONTH_*.csv")
    file_list = glob.glob(file_pattern)

    # 파일이 잘 검색되었는지 확인
    if not file_list:
        print(f"해당 경로({folder_path})에서 파일을 찾을 수 없습니다.")
        return None

    print(f"총 {len(file_list)}개의 파일을 찾았습니다.")

    # 2. 각 파일을 읽어서 리스트에 담기
    df_list = []

    for file in file_list:
        try:
            # 한글 데이터는 주로 'utf-8' 또는 'cp949' (euc-kr) 인코딩을 사용합니다.
            # 에러 방지를 위해 먼저 utf-8로 시도하고 실패 시 cp949로 읽습니다.
            try:
                temp_df = pd.read_csv(file, encoding="utf-8")
            except UnicodeDecodeError:
                temp_df = pd.read_csv(file, encoding="cp949")

            df_list.append(temp_df)
            print(f"[완료] {os.path.basename(file)} 읽기 성공 ({len(temp_df)}행)")

        except Exception as e:
            print(f"[에러] {os.path.basename(file)} 파일을 읽는 중 오류 발생: {e}")

    # 3. 데이터프레임 하나로 합치기 (Concatenate)
    if df_list:
        full_df = pd.concat(df_list, ignore_index=True)
        print("-" * 30)
        print("모든 파일 병합 완료!")
        return full_df
    else:
        return None


# --- 실행 부분 ---

# 실제 CSV 파일들이 있는 폴더 경로를 입력하세요. (현재 폴더면 './')
folder_path = "./"

# 함수 실행
df_subway = read_subway_files(folder_path)

# 결과 확인
if df_subway is not None:
    print("\n--- 데이터 미리보기 (상위 5행) ---")
    print(df_subway.head())

    print("\n--- 데이터 정보 ---")
    print(df_subway.info())

    # 필요하다면 합친 데이터를 새로운 CSV로 저장
    df_subway.to_csv("Subway_Total_2015_2022.csv", index=False, encoding="utf-8-sig")
