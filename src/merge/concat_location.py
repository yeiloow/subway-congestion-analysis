import pandas as pd
import glob
import os

def merge_by_year(target_years, folder_path='./'):
    """
    지정된 연도 리스트(target_years)를 순회하며,
    해당 연도에 해당하는 월별 파일들을 찾아 연도별 파일로 병합 및 저장합니다.
    """
    
    for year in target_years:
        # 1. 해당 연도로 시작하는 모든 파일 찾기 (예: CARD_SUBWAY_MONTH_2023*.csv)
        # 2023*.csv 패턴은 202301, 202302 ... 등을 모두 포함합니다.
        search_pattern = os.path.join(folder_path, 'boarding', f"CARD_SUBWAY_MONTH_{year}*.csv")
        file_list = glob.glob(search_pattern)
        
        # 정렬을 하여 1월부터 순서대로 합쳐지도록 함
        file_list.sort()
        
        if not file_list:
            print(f"⚠️ {year}년도 파일을 찾을 수 없습니다. (패턴: {search_pattern})")
            continue
            
        print(f"\n>> {year}년도 작업 시작: 총 {len(file_list)}개의 월별 파일 발견")
        
        # 2. 파일 읽어서 리스트에 담기
        df_list = []
        for file in file_list:
            try:
                # 인코딩 처리 (utf-8 시도 후 실패 시 cp949)
                try:
                    df = pd.read_csv(file, encoding='utf-8', index_col=False)
                except UnicodeDecodeError:
                    df = pd.read_csv(file, encoding='cp949')
                
                df_list.append(df)
                # print(f"  - {os.path.basename(file)} 추가됨") # 너무 길면 주석 처리
                
            except Exception as e:
                print(f"  - [에러] {os.path.basename(file)} 읽기 실패: {e}")
        
        # 3. 하나로 합치기 및 저장
        if df_list:
            year_df = pd.concat(df_list, ignore_index=True)
            print(year_df)
            
            # 저장할 파일명 정의 (예: CARD_SUBWAY_MONTH_2023.csv)
            output_filename = f"CARD_SUBWAY_MONTH_{year}.csv"
            output_path = os.path.join(folder_path, output_filename)
            
            # CSV 파일로 저장
            year_df.to_csv(output_path, index=False, encoding='utf-8-sig')
            
            print(f"✅ {year}년 데이터 병합 완료! -> 저장됨: {output_filename}")
            print(f"   (총 행 개수: {len(year_df)})")

# --- 실행 설정 ---

# 1. 작업할 연도 리스트 설정
target_years = ['2023', '2024', '2025']
# target_years = ['2023']

# 2. 파일이 있는 폴더 경로 (현재 폴더면 './')
folder_path = './'

# 3. 함수 실행
merge_by_year(target_years, folder_path)