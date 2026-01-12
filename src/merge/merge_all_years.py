import pandas as pd
import os

def merge_all_years(start_year, end_year, folder_path='./'):
    print(f"[{start_year}년 ~ {end_year}년] 연도별 파일 통합 시작...\n")
    
    df_list = []
    
    # 2015부터 2025까지 반복 (range는 끝번호 포함 안하므로 +1)
    for year in range(start_year, end_year + 1):
        # 읽어올 파일명: CARD_SUBWAY_MONTH_2015.csv 형식
        filename = f"CARD_SUBWAY_MONTH_{year}.csv"
        file_path = os.path.join(folder_path, 'data/boarding-1', filename)
        
        
        # 파일이 실제로 존재하는지 확인
        if os.path.exists(file_path):
            try:
                # 앞서 저장할 때 utf-8-sig로 저장했으므로 utf-8로 읽습니다.
                # 만약 에러나면 cp949로 시도
                try:
                    df = pd.read_csv(file_path, encoding='utf-8', index_col=False)
                except UnicodeDecodeError:
                    df = pd.read_csv(file_path, encoding='cp949', index_col=False)
                
                df_list.append(df)
                print(f"✅ {filename} 병합 성공 ({len(df)}행)")
                
            except Exception as e:
                print(f"❌ {filename} 읽기 실패: {e}")
        else:
            print(f"⚠️ {filename} 파일이 없습니다. (건너뜀)")
            
    # 최종 병합
    if df_list:
        final_df = pd.concat(df_list, ignore_index=True)
        
        output_name = f"CARD_SUBWAY_TOTAL_{start_year}_{end_year}.csv"
        output_path = os.path.join(folder_path, output_name)
        
        final_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        print("\n" + "="*40)
        print(f"🎉 모든 통합이 완료되었습니다!")
        print(f"파일명: {output_name}")
        print(f"총 데이터 행 수: {len(final_df)}개")
        print("="*40)
    else:
        print("합칠 데이터가 없습니다.")

# --- 실행 ---
# 2015년부터 2025년까지
merge_all_years(2023, 2025)