from huggingface_hub import hf_hub_download
import sqlite3
import pandas as pd
import numpy as np
from scipy import stats

repo_id = 'alrq/subway'
filename = 'db/subway.db'
local_dir = '.'
DB_PATH = hf_hub_download(repo_id=repo_id, filename=filename, repo_type='dataset', local_dir=local_dir)

conn = sqlite3.connect(DB_PATH)

# 1. 유동인구 데이터 로드
query_floating = """
SELECT
    admin_dong_code,
    admin_dong_name,
    total_floating_pop,
    time_00_06_floating_pop,
    time_06_11_floating_pop,
    time_11_14_floating_pop,
    time_14_17_floating_pop,
    time_17_21_floating_pop,
    time_21_24_floating_pop
FROM Dong_Floating_Population
WHERE quarter_code = (SELECT MAX(quarter_code) FROM Dong_Floating_Population)
"""
df_floating = pd.read_sql(query_floating, conn)

# 2. 혼잡도 데이터 로드
query_congestion = """
SELECT station_code, day_of_week, time_slot, congestion_level
FROM Station_Congestion
WHERE day_of_week = 0
"""
df_congestion = pd.read_sql(query_congestion, conn)

# 3. 역-행정동 매핑
query_mapping = """
SELECT station_code, station_id, admin_dong_code, admin_dong_name
FROM Station_Routes
WHERE admin_dong_code IS NOT NULL
"""
df_mapping = pd.read_sql(query_mapping, conn)

query_stations = "SELECT station_id, station_name_kr FROM Stations"
df_stations = pd.read_sql(query_stations, conn)
df_mapping = df_mapping.merge(df_stations, on='station_id', how='left')

conn.close()

# 데이터 전처리
df_mapping['admin_dong_code'] = df_mapping['admin_dong_code'].astype(str).str[:8]
df_floating['admin_dong_code'] = df_floating['admin_dong_code'].astype(str).str.strip()

print('=== 데이터 현황 ===')
print(f'유동인구 데이터: {len(df_floating)}개 행정동')
print(f'혼잡도 데이터: {len(df_congestion)}개 레코드')
print(f'매핑 데이터: {len(df_mapping)}개 역')

# 혼잡도에 행정동 정보 결합
df_merged = df_congestion.merge(df_mapping[['station_code', 'admin_dong_code', 'station_name_kr']], on='station_code', how='inner')

# 행정동별, 시간대별 평균 혼잡도
df_dong_congestion = df_merged.groupby(['admin_dong_code', 'time_slot'])['congestion_level'].mean().reset_index()

# 유동인구 데이터 결합
df_final = df_dong_congestion.merge(df_floating, on='admin_dong_code', how='inner')

# 시간대 변환 함수
def convert_time_slot(slot):
    start_hour = 5
    start_min = 30
    total_min = start_min + (slot - 1) * 30
    hour = start_hour + total_min // 60
    minute = total_min % 60
    return f'{int(hour):02d}:{int(minute):02d}'

df_final['time_label'] = df_final['time_slot'].apply(convert_time_slot)

# 시간대에 맞는 유동인구 매핑
def get_floating_pop_for_slot(row):
    hour = int(row['time_label'].split(':')[0])
    if 0 <= hour < 6:
        return row['time_00_06_floating_pop']
    elif 6 <= hour < 11:
        return row['time_06_11_floating_pop']
    elif 11 <= hour < 14:
        return row['time_11_14_floating_pop']
    elif 14 <= hour < 17:
        return row['time_14_17_floating_pop']
    elif 17 <= hour < 21:
        return row['time_17_21_floating_pop']
    else:
        return row['time_21_24_floating_pop']

df_final['matched_floating_pop'] = df_final.apply(get_floating_pop_for_slot, axis=1)

print(f'\n분석 데이터: {len(df_final)}개 레코드')
print(f'행정동 수: {df_final["admin_dong_code"].nunique()}개')

# ============================================
# 1. 전체 유동인구와 혼잡도 상관분석
# ============================================
print('\n' + '='*60)
print('1. 전체 유동인구와 혼잡도 상관관계')
print('='*60)
overall_corr, overall_p = stats.pearsonr(df_final['congestion_level'], df_final['total_floating_pop'])
print(f'상관계수: {overall_corr:.4f}')
print(f'p-value: {overall_p:.4e}')

# ============================================
# 2. 시간대 매칭 유동인구와 혼잡도 상관분석
# ============================================
print('\n' + '='*60)
print('2. 시간대별 매칭 유동인구와 혼잡도 상관관계')
print('='*60)
matched_corr, matched_p = stats.pearsonr(df_final['congestion_level'], df_final['matched_floating_pop'])
print(f'상관계수: {matched_corr:.4f}')
print(f'p-value: {matched_p:.4e}')

# ============================================
# 3. 유동인구 시간대별 상관분석
# ============================================
print('\n' + '='*60)
print('3. 유동인구 시간대별 상관계수')
print('='*60)

time_bands = {
    '00:00~06:00': 'time_00_06_floating_pop',
    '06:00~11:00': 'time_06_11_floating_pop',
    '11:00~14:00': 'time_11_14_floating_pop',
    '14:00~17:00': 'time_14_17_floating_pop',
    '17:00~21:00': 'time_17_21_floating_pop',
    '21:00~24:00': 'time_21_24_floating_pop'
}

for time_band, col in time_bands.items():
    corr, p = stats.pearsonr(df_final['congestion_level'], df_final[col])
    sig = '***' if p < 0.001 else '**' if p < 0.01 else '*' if p < 0.05 else ''
    print(f'{time_band}: r={corr:.4f} (p={p:.4e}) {sig}')

# ============================================
# 4. 혼잡도 시간대 그룹별 분석
# ============================================
print('\n' + '='*60)
print('4. 혼잡도 시간대 그룹별 유동인구 상관관계')
print('='*60)

morning = df_final[df_final['time_label'].isin(['07:00', '07:30', '08:00', '08:30'])]
evening = df_final[df_final['time_label'].isin(['18:00', '18:30', '19:00', '19:30'])]
other = df_final[~df_final['time_label'].isin(['07:00', '07:30', '08:00', '08:30', '18:00', '18:30', '19:00', '19:30'])]

m_corr, m_p = stats.pearsonr(morning['congestion_level'], morning['total_floating_pop'])
e_corr, e_p = stats.pearsonr(evening['congestion_level'], evening['total_floating_pop'])
o_corr, o_p = stats.pearsonr(other['congestion_level'], other['total_floating_pop'])

print(f'출근시간대 (07:00~09:00): r={m_corr:.4f} (p={m_p:.4e})')
print(f'퇴근시간대 (18:00~20:00): r={e_corr:.4f} (p={e_p:.4e})')
print(f'기타시간대: r={o_corr:.4f} (p={o_p:.4e})')

# ============================================
# 5. 시간대별 상세 상관계수
# ============================================
print('\n' + '='*60)
print('5. 시간 슬롯별 상관계수 (전체 유동인구 기준)')
print('='*60)

correlations = []
for time_slot in sorted(df_final['time_slot'].unique()):
    subset = df_final[df_final['time_slot'] == time_slot]
    if len(subset) > 10:
        corr, p_value = stats.pearsonr(subset['congestion_level'], subset['total_floating_pop'])
        time_label = subset['time_label'].iloc[0]
        correlations.append({'time_slot': time_slot, 'time_label': time_label, 'correlation': corr, 'p_value': p_value})

df_corr = pd.DataFrame(correlations)
print(df_corr.to_string(index=False))

# ============================================
# 6. 유동인구 상위/하위 지역 혼잡도 비교
# ============================================
print('\n' + '='*60)
print('6. 유동인구 상위/하위 지역 혼잡도 비교')
print('='*60)

q75 = df_floating['total_floating_pop'].quantile(0.75)
q25 = df_floating['total_floating_pop'].quantile(0.25)

high_float = df_final[df_final['total_floating_pop'] >= q75]['congestion_level'].mean()
low_float = df_final[df_final['total_floating_pop'] <= q25]['congestion_level'].mean()

print(f'유동인구 상위 25% 지역 평균 혼잡도: {high_float:.2f}%')
print(f'유동인구 하위 25% 지역 평균 혼잡도: {low_float:.2f}%')
print(f'차이: {high_float - low_float:.2f}%p')

high_data = df_final[df_final['total_floating_pop'] >= q75]['congestion_level']
low_data = df_final[df_final['total_floating_pop'] <= q25]['congestion_level']
t_stat, t_p = stats.ttest_ind(high_data, low_data)
print(f't-통계량: {t_stat:.4f}, p-value: {t_p:.4e}')

# ============================================
# 7. 피크 시간대 분석
# ============================================
print('\n' + '='*60)
print('7. 피크 시간대 분석')
print('='*60)

peak_morning = df_final[df_final['time_label'] == '08:00']
peak_evening = df_final[df_final['time_label'] == '18:00']

pm_corr, pm_p = stats.pearsonr(peak_morning['congestion_level'], peak_morning['total_floating_pop'])
pe_corr, pe_p = stats.pearsonr(peak_evening['congestion_level'], peak_evening['total_floating_pop'])

print(f'출근 피크(08:00) 상관계수: {pm_corr:.4f} (p={pm_p:.4e})')
print(f'퇴근 피크(18:00) 상관계수: {pe_corr:.4f} (p={pe_p:.4e})')

max_corr_row = df_corr.loc[df_corr['correlation'].idxmax()]
min_corr_row = df_corr.loc[df_corr['correlation'].idxmin()]
print(f'\n최고 상관계수 시간: {max_corr_row["time_label"]} (r={max_corr_row["correlation"]:.4f})')
print(f'최저 상관계수 시간: {min_corr_row["time_label"]} (r={min_corr_row["correlation"]:.4f})')
