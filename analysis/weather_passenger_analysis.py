import pandas as pd
import numpy as np
from scipy import stats
from huggingface_hub import hf_hub_download
import sqlite3

# 데이터 로드
repo_id = 'alrq/subway'
filename = 'db/subway.db'
local_dir = '.'
DB_PATH = hf_hub_download(repo_id=repo_id, filename=filename, repo_type='dataset', local_dir=local_dir)

# 날씨 데이터 로드 (weather.db)
weather_conn = sqlite3.connect('db/weather.db')
df_daily_temp = pd.read_sql("SELECT * FROM Daily_Temperature", weather_conn)
df_hourly_weather = pd.read_sql("SELECT * FROM Hourly_Weather", weather_conn)
weather_conn.close()

print('=== 날씨 데이터 ===')
print(f'일별 기온: {len(df_daily_temp)} 레코드')
print(f'시간별 날씨: {len(df_hourly_weather)} 레코드')
print(f'날짜 범위: {df_daily_temp["base_date"].min()} ~ {df_daily_temp["base_date"].max()}')

# 일별 날씨 특성 계산
df_daily_weather = df_hourly_weather.groupby('base_date').agg({
    'temperature': 'mean',
    'rain_prob': 'max',
    'rain_type': 'max'
}).reset_index()
df_daily_weather.columns = ['base_date', 'avg_temp', 'max_rain_prob', 'rain_type']

# 일별 기온 데이터 병합
df_daily_weather = df_daily_weather.merge(
    df_daily_temp[['base_date', 'min_temp', 'max_temp']],
    on='base_date', how='left'
)

# 승하차 데이터 로드
conn = sqlite3.connect(DB_PATH)
df_passengers = pd.read_sql("""
SELECT usage_date, line_name, station_name, boarding_count, alighting_count
FROM Station_Daily_Passengers
""", conn)
conn.close()

print(f'\n=== 승하차 데이터 ===')
print(f'레코드 수: {len(df_passengers)}')
print(f'날짜 범위: {df_passengers["usage_date"].min()} ~ {df_passengers["usage_date"].max()}')

# 일별 총 승하차 집계
df_daily_passengers = df_passengers.groupby('usage_date').agg({
    'boarding_count': 'sum',
    'alighting_count': 'sum'
}).reset_index()
df_daily_passengers['total_passengers'] = df_daily_passengers['boarding_count'] + df_daily_passengers['alighting_count']

# 데이터 병합
df_merged = df_daily_passengers.merge(
    df_daily_weather,
    left_on='usage_date',
    right_on='base_date',
    how='inner'
)

print(f'\n=== 병합 데이터 ===')
print(f'레코드 수: {len(df_merged)}')

# 요일 추가
df_merged['date'] = pd.to_datetime(df_merged['usage_date'], format='%Y%m%d')
df_merged['day_of_week'] = df_merged['date'].dt.dayofweek
df_merged['is_weekend'] = df_merged['day_of_week'].isin([5, 6])

# 계절 추가
def get_season(date):
    month = date.month
    if month in [3, 4, 5]:
        return 'spring'
    elif month in [6, 7, 8]:
        return 'summer'
    elif month in [9, 10, 11]:
        return 'fall'
    else:
        return 'winter'

df_merged['season'] = df_merged['date'].apply(get_season)

# 날씨 조건 분류
df_merged['weather_condition'] = 'normal'
df_merged.loc[df_merged['rain_type'] > 0, 'weather_condition'] = 'rain'
df_merged.loc[df_merged['rain_type'] >= 3, 'weather_condition'] = 'snow'

# 기온 구간 분류
df_merged['temp_category'] = pd.cut(
    df_merged['avg_temp'],
    bins=[-20, 0, 10, 20, 30, 40],
    labels=['freezing', 'cold', 'mild', 'warm', 'hot']
)

print('\n' + '='*60)
print('1. 전체 상관관계 분석')
print('='*60)

# 전체 상관관계
corr_temp, p_temp = stats.pearsonr(df_merged['avg_temp'], df_merged['total_passengers'])
corr_rain, p_rain = stats.pearsonr(df_merged['max_rain_prob'], df_merged['total_passengers'])

print(f'평균기온 vs 총승하차: r={corr_temp:.4f} (p={p_temp:.4e})')
print(f'강수확률 vs 총승하차: r={corr_rain:.4f} (p={p_rain:.4e})')

# 최저/최고 기온
if 'min_temp' in df_merged.columns:
    df_temp_valid = df_merged.dropna(subset=['min_temp', 'max_temp'])
    corr_min, p_min = stats.pearsonr(df_temp_valid['min_temp'], df_temp_valid['total_passengers'])
    corr_max, p_max = stats.pearsonr(df_temp_valid['max_temp'], df_temp_valid['total_passengers'])
    print(f'최저기온 vs 총승하차: r={corr_min:.4f} (p={p_min:.4e})')
    print(f'최고기온 vs 총승하차: r={corr_max:.4f} (p={p_max:.4e})')

print('\n' + '='*60)
print('2. 평일/주말 구분 분석')
print('='*60)

for is_weekend, label in [(False, '평일'), (True, '주말')]:
    subset = df_merged[df_merged['is_weekend'] == is_weekend]
    corr, p = stats.pearsonr(subset['avg_temp'], subset['total_passengers'])
    print(f'{label} - 평균기온 vs 승하차: r={corr:.4f} (p={p:.4e})')

    corr_r, p_r = stats.pearsonr(subset['max_rain_prob'], subset['total_passengers'])
    print(f'{label} - 강수확률 vs 승하차: r={corr_r:.4f} (p={p_r:.4e})')

print('\n' + '='*60)
print('3. 계절별 분석')
print('='*60)

for season in ['spring', 'summer', 'fall', 'winter']:
    subset = df_merged[df_merged['season'] == season]
    if len(subset) > 10:
        corr, p = stats.pearsonr(subset['avg_temp'], subset['total_passengers'])
        avg_passengers = subset['total_passengers'].mean()
        print(f'{season:8s}: r={corr:+.4f} (p={p:.4e}), 평균 승하차={avg_passengers/1e6:.2f}백만')

print('\n' + '='*60)
print('4. 날씨 조건별 승하차 비교')
print('='*60)

weather_stats = df_merged.groupby('weather_condition').agg({
    'total_passengers': ['mean', 'std', 'count']
}).round(0)
weather_stats.columns = ['평균', '표준편차', '일수']
print(weather_stats)

# 맑은 날 vs 비 오는 날 t-test
normal_days = df_merged[df_merged['weather_condition'] == 'normal']['total_passengers']
rain_days = df_merged[df_merged['weather_condition'] == 'rain']['total_passengers']

if len(rain_days) > 0:
    t_stat, t_p = stats.ttest_ind(normal_days, rain_days)
    diff = normal_days.mean() - rain_days.mean()
    diff_pct = (diff / normal_days.mean()) * 100
    print(f'\n맑은 날 평균: {normal_days.mean()/1e6:.2f}백만')
    print(f'비 오는 날 평균: {rain_days.mean()/1e6:.2f}백만')
    print(f'차이: {diff/1e6:.2f}백만 ({diff_pct:+.2f}%)')
    print(f't-test: t={t_stat:.4f}, p={t_p:.4e}')

print('\n' + '='*60)
print('5. 기온 구간별 승하차 비교')
print('='*60)

temp_stats = df_merged.groupby('temp_category', observed=True).agg({
    'total_passengers': ['mean', 'std', 'count'],
    'avg_temp': 'mean'
}).round(0)
print(temp_stats)

print('\n' + '='*60)
print('6. 극단 날씨 영향 분석')
print('='*60)

# 폭염/한파 기준
hot_days = df_merged[df_merged['avg_temp'] >= 30]
cold_days = df_merged[df_merged['avg_temp'] <= 0]
normal_temp = df_merged[(df_merged['avg_temp'] > 10) & (df_merged['avg_temp'] < 25)]

print(f'폭염일 (30도 이상): {len(hot_days)}일')
if len(hot_days) > 0:
    print(f'  평균 승하차: {hot_days["total_passengers"].mean()/1e6:.2f}백만')

print(f'한파일 (0도 이하): {len(cold_days)}일')
if len(cold_days) > 0:
    print(f'  평균 승하차: {cold_days["total_passengers"].mean()/1e6:.2f}백만')

print(f'쾌적한 날 (10-25도): {len(normal_temp)}일')
if len(normal_temp) > 0:
    print(f'  평균 승하차: {normal_temp["total_passengers"].mean()/1e6:.2f}백만')

# 강수확률 높은 날
high_rain_prob = df_merged[df_merged['max_rain_prob'] >= 80]
low_rain_prob = df_merged[df_merged['max_rain_prob'] <= 20]

print(f'\n강수확률 높은 날 (80% 이상): {len(high_rain_prob)}일')
if len(high_rain_prob) > 0:
    print(f'  평균 승하차: {high_rain_prob["total_passengers"].mean()/1e6:.2f}백만')

print(f'강수확률 낮은 날 (20% 이하): {len(low_rain_prob)}일')
if len(low_rain_prob) > 0:
    print(f'  평균 승하차: {low_rain_prob["total_passengers"].mean()/1e6:.2f}백만')

if len(high_rain_prob) > 0 and len(low_rain_prob) > 0:
    t_stat, t_p = stats.ttest_ind(low_rain_prob['total_passengers'], high_rain_prob['total_passengers'])
    diff = low_rain_prob['total_passengers'].mean() - high_rain_prob['total_passengers'].mean()
    diff_pct = (diff / low_rain_prob['total_passengers'].mean()) * 100
    print(f'차이: {diff/1e6:.2f}백만 ({diff_pct:+.2f}%)')
    print(f't-test: t={t_stat:.4f}, p={t_p:.4e}')

print('\n' + '='*60)
print('7. 호선별 날씨 영향 분석')
print('='*60)

# 호선별 일별 승하차
df_line_daily = df_passengers.groupby(['usage_date', 'line_name']).agg({
    'boarding_count': 'sum',
    'alighting_count': 'sum'
}).reset_index()
df_line_daily['total'] = df_line_daily['boarding_count'] + df_line_daily['alighting_count']

# 날씨 데이터 병합
df_line_weather = df_line_daily.merge(
    df_daily_weather,
    left_on='usage_date',
    right_on='base_date',
    how='inner'
)

# 주요 호선별 상관계수
main_lines = ['2호선', '1호선', '3호선', '4호선', '5호선', '7호선']
print('\n호선별 기온-승하차 상관계수:')
for line in main_lines:
    subset = df_line_weather[df_line_weather['line_name'] == line]
    if len(subset) > 10:
        corr, p = stats.pearsonr(subset['avg_temp'], subset['total'])
        print(f'{line}: r={corr:+.4f} (p={p:.4e})')

print('\n' + '='*60)
print('8. 요약 통계')
print('='*60)

print(f'분석 기간: {df_merged["usage_date"].min()} ~ {df_merged["usage_date"].max()}')
print(f'총 분석 일수: {len(df_merged)}일')
print(f'평균 기온: {df_merged["avg_temp"].mean():.1f}도')
print(f'기온 범위: {df_merged["avg_temp"].min():.1f}도 ~ {df_merged["avg_temp"].max():.1f}도')
print(f'평균 일일 승하차: {df_merged["total_passengers"].mean()/1e6:.2f}백만명')
