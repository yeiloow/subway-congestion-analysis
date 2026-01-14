from huggingface_hub import hf_hub_download
import sqlite3
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from scipy import stats
from IPython.display import display
import warnings
warnings.filterwarnings('ignore')

repo_id = "alrq/subway"       # 데이터셋 리포지토리 ID
filename = "db/subway.db"     # 리포지토리 내 파일 경로
local_dir = "."               # 다운로드 받을 로컬 기본 경로 (이 경우 ./db/subway.db 로 저장됨)
# 파일 다운로드
# local_dir을 지정하면 리포지토리의 폴더 구조를 유지하며 파일을 저장합니다.
DB_PATH = hf_hub_download(
    repo_id=repo_id,
    filename=filename,
    repo_type="dataset",
    local_dir=local_dir
)

def get_connection():
    return sqlite3.connect(DB_PATH)

print("라이브러리 로드 완료")

conn = get_connection()

# 분석 대상 호선
TARGET_LINES = ['2호선', '4호선', '5호선']

# 역 정보 로드 (2, 4, 5호선만)
stations_query = """
SELECT 
    s.station_id,
    s.station_name_kr,
    sr.station_code,
    sr.admin_dong_code,
    sr.admin_dong_name,
    l.line_name
FROM Stations s
JOIN Station_Routes sr ON s.station_id = sr.station_id
JOIN Lines l ON sr.line_id = l.line_id
WHERE l.line_name IN ('2호선', '4호선', '5호선')
"""
df_stations = pd.read_sql(stations_query, conn)

target_station_ids = df_stations['station_id'].unique().tolist()
target_station_codes = df_stations['station_code'].unique().tolist()
target_dong_codes = df_stations['admin_dong_code'].dropna().unique().tolist()

print(f"분석 대상 호선: {TARGET_LINES}")
print(f"역 수: {len(target_station_ids)}개")
print(f"행정동 수: {len(target_dong_codes)}개")

print("\n호선별 역 수:")
print(df_stations.groupby('line_name')['station_id'].nunique())

# 추정매출 데이터 로드 (대상 행정동만)
# 행정동 코드 형식 통일 (앞 8자리)
dong_codes_short = list(set([str(d)[:8] for d in target_dong_codes]))

revenue_query = """
SELECT * FROM Dong_Estimated_Revenue
"""
df_revenue_all = pd.read_sql(revenue_query, conn)

# 행정동 코드 앞 8자리로 매칭
df_revenue_all['admin_dong_code_short'] = df_revenue_all['admin_dong_code'].astype(str).str[:8]
df_revenue = df_revenue_all[df_revenue_all['admin_dong_code_short'].isin(dong_codes_short)].copy()

print(f"추정매출 데이터: {len(df_revenue):,} rows")
print(f"분기 코드: {df_revenue['quarter_code'].unique().tolist()}")
print(f"\n업종 수: {df_revenue['service_type_name'].nunique()}개")

# 혼잡도 데이터 로드
congestion_query = f"""
SELECT * FROM Station_Congestion
WHERE station_code IN ({','.join([f"'{c}'" for c in target_station_codes])})
"""
df_congestion = pd.read_sql(congestion_query, conn)

print(f"혼잡도 데이터: {len(df_congestion):,} rows")

# 시간대 변환 함수
def slot_to_hour(slot):
    total_minutes = 5 * 60 + 30 + slot * 30
    return (total_minutes // 60) % 24

def categorize_time_period(slot):
    hour = slot_to_hour(slot)
    if 0 <= hour < 6:
        return '00_06'
    elif 6 <= hour < 11:
        return '06_11'
    elif 11 <= hour < 14:
        return '11_14'
    elif 14 <= hour < 17:
        return '14_17'
    elif 17 <= hour < 21:
        return '17_21'
    else:
        return '21_24'

df_congestion['time_period'] = df_congestion['time_slot'].apply(categorize_time_period)

# station_code -> station_id 매핑
station_code_to_id = df_stations[['station_id', 'station_code']].drop_duplicates()
df_congestion = df_congestion.merge(station_code_to_id, on='station_code', how='left')

print("시간대 분류 완료")
print(df_congestion['time_period'].value_counts())

# 평일 데이터만 사용
df_cong_weekday = df_congestion[df_congestion['day_of_week'] == 0].copy()

# 역별, 시간대별 평균 혼잡도
congestion_by_time = df_cong_weekday.groupby(['station_id', 'time_period'])['congestion_level'].mean().unstack()
congestion_by_time.columns = [f'cong_{c}' for c in congestion_by_time.columns]
congestion_by_time = congestion_by_time.reset_index()

# 전체 평균 혼잡도
avg_cong = df_cong_weekday.groupby('station_id')['congestion_level'].mean().reset_index()
avg_cong.columns = ['station_id', 'cong_avg']

congestion_by_time = congestion_by_time.merge(avg_cong, on='station_id', how='left')

print(f"역별 시간대별 혼잡도: {len(congestion_by_time)} 역")
display(congestion_by_time.head())

# 역-행정동 매핑
station_dong = df_stations[['station_id', 'station_name_kr', 'admin_dong_code']].drop_duplicates(subset='station_id')
station_dong['admin_dong_code_short'] = station_dong['admin_dong_code'].astype(str).str[:8]

# 행정동별 매출 집계 (최신 분기 사용)
latest_quarter = df_revenue['quarter_code'].max()
df_revenue_latest = df_revenue[df_revenue['quarter_code'] == latest_quarter].copy()

print(f"사용 분기: {latest_quarter}")

# 행정동별 총 매출 (모든 업종 합계)
dong_revenue = df_revenue_latest.groupby('admin_dong_code_short').agg({
    'month_sales_amt': 'sum',
    'time_00_06_sales_amt': 'sum',
    'time_06_11_sales_amt': 'sum',
    'time_11_14_sales_amt': 'sum',
    'time_14_17_sales_amt': 'sum',
    'time_17_21_sales_amt': 'sum',
    'time_21_24_sales_amt': 'sum',
    'weekday_sales_amt': 'sum',
    'weekend_sales_amt': 'sum'
}).reset_index()

# 컬럼명 변경
dong_revenue.columns = ['admin_dong_code_short', 'total_sales', 'sales_00_06', 'sales_06_11', 
                        'sales_11_14', 'sales_14_17', 'sales_17_21', 'sales_21_24',
                        'weekday_sales', 'weekend_sales']

# 역과 매출 연결
station_revenue = station_dong.merge(dong_revenue, on='admin_dong_code_short', how='left')

print(f"역별 매출 데이터: {len(station_revenue)} 역")
display(station_revenue.head())

# 매출과 혼잡도 병합
df_analysis = station_revenue.merge(congestion_by_time, on='station_id', how='inner')

# 결측치 제거
df_analysis = df_analysis.dropna(subset=['total_sales', 'cong_avg'])

print(f"분석 대상 역 수: {len(df_analysis)}")
display(df_analysis.head())

# 상관관계 분석
sales_cols = ['total_sales', 'sales_00_06', 'sales_06_11', 'sales_11_14', 
              'sales_14_17', 'sales_17_21', 'sales_21_24', 'weekday_sales', 'weekend_sales']
cong_cols = ['cong_00_06', 'cong_06_11', 'cong_11_14', 'cong_14_17', 
             'cong_17_21', 'cong_21_24', 'cong_avg']

# 존재하는 컬럼만 선택
sales_cols = [c for c in sales_cols if c in df_analysis.columns]
cong_cols = [c for c in cong_cols if c in df_analysis.columns]

# 상관계수 계산
corr_matrix = df_analysis[sales_cols + cong_cols].corr()
corr_sales_cong = corr_matrix.loc[sales_cols, cong_cols]

print("매출과 혼잡도 간 상관계수:")
display(corr_sales_cong.round(3))

# 상관관계 히트맵
fig = go.Figure(data=go.Heatmap(
    z=corr_sales_cong.values,
    x=['혼잡도(00-06)', '혼잡도(06-11)', '혼잡도(11-14)', '혼잡도(14-17)', 
       '혼잡도(17-21)', '혼잡도(21-24)', '평균혼잡도'],
    y=['총매출', '매출(00-06)', '매출(06-11)', '매출(11-14)', 
       '매출(14-17)', '매출(17-21)', '매출(21-24)', '평일매출', '주말매출'],
    colorscale='RdBu_r',
    zmid=0,
    text=corr_sales_cong.values.round(2),
    texttemplate='%{text}',
    textfont={'size': 10},
    colorbar=dict(title='상관계수')
))

fig.update_layout(
    title='추정매출과 시간대별 혼잡도 상관관계',
    height=500,
    width=700
)
fig.show()

# 동일 시간대 매출과 혼잡도 비교
time_pairs = [
    ('sales_00_06', 'cong_00_06', '00-06시'),
    ('sales_06_11', 'cong_06_11', '06-11시'),
    ('sales_11_14', 'cong_11_14', '11-14시'),
    ('sales_14_17', 'cong_14_17', '14-17시'),
    ('sales_17_21', 'cong_17_21', '17-21시'),
    ('sales_21_24', 'cong_21_24', '21-24시')
]

print("=== 동일 시간대 매출-혼잡도 상관계수 ===")
same_time_corrs = []
for sales_col, cong_col, label in time_pairs:
    if sales_col in df_analysis.columns and cong_col in df_analysis.columns:
        corr, pval = stats.pearsonr(
            df_analysis[sales_col].fillna(0), 
            df_analysis[cong_col].fillna(0)
        )
        same_time_corrs.append({'시간대': label, '상관계수': corr, 'p-value': pval})
        print(f"{label}: r = {corr:.3f} (p = {pval:.4f})")

df_same_time = pd.DataFrame(same_time_corrs)

# 동일 시간대 상관계수 시각화
fig = go.Figure(data=go.Bar(
    x=df_same_time['시간대'],
    y=df_same_time['상관계수'],
    marker_color=['red' if x < 0 else 'blue' for x in df_same_time['상관계수']],
    text=df_same_time['상관계수'].round(3),
    textposition='outside'
))

fig.update_layout(
    title='동일 시간대 매출-혼잡도 상관계수',
    xaxis_title='시간대',
    yaxis_title='상관계수',
    yaxis_range=[-0.5, 0.5],
    height=400
)
fig.add_hline(y=0, line_dash='dash', line_color='gray')
fig.show()

# 총 매출 vs 평균 혼잡도
fig = go.Figure()

fig.add_trace(go.Scatter(
    x=df_analysis['total_sales'] / 1e8,  # 억 단위
    y=df_analysis['cong_avg'],
    mode='markers',
    marker=dict(size=8, opacity=0.6),
    text=df_analysis['station_name_kr'],
    hovertemplate='%{text}<br>매출: %{x:.1f}억<br>혼잡도: %{y:.1f}<extra></extra>'
))

# 추세선
x = df_analysis['total_sales'].fillna(0) / 1e8
y = df_analysis['cong_avg'].fillna(0)
z = np.polyfit(x, y, 1)
p = np.poly1d(z)
x_line = np.linspace(x.min(), x.max(), 100)
fig.add_trace(go.Scatter(
    x=x_line, y=p(x_line),
    mode='lines',
    line=dict(color='red', dash='dash'),
    name='추세선'
))

corr = df_analysis['total_sales'].corr(df_analysis['cong_avg'])
fig.update_layout(
    title=f'총 매출 vs 평균 혼잡도 (r = {corr:.3f})',
    xaxis_title='총 매출 (억원)',
    yaxis_title='평균 혼잡도',
    height=500
)
fig.show()

# 시간대별 산점도
fig = make_subplots(rows=2, cols=3, subplot_titles=[
    '00-06시', '06-11시', '11-14시', '14-17시', '17-21시', '21-24시'
])

time_pairs = [
    ('sales_00_06', 'cong_00_06'),
    ('sales_06_11', 'cong_06_11'),
    ('sales_11_14', 'cong_11_14'),
    ('sales_14_17', 'cong_14_17'),
    ('sales_17_21', 'cong_17_21'),
    ('sales_21_24', 'cong_21_24')
]

positions = [(1,1), (1,2), (1,3), (2,1), (2,2), (2,3)]

for (sales_col, cong_col), pos in zip(time_pairs, positions):
    if sales_col in df_analysis.columns and cong_col in df_analysis.columns:
        fig.add_trace(
            go.Scatter(
                x=df_analysis[sales_col] / 1e8,
                y=df_analysis[cong_col],
                mode='markers',
                marker=dict(size=5, opacity=0.6),
                text=df_analysis['station_name_kr'],
                showlegend=False
            ),
            row=pos[0], col=pos[1]
        )

fig.update_layout(height=500, width=900, title_text='시간대별 매출-혼잡도 관계')
fig.update_xaxes(title_text='매출(억)')
fig.update_yaxes(title_text='혼잡도')
fig.show()

# 주요 업종 선정
major_services = ['한식음식점', '커피-Loss', '편의점', '치킨전문점', '호프-Loss집', 
                  '일반의류', '화장품', '분식전문점', '의약품']

# 업종별 행정동 매출 집계
service_dong_revenue = df_revenue_latest.groupby(['admin_dong_code_short', 'service_type_name'])['month_sales_amt'].sum().unstack(fill_value=0)

# 존재하는 업종만 선택
available_services = [s for s in major_services if s in service_dong_revenue.columns]
service_dong_revenue = service_dong_revenue[available_services].reset_index()

# 역과 연결
station_service = station_dong[['station_id', 'station_name_kr', 'admin_dong_code_short']].merge(
    service_dong_revenue, on='admin_dong_code_short', how='left'
)

# 혼잡도와 병합
df_service_analysis = station_service.merge(congestion_by_time, on='station_id', how='inner')

print(f"업종별 분석 대상: {len(df_service_analysis)} 역")
print(f"분석 업종: {available_services}")

# 업종별 매출과 평균 혼잡도 상관관계
service_corrs = []
for service in available_services:
    if service in df_service_analysis.columns:
        corr = df_service_analysis[service].corr(df_service_analysis['cong_avg'])
        service_corrs.append({'업종': service, '상관계수': corr})

df_service_corr = pd.DataFrame(service_corrs).sort_values('상관계수', ascending=False)

print("업종별 매출과 평균 혼잡도 상관계수:")
display(df_service_corr)

# 업종별 상관계수 시각화
fig = go.Figure(data=go.Bar(
    x=df_service_corr['업종'],
    y=df_service_corr['상관계수'],
    marker_color=['blue' if x > 0 else 'red' for x in df_service_corr['상관계수']],
    text=df_service_corr['상관계수'].round(3),
    textposition='outside'
))

fig.update_layout(
    title='업종별 매출-혼잡도 상관계수',
    xaxis_title='업종',
    yaxis_title='상관계수',
    height=400,
    xaxis_tickangle=-45
)
fig.add_hline(y=0, line_dash='dash', line_color='gray')
fig.show()

# 혼잡도 구간 분류
df_analysis['혼잡도_구간'] = pd.qcut(
    df_analysis['cong_avg'], 
    q=4, 
    labels=['낮음', '보통', '높음', '매우높음']
)

# 혼잡도 구간별 평균 매출
group_sales = df_analysis.groupby('혼잡도_구간').agg({
    'total_sales': 'mean',
    'sales_06_11': 'mean',
    'sales_11_14': 'mean',
    'sales_17_21': 'mean',
    'station_id': 'count'
}).rename(columns={'station_id': '역수'})

# 억 단위로 변환
for col in ['total_sales', 'sales_06_11', 'sales_11_14', 'sales_17_21']:
    group_sales[col] = group_sales[col] / 1e8

print("혼잡도 구간별 평균 매출 (억원):")
display(group_sales.round(1))

# 혼잡도 구간별 매출 시각화
fig = go.Figure()

fig.add_trace(go.Bar(
    name='총 매출',
    x=group_sales.index,
    y=group_sales['total_sales'],
    text=group_sales['total_sales'].round(1),
    textposition='outside'
))

fig.update_layout(
    title='혼잡도 구간별 평균 총 매출',
    xaxis_title='혼잡도 구간',
    yaxis_title='평균 매출 (억원)',
    height=400
)
fig.show()

# 매출 상위 역
top_sales = df_analysis.nlargest(10, 'total_sales')[[
    'station_name_kr', 'total_sales', 'cong_avg', 'cong_17_21'
]].copy()
top_sales['total_sales'] = (top_sales['total_sales'] / 1e8).round(1)
top_sales.columns = ['역명', '총매출(억)', '평균혼잡도', '퇴근혼잡도']

print("=== 매출 상위 10개 역 ===")
display(top_sales)

# 혼잡도 상위 역
top_cong = df_analysis.nlargest(10, 'cong_avg')[[
    'station_name_kr', 'total_sales', 'cong_avg', 'cong_17_21'
]].copy()
top_cong['total_sales'] = (top_cong['total_sales'] / 1e8).round(1)
top_cong.columns = ['역명', '총매출(억)', '평균혼잡도', '퇴근혼잡도']

print("\n=== 혼잡도 상위 10개 역 ===")
display(top_cong)

print("=" * 70)
print("추정매출과 시간대별 지하철 혼잡도 상관관계 분석 결과")
print("=" * 70)

print(f"""
[분석 개요]
  - 분석 대상: 2호선, 4호선, 5호선 ({len(df_analysis)}개 역)
  - 매출 데이터: {latest_quarter} 분기
  - 혼잡도 데이터: 평일 기준
""")

# 총 매출과 평균 혼잡도 상관계수
total_corr = df_analysis['total_sales'].corr(df_analysis['cong_avg'])
print(f"[총 매출 vs 평균 혼잡도]")
print(f"  상관계수: r = {total_corr:.3f}")
if total_corr > 0.3:
    print(f"  해석: 양의 상관관계 - 혼잡한 역일수록 매출이 높은 경향")
elif total_corr < -0.3:
    print(f"  해석: 음의 상관관계 - 혼잡한 역일수록 매출이 낮은 경향")
else:
    print(f"  해석: 약한 상관관계")

print(f"\n[동일 시간대 매출-혼잡도 상관계수]")
for _, row in df_same_time.iterrows():
    print(f"  {row['시간대']}: r = {row['상관계수']:.3f}")

print(f"\n[업종별 상관계수 (상위 3개)]")
for _, row in df_service_corr.head(3).iterrows():
    print(f"  {row['업종']}: r = {row['상관계수']:.3f}")

print(f"\n[혼잡도 구간별 평균 매출]")
for idx, row in group_sales.iterrows():
    print(f"  {idx}: {row['total_sales']:.1f}억원 ({int(row['역수'])}개 역)")

conn.close()
print("\n" + "=" * 70)
print("분석 완료!")

# 1. 업종별로 매출 집계
industry_revenue = df_revenue_latest.groupby(['admin_dong_code_short', 'service_type_name'])['month_sales_amt'].sum().reset_index()

# 2. 주요 업종 선정 (매출 총액 기준 상위 20개)
top_industries = industry_revenue.groupby('service_type_name')['month_sales_amt'].sum().sort_values(ascending=False).head(20).index.tolist()
print(f"분석 대상 상위 20개 업종: {top_industries}")

# 3. 피벗 테이블 생성 (행: 행정동, 열: 업종별 매출)
industry_pivot = industry_revenue[industry_revenue['service_type_name'].isin(top_industries)].pivot(
    index='admin_dong_code_short', 
    columns='service_type_name', 
    values='month_sales_amt'
).fillna(0)

# 4. 역 정보와 병합
# 하나의 역이 여러 행정동에 걸쳐있을 수 있으므로, 역 단위로 평균/합계를 내거나 해야 하는데,
# 여기서는 '역-행정동' 매핑 테이블(station_dong)을 기준으로 병합합니다.
station_industry_sales = station_dong.merge(industry_pivot, on='admin_dong_code_short', how='left')

# 5. 혼잡도 데이터와 병합
# station_id를 기준으로 혼잡도 데이터 병합
analysis_df = station_industry_sales.merge(congestion_by_time, on='station_id', how='inner')

# 결측치 처리 (매출 데이터가 없는 역 제외)
analysis_df = analysis_df.dropna(subset=top_industries)

print(f"최종 분석 대상 데이터: {len(analysis_df)}개 역")
display(analysis_df.head())

# 상관관계 분석
# X축: 혼잡도 시간대 (cong_00_06 ~ cong_21_24, cong_avg)
# Y축: 업종별 매출

cong_cols = [c for c in analysis_df.columns if c.startswith('cong_')]
industry_cols = top_industries

# 상관계수 계산
corr_matrix = pd.DataFrame(index=industry_cols, columns=cong_cols)

for ind in industry_cols:
    for cong in cong_cols:
        # 피어슨 상관계수
        corr, _ = stats.pearsonr(analysis_df[ind], analysis_df[cong])
        corr_matrix.loc[ind, cong] = corr

corr_matrix = corr_matrix.astype(float)

# 시각화 (히트맵)
fig = go.Figure(data=go.Heatmap(
    z=corr_matrix.values,
    x=corr_matrix.columns,
    y=corr_matrix.index,
    colorscale='RdBu_r', # 빨간색이 양의 상관관계
    zmin=-1, zmax=1,
    text=np.round(corr_matrix.values, 2),
    texttemplate="%{text}",
    textfont=dict(size=10)
))

fig.update_layout(
    title='업종별 매출과 시간대별 지하철 혼잡도 상관관계',
    xaxis_title='혼잡도 시간대',
    yaxis_title='업종',
    width=1000,
    height=800,
    xaxis=dict(tickangle=45)
)

fig.show()

# 주요 상관관계 상세 시각화 (산점도)
# 상관계수가 가장 높은 조합 Top 3 찾기

stacked_corr = corr_matrix.stack().reset_index()
stacked_corr.columns = ['Business', 'Time', 'Correlation']
# 자기 자신과의 상관관계 등 제외 (여기서는 서로 다른 변수라 괜찮음)
top_corr = stacked_corr.reindex(stacked_corr.Correlation.abs().sort_values(ascending=False).index).head(3)

print("가장 강한 상관관계를 보인 조합 Top 3:")
print(top_corr)

# 시각화
for _, row in top_corr.iterrows():
    ind = row['Business']
    cong = row['Time']
    corr_val = row['Correlation']
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=analysis_df[cong],
        y=analysis_df[ind],
        mode='markers',
        text=analysis_df['station_name_kr'], # 호버 시 역 이름 표시
        marker=dict(size=8, opacity=0.6)
    ))
    
    # 추세선 (Using numpy polyfit)
    z = np.polyfit(analysis_df[cong], analysis_df[ind], 1)
    p = np.poly1d(z)
    
    fig.add_trace(go.Scatter(
        x=analysis_df[cong],
        y=p(analysis_df[cong]),
        mode='lines',
        name='Trendline',
        line=dict(color='red', dash='dash')
    ))
    
    fig.update_layout(
        title=f'{ind} 매출 vs {cong} (Corr: {corr_val:.2f})',
        xaxis_title=f'{cong} (혼잡도)',
        yaxis_title=f'{ind} 월 매출',
        hovermode='closest'
    )
    
    fig.show()