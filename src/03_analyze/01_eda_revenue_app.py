import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import sys

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "..", ".."))
if project_root not in sys.path:
    sys.path.append(project_root)

from src.utils.db_util import get_connection

# Page Config
st.set_page_config(
    page_title="추정매출 분석 (Estimated Revenue)", page_icon="💰", layout="wide"
)

# Title
st.title("💰 상권 추정 매출 데이터 분석 대시보드")
st.markdown(
    "서울시 행정동별 상권 추정 매출 데이터를 업종별, 요일별, 시간대별로 분석하고 시각화합니다."
)


# 1. Load Data
@st.cache_data
def load_data():
    conn = get_connection()
    try:
        query = "SELECT * FROM Dong_Estimated_Revenue"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        conn.close()
        st.error(f"데이터 로드 중 오류 발생: {e}")
        return pd.DataFrame()


df = load_data()

if df.empty:
    st.warning("데이터가 없습니다.")
    st.stop()

# Sidebar
st.sidebar.header("설정 및 필터")

# Service Type Filter
all_services = sorted(df["service_type_name"].dropna().unique())
selected_services = st.sidebar.multiselect("업종 선택", all_services, default=[])

# Dong Filter
all_dongs = sorted(df["admin_dong_name"].dropna().unique())
selected_dongs = st.sidebar.multiselect("행정동 선택", all_dongs, default=[])

# Apply Filters
filtered_df = df.copy()
if selected_services:
    filtered_df = filtered_df[filtered_df["service_type_name"].isin(selected_services)]
if selected_dongs:
    filtered_df = filtered_df[filtered_df["admin_dong_name"].isin(selected_dongs)]

show_raw_data = st.sidebar.checkbox("원본 데이터 보기", value=False)

# 2. Data Overview
if show_raw_data:
    st.subheader("📋 데이터 미리보기")
    st.dataframe(filtered_df.head(10))
    st.write(f"조회된 데이터 수: {len(filtered_df):,} 개")

st.markdown("---")

# 3. Key Metrics
st.subheader("💡 주요 지표 (선택된 범위 합계/평균)")

total_sales = filtered_df["month_sales_amt"].sum()
total_count = filtered_df["month_sales_cnt"].sum()
avg_sales = filtered_df["month_sales_amt"].mean() if not filtered_df.empty else 0

col1, col2, col3 = st.columns(3)
col1.metric("총 매출 금액", f"{total_sales:,.0f}원")
col2.metric("총 매출 건수", f"{total_count:,.0f}건")
col3.metric("평균 월 매출", f"{avg_sales:,.0f}원")

st.markdown("---")

# 4. Top/Bottom Analysis
st.header("📊 순위 분석")

col_top_l, col_top_r = st.columns(2)

with col_top_l:
    st.subheader("매출 상위 10개 행정동")
    top_dongs = (
        filtered_df.groupby("admin_dong_name")["month_sales_amt"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
        .reset_index()
    )
    fig_top_dongs = px.bar(
        top_dongs,
        x="month_sales_amt",
        y="admin_dong_name",
        orientation="h",
        title="행정동별 총 매출 Top 10",
        labels={"month_sales_amt": "총 매출 금액", "admin_dong_name": "행정동"},
        text_auto=".2s",
    )
    fig_top_dongs.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig_top_dongs, use_container_width=True)

with col_top_r:
    st.subheader("매출 상위 10개 업종")
    top_services = (
        filtered_df.groupby("service_type_name")["month_sales_amt"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
        .reset_index()
    )
    fig_top_svc = px.bar(
        top_services,
        x="month_sales_amt",
        y="service_type_name",
        orientation="h",
        title="업종별 총 매출 Top 10",
        labels={"month_sales_amt": "총 매출 금액", "service_type_name": "업종"},
        text_auto=".2s",
    )
    fig_top_svc.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig_top_svc, use_container_width=True)

# 5. Temporal Analysis
st.header("🕒 시계열 및 요일 분석")

col_day_1, col_day_2 = st.columns(2)

# Day of Week
day_cols = [
    "mon_sales_amt",
    "tue_sales_amt",
    "wed_sales_amt",
    "thu_sales_amt",
    "fri_sales_amt",
    "sat_sales_amt",
    "sun_sales_amt",
]
day_labels = ["월", "화", "수", "목", "금", "토", "일"]
day_data = filtered_df[day_cols].sum()
day_df = pd.DataFrame({"Day": day_labels, "Sales": day_data.values})

with col_day_1:
    fig_day = px.bar(day_df, x="Day", y="Sales", title="요일별 총 매출", color="Day")
    st.plotly_chart(fig_day, use_container_width=True)

# Time Slot
time_cols = [c for c in df.columns if "time_" in c and "_sales_amt" in c]
# Simplify labels: time_00_06_sales_amt -> 00~06시
time_labels = [
    c.replace("time_", "").replace("_sales_amt", "").replace("_", "~") + "시"
    for c in time_cols
]
time_data = filtered_df[time_cols].sum()
time_df = pd.DataFrame({"Time": time_labels, "Sales": time_data.values})

with col_day_2:
    fig_time = px.line(
        time_df,
        x="Time",
        y="Sales",
        markers=True,
        title="시간대별 총 매출",
        groupnorm=None,
    )
    st.plotly_chart(fig_time, use_container_width=True)


# 6. Demographics (Gender/Age)
st.header("👥 인구 통계적 매출 분석")

col_dem_1, col_dem_2 = st.columns(2)

# Gender
with col_dem_1:
    male_sales = filtered_df["male_sales_amt"].sum()
    female_sales = filtered_df["female_sales_amt"].sum()
    fig_gender = px.pie(
        names=["남성", "여성"],
        values=[male_sales, female_sales],
        title="성별 매출 기여도",
        color_discrete_sequence=["skyblue", "lightpink"],
    )
    st.plotly_chart(fig_gender, use_container_width=True)

# Age
with col_dem_2:
    age_cols = [c for c in df.columns if "age_" in c and "_sales_amt" in c]
    age_labels = [
        c.replace("age_", "").replace("_sales_amt", "").replace("60_over", "60대 이상")
        + "대"
        for c in age_cols
    ]
    age_data = filtered_df[age_cols].sum()
    age_df = pd.DataFrame({"Age": age_labels, "Sales": age_data.values})

    fig_age = px.bar(
        age_df, x="Age", y="Sales", title="연령대별 매출 기여도", color="Sales"
    )
    st.plotly_chart(fig_age, use_container_width=True)

# 7. Correlation
st.subheader("🔗 주요 변수 상관관계")
numeric_cols = [
    "month_sales_amt",
    "month_sales_cnt",
    "weekday_sales_amt",
    "weekend_sales_amt",
] + day_cols
if len(filtered_df) > 1:
    corr = filtered_df[numeric_cols].corr()
    fig_heatmap = px.imshow(
        corr,
        text_auto=False,
        aspect="auto",
        title="매출 및 요일 변수 상관관계",
        color_continuous_scale="RdBu_r",
    )
    st.plotly_chart(fig_heatmap, use_container_width=True)

st.markdown("---")
st.markdown("Developed for **Subway Congestion Analysis Project**")
