import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import sys

# Add project root to path to allow importing from src
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "..", ".."))
if project_root not in sys.path:
    sys.path.append(project_root)

from src.utils.db_util import get_connection

# Page Config
st.set_page_config(
    page_title="생활인구 분석 (Living Population)", page_icon="🏠", layout="wide"
)

# Title
st.title("🏠 생활인구 데이터 분석 대시보드")
st.markdown(
    "서울시 행정동별 생활인구(내국인/외국인) 데이터를 시간대별로 분석하고 시각화합니다."
)


# 1. Load Data
@st.cache_data
def load_data():
    conn = get_connection()
    try:
        # 1. Load Main Data
        # Assuming the dataset size is manageable. If large, we might need to agg in SQL.
        query_main = "SELECT * FROM Dong_Living_Population"
        df = pd.read_sql(query_main, conn)

        # 2. Load Mapping for Admin Dong Names
        # Dong_Workplace_Population usually has a good mapping
        query_map = "SELECT DISTINCT admin_dong_code, admin_dong_name FROM Dong_Workplace_Population"
        df_map = pd.read_sql(query_map, conn)

        conn.close()

        if df.empty:
            return pd.DataFrame()

        # Merge Name
        if not df_map.empty:
            df = pd.merge(df, df_map, on="admin_dong_code", how="left")
            # Fill missing names with code if any
            df["admin_dong_name"] = df["admin_dong_name"].fillna(df["admin_dong_code"])
        else:
            df["admin_dong_name"] = df["admin_dong_code"]

        # 3. Preprocessing
        # Calculated Columns
        df["total_long_term"] = (
            df["long_term_chinese_stay_pop"] + df["long_term_non_chinese_stay_pop"]
        )
        df["total_short_term"] = (
            df["short_term_chinese_stay_pop"] + df["short_term_non_chinese_stay_pop"]
        )
        df["total_living_pop"] = (
            df["local_total_living_pop"]
            + df["total_long_term"]
            + df["total_short_term"]
        )

        # Sort by date/time
        if "time_slot" in df.columns:
            df["time_slot"] = df["time_slot"].astype(int)

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

# Date Filter (if multiple dates exist)
dates = sorted(df["base_date"].unique())
selected_dates = st.sidebar.multiselect("날짜 선택", dates, default=dates)
if selected_dates:
    df = df[df["base_date"].isin(selected_dates)]

# Dong Filter
all_dongs = sorted(df["admin_dong_name"].dropna().unique())
selected_dongs = st.sidebar.multiselect(
    "행정동 선택 (복수 선택 가능)", all_dongs, default=[]
)
if selected_dongs:
    filtered_df = df[df["admin_dong_name"].isin(selected_dongs)]
else:
    filtered_df = df  # Analyze all if none selected (or maybe top 5 default later)

show_raw_data = st.sidebar.checkbox("원본 데이터 보기", value=False)

# 2. Data Overview
if show_raw_data:
    st.subheader("📋 데이터 미리보기")
    st.dataframe(filtered_df.head(10))
    st.write(f"조회된 데이터 수: {len(filtered_df):,} 개")

st.markdown("---")

# 3. Key Metrics (Aggregated over selection)
st.subheader("💡 주요 지표 (선택된 범위 평균)")
# Aggregation for metrics
avg_total = filtered_df["total_living_pop"].mean()
avg_local = filtered_df["local_total_living_pop"].mean()
avg_foreigner = (
    filtered_df["total_long_term"] + filtered_df["total_short_term"]
).mean()

col1, col2, col3 = st.columns(3)
col1.metric("평균 총 생활인구", f"{avg_total:,.0f}명")
col2.metric("평균 내국인 수", f"{avg_local:,.0f}명")
col3.metric("평균 외국인 수", f"{avg_foreigner:,.0f}명")

st.markdown("---")

# 4. Outlier / Rankings
st.subheader("🏆 생활인구 상위 10개 행정동 (평균)")
# Group by Dong
dong_stats = (
    filtered_df.groupby("admin_dong_name")[
        [
            "total_living_pop",
            "local_total_living_pop",
            "total_long_term",
            "total_short_term",
        ]
    ]
    .mean()
    .reset_index()
)
top10_dongs = dong_stats.sort_values("total_living_pop", ascending=False).head(10)

fig_top = px.bar(
    top10_dongs,
    x="total_living_pop",
    y="admin_dong_name",
    orientation="h",
    title="생활인구 많은 행정동 Top 10",
    text_auto=".2s",
    labels={"total_living_pop": "평균 총 생활인구", "admin_dong_name": "행정동"},
)
fig_top.update_layout(yaxis={"categoryorder": "total ascending"})
st.plotly_chart(fig_top, width="stretch")


# 5. Visualizations
st.header("📊 상세 분석")

# 5.1 Time Trends
st.subheader("1. 시간대별 생활인구 변화")
# Group by time_slot
time_stats = (
    filtered_df.groupby("time_slot")[
        ["local_total_living_pop", "total_long_term", "total_short_term"]
    ]
    .mean()
    .reset_index()
)

fig_time = make_subplots(specs=[[{"secondary_y": True}]])
fig_time.add_trace(
    go.Scatter(
        x=time_stats["time_slot"],
        y=time_stats["local_total_living_pop"],
        name="내국인",
        mode="lines+markers",
    ),
    secondary_y=False,
)
fig_time.add_trace(
    go.Scatter(
        x=time_stats["time_slot"],
        y=time_stats["total_long_term"],
        name="장기체류 외국인",
        mode="lines+markers",
    ),
    secondary_y=True,
)
fig_time.add_trace(
    go.Scatter(
        x=time_stats["time_slot"],
        y=time_stats["total_short_term"],
        name="단기체류 외국인",
        mode="lines+markers",
    ),
    secondary_y=True,
)
fig_time.update_layout(title="시간대별 인구 추이 (평균)", hovermode="x unified")
fig_time.update_xaxes(title="시간대 (Time Slot)")
fig_time.update_yaxes(title="내국인 수", secondary_y=False)
fig_time.update_yaxes(title="외국인 수", secondary_y=True)
st.plotly_chart(fig_time, width="stretch")

# 5.2 Population Pyramid (Age/Gender)
st.subheader("2. 내국인 인구 피라미드 (평균)")
# Identify Columns
age_cols_male = [c for c in df.columns if "local_male" in c]
age_cols_female = [c for c in df.columns if "local_female" in c]

if age_cols_male and age_cols_female:
    # Calculate means
    male_means = filtered_df[age_cols_male].mean()
    female_means = filtered_df[age_cols_female].mean()

    # Create labels
    def get_age_label(col):
        # e.g. local_male_age_0_9_pop -> 0~9세
        parts = col.split("_age_")
        if len(parts) < 2:
            return col
        suffix = parts[1].replace("_pop", "")
        if "over" in suffix:
            return "70세 이상"
        return suffix.replace("_", "~") + "세"

    age_labels = [get_age_label(c) for c in age_cols_male]

    fig_pyr = go.Figure()
    fig_pyr.add_trace(
        go.Bar(
            y=age_labels,
            x=-male_means.values,
            name="남성",
            orientation="h",
            marker_color="cornflowerblue",
            hovertemplate="남성: %{customdata:.0f}명<extra></extra>",
            customdata=male_means.values,
        )
    )
    fig_pyr.add_trace(
        go.Bar(
            y=age_labels,
            x=female_means.values,
            name="여성",
            orientation="h",
            marker_color="lightpink",
            hovertemplate="여성: %{x:.0f}명<extra></extra>",
        )
    )

    # Fix X-axis ticks to be positive
    max_val = max(male_means.max(), female_means.max())
    tick_vals = list(range(0, int(max_val) + 1000, 5000))  # Adjust step as needed
    # Simple trick: just hide tick labels or use custom ticktext if needed.
    # For now, let's keep it simple or mirroring.

    fig_pyr.update_layout(
        title="성별/연령별 인구 분포",
        barmode="overlay",
        bargap=0.1,
        xaxis=dict(title="인구 수 (왼쪽: 남성, 오른쪽: 여성)", tickformat="s"),
    )
    st.plotly_chart(fig_pyr, width="stretch")

# 5.3 Composition Pie
st.subheader("3. 생활인구 구성 비율")
avg_counts = [avg_local, avg_foreigner]  # Simplified
# Or detailed
avg_long = filtered_df["total_long_term"].mean()
avg_short = filtered_df["total_short_term"].mean()

fig_pie = px.pie(
    names=["내국인", "장기체류 외국인", "단기체류 외국인"],
    values=[avg_local, avg_long, avg_short],
    title="생활인구 구성 (내국인 vs 외국인)",
    color_discrete_sequence=px.colors.qualitative.Pastel,
)
st.plotly_chart(fig_pie, width="stretch")

# 5.4 Correlation Heatmap
st.subheader("4. 주요 변수 상관관계")
numeric_cols = [
    "total_living_pop",
    "local_total_living_pop",
    "total_long_term",
    "total_short_term",
    "time_slot",
]
if len(filtered_df) > 1:
    corr = filtered_df[numeric_cols].corr()
    fig_heatmap = px.imshow(
        corr,
        text_auto=".2f",
        aspect="auto",
        title="주요 변수 상관관계",
        color_continuous_scale="RdBu_r",
        origin="lower",
    )
    st.plotly_chart(fig_heatmap, width="stretch")

st.markdown("---")
st.markdown("Developed for **Subway Congestion Analysis Project**")
