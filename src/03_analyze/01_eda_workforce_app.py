import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
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
    page_title="생활인구 분석 (Workplace)", page_icon="🏢", layout="wide"
)

# Title
st.title("🏢 직장인구 데이터 분석 대시보드 (Workplace Population)")
st.markdown("서울시 행정동별 직장인구 데이터를 분석하고 시각화합니다.")


# 1. Load Data
@st.cache_data
def load_data():
    conn = get_connection()
    query = "SELECT * FROM Dong_Workplace_Population"
    try:
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        conn.close()
        st.error(f"Error reading from database: {e}")
        return pd.DataFrame()


df = load_data()

if df.empty:
    st.warning("데이터가 없습니다.")
    st.stop()

# Sidebar
st.sidebar.header("설정 및 필터")
show_raw_data = st.sidebar.checkbox("원본 데이터 보기", value=False)

# 2. Data Overview
if show_raw_data:
    st.subheader("📋 데이터 미리보기")
    st.dataframe(df.head())
    st.write(f"총 {len(df)} 개의 행정동 데이터가 있습니다.")

# 3. Key Metrics
st.subheader("💡 주요 지표")
col1, col2, col3 = st.columns(3)
if "total_pop" in df.columns:
    total_pop_sum = df["total_pop"].sum()
    avg_pop = df["total_pop"].mean()
    col1.metric("총 직장인구 수", f"{total_pop_sum:,.0f}명")
    col2.metric("평균 직장인구 수 (동별)", f"{avg_pop:,.0f}명")
    col3.metric("데이터 집계 행정동 수", f"{len(df)}개")

st.markdown("---")

# 4. Outlier Analysis (Summary)
st.subheader("🔍 이상치 및 결측치 확인")
col_miss, col_outlier = st.columns(2)

with col_miss:
    st.markdown("**결측치 확인**")
    missing = df.isnull().sum()
    missing = missing[missing > 0]
    if missing.empty:
        st.success("결측치가 없습니다.")
    else:
        st.dataframe(missing)

with col_outlier:
    st.markdown("**이상치 (총 직장인구 기준, 1.5 IQR)**")
    if "total_pop" in df.columns:
        Q1 = df["total_pop"].quantile(0.25)
        Q3 = df["total_pop"].quantile(0.75)
        IQR = Q3 - Q1
        outliers = df[
            (df["total_pop"] < (Q1 - 1.5 * IQR)) | (df["total_pop"] > (Q3 + 1.5 * IQR))
        ]
        st.write(f"이상치 개수: {len(outliers)}개")
        if not outliers.empty:
            st.dataframe(
                outliers[["admin_dong_name", "total_pop"]]
                .sort_values("total_pop", ascending=False)
                .head()
            )

st.markdown("---")

# 5. Top/Bottom 5
st.subheader("🏆 상위/하위 5개 지역 (총 직장인구)")
col_top, col_bot = st.columns(2)

if "total_pop" in df.columns:
    with col_top:
        st.markdown("**상위 5개 지역**")
        top5 = df.sort_values("total_pop", ascending=False)[
            ["admin_dong_name", "total_pop"]
        ].head(5)
        st.table(top5)

    with col_bot:
        st.markdown("**하위 5개 지역**")
        bottom5 = df.sort_values("total_pop", ascending=True)[
            ["admin_dong_name", "total_pop"]
        ].head(5)
        st.table(bottom5)

# 6. Visualizations
st.header("📊 시각화 분석")

# 6.1 Top 10 Bar Chart
st.subheader("1. 직장인구 상위 10개 행정동")
if "total_pop" in df.columns:
    top10 = df.sort_values("total_pop", ascending=False).head(10)
    fig_top10 = px.bar(
        top10,
        x="total_pop",
        y="admin_dong_name",
        orientation="h",
        title="직장인구 상위 10개 행정동",
        labels={"total_pop": "총 직장인구", "admin_dong_name": "행정동"},
        text_auto=".2s",
    )
    fig_top10.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig_top10, use_container_width=True)

# 6.2 Distribution Histogram
st.subheader("2. 행정동별 총 직장인구 분포")
if "total_pop" in df.columns:
    fig_hist = px.histogram(
        df,
        x="total_pop",
        nbins=30,
        title="총 직장인구 분포",
        labels={"total_pop": "총 직장인구"},
        marginal="box",  # Adds a box plot at the top
    )
    st.plotly_chart(fig_hist, use_container_width=True)

# 6.3 Population Pyramid
st.subheader("3. 전체 직장인구 인구 피라미드")
age_cols_male = [
    c for c in df.columns if "male" in c and "female" not in c and "age" in c
]
age_cols_female = [c for c in df.columns if "female" in c and "age" in c]

if age_cols_male and age_cols_female:
    total_male = df[age_cols_male].sum()
    total_female = df[age_cols_female].sum()

    def extract_age_label(col_name):
        parts = col_name.split("_")
        for p in parts:
            if p.isdigit():
                return f"{p}대"
            if p == "over":
                return "60대 이상"
        return "기타"

    age_labels = [extract_age_label(c) for c in age_cols_male]

    # Create DF for Plotly
    # Plotly Bar chart for pyramid: Male negative, Female positive
    pyramid_df = pd.DataFrame(
        {
            "Age": age_labels,
            "Male": total_male.values * -1,  # Make male negative for left side
            "Female": total_female.values,
            "Male_Abs": total_male.values,  # For hover text
        }
    )

    fig_pyramid = go.Figure()

    fig_pyramid.add_trace(
        go.Bar(
            y=pyramid_df["Age"],
            x=pyramid_df["Male"],
            name="남성",
            orientation="h",
            customdata=pyramid_df["Male_Abs"],
            hovertemplate="남성: %{customdata:,.0f}명<extra></extra>",
        )
    )

    fig_pyramid.add_trace(
        go.Bar(
            y=pyramid_df["Age"],
            x=pyramid_df["Female"],
            name="여성",
            orientation="h",
            hovertemplate="여성: %{x:,.0f}명<extra></extra>",
        )
    )

    fig_pyramid.update_layout(
        title="성별/연령별 인구 피라미드",
        barmode="overlay",  # Or 'relative'
        bargap=0.1,
        xaxis=dict(
            tickmode="array",
            # Custom ticks to show positive numbers
            tickvals=[
                -val
                for val in range(0, int(pyramid_df["Male"].min() * -1) + 10000, 50000)
            ]
            + [val for val in range(0, int(pyramid_df["Female"].max()) + 10000, 50000)],
            # Simplified for auto-scale usually, but basic absolute formatting:
            ticktext=[
                str(abs(x))
                for x in [
                    -val for val in range(0, int(pyramid_df["Male"].min() * -1), 50000)
                ]
            ],  # Complicated to get right dynamically without max, let's rely on hover
        ),
    )
    # Simpler approach for axes labels: just rely on hover and absolute values in text
    fig_pyramid.update_xaxes(title="인구 수", tickformat="s", showticklabels=True)

    st.plotly_chart(fig_pyramid, use_container_width=True)

# 6.4 Gender Ratio Pie
st.subheader("4. 전체 성별 비율")
if "male_pop" in df.columns and "female_pop" in df.columns:
    col_pie1, col_pie2 = st.columns([1, 2])  # Adjust width
    total_male_all = df["male_pop"].sum()
    total_female_all = df["female_pop"].sum()

    fig_pie = px.pie(
        values=[total_male_all, total_female_all],
        names=["남성", "여성"],
        title="전체 직장인구 성별 비율",
        color=["남성", "여성"],
        color_discrete_map={"남성": "skyblue", "여성": "lightpink"},
    )
    st.plotly_chart(fig_pie, use_container_width=True)

# 6.5 Correlation Heatmap
st.subheader("5. 주요 변수 간 상관관계")
numeric_df = df.select_dtypes(include=["number"])
drop_cols = ["id", "quarter_code", "admin_dong_code"]
numeric_df = numeric_df.drop(columns=[c for c in drop_cols if c in numeric_df.columns])

# Pre-select interesting columns for clear visualization
key_cols = ["total_pop", "male_pop", "female_pop"] + [
    c for c in df.columns if "age_30" in c
]
subset_cols = [c for c in key_cols if c in numeric_df.columns]

if len(subset_cols) > 1:
    corr_df = numeric_df[subset_cols]
    corr = corr_df.corr()

    fig_heatmap = px.imshow(
        corr,
        text_auto=".2f",
        aspect="auto",
        title="주요 변수 상관관계 히트맵 (30대 직장인 포함)",
        color_continuous_scale="RdBu_r",
        origin="lower",
    )
    st.plotly_chart(fig_heatmap, use_container_width=True)
else:
    st.info("상관관계를 분석할 충분한 숫자형 컬럼이 선택되지 않았습니다.")

st.markdown("---")
st.markdown("Developed for **Subway Congestion Analysis Project**")
