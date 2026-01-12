import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import sys

# project root setting
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "..", ".."))
if project_root not in sys.path:
    sys.path.append(project_root)

from src.utils.visualization import apply_theme

# Apply Plotly Theme
apply_theme()

# Page Config
st.set_page_config(
    page_title="혼잡도-건물 특성 상관관계 분석",
    page_icon="🏢",
    layout="wide",
)

st.title("🏢 역세권 건물 특성와 지하철 혼잡도 상관관계 분석")
st.markdown(
    "지하철역 주변의 **건물 특성(연면적, 세대수 등)**이 시간대별 **지하철 혼잡도**와 어떤 관계가 있는지 분석한 결과를 시각화합니다."
)


# 1. Load Data
@st.cache_data
def load_data():
    try:
        # Load time slot correlation data
        corr_path = os.path.join(project_root, "output", "correlation_by_time_slot.csv")
        df_corr = pd.read_csv(corr_path)

        # Load detailed merged data (for scatter plots)
        detail_path = os.path.join(
            project_root, "output", "building_congestion_by_timeslot.csv"
        )
        df_detail = pd.read_csv(detail_path)

        return df_corr, df_detail
    except Exception as e:
        st.error(f"데이터 로드 실패: {e}")
        return pd.DataFrame(), pd.DataFrame()


df_corr, df_detail = load_data()

if df_corr.empty or df_detail.empty:
    st.warning("분석 데이터가 존재하지 않습니다. 먼저 분석 스크립트를 실행해주세요.")
    st.stop()

# Sidebar
st.sidebar.header("분석 설정")
show_raw_data = st.sidebar.checkbox("원본 데이터 보기", value=False)

if show_raw_data:
    st.subheader("📋 상관분석 데이터 미리보기")
    st.dataframe(df_corr.head())

# Tab Layout
tab1, tab2, tab3, tab4 = st.tabs(
    [
        "📈 시간대별 상관관계",
        "🌡️ 상관 히트맵",
        "📉 피크타임 비교 (Scatter)",
        "📝 분석 보고서",
    ]
)

# Tab 1: Correlation by Time Slot
with tab1:
    st.header("시간대별 상관관계 변화")
    st.markdown("""
    - **X축**: 시간대 (05:00 ~ 24:00)
    - **Y축**: 피어슨 상관계수 (r)
    - **의미**: 0에 가까우면 관계 없음, 양수(+)면 건물 규모가 클수록 혼잡도가 높음.
    """)

    # Feature Selection for Line Plot
    features = df_corr["feature"].unique()
    selected_features = st.multiselect("확인할 특성 선택", features, default=features)

    filtered_corr = df_corr[df_corr["feature"].isin(selected_features)]

    fig_line = px.line(
        filtered_corr,
        x="time_label",
        y="pearson_r",
        color="feature",
        markers=True,
        title="시간대별 상관계수 변화 추이",
        labels={
            "pearson_r": "상관계수 (r)",
            "time_label": "시간대",
            "feature": "건물 특성",
        },
        hover_data=["pearson_p"],
    )
    # Add Reference Line (0)
    fig_line.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)

    st.plotly_chart(fig_line, width="stretch")

    st.info("""
    **💡 주요 발견**:
    - **심야(24:00) 및 새벽(05:00)** 시간대에 상관관계가 가장 높게 나타납니다. (역 주변 거주/활동 인구 영향력 증대)
    - **출근 시간대(07:00~09:00)**에는 상관관계가 낮습니다. (환승 등 외부 요인 우세)
    """)

# Tab 2: Heatmap
with tab2:
    st.header("특성별 상관관계 히트맵")

    pivot_data = df_corr.pivot(
        index="time_label", columns="feature", values="pearson_r"
    )

    fig_heatmap = px.imshow(
        pivot_data,
        text_auto=".2f",
        aspect="auto",
        color_continuous_scale="RdBu_r",
        range_color=[
            -0.3,
            0.3,
        ],  # Adjusted range for better contrast given weak correlations
        title="시간대 vs 건물 특성 상관계수 히트맵",
        labels={"color": "상관계수 (r)"},
    )
    st.plotly_chart(fig_heatmap, width="stretch")

# Tab 3: Detailed Scatter Plot
with tab3:
    st.header("혼잡도 vs 건물 연면적 상세 분석")

    # Filtering for lighter plotting
    st.markdown("데이터가 많으므로 특정 시간대를 선택하여 분석합니다.")

    times = sorted(df_detail["time_slot"].unique())
    # Default to a morning peak, off-peak, and night time
    # 8:00 (slot 6), 14:00 (slot 18), 23:00 (slot 36) -> approx
    # Let's use slider or select box

    # Mapping slot to label
    def get_time_label(slot):
        # reuse logic or just map roughly if needed, but we have it in correlation csv
        # Just use slider for simplicity
        base = 5.5
        hr = int(base + (slot - 1) * 0.5)
        mn = "30" if (base + (slot - 1) * 0.5) % 1 != 0 else "00"
        return f"{hr:02d}:{mn}"

    cols = st.columns([1, 2])
    with cols[0]:
        selected_slot = st.selectbox(
            "분석할 시간대 선택",
            options=times,
            format_func=get_time_label,
            index=times.index(6) if 6 in times else 0,  # Default around 8 am
        )

    filtered_detail = df_detail[df_detail["time_slot"] == selected_slot]

    fig_scatter = px.scatter(
        filtered_detail,
        x="total_area",
        y="congestion_level",
        hover_data=["station_name", "line_name"],
        color="line_name",  # Color by subway line
        title=f"{get_time_label(selected_slot)} 기준 건물 연면적 vs 혼잡도",
        labels={"total_area": "총 건물 연면적 (m²)", "congestion_level": "혼잡도"},
        trendline="ols",  # Add trendline
    )
    st.plotly_chart(fig_scatter, width="stretch")

    st.subheader("📊 호선별 분석")
    st.markdown("선택된 시간대의 호선별 평균 혼잡도와 평균 건물 연면적입니다.")
    line_stats = (
        filtered_detail.groupby("line_name")[["congestion_level", "total_area"]]
        .mean()
        .reset_index()
    )

    fig_bubble = px.scatter(
        line_stats,
        x="total_area",
        y="congestion_level",
        size="congestion_level",
        color="line_name",
        text="line_name",
        title="호선별 평균 비교",
        labels={"total_area": "평균 건물 연면적", "congestion_level": "평균 혼잡도"},
    )
    st.plotly_chart(fig_bubble, use_container_width=True)

# Tab 4: Report
with tab4:
    st.header("분석 결과 보고서")

    report_path = os.path.join(
        project_root, "src/03_analyze/02_report_congestion_building_correlation.md"
    )
    if os.path.exists(report_path):
        with open(report_path, "r", encoding="utf-8") as f:
            report_content = f.read()
        st.markdown(report_content)
    else:
        st.warning("보고서 파일이 없습니다.")

st.markdown("---")
st.markdown(
    "Developed for **Subway Congestion Analysis Project** | Data Source: Seoul Open Data Plaza"
)
