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

from src.utils.db_util import get_engine
from src.utils.visualization import apply_theme

# Apply Plotly Theme
apply_theme()


# 1. Load Data
@st.cache_data
def load_data():
    try:
        engine = get_engine()
        query = """
        SELECT 
            s.station_name_kr,
            l.line_name,
            b.usage_type,
            b.total_area,
            b.total_households,
            b.total_families
        FROM Station_Catchment_Building_Stats b
        JOIN Stations s ON b.station_id = s.station_id
        JOIN Lines l ON b.line_id = l.line_id
        """
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Failed to load data: {e}")
        return pd.DataFrame()


def main():
    # Page Config
    st.set_page_config(
        page_title="역세권 건물 데이터 분석 (Station Catchment)",
        page_icon="🏢",
        layout="wide",
    )

    # Title
    st.title("🏢 역세권 건물 데이터 분석 대시보드")
    st.markdown("역세권 내의 건물 용도, 연면적, 세대수 등을 분석하여 시각화합니다.")

    df = load_data()

    if df.empty:
        st.warning("데이터가 없습니다. 데이터베이스를 확인해주세요.")
        st.stop()

    # Sidebar Filters
    st.sidebar.header("설정 및 필터")

    # Line Filter
    all_lines = sorted(df["line_name"].unique())
    selected_lines = st.sidebar.multiselect("호선 선택", all_lines, default=all_lines)

    # Station Filter (Dynamic based on line selection)
    if selected_lines:
        filtered_df_step1 = df[df["line_name"].isin(selected_lines)]
    else:
        filtered_df_step1 = df

    all_stations = sorted(filtered_df_step1["station_name_kr"].unique())
    selected_stations = st.sidebar.multiselect(
        "역 선택 (복수 선택 가능)", all_stations, default=[]
    )

    # Apply Filters
    if selected_stations:
        filtered_df = filtered_df_step1[
            filtered_df_step1["station_name_kr"].isin(selected_stations)
        ]
    else:
        filtered_df = filtered_df_step1

    # Show summary of selection
    st.sidebar.markdown("---")
    st.sidebar.write(f"선택된 데이터 수: {len(filtered_df):,} 개")

    # Checkbox for raw data
    show_raw_data = st.sidebar.checkbox("원본 데이터 보기", value=False)

    if show_raw_data:
        st.subheader("📋 데이터 미리보기")
        st.dataframe(filtered_df.head(100))

    st.markdown("---")

    # 2. Key Metrics
    st.subheader("💡 주요 지표 (선택된 범위)")
    total_area_sum = filtered_df["total_area"].sum()
    total_households_sum = filtered_df["total_households"].sum()
    avg_area = filtered_df["total_area"].mean()

    col1, col2, col3 = st.columns(3)
    col1.metric("총 연면적 합계", f"{total_area_sum:,.0f} m²")
    col2.metric("총 세대 수 합계", f"{total_households_sum:,.0f} 세대")
    col3.metric("평균 연면적 (레코드 당)", f"{avg_area:,.0f} m²")

    st.markdown("---")

    # Tabs for different analyses
    tab1, tab2, tab3 = st.tabs(
        ["🏗️ 건물 용도 분석", "📊 호선/역별 분석", "🔍 상관관계 분석"]
    )

    with tab1:
        st.header("건물 용도별 상세 분석")

        col_t1_1, col_t1_2 = st.columns(2)

        with col_t1_1:
            st.subheader("1. 건물 용도별 빈도수")
            usage_counts = filtered_df["usage_type"].value_counts().reset_index()
            usage_counts.columns = ["usage_type", "count"]

            fig_usage_count = px.bar(
                usage_counts,
                x="usage_type",
                y="count",
                title="건물 용도별 레코드 수",
                labels={"usage_type": "건물 용도", "count": "레코드 수"},
                color="usage_type",
            )
            st.plotly_chart(fig_usage_count, width="stretch")

        with col_t1_2:
            st.subheader("2. 건물 용도별 총 연면적")
            usage_area = (
                filtered_df.groupby("usage_type")["total_area"].sum().reset_index()
            )

            fig_usage_area = px.bar(
                usage_area,
                x="usage_type",
                y="total_area",
                title="건물 용도별 총 연면적 합계",
                labels={"usage_type": "건물 용도", "total_area": "총 연면적 (m²)"},
                color="usage_type",
            )
            st.plotly_chart(fig_usage_area, width="stretch")

        st.subheader("3. 건물 용도별 연면적 분포 (Box Plot)")
        fig_box = px.box(
            filtered_df,
            x="usage_type",
            y="total_area",
            title="건물 용도별 연면적 분포 (Log Scale)",
            labels={"usage_type": "건물 용도", "total_area": "연면적 (m²)"},
            color="usage_type",
            log_y=True,
        )
        st.plotly_chart(fig_box, width="stretch")

    with tab2:
        st.header("호선 및 역별 분석")

        # Aggregation
        station_area = (
            filtered_df.groupby(["line_name", "station_name_kr"])["total_area"]
            .sum()
            .reset_index()
        )
        station_area["station_label"] = (
            station_area["line_name"] + " " + station_area["station_name_kr"]
        )

        st.subheader("1. 연면적 상위 역 (Top 20)")
        top_20 = station_area.sort_values("total_area", ascending=False).head(20)

        fig_top20 = px.bar(
            top_20,
            x="station_label",
            y="total_area",
            title="연면적 합계 상위 20개 역",
            labels={"station_label": "역 (호선)", "total_area": "총 연면적 (m²)"},
            color="total_area",
            color_continuous_scale="Viridis",
        )
        fig_top20.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig_top20, width="stretch")

        st.subheader("2. 호선별 총 연면적 비교")
        line_area = filtered_df.groupby("line_name")["total_area"].sum().reset_index()

        fig_line = px.pie(
            line_area,
            names="line_name",
            values="total_area",
            title="호선별 연면적 점유율",
            hole=0.4,
        )
        st.plotly_chart(fig_line, width="stretch")

    with tab3:
        st.header("변수 간 상관관계 분석")

        st.subheader("세대 수 vs 가구 수 Scatter Plot")
        fig_scatter = px.scatter(
            filtered_df,
            x="total_households",
            y="total_families",
            color="usage_type",
            size="total_area",
            hover_data=["station_name_kr", "line_name"],
            title="총 세대 수 vs 총 가구 수 (점 크기: 연면적)",
            labels={"total_households": "총 세대 수", "total_families": "총 가구 수"},
            log_x=True,
            log_y=True,
        )
        st.plotly_chart(fig_scatter, width="stretch")

        st.subheader("상관관계 히트맵")
        numeric_df = filtered_df[["total_area", "total_households", "total_families"]]
        if not numeric_df.empty:
            corr = numeric_df.corr()
            fig_corr = px.imshow(
                corr,
                text_auto=".2f",
                aspect="auto",
                title="수치형 변수 상관관계",
                color_continuous_scale="RdBu_r",
                origin="lower",
            )
            st.plotly_chart(fig_corr, width="stretch")

    st.markdown("---")
    st.markdown("Developed for **Subway Congestion Analysis Project**")


if __name__ == "__main__":
    main()
