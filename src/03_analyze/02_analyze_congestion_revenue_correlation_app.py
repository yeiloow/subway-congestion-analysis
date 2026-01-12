import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
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
    page_title="매출-혼잡도 상관관계 분석", page_icon="🔗", layout="wide"
)

# Title
st.title("🔗 매출-혼잡도 상관관계 분석")
st.markdown(
    """
    시간대별 추정 매출과 지하철 혼잡도 간의 상관관계를 행정동 단위로 분석합니다.
    - **매출 데이터**: 행정동별 추정 매출 (단위: 원)
    - **혼잡도 데이터**: 지하철역 기준 혼잡도를 행정동 단위로 평균 내어 산출 (단위: %)
    """
)


# 1. Load Data
@st.cache_data
def load_and_process_data():
    conn = get_connection()
    try:
        # Time Mapping Configuration
        time_mapping = [
            {
                "label": "06:00 - 11:00",
                "rev_col": "time_06_11_sales_amt",
                "cong_start": 2,  # 06:00
                "cong_end": 11,  # 10:30 (ends at 11:00)
            },
            {
                "label": "11:00 - 14:00",
                "rev_col": "time_11_14_sales_amt",
                "cong_start": 12,  # 11:00
                "cong_end": 17,  # 13:30 (ends at 14:00)
            },
            {
                "label": "14:00 - 17:00",
                "rev_col": "time_14_17_sales_amt",
                "cong_start": 18,  # 14:00
                "cong_end": 23,  # 16:30 (ends at 17:00)
            },
            {
                "label": "17:00 - 21:00",
                "rev_col": "time_17_21_sales_amt",
                "cong_start": 24,  # 17:00
                "cong_end": 31,  # 20:30 (ends at 21:00)
            },
            {
                "label": "21:00 - 24:00",
                "rev_col": "time_21_24_sales_amt",
                "cong_start": 32,  # 21:00
                "cong_end": 37,  # 23:30 (ends at 24:00)
            },
        ]

        processed_data = []

        for item in time_mapping:
            label = item["label"]
            rev_col = item["rev_col"]
            start = item["cong_start"]
            end = item["cong_end"]

            # Fetch Congestion Data
            query_congestion = f"""
                SELECT 
                    r.administrative_dong,
                    c.quarter_code,
                    AVG(c.congestion_level) as avg_congestion
                FROM Station_Congestion c
                JOIN Station_Routes r ON c.station_number = r.station_number
                WHERE c.time_slot BETWEEN {start} AND {end}
                GROUP BY r.administrative_dong, c.quarter_code
            """
            df_congestion = pd.read_sql_query(query_congestion, conn)

            # Fetch Estimated Revenue Data
            query_revenue = f"""
                SELECT 
                    admin_dong_name as administrative_dong,
                    quarter_code,
                    SUM({rev_col}) as total_sales_amt
                FROM Dong_Estimated_Revenue
                GROUP BY admin_dong_name, quarter_code
            """
            df_revenue = pd.read_sql_query(query_revenue, conn)

            # Merge
            df_congestion["quarter_code"] = df_congestion["quarter_code"].astype(str)
            df_revenue["quarter_code"] = df_revenue["quarter_code"].astype(str)

            merged_df = pd.merge(
                df_congestion,
                df_revenue,
                on=["administrative_dong", "quarter_code"],
                how="inner",
            )

            if not merged_df.empty:
                processed_data.append(
                    {
                        "label": label,
                        "data": merged_df,
                        "corr": merged_df["avg_congestion"].corr(
                            merged_df["total_sales_amt"]
                        ),
                    }
                )

        return processed_data

    except Exception as e:
        st.error(f"데이터 로드 중 오류 발생: {e}")
        return []
    finally:
        conn.close()


data_list = load_and_process_data()

if not data_list:
    st.warning("데이터가 없습니다.")
    st.stop()

# 2. Summary
st.header("📊 시간대별 상관관계 요약")

summary_data = []
for d in data_list:
    summary_data.append(
        {"시간대": d["label"], "상관계수": d["corr"], "데이터 수": len(d["data"])}
    )

summary_df = pd.DataFrame(summary_data)
st.dataframe(summary_df.style.format({"상관계수": "{:.4f}", "데이터 수": "{:,}"}))


# 3. Detailed Plots
st.header("📈 시간대별 상세 산점도")

# Visualization Mode
viz_mode = st.radio(
    "보기 모드", ["모두 보기 (Grid)", "개별 보기 (Tab)"], horizontal=True
)

if viz_mode == "모두 보기 (Grid)":
    rows = (len(data_list) + 1) // 2
    fig = make_subplots(
        rows=rows,
        cols=2,
        subplot_titles=[f"{d['label']} (Corr: {d['corr']:.2f})" for d in data_list],
        horizontal_spacing=0.1,
        vertical_spacing=0.15,
    )

    for i, d in enumerate(data_list):
        row = (i // 2) + 1
        col = (i % 2) + 1

        df = d["data"]

        fig.add_trace(
            go.Scatter(
                x=df["total_sales_amt"],
                y=df["avg_congestion"],
                mode="markers",
                marker=dict(
                    size=8, opacity=0.6, line=dict(width=1, color="DarkSlateGrey")
                ),
                text=df["administrative_dong"] + " (" + df["quarter_code"] + ")",
                hovertemplate=(
                    "<b>%{text}</b><br>"
                    + "매출: %{x:,.0f} 원<br>"
                    + "혼잡도: %{y:.1f}%<br>"
                    + "<extra></extra>"
                ),
                name=d["label"],
            ),
            row=row,
            col=col,
        )

        fig.update_xaxes(title_text="매출 (원)", row=row, col=col)
        fig.update_yaxes(title_text="혼잡도 (%)", row=row, col=col)

    fig.update_layout(
        height=400 * rows, title_text="시간대별 매출 vs 혼잡도", showlegend=False
    )
    st.plotly_chart(fig, width="stretch")

else:
    tabs = st.tabs([d["label"] for d in data_list])

    for i, tab in enumerate(tabs):
        with tab:
            d = data_list[i]
            df = d["data"]

            fig = go.Figure()
            fig.add_trace(
                go.Scatter(
                    x=df["total_sales_amt"],
                    y=df["avg_congestion"],
                    mode="markers",
                    marker=dict(
                        size=10, opacity=0.7, line=dict(width=1, color="DarkSlateGrey")
                    ),
                    text=df["administrative_dong"] + " (" + df["quarter_code"] + ")",
                    hovertemplate=(
                        "<b>%{text}</b><br>"
                        + "매출: %{x:,.0f} 원<br>"
                        + "혼잡도: %{y:.1f}%<br>"
                        + "<extra></extra>"
                    ),
                )
            )

            fig.update_layout(
                title=f"{d['label']} 상관관계 (Corr: {d['corr']:.4f})",
                xaxis_title="총 매출 (원)",
                yaxis_title="평균 혼잡도 (%)",
                height=600,
            )
            st.plotly_chart(fig, width="stretch")

st.markdown("---")
st.markdown("Developed for **Subway Congestion Analysis Project**")
