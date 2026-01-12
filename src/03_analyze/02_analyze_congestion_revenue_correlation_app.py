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

            # A. Fetch Congestion Data by Line & Dong
            query_congestion_by_line = f"""
                SELECT 
                    r.administrative_dong,
                    l.line_name,
                    c.quarter_code,
                    AVG(c.congestion_level) as avg_congestion
                FROM Station_Congestion c
                JOIN Station_Routes r ON c.station_number = r.station_number
                JOIN Lines l ON r.line_id = l.line_id
                WHERE c.time_slot BETWEEN {start} AND {end}
                  AND l.line_name IN ('2호선', '4호선', '5호선')
                GROUP BY r.administrative_dong, l.line_name, c.quarter_code
            """
            df_cong_by_line = pd.read_sql_query(query_congestion_by_line, conn)
            df_cong_by_line["quarter_code"] = df_cong_by_line["quarter_code"].astype(
                str
            )

            # B. Fetch Estimated Revenue Data (By Dong)
            query_revenue = f"""
                SELECT 
                    admin_dong_name as administrative_dong,
                    quarter_code,
                    SUM({rev_col}) as total_sales_amt
                FROM Dong_Estimated_Revenue
                GROUP BY admin_dong_name, quarter_code
            """
            df_revenue = pd.read_sql_query(query_revenue, conn)
            df_revenue["quarter_code"] = df_revenue["quarter_code"].astype(str)

            # C. Fetch Station Names (By Line & Dong to match A)
            query_stations = """
                SELECT 
                    r.administrative_dong,
                    l.line_name,
                    GROUP_CONCAT(DISTINCT s.station_name_kr) as station_names
                FROM Station_Routes r
                JOIN Lines l ON r.line_id = l.line_id
                JOIN Stations s ON r.station_id = s.station_id
                WHERE l.line_name IN ('2호선', '4호선', '5호선')
                GROUP BY r.administrative_dong, l.line_name
            """
            df_stations = pd.read_sql_query(query_stations, conn)

            # --- Prepare Line-based Dataset ---
            # Merge Station Names into Congestion
            df_line_merged = pd.merge(
                df_cong_by_line,
                df_stations,
                on=["administrative_dong", "line_name"],
                how="left",
            )
            df_line_merged["station_names"] = df_line_merged["station_names"].fillna(
                "Unknown"
            )

            # Merge Revenue
            df_line_final = pd.merge(
                df_line_merged,
                df_revenue,
                on=["administrative_dong", "quarter_code"],
                how="inner",
            )

            # --- Prepare Combined Dataset (Aggregation by Dong) ---
            # Aggregate congestion: Dong, Quarter -> Mean of Lines
            # Note: We can average the 'avg_congestion' of lines.
            df_cong_combined = df_cong_by_line.groupby(
                ["administrative_dong", "quarter_code"], as_index=False
            )["avg_congestion"].mean()

            # Aggregate station names: Join all distinct station names for the dong
            # Simple aggregation by unique names
            df_stations_combined = (
                df_stations.groupby("administrative_dong")["station_names"]
                .apply(lambda x: ", ".join(sorted(list(set(",".join(x).split(","))))))
                .reset_index()
            )

            merged_combined = pd.merge(
                df_cong_combined,
                df_revenue,
                on=["administrative_dong", "quarter_code"],
                how="inner",
            )
            merged_combined = pd.merge(
                merged_combined,
                df_stations_combined,
                on="administrative_dong",
                how="left",
            )
            merged_combined["station_names"] = merged_combined["station_names"].fillna(
                "Unknown"
            )

            if not merged_combined.empty:
                # Calculate Combined Correlation
                corr_combined = merged_combined["avg_congestion"].corr(
                    merged_combined["total_sales_amt"]
                )

                # Calculate Correlation by Line
                lines = sorted(df_line_final["line_name"].unique())
                corr_by_line = {}
                for ln in lines:
                    subset = df_line_final[df_line_final["line_name"] == ln]
                    if len(subset) > 1:  # Need at least 2 points for correlation
                        corr_by_line[ln] = subset["avg_congestion"].corr(
                            subset["total_sales_amt"]
                        )
                    else:
                        corr_by_line[ln] = 0.0

                processed_data.append(
                    {
                        "label": label,
                        "combined": {"df": merged_combined, "corr": corr_combined},
                        "by_line": {"df": df_line_final, "corr": corr_by_line},
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
    row = {
        "시간대": d["label"],
        "전체 상관계수": d["combined"]["corr"],
        "데이터 수 (전체)": len(d["combined"]["df"]),
    }
    # Add line correlations
    for ln, corr in d["by_line"]["corr"].items():
        row[f"{ln} 상관계수"] = corr

    summary_data.append(row)

summary_df = pd.DataFrame(summary_data)
# Format columns dynamically
format_dict = {"전체 상관계수": "{:.4f}", "데이터 수 (전체)": "{:,}"}
for col in summary_df.columns:
    if "상관계수" in col:
        format_dict[col] = "{:.4f}"

st.dataframe(summary_df.style.format(format_dict))


# 3. Detailed Plots - Combined
st.header("📈 시간대별 상세 산점도 (전체 호선 통합)")

# Visualization Mode
viz_mode = st.radio(
    "보기 모드",
    ["모두 보기 (Grid)", "개별 보기 (Tab)"],
    horizontal=True,
    key="combined_viz",
)

if viz_mode == "모두 보기 (Grid)":
    rows = (len(data_list) + 1) // 2
    fig = make_subplots(
        rows=rows,
        cols=2,
        subplot_titles=[
            f"{d['label']} (Corr: {d['combined']['corr']:.2f})" for d in data_list
        ],
        horizontal_spacing=0.1,
        vertical_spacing=0.15,
    )

    for i, d in enumerate(data_list):
        row = (i // 2) + 1
        col = (i % 2) + 1

        df = d["combined"]["df"]

        fig.add_trace(
            go.Scatter(
                x=df["total_sales_amt"],
                y=df["avg_congestion"],
                mode="markers",
                marker=dict(
                    size=8, opacity=0.6, line=dict(width=1, color="DarkSlateGrey")
                ),
                text=df["administrative_dong"] + " (" + df["quarter_code"] + ")",
                customdata=df[["station_names"]].fillna(""),
                hovertemplate=(
                    "<b>%{text}</b><br>"
                    + "역: %{customdata[0]}<br>"
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
        height=400 * rows, title_text="시간대별 매출 vs 혼잡도 (전체)", showlegend=False
    )
    st.plotly_chart(fig, width="stretch")

else:
    tabs = st.tabs([d["label"] for d in data_list])

    for i, tab in enumerate(tabs):
        with tab:
            d = data_list[i]
            df = d["combined"]["df"]

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
                    customdata=df[["station_names"]].fillna(""),
                    hovertemplate=(
                        "<b>%{text}</b><br>"
                        + "역: %{customdata[0]}<br>"
                        + "매출: %{x:,.0f} 원<br>"
                        + "혼잡도: %{y:.1f}%<br>"
                        + "<extra></extra>"
                    ),
                )
            )

            fig.update_layout(
                title=f"{d['label']} 상관관계 (Corr: {d['combined']['corr']:.4f})",
                xaxis_title="총 매출 (원)",
                yaxis_title="평균 혼잡도 (%)",
                height=600,
            )
            st.plotly_chart(fig, width="stretch")


# 4. Detailed Plots - By Line
st.header("🚇 호선별 상세 산점도")
st.markdown("각 시간대별로 2, 4, 5호선을 구분하여 상관관계를 분석합니다.")

line_tabs = st.tabs(["2호선", "4호선", "5호선"])

for tab, target_line in zip(line_tabs, ["2호선", "4호선", "5호선"]):
    with tab:
        # Create a subplot for each line: Rows = Time Slots
        # Actually, let's just make a grid of time slots for THIS line.

        rows = (len(data_list) + 1) // 2
        fig_line = make_subplots(
            rows=rows,
            cols=2,
            subplot_titles=[
                f"{d['label']} (Corr: {d['by_line']['corr'].get(target_line, 0):.2f})"
                for d in data_list
            ],
            horizontal_spacing=0.1,
            vertical_spacing=0.15,
        )

        for i, d in enumerate(data_list):
            row = (i // 2) + 1
            col = (i % 2) + 1

            df_all_lines = d["by_line"]["df"]
            df_target = df_all_lines[df_all_lines["line_name"] == target_line]

            if df_target.empty:
                continue

            fig_line.add_trace(
                go.Scatter(
                    x=df_target["total_sales_amt"],
                    y=df_target["avg_congestion"],
                    mode="markers",
                    marker=dict(
                        size=8,
                        opacity=0.6,
                        line=dict(width=1, color="DarkSlateGrey"),
                        # You could color code by line if mixed, but here we separated by tabs
                    ),
                    text=df_target["administrative_dong"]
                    + " ("
                    + df_target["quarter_code"]
                    + ")",
                    customdata=df_target[["station_names"]].fillna(""),
                    hovertemplate=(
                        "<b>%{text}</b><br>"
                        + "호선: "
                        + target_line
                        + "<br>"
                        + "역: %{customdata[0]}<br>"
                        + "매출: %{x:,.0f} 원<br>"
                        + "혼잡도: %{y:.1f}%<br>"
                        + "<extra></extra>"
                    ),
                    name=d["label"],
                ),
                row=row,
                col=col,
            )

            fig_line.update_xaxes(title_text="매출 (원)", row=row, col=col)
            fig_line.update_yaxes(title_text="혼잡도 (%)", row=row, col=col)

        fig_line.update_layout(
            height=400 * rows,
            title_text=f"{target_line} 시간대별 매출 vs 혼잡도",
            showlegend=False,
        )
        st.plotly_chart(fig_line, width="stretch")

st.markdown("---")
st.markdown("Developed for **Subway Congestion Analysis Project**")
