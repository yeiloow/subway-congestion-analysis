import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import logging
from src.utils.db_util import get_connection
from src.utils.config import PLOTS_DIR, LOG_FORMAT, LOG_LEVEL

# Configure Logging
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)


def analyze_revenue_congestion():
    try:
        conn = get_connection()
    except Exception as e:
        logger.error(f"Cannot proceed without database connection: {e}")
        return

    try:
        # Define mappings between Revenue Time Columns and Congestion Time Slots
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
                "cong_end": 37,  # 23:30 (ends at 24:00) - approx
            },
        ]

        # 1. Prepare Data and Calculate Correlations
        logger.info(f"{'Time Range':<20} | {'Correlation':<12} | {'Records':<8}")
        logger.info("-" * 46)

        plot_data = []

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

            if len(merged_df) == 0:
                logger.info(f"{label:<20} | {'N/A':<12} | {0:<8}")
                plot_data.append(
                    {
                        "label": label,
                        "title": f"{label} (N/A)",
                        "df": None,
                        "corr": None,
                    }
                )
                continue

            # Calculate Correlation
            corr = merged_df["avg_congestion"].corr(merged_df["total_sales_amt"])
            logger.info(f"{label:<20} | {corr:.4f}       | {len(merged_df):<8}")

            plot_data.append(
                {
                    "label": label,
                    "title": f"{label} (Corr: {corr:.4f})",
                    "df": merged_df,
                    "corr": corr,
                }
            )

        # 2. Create Plot with Dynamic Titles
        fig = make_subplots(
            rows=2,
            cols=3,
            subplot_titles=[p["title"] for p in plot_data],
            horizontal_spacing=0.1,
            vertical_spacing=0.15,
        )

        for i, data in enumerate(plot_data):
            if data["df"] is None:
                continue

            merged_df = data["df"]
            row = (i // 3) + 1
            col = (i % 3) + 1

            fig.add_trace(
                go.Scatter(
                    x=merged_df["total_sales_amt"],
                    y=merged_df["avg_congestion"],
                    mode="markers",
                    marker=dict(
                        size=8, opacity=0.6, line=dict(width=1, color="DarkSlateGrey")
                    ),
                    text=merged_df["administrative_dong"]
                    + " ("
                    + merged_df["quarter_code"]
                    + ")",
                    hovertemplate=(
                        "행정동: %{text}<br>"
                        + "매출: %{x:,.0f} 원<br>"
                        + "혼잡도: %{y:.1f}<br>"
                        + "<extra></extra>"
                    ),
                    name=data["label"],
                    showlegend=False,
                ),
                row=row,
                col=col,
            )

            # Update axes titles
            fig.update_xaxes(title_text="총 매출 (원)", row=row, col=col)
            fig.update_yaxes(title_text="평균 혼잡도", row=row, col=col)

        # Update layout
        fig.update_layout(
            title_text="시간대별 매출 vs 혼잡도 상관관계",
            height=900,
            width=1400,
            showlegend=False,
            template="plotly_white",
        )

        output_path = PLOTS_DIR / "revenue_vs_congestion_by_time.html"
        fig.write_html(str(output_path))
        logger.info(f"\nInteractive plot saved to {output_path}")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        import traceback

        traceback.print_exc()
    finally:
        conn.close()


if __name__ == "__main__":
    analyze_revenue_congestion()
