import pandas as pd
import logging
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from src.utils.db_util import get_connection
from src.utils.config import OUTPUT_DIR, LOG_FORMAT, LOG_LEVEL
from src.utils.visualization import save_plot, apply_theme

# Apply Theme
apply_theme()

# Configure Logging
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)


def analyze_correlation():
    try:
        conn = get_connection()
    except Exception as e:
        logger.error(f"Failed to connect to DB: {e}")
        return

    try:
        # Time Slot Mapping
        # (Label, Floating Pop Column, Start Time Slot, End Time Slot)
        time_mapping = [
            ("06:00 - 11:00", "time_06_11_floating_pop", 2, 11),
            ("11:00 - 14:00", "time_11_14_floating_pop", 12, 17),
            ("14:00 - 17:00", "time_14_17_floating_pop", 18, 23),
            ("17:00 - 21:00", "time_17_21_floating_pop", 24, 31),
            ("21:00 - 24:00", "time_21_24_floating_pop", 32, 37),
        ]

        # Initialize Plotly Subplots
        fig = make_subplots(
            rows=2,
            cols=3,
            subplot_titles=[label for label, _, _, _ in time_mapping],
            horizontal_spacing=0.1,
            vertical_spacing=0.15,
        )

        logger.info(f"{'Time Range':<20} | {'Correlation':<12} | {'Records':<8}")
        logger.info("-" * 46)

        for i, (label, float_col, start_slot, end_slot) in enumerate(time_mapping):
            row = (i // 3) + 1
            col = (i % 3) + 1

            # 1. Fetch Congestion Data for specific time slots (2, 4, 5호선 only)
            query_congestion = f"""
                SELECT
                    r.administrative_dong,
                    c.quarter_code,
                    AVG(c.congestion_level) as avg_congestion
                FROM Station_Congestion c
                JOIN Station_Routes r ON c.station_number = r.station_number
                JOIN Lines l ON r.line_id = l.line_id
                WHERE c.time_slot BETWEEN {start_slot} AND {end_slot}
                AND l.line_name IN ('2호선', '4호선', '5호선')
                GROUP BY r.administrative_dong, c.quarter_code
            """
            df_congestion = pd.read_sql_query(query_congestion, conn)

            # 2. Fetch Floating Population Data for specific column
            query_floating = f"""
                SELECT 
                    admin_dong_name as administrative_dong,
                    quarter_code,
                    {float_col} as floating_pop
                FROM Dong_Floating_Population
            """
            df_floating = pd.read_sql_query(query_floating, conn)

            # 3. Merge
            df_congestion["quarter_code"] = df_congestion["quarter_code"].astype(str)
            df_floating["quarter_code"] = df_floating["quarter_code"].astype(str)

            merged_df = pd.merge(
                df_congestion,
                df_floating,
                on=["administrative_dong", "quarter_code"],
                how="inner",
            )

            if len(merged_df) == 0:
                logger.info(f"{label:<20} | {'N/A':<12} | {0:<8}")
                continue

            # 4. Calculate Correlation
            corr = merged_df["avg_congestion"].corr(merged_df["floating_pop"])
            logger.info(f"{label:<20} | {corr:.4f}       | {len(merged_df):<8}")

            # 5. Add Trace to Subplot
            fig.add_trace(
                go.Scatter(
                    x=merged_df["floating_pop"],
                    y=merged_df["avg_congestion"],
                    mode="markers",
                    marker=dict(opacity=0.5, size=6),
                    name=label,
                    showlegend=False,
                ),
                row=row,
                col=col,
            )

            # Update Axis Titles (Only for outer plots or verify each)
            fig.update_xaxes(title_text="유동인구", row=row, col=col)
            fig.update_yaxes(title_text="평균 혼잡도", row=row, col=col)

        fig.update_layout(
            title_text="시간대별 유동인구 vs 혼잡도 상관관계",
            height=800,
            width=1200,
            showlegend=False,
        )

        output_path = OUTPUT_DIR / "congestion_vs_floating_pop_by_time.html"
        save_plot(fig, output_path)
        logger.info(f"\nInteractive plot saved to {output_path}")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    analyze_correlation()
