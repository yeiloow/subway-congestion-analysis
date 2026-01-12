import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import matplotlib.font_manager as fm
import logging
from src.utils.db_util import get_connection
from src.utils.config import PLOTS_DIR, LOG_FORMAT, LOG_LEVEL

# Configure Logging
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)


def set_korean_font():
    """Sets a Korean font for matplotlib to avoid broken characters."""
    font_candidates = [
        "AppleGothic",  # Mac
        "Malgun Gothic",  # Windows
        "NanumGothic",  # Linux/Custom
    ]

    found = False
    for font_name in font_candidates:
        if any(f.name == font_name for f in fm.fontManager.ttflist):
            plt.rcParams["font.family"] = font_name
            found = True
            break

    if not found:
        if os.name == "posix":
            plt.rcParams["font.family"] = "AppleGothic"

    plt.rcParams["axes.unicode_minus"] = False


def analyze_correlation():
    try:
        conn = get_connection()
    except Exception as e:
        logger.error(f"Failed to connect to DB: {e}")
        return

    try:
        # Time Slot Mapping
        # (Label, Floating Pop Column, Start Time Slot, End Time Slot)
        # Note: Time Slot 1 is 05:30.
        # Slot 2 (06:00) ~ Slot 11 (10:30, ends 11:00)
        time_mapping = [
            ("06:00 - 11:00", "time_06_11_floating_pop", 2, 11),
            ("11:00 - 14:00", "time_11_14_floating_pop", 12, 17),
            ("14:00 - 17:00", "time_14_17_floating_pop", 18, 23),
            ("17:00 - 21:00", "time_17_21_floating_pop", 24, 31),
            ("21:00 - 24:00", "time_21_24_floating_pop", 32, 37),
        ]

        set_korean_font()
        fig, axes = plt.subplots(2, 3, figsize=(18, 12))
        axes = axes.flatten()

        results = []

        logger.info(f"{'Time Range':<20} | {'Correlation':<12} | {'Records':<8}")
        logger.info("-" * 46)

        for i, (label, float_col, start_slot, end_slot) in enumerate(time_mapping):
            # 1. Fetch Congestion Data for specific time slots
            query_congestion = f"""
                SELECT 
                    r.administrative_dong,
                    c.quarter_code,
                    AVG(c.congestion_level) as avg_congestion
                FROM Station_Congestion c
                JOIN Station_Routes r ON c.station_number = r.station_number
                WHERE c.time_slot BETWEEN {start_slot} AND {end_slot}
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
            results.append((label, corr))
            logger.info(f"{label:<20} | {corr:.4f}       | {len(merged_df):<8}")

            # 5. Plot on Subplot
            ax = axes[i]
            sns.scatterplot(
                data=merged_df, x="floating_pop", y="avg_congestion", alpha=0.5, ax=ax
            )
            ax.set_title(f"{label}\nCorr: {corr:.4f}")
            ax.set_xlabel("Floating Pop")
            ax.set_ylabel("Avg Congestion")
            ax.grid(True, linestyle="--", alpha=0.5)

        # Remove empty last subplot if any (we have 5 plots, 6 slots)
        if len(time_mapping) < len(axes):
            for j in range(len(time_mapping), len(axes)):
                fig.delaxes(axes[j])

        plt.tight_layout()

        # Ensure PLOTS_DIR exists (it should via config import but let's be safe)
        if not PLOTS_DIR.exists():
            PLOTS_DIR.mkdir(parents=True, exist_ok=True)

        output_path = PLOTS_DIR / "congestion_vs_floating_pop_by_time.png"
        plt.savefig(output_path)
        logger.info(f"\nPlot saved to {output_path}")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    analyze_correlation()
