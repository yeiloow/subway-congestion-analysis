import pandas as pd
import numpy as np
from scipy import stats
import logging
import sqlite3
from src.utils.db_util import get_connection
from src.utils.config import OUTPUT_DIR, LOG_FORMAT, LOG_LEVEL

# Configure Logging
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

# Database connection
try:
    conn = get_connection()
except Exception as e:
    logger.error(f"Failed to connect to DB: {e}")
    exit(1)

# ============================================================
# 1. Data Extraction & Preparation
# ============================================================

# Query congestion data aggregated by time slot
congestion_query = """
SELECT
    time_slot,
    AVG(congestion_level) as avg_congestion,
    is_weekend,
    is_upline,
    COUNT(*) as record_count
FROM Station_Congestion
GROUP BY time_slot, is_weekend, is_upline
ORDER BY time_slot, is_weekend, is_upline
"""

# Query living population data aggregated by time slot
living_pop_query = """
SELECT
    CAST(time_slot AS INTEGER) as hour,
    AVG(local_total_living_pop) as avg_local_living_pop,
    AVG(long_term_chinese_stay_pop) as avg_chinese_foreigner,
    AVG(long_term_non_chinese_stay_pop) as avg_other_foreigner
FROM Dong_Living_Population
WHERE local_total_living_pop IS NOT NULL
GROUP BY time_slot
ORDER BY hour
"""

congestion_df = pd.read_sql_query(congestion_query, conn)
living_pop_df = pd.read_sql_query(living_pop_query, conn)

logger.info("=" * 70)
logger.info("CONGESTION DATA (sample)")
logger.info("=" * 70)
logger.info("\n" + str(congestion_df.head(10)))
logger.info(f"\nTotal congestion records: {len(congestion_df)}\n")

logger.info("=" * 70)
logger.info("LIVING POPULATION DATA (sample)")
logger.info("=" * 70)
logger.info("\n" + str(living_pop_df.head()))
logger.info(f"\nTotal living population records: {len(living_pop_df)}\n")

# ============================================================
# 2. Time Slot Mapping
# ============================================================
# Station_Congestion: time_slot 1-48 represents 30-min intervals starting at 05:30
# Mapping: 1->05:30, 2->06:00, ..., need to map to hourly format for Living Population


def congestion_slot_to_hour(slot):
    """Convert congestion time slot (1-48) to hour (0-23)"""
    minutes = 330 + (slot - 1) * 30  # 330 = 05:30
    hour = (minutes // 60) % 24
    return hour


congestion_df["hour"] = congestion_df["time_slot"].apply(congestion_slot_to_hour)

# Aggregate congestion by hour (averaging across multiple 30-min slots per hour)
congestion_by_hour = (
    congestion_df.groupby("hour")
    .agg({"avg_congestion": "mean", "record_count": "sum"})
    .reset_index()
)

logger.info("=" * 70)
logger.info("CONGESTION AGGREGATED BY HOUR")
logger.info("=" * 70)
logger.info("\n" + str(congestion_by_hour))

# ============================================================
# 3. Correlation Analysis
# ============================================================

# Merge data on hour
merged_df = pd.merge(
    congestion_by_hour[["hour", "avg_congestion"]],
    living_pop_df[
        ["hour", "avg_local_living_pop", "avg_chinese_foreigner", "avg_other_foreigner"]
    ],
    on="hour",
)

logger.info("\n" + "=" * 70)
logger.info("MERGED DATA FOR CORRELATION ANALYSIS")
logger.info("=" * 70)
logger.info("\n" + merged_df.to_string(index=False))

# Calculate Pearson and Spearman correlations
logger.info("\n" + "=" * 70)
logger.info("CORRELATION ANALYSIS RESULTS")
logger.info("=" * 70)

correlations = {}

# Congestion vs Local Population
pearson_r, pearson_p = stats.pearsonr(
    merged_df["avg_congestion"], merged_df["avg_local_living_pop"]
)
spearman_r, spearman_p = stats.spearmanr(
    merged_df["avg_congestion"], merged_df["avg_local_living_pop"]
)

logger.info(f"\n1. Congestion vs Local Living Population:")
logger.info(f"   Pearson Correlation:  r = {pearson_r:.4f}, p-value = {pearson_p:.4f}")
logger.info(
    f"   Spearman Correlation: ρ = {spearman_r:.4f}, p-value = {spearman_p:.4f}"
)
correlations["local_pop"] = {
    "pearson_r": pearson_r,
    "pearson_p": pearson_p,
    "spearman_r": spearman_r,
    "spearman_p": spearman_p,
}

# Congestion vs Chinese Foreigners
if merged_df["avg_chinese_foreigner"].notna().sum() > 1:
    pearson_r, pearson_p = stats.pearsonr(
        merged_df["avg_congestion"], merged_df["avg_chinese_foreigner"]
    )
    spearman_r, spearman_p = stats.spearmanr(
        merged_df["avg_congestion"], merged_df["avg_chinese_foreigner"]
    )
    logger.info(f"\n2. Congestion vs Chinese Foreigners (Long-term):")
    logger.info(
        f"   Pearson Correlation:  r = {pearson_r:.4f}, p-value = {pearson_p:.4f}"
    )
    logger.info(
        f"   Spearman Correlation: ρ = {spearman_r:.4f}, p-value = {spearman_p:.4f}"
    )
    correlations["chinese"] = {
        "pearson_r": pearson_r,
        "pearson_p": pearson_p,
        "spearman_r": spearman_r,
        "spearman_p": spearman_p,
    }

# Congestion vs Other Foreigners
if merged_df["avg_other_foreigner"].notna().sum() > 1:
    pearson_r, pearson_p = stats.pearsonr(
        merged_df["avg_congestion"], merged_df["avg_other_foreigner"]
    )
    spearman_r, spearman_p = stats.spearmanr(
        merged_df["avg_congestion"], merged_df["avg_other_foreigner"]
    )
    logger.info(f"\n3. Congestion vs Other Foreigners (Long-term):")
    logger.info(
        f"   Pearson Correlation:  r = {pearson_r:.4f}, p-value = {pearson_p:.4f}"
    )
    logger.info(
        f"   Spearman Correlation: ρ = {spearman_r:.4f}, p-value = {spearman_p:.4f}"
    )
    correlations["other_foreigner"] = {
        "pearson_r": pearson_r,
        "pearson_p": pearson_p,
        "spearman_r": spearman_r,
        "spearman_p": spearman_p,
    }


# ============================================================
# 4. Visualization (Refactored to Plotly)
# ============================================================
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import plotly.express as px
from src.utils.visualization import save_plot


def visualize_results(merged_df, correlations):
    logger.info("Generating visualizations with Plotly...")

    # 1. Congestion and Local Population over time (Dual Axis)
    fig1 = make_subplots(specs=[[{"secondary_y": True}]])

    fig1.add_trace(
        go.Scatter(
            x=merged_df["hour"],
            y=merged_df["avg_congestion"],
            name="혼잡도",
            mode="lines+markers",
        ),
        secondary_y=False,
    )

    fig1.add_trace(
        go.Scatter(
            x=merged_df["hour"],
            y=merged_df["avg_local_living_pop"],
            name="생활인구 (내국인)",
            mode="lines+markers",
            line=dict(color="red"),
        ),
        secondary_y=True,
    )

    fig1.update_layout(
        title_text="시간대별 혼잡도 및 생활인구 추이", hovermode="x unified"
    )
    fig1.update_xaxes(title_text="시간 (Hour)")
    fig1.update_yaxes(title_text="혼잡도", secondary_y=False)
    fig1.update_yaxes(title_text="생활인구", secondary_y=True)

    save_plot(fig1, OUTPUT_DIR / "eda_congestion_population_trend.html")

    # 2. Scatter Plot
    fig2 = px.scatter(
        merged_df,
        x="avg_local_living_pop",
        y="avg_congestion",
        # trendline="ols",  # Removed due to missing statsmodels
        hover_data=["hour"],
        title=f"생활인구 vs 혼잡도 산점도 (r={correlations['local_pop']['pearson_r']:.3f})",
    )
    save_plot(fig2, OUTPUT_DIR / "eda_congestion_population_scatter.html")

    # 3. Normalized Comparison Bar Chart using Melt for tidy format
    merged_norm = merged_df.copy()
    # Normalize for visualization
    merged_norm["norm_congestion"] = (
        merged_df["avg_congestion"] - merged_df["avg_congestion"].min()
    ) / (merged_df["avg_congestion"].max() - merged_df["avg_congestion"].min())
    merged_norm["norm_population"] = (
        merged_df["avg_local_living_pop"] - merged_df["avg_local_living_pop"].min()
    ) / (
        merged_df["avg_local_living_pop"].max()
        - merged_df["avg_local_living_pop"].min()
    )

    df_melt = merged_norm.melt(
        id_vars=["hour"],
        value_vars=["norm_congestion", "norm_population"],
        var_name="Type",
        value_name="Normalized Value",
    )

    fig3 = px.bar(
        df_melt,
        x="hour",
        y="Normalized Value",
        color="Type",
        barmode="group",
        title="정규화된 혼잡도 및 생활인구 시간대별 비교",
    )
    save_plot(fig3, OUTPUT_DIR / "eda_congestion_population_bar.html")


def analyze_weekend_pattern(congestion_df):
    logger.info("\n" + "=" * 70)
    logger.info("ADDITIONAL ANALYSIS: WEEKDAY VS WEEKEND")
    logger.info("=" * 70)

    congestion_weekend = (
        congestion_df[congestion_df["is_weekend"] == 1]
        .groupby("hour")["avg_congestion"]
        .mean()
        .reset_index()
    )
    congestion_weekday = (
        congestion_df[congestion_df["is_weekend"] == 0]
        .groupby("hour")["avg_congestion"]
        .mean()
        .reset_index()
    )

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=congestion_weekday["hour"],
            y=congestion_weekday["avg_congestion"],
            name="평일",
            mode="lines+markers",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=congestion_weekend["hour"],
            y=congestion_weekend["avg_congestion"],
            name="주말",
            mode="lines+markers",
        )
    )

    fig.update_layout(
        title="평일 vs 주말 혼잡도 패턴 비교",
        xaxis_title="시간",
        yaxis_title="평균 혼잡도",
    )
    save_plot(fig, OUTPUT_DIR / "eda_congestion_weekday_weekend.html")


# Main Execution Flow
if __name__ == "__main__":
    visualize_results(merged_df, correlations)
    analyze_weekend_pattern(congestion_df)

    conn.close()
    logger.info("Analysis and Visualization Complete.")
