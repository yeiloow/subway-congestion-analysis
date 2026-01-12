#!/usr/bin/env python3
"""
Correlation analysis between building near subway and subway congestion
Analysis by time slot (not averaged)
"""

import pandas as pd
import numpy as np
from scipy import stats
import logging
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
from src.utils.db_util import get_connection
from src.utils.config import OUTPUT_DIR, LOG_FORMAT, LOG_LEVEL
from src.utils.visualization import save_plot, apply_theme

# Apply Plotly Theme
apply_theme()

# Configure Logging
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

# Paths
csv_path = OUTPUT_DIR / "station_catchment_stats.csv"


# Time slot mapping: 05:30 = 1, 06:00 = 2, etc. (30-minute intervals)
def time_slot_to_label(slot):
    """Convert time slot number to readable time label."""
    base_hour = 5
    base_minute = 30
    total_minutes = base_minute + (slot - 1) * 30
    hour = base_hour + total_minutes // 60
    minute = total_minutes % 60
    return f"{hour:02d}:{minute:02d}"


# Load building data from CSV
logger.info("Loading building catchment data...")
if not csv_path.exists():
    logger.error(f"File not found: {csv_path}")
    exit(1)

df_buildings = pd.read_csv(csv_path)

logger.info(f"Building data shape: {df_buildings.shape}")
logger.info(f"Columns: {df_buildings.columns.tolist()}")

# Connect to database
logger.info("Connecting to database...")
try:
    conn = get_connection()
except Exception as e:
    logger.error(f"Failed to connect to DB: {e}")
    exit(1)

# Query congestion data per time slot (not averaged)
logger.info("Querying congestion data per time slot...")
query = """
SELECT
    sr.station_id,
    sr.line_id,
    s.station_name_kr,
    l.line_name,
    sc.time_slot,
    sc.is_weekend,
    sc.is_upline,
    sc.congestion_level
FROM Station_Congestion sc
JOIN Station_Routes sr ON sc.station_number = sr.station_number
JOIN Stations s ON sr.station_id = s.station_id
JOIN Lines l ON sr.line_id = l.line_id
ORDER BY sr.station_id, sr.line_id, sc.time_slot
"""

df_congestion = pd.read_sql_query(query, conn)
conn.close()

# Add time label
df_congestion["time_label"] = df_congestion["time_slot"].apply(time_slot_to_label)

logger.info(f"Congestion data shape: {df_congestion.shape}")

# Aggregate building data by station and line
logger.info("\nAggregating building data by station and line...")
df_buildings_agg = (
    df_buildings.groupby(["station_id", "station_name", "line_name"])
    .agg({"total_area": "sum", "total_households": "sum", "total_families": "sum"})
    .reset_index()
)

# Also calculate building diversity (number of different usage types per station)
building_diversity = (
    df_buildings.groupby(["station_id", "station_name", "line_name"])
    .size()
    .reset_index(name="building_types_count")
)

df_buildings_agg = df_buildings_agg.merge(
    building_diversity, on=["station_id", "station_name", "line_name"]
)

# Merge building and congestion data (per time slot)
logger.info("\nMerging building and congestion data...")
df_merged = df_buildings_agg.merge(
    df_congestion,
    left_on=["station_id", "station_name", "line_name"],
    right_on=["station_id", "station_name_kr", "line_name"],
    how="inner",
)

logger.info(f"Merged data shape: {df_merged.shape}")

if df_merged.shape[0] == 0:
    logger.error("ERROR: No matching data found between buildings and congestion!")
    exit(1)
else:
    # Features to analyze
    features_to_analyze = {
        "total_area": "총 건물 연면적",
        "total_households": "총 세대수",
        "total_families": "총 거주가구수",
        "building_types_count": "건물 용도 다양성",
    }

    # ============================================================
    # 1. CORRELATION ANALYSIS BY TIME SLOT
    # ============================================================
    logger.info("\n" + "=" * 60)
    logger.info("CORRELATION ANALYSIS BY TIME SLOT")
    logger.info("=" * 60)

    time_slots = sorted(df_merged["time_slot"].unique())
    time_slot_results = []

    for slot in time_slots:
        slot_data = df_merged[df_merged["time_slot"] == slot]
        time_label = time_slot_to_label(slot)

        for feature, label in features_to_analyze.items():
            if feature in slot_data.columns:
                valid_data = slot_data[[feature, "congestion_level"]].dropna()

                if len(valid_data) > 2:
                    pearson_r, pearson_p = stats.pearsonr(
                        valid_data[feature], valid_data["congestion_level"]
                    )
                    spearman_r, spearman_p = stats.spearmanr(
                        valid_data[feature], valid_data["congestion_level"]
                    )

                    time_slot_results.append(
                        {
                            "time_slot": slot,
                            "time_label": time_label,
                            "feature": label,
                            "pearson_r": pearson_r,
                            "pearson_p": pearson_p,
                            "spearman_r": spearman_r,
                            "spearman_p": spearman_p,
                            "n_samples": len(valid_data),
                        }
                    )

    df_time_slot_results = pd.DataFrame(time_slot_results)

    # Print summary for total_area (strongest predictor)
    logger.info("\nCorrelation by Time Slot (Total Building Area vs Congestion):")
    logger.info("-" * 60)
    # Using the localized label
    area_results = df_time_slot_results[
        df_time_slot_results["feature"] == "총 건물 연면적"
    ]
    for _, row in area_results.iterrows():
        sig = (
            "***"
            if row["pearson_p"] < 0.001
            else "**"
            if row["pearson_p"] < 0.01
            else "*"
            if row["pearson_p"] < 0.05
            else ""
        )
        logger.info(f"  {row['time_label']}: r={row['pearson_r']:.4f} {sig}")

    # Save time slot results
    df_time_slot_results.to_csv(
        OUTPUT_DIR / "correlation_by_time_slot.csv", index=False
    )
    logger.info(
        f"\n✓ Time slot results saved to {OUTPUT_DIR / 'correlation_by_time_slot.csv'}"
    )

    # ============================================================
    # 4. VISUALIZATIONS (Plotly Refactor)
    # ============================================================
    logger.info("\nGenerating visualizations...")

    # Plot 1: Correlation by time of day (Line Plot)
    fig1 = px.line(
        df_time_slot_results,
        x="time_label",
        y="pearson_r",
        color="feature",
        markers=True,
        title="시간대별 상관관계 (건물 특성 vs 혼잡도)",
    )
    fig1.update_yaxes(title="피어슨 상관계수 (r)")
    fig1.update_xaxes(title="시간대")
    save_plot(fig1, OUTPUT_DIR / "correlation_by_time_slot.html")

    # Plot 2: Scatter plots for peak hours vs off-peak
    # Define peak hours
    morning_peak = df_merged[df_merged["time_slot"].between(5, 8)].copy()
    morning_peak["Period"] = "오전 피크 (07:00-09:00)"

    evening_peak = df_merged[df_merged["time_slot"].between(26, 30)].copy()
    evening_peak["Period"] = "오후 피크 (18:00-20:00)"

    off_peak = df_merged[df_merged["time_slot"].between(12, 20)].copy()
    off_peak["Period"] = "비-피크 (11:00-15:00)"

    combined_scatter = pd.concat([morning_peak, evening_peak, off_peak])

    fig2 = px.scatter(
        combined_scatter,
        x="total_area",
        y="congestion_level",
        color="Period",
        # trendline="ols", # Removed due to missing statsmodels
        facet_col="Period",
        opacity=0.4,
        title="건물 연면적 vs 혼잡도: 피크타임 vs 비-피크타임 비교",
    )
    save_plot(fig2, OUTPUT_DIR / "correlation_peak_offpeak.html")

    # Plot 3: Heatmap of correlations
    pivot_data = df_time_slot_results.pivot(
        index="time_label", columns="feature", values="pearson_r"
    )

    fig3 = px.imshow(
        pivot_data,
        text_auto=".2f",
        aspect="auto",
        color_continuous_scale="RdBu_r",
        range_color=[-0.5, 0.5],
        title="상관계수 히트맵: 건물 특성 vs 시간대별 혼잡도",
    )
    save_plot(fig3, OUTPUT_DIR / "correlation_heatmap.html")

    # ============================================================
    # 5. SUMMARY STATISTICS
    # ============================================================
    logger.info("\n" + "=" * 60)
    logger.info("SUMMARY STATISTICS")
    logger.info("=" * 60)

    # Find peak correlation times
    area_results = df_time_slot_results[
        df_time_slot_results["feature"] == "총 건물 연면적"
    ]
    max_corr = area_results.loc[area_results["pearson_r"].idxmax()]
    min_corr = area_results.loc[area_results["pearson_r"].idxmin()]

    logger.info(f"\nTotal Building Area - Strongest Correlation:")
    logger.info(f"  Time: {max_corr['time_label']}, r={max_corr['pearson_r']:.4f}")
    logger.info(f"\nTotal Building Area - Weakest Correlation:")
    logger.info(f"  Time: {min_corr['time_label']}, r={min_corr['pearson_r']:.4f}")

    # Average correlation by feature
    logger.info("\nAverage Correlation Across All Time Slots:")
    avg_corr = (
        df_time_slot_results.groupby("feature")["pearson_r"]
        .agg(["mean", "std", "min", "max"])
        .round(4)
    )
    logger.info("\n" + str(avg_corr))

    # Save merged data
    df_merged.to_csv(OUTPUT_DIR / "building_congestion_by_timeslot.csv", index=False)
    logger.info(
        f"\n✓ Full data saved to {OUTPUT_DIR / 'building_congestion_by_timeslot.csv'}"
    )

    logger.info("\n" + "=" * 60)
    logger.info("Analysis complete!")
    logger.info("=" * 60)
