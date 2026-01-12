#!/usr/bin/env python3
"""
Correlation analysis between building near subway and subway congestion
Analysis by time slot (not averaged)
"""

import pandas as pd
import numpy as np
import sqlite3
import matplotlib.pyplot as plt
from scipy import stats
import logging
import platform
from src.utils.db_util import get_connection
from src.utils.config import OUTPUT_DIR, LOG_FORMAT, LOG_LEVEL

# Configure Logging
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

# Paths
csv_path = OUTPUT_DIR / "station_catchment_stats.csv"

# Set Korean Font
system_name = platform.system()
if system_name == "Darwin":  # Mac
    font_family = "AppleGothic"
elif system_name == "Windows":
    font_family = "Malgun Gothic"
else:
    font_family = "Malgun Gothic"

plt.rcParams["font.family"] = font_family
plt.rcParams["axes.unicode_minus"] = False


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
logger.info(f"\nSample building data:\n{df_buildings.head(10)}")

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
logger.info(f"Unique time slots: {sorted(df_congestion['time_slot'].unique())}")
logger.info(f"\nSample congestion data:\n{df_congestion.head(10)}")

# Aggregate building data by station and line
logger.info("\nAggregating building data by station and line...")
df_buildings_agg = (
    df_buildings.groupby(["station_id", "station_name", "line_name"])
    .agg({"total_area": "sum", "total_households": "sum", "total_families": "sum"})
    .reset_index()
)

logger.info(f"Aggregated building data shape: {df_buildings_agg.shape}")
logger.info(df_buildings_agg.head(10))

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
logger.info(df_merged.head(10))

if df_merged.shape[0] == 0:
    logger.error("ERROR: No matching data found between buildings and congestion!")
    logger.error(
        f"\nStation IDs in buildings data: {df_buildings_agg['station_id'].unique()[:20]}"
    )
    logger.error(
        f"Station IDs in congestion data: {df_congestion['station_id'].unique()[:20]}"
    )
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
    # 2. CORRELATION BY WEEKDAY VS WEEKEND
    # ============================================================
    logger.info("\n" + "=" * 60)
    logger.info("CORRELATION ANALYSIS: WEEKDAY VS WEEKEND")
    logger.info("=" * 60)

    weekday_weekend_results = []
    for is_weekend in [0, 1]:
        day_type = "주말" if is_weekend else "평일"
        day_data = df_merged[df_merged["is_weekend"] == is_weekend]

        for feature, label in features_to_analyze.items():
            if feature in day_data.columns:
                valid_data = day_data[[feature, "congestion_level"]].dropna()

                if len(valid_data) > 2:
                    pearson_r, pearson_p = stats.pearsonr(
                        valid_data[feature], valid_data["congestion_level"]
                    )

                    weekday_weekend_results.append(
                        {
                            "day_type": day_type,
                            "feature": label,
                            "pearson_r": pearson_r,
                            "pearson_p": pearson_p,
                            "n_samples": len(valid_data),
                        }
                    )

                    logger.info(
                        f"  {day_type} - {label}: r={pearson_r:.4f}, p={pearson_p:.4f}"
                    )

    df_weekday_weekend = pd.DataFrame(weekday_weekend_results)
    df_weekday_weekend.to_csv(
        OUTPUT_DIR / "correlation_weekday_weekend.csv", index=False
    )

    # ============================================================
    # 3. CORRELATION BY DIRECTION (UPLINE VS DOWNLINE)
    # ============================================================
    logger.info("\n" + "=" * 60)
    logger.info("CORRELATION ANALYSIS: UPLINE VS DOWNLINE")
    logger.info("=" * 60)

    direction_results = []
    for is_upline in [0, 1]:
        direction = "상행" if is_upline else "하행"
        dir_data = df_merged[df_merged["is_upline"] == is_upline]

        for feature, label in features_to_analyze.items():
            if feature in dir_data.columns:
                valid_data = dir_data[[feature, "congestion_level"]].dropna()

                if len(valid_data) > 2:
                    pearson_r, pearson_p = stats.pearsonr(
                        valid_data[feature], valid_data["congestion_level"]
                    )

                    direction_results.append(
                        {
                            "direction": direction,
                            "feature": label,
                            "pearson_r": pearson_r,
                            "pearson_p": pearson_p,
                            "n_samples": len(valid_data),
                        }
                    )

                    logger.info(
                        f"  {direction} - {label}: r={pearson_r:.4f}, p={pearson_p:.4f}"
                    )

    df_direction = pd.DataFrame(direction_results)
    df_direction.to_csv(OUTPUT_DIR / "correlation_by_direction.csv", index=False)

    # ============================================================
    # 4. VISUALIZATIONS
    # ============================================================
    logger.info("\nGenerating visualizations...")

    # Plot 1: Correlation by time of day
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle(
        "시간대별 상관관계 (건물 특성 vs 혼잡도)",
        fontsize=16,
        fontweight="bold",
    )

    for idx, (feature, label) in enumerate(features_to_analyze.items()):
        row = idx // 2
        col = idx % 2
        ax = axes[row, col]

        feature_data = df_time_slot_results[df_time_slot_results["feature"] == label]

        ax.plot(
            feature_data["time_label"],
            feature_data["pearson_r"],
            marker="o",
            linewidth=2,
            markersize=6,
        )
        ax.axhline(y=0, color="gray", linestyle="--", alpha=0.5)

        # Highlight significant correlations
        sig_mask = feature_data["pearson_p"] < 0.05
        ax.scatter(
            feature_data[sig_mask]["time_label"],
            feature_data[sig_mask]["pearson_r"],
            color="red",
            s=100,
            zorder=5,
            label="p < 0.05",
        )

        ax.set_xlabel("시간대", fontsize=10)
        ax.set_ylabel("피어슨 상관계수 (r)", fontsize=10)
        ax.set_title(label, fontsize=12)
        ax.tick_params(axis="x", rotation=45)
        ax.grid(True, alpha=0.3)
        ax.legend(loc="upper right")

    plt.tight_layout()
    plt.savefig(
        OUTPUT_DIR / "correlation_by_time_slot.png", dpi=300, bbox_inches="tight"
    )
    logger.info(
        f"✓ Time slot visualization saved to {OUTPUT_DIR / 'correlation_by_time_slot.png'}"
    )

    # Plot 2: Scatter plots for peak hours vs off-peak
    fig, axes = plt.subplots(2, 2, figsize=(14, 12))
    fig.suptitle(
        "건물 연면적 vs 혼잡도: 피크타임 vs 비-피크타임",
        fontsize=16,
        fontweight="bold",
    )

    # Define peak hours (morning rush 7-9, evening rush 18-20)
    morning_peak = df_merged[df_merged["time_slot"].between(4, 8)]  # ~07:00-09:00
    evening_peak = df_merged[df_merged["time_slot"].between(26, 30)]  # ~18:00-20:00
    off_peak = df_merged[df_merged["time_slot"].between(12, 20)]  # ~11:00-15:00
    late_night = df_merged[df_merged["time_slot"] >= 35]  # ~23:00+

    periods = [
        (morning_peak, "오전 피크 (07:00-09:00)"),
        (evening_peak, "오후 피크 (18:00-20:00)"),
        (off_peak, "비-피크 (11:00-15:00)"),
        (late_night, "심야 (23:00+)"),
    ]

    for idx, (period_data, period_name) in enumerate(periods):
        row = idx // 2
        col = idx % 2
        ax = axes[row, col]

        if len(period_data) > 0:
            valid_data = period_data[["total_area", "congestion_level"]].dropna()

            ax.scatter(
                valid_data["total_area"],
                valid_data["congestion_level"],
                alpha=0.3,
                s=20,
            )

            if len(valid_data) > 2:
                z = np.polyfit(
                    valid_data["total_area"], valid_data["congestion_level"], 1
                )
                p = np.poly1d(z)
                x_line = np.linspace(
                    valid_data["total_area"].min(), valid_data["total_area"].max(), 100
                )
                ax.plot(x_line, p(x_line), "r--", alpha=0.8, linewidth=2)

                pearson_r, pearson_p = stats.pearsonr(
                    valid_data["total_area"], valid_data["congestion_level"]
                )
                sig = (
                    "***"
                    if pearson_p < 0.001
                    else "**"
                    if pearson_p < 0.01
                    else "*"
                    if pearson_p < 0.05
                    else "ns"
                )
                ax.text(
                    0.05,
                    0.95,
                    f"r = {pearson_r:.3f} ({sig})\nn = {len(valid_data)}",
                    transform=ax.transAxes,
                    fontsize=11,
                    verticalalignment="top",
                    bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.5),
                )

        ax.set_xlabel("총 건물 연면적 (m²)", fontsize=10)
        ax.set_ylabel("혼잡도", fontsize=10)
        ax.set_title(period_name, fontsize=12)
        ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(
        OUTPUT_DIR / "correlation_peak_offpeak.png", dpi=300, bbox_inches="tight"
    )
    logger.info(
        f"✓ Peak/off-peak visualization saved to {OUTPUT_DIR / 'correlation_peak_offpeak.png'}"
    )

    # Plot 3: Heatmap of correlations by time slot and feature
    pivot_data = df_time_slot_results.pivot(
        index="time_label", columns="feature", values="pearson_r"
    )

    fig, ax = plt.subplots(figsize=(12, 14))
    im = ax.imshow(pivot_data.values, aspect="auto", cmap="RdBu_r", vmin=-0.5, vmax=0.5)

    ax.set_xticks(range(len(pivot_data.columns)))
    ax.set_xticklabels(pivot_data.columns, rotation=45, ha="right")
    ax.set_yticks(range(len(pivot_data.index)))
    ax.set_yticklabels(pivot_data.index)

    plt.colorbar(im, ax=ax, label="피어슨 상관계수 (r)")
    ax.set_title(
        "상관계수 히트맵: 건물 특성 vs 시간대별 혼잡도",
        fontsize=14,
        fontweight="bold",
    )
    ax.set_xlabel("건물 특성")
    ax.set_ylabel("시간대")

    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "correlation_heatmap.png", dpi=300, bbox_inches="tight")
    logger.info(f"✓ Heatmap saved to {OUTPUT_DIR / 'correlation_heatmap.png'}")

    # ============================================================
    # 5. SUMMARY STATISTICS
    # ============================================================
    logger.info("\n" + "=" * 60)
    logger.info("SUMMARY STATISTICS")
    logger.info("=" * 60)

    # Find peak correlation times
    # Using localized name
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
