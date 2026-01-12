import pandas as pd
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
import logging
import sqlite3
from src.utils.db_util import get_connection
from src.utils.config import PLOTS_DIR, LOG_FORMAT, LOG_LEVEL

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
# 4. Visualization
# ============================================================

fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle(
    "Subway Congestion vs Living Population by Time Slot",
    fontsize=16,
    fontweight="bold",
)

# Plot 1: Congestion and Local Population over time
ax = axes[0, 0]
ax2 = ax.twinx()
line1 = ax.plot(
    merged_df["hour"],
    merged_df["avg_congestion"],
    "b-o",
    linewidth=2,
    label="Avg Congestion Level",
)
line2 = ax2.plot(
    merged_df["hour"],
    merged_df["avg_local_living_pop"],
    "r-s",
    linewidth=2,
    label="Avg Local Population",
)
ax.set_xlabel("Hour of Day")
ax.set_ylabel("Congestion Level", color="b")
ax2.set_ylabel("Living Population", color="r")
ax.tick_params(axis="y", labelcolor="b")
ax2.tick_params(axis="y", labelcolor="r")
ax.set_xticks(range(0, 24, 2))
ax.grid(True, alpha=0.3)
ax.set_title("Time Series: Congestion vs Local Population")
lines = line1 + line2
labels = [l.get_label() for l in lines]
ax.legend(lines, labels, loc="upper left")

# Plot 2: Scatter plot - Congestion vs Local Population
ax = axes[0, 1]
ax.scatter(
    merged_df["avg_local_living_pop"],
    merged_df["avg_congestion"],
    s=100,
    alpha=0.6,
    color="green",
)
z = np.polyfit(merged_df["avg_local_living_pop"], merged_df["avg_congestion"], 1)
p = np.poly1d(z)
ax.plot(
    merged_df["avg_local_living_pop"],
    p(merged_df["avg_local_living_pop"]),
    "g--",
    linewidth=2,
)
ax.set_xlabel("Average Local Living Population")
ax.set_ylabel("Average Congestion Level")
ax.set_title(
    f"Congestion vs Local Population\n(r={correlations['local_pop']['pearson_r']:.3f}, p={correlations['local_pop']['pearson_p']:.3f})"
)
ax.grid(True, alpha=0.3)

# Plot 3: Hourly distribution comparison
ax = axes[1, 0]
x = np.arange(len(merged_df))
width = 0.35
ax_norm = (merged_df["avg_congestion"] - merged_df["avg_congestion"].min()) / (
    merged_df["avg_congestion"].max() - merged_df["avg_congestion"].min()
)
pop_norm = (
    merged_df["avg_local_living_pop"] - merged_df["avg_local_living_pop"].min()
) / (merged_df["avg_local_living_pop"].max() - merged_df["avg_local_living_pop"].min())
ax.bar(x - width / 2, ax_norm, width, label="Congestion (normalized)", alpha=0.8)
ax.bar(
    x + width / 2, pop_norm, width, label="Living Population (normalized)", alpha=0.8
)
ax.set_xlabel("Hour of Day")
ax.set_ylabel("Normalized Value")
ax.set_title("Normalized Congestion vs Population by Hour")
ax.set_xticks(x)
ax.set_xticklabels([f"{int(h):02d}" for h in merged_df["hour"]])
ax.legend()
ax.grid(True, alpha=0.3, axis="y")

# Plot 4: Summary statistics
ax = axes[1, 1]
ax.axis("off")
summary_text = f"""
CORRELATION SUMMARY

Congestion vs Local Population:
  Pearson r:  {correlations["local_pop"]["pearson_r"]:.4f}
  p-value:    {correlations["local_pop"]["pearson_p"]:.4f}
  Spearman ρ: {correlations["local_pop"]["spearman_r"]:.4f}
  p-value:    {correlations["local_pop"]["spearman_p"]:.4f}

Key Findings:
  • Total Hours: {len(merged_df)}
  • Peak Congestion Hour: {merged_df.loc[merged_df["avg_congestion"].idxmax(), "hour"]:.0f}:00
  • Peak Population Hour: {merged_df.loc[merged_df["avg_local_living_pop"].idxmax(), "hour"]:.0f}:00
  • Avg Congestion: {merged_df["avg_congestion"].mean():.2f}
  • Avg Population: {merged_df["avg_local_living_pop"].mean():.2f}

Interpretation:
  {"Strong" if abs(correlations["local_pop"]["pearson_r"]) > 0.7 else "Moderate" if abs(correlations["local_pop"]["pearson_r"]) > 0.4 else "Weak"} correlation detected.
  {"Statistically significant" if correlations["local_pop"]["pearson_p"] < 0.05 else "Not statistically significant"} (p < 0.05)
"""
ax.text(
    0.1,
    0.9,
    summary_text,
    transform=ax.transAxes,
    fontsize=10,
    verticalalignment="top",
    fontfamily="monospace",
    bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.5),
)

plt.tight_layout()
output_dir = OUTPUT_DIR
if (
    not output_dir.exists()
):  # OUTPUT_DIR is config, which is 'output'. But we probably want output/plots?
    # config says OUTPUT_DIR = project/output.
    # PLOTS_DIR = project/output/plots.
    # The original script saved to output/eda_congestion_population_correlation.png directly in output.
    # Let's align with config. Ideally PLOTS_DIR?
    # But let's keep consistency with original path if possible or improve.
    # Original: "output/eda_congestion_population_correlation.png"
    # Let's use OUTPUT_DIR / "eda_congestion_population_correlation.png"
    pass

output_path = OUTPUT_DIR / "eda_congestion_population_correlation.png"
plt.savefig(output_path, dpi=300, bbox_inches="tight")
logger.info(f"\n✓ Visualization saved to: {output_path}")

# ============================================================
# 5. Additional Analysis: Weekday vs Weekend
# ============================================================

logger.info("\n" + "=" * 70)
logger.info("ADDITIONAL ANALYSIS: WEEKDAY VS WEEKEND")
logger.info("=" * 70)

congestion_weekend = (
    congestion_df[congestion_df["is_weekend"] == 1]
    .groupby("hour")["avg_congestion"]
    .mean()
)
congestion_weekday = (
    congestion_df[congestion_df["is_weekend"] == 0]
    .groupby("hour")["avg_congestion"]
    .mean()
)

fig, ax = plt.subplots(figsize=(12, 6))
ax.plot(
    congestion_weekday.index,
    congestion_weekday.values,
    "b-o",
    label="Weekday",
    linewidth=2,
)
ax.plot(
    congestion_weekend.index,
    congestion_weekend.values,
    "r-s",
    label="Weekend",
    linewidth=2,
)
ax.set_xlabel("Hour of Day")
ax.set_ylabel("Average Congestion Level")
ax.set_title("Subway Congestion: Weekday vs Weekend Pattern")
ax.set_xticks(range(0, 24, 2))
ax.legend()
ax.grid(True, alpha=0.3)
plt.tight_layout()
output_path_ww = OUTPUT_DIR / "eda_congestion_weekday_weekend.png"
plt.savefig(output_path_ww, dpi=300, bbox_inches="tight")
logger.info(f"✓ Weekday/Weekend analysis saved to: {output_path_ww}")

conn.close()
logger.info("\n" + "=" * 70)
logger.info("ANALYSIS COMPLETE")
logger.info("=" * 70)
