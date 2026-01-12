import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
import os
import sys
import platform
import logging
from src.utils.db_util import get_connection
from src.utils.config import OUTPUT_DIR, LOG_FORMAT, LOG_LEVEL

# Configure Logging
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

# Configuration
EDA_OUTPUT_DIR = OUTPUT_DIR / "eda_revenue"

# Create output directory
EDA_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# 1. Load Data
logger.info("Connecting to database...")
try:
    conn = get_connection()
except Exception as e:
    logger.error(f"Failed to connect to DB: {e}")
    sys.exit(1)

query = "SELECT * FROM Dong_Estimated_Revenue"
try:
    df = pd.read_sql(query, conn)
except Exception as e:
    logger.error(f"Error reading from database: {e}")
    conn.close()
    sys.exit(1)
conn.close()

if df.empty:
    logger.warning("Warning: The table Dong_Estimated_Revenue is empty.")
    sys.exit(0)

# Set Korean Font
system_name = platform.system()
if system_name == "Darwin":  # Mac
    plt.rcParams["font.family"] = "AppleGothic"
elif system_name == "Windows":
    plt.rcParams["font.family"] = "Malgun Gothic"
plt.rcParams["axes.unicode_minus"] = False

# Summary File
summary_file = EDA_OUTPUT_DIR / "eda_summary.txt"

with open(summary_file, "w") as f:
    f.write("# EDA Report: Dong Estimated Revenue\n\n")

    # 2. Missing Values & Basic Info
    f.write("## 1. Basic Info & Missing Values\n")
    f.write(f"Total Rows: {len(df)}\n")
    f.write(f"Total Columns: {len(df.columns)}\n\n")

    missing = df.isnull().sum()
    if missing.sum() == 0:
        f.write("No missing values found.\n")
    else:
        f.write("Missing Values:\n")
        f.write(missing[missing > 0].to_string())
        f.write("\n")
    f.write("\n")

    # 3. Descriptive Statistics
    f.write("## 2. Descriptive Statistics (Sales Amount & Count)\n")
    desc_cols = [
        "month_sales_amt",
        "month_sales_cnt",
        "weekday_sales_amt",
        "weekend_sales_amt",
    ]
    # Filter only existing columns
    desc_cols = [c for c in desc_cols if c in df.columns]

    if desc_cols:
        f.write(df[desc_cols].describe().to_string())
    f.write("\n\n")

    # 4. Correlation Analysis
    f.write("## 3. Correlation Analysis\n")
    numeric_df = df.select_dtypes(include=["number"])
    if "id" in numeric_df.columns:
        numeric_df = numeric_df.drop(columns=["id"])

    corr = numeric_df.corr()
    if "month_sales_amt" in corr.columns:
        corr_with_sales = corr["month_sales_amt"].sort_values(ascending=False).head(10)
        f.write("Top 10 Correlations with Monthly Sales Amount:\n")
        f.write(corr_with_sales.to_string())
    f.write("\n\n")

# -- Visualizations --
logger.info("Generating visualizations...")

# 1. Distribution of Monthly Sales Amount
if "month_sales_amt" in df.columns:
    plt.figure(figsize=(10, 6))
    sns.histplot(df["month_sales_amt"], kde=True, bins=50)
    plt.title("Distribution of Monthly Sales Amount")
    plt.xlabel("Sales Amount")
    plt.ylabel("Frequency")
    plt.savefig(EDA_OUTPUT_DIR / "dist_month_sales_amt.png")
    plt.close()

# 2. Boxplot of Sales by Service Type (Top 10 types by total sales if too many)
if "service_type_name" in df.columns and "month_sales_amt" in df.columns:
    top_services = (
        df.groupby("service_type_name")["month_sales_amt"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
        .index
    )
    plt.figure(figsize=(12, 6))
    sns.boxplot(
        data=df[df["service_type_name"].isin(top_services)],
        x="service_type_name",
        y="month_sales_amt",
    )
    plt.title("Monthly Sales Distribution by Top 10 Service Types")
    plt.xticks(rotation=45)
    plt.xlabel("Service Type")
    plt.ylabel("Monthly Sales Amount")
    plt.tight_layout()
    plt.savefig(EDA_OUTPUT_DIR / "boxplot_sales_by_service.png")
    plt.close()

# 3. Correlation Heatmap
# Select key columns for cleaner heatmap
key_cols = [
    "month_sales_amt",
    "month_sales_cnt",
    "weekday_sales_amt",
    "weekend_sales_amt",
    "male_sales_amt",
    "female_sales_amt",
    "age_20_sales_amt",
    "age_30_sales_amt",
    "age_40_sales_amt",
    "age_50_sales_amt",
]
existing_key_cols = [c for c in key_cols if c in numeric_df.columns]
if existing_key_cols:
    plt.figure(figsize=(10, 8))
    sns.heatmap(
        numeric_df[existing_key_cols].corr(),
        cmap="RdBu_r",
        center=0,
        annot=True,
        fmt=".2f",
    )
    plt.title("Correlation Heatmap of Key Sales Variables")
    plt.tight_layout()
    plt.savefig(EDA_OUTPUT_DIR / "correlation_heatmap.png")
    plt.close()

# 4. Scatter Plot: Sales Amount vs Sales Count
if "month_sales_cnt" in df.columns and "month_sales_amt" in df.columns:
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x="month_sales_cnt", y="month_sales_amt", alpha=0.5)
    plt.title("Monthly Sales Amount vs Sales Count")
    plt.xlabel("Sales Count")
    plt.ylabel("Sales Amount")
    plt.savefig(EDA_OUTPUT_DIR / "scatter_sales_amt_vs_cnt.png")
    plt.close()

# 5. Day of Week Sales Analysis
day_cols = [
    "mon_sales_amt",
    "tue_sales_amt",
    "wed_sales_amt",
    "thu_sales_amt",
    "fri_sales_amt",
    "sat_sales_amt",
    "sun_sales_amt",
]
existing_day_cols = [c for c in day_cols if c in df.columns]
if existing_day_cols:
    day_sums = df[existing_day_cols].sum()
    # Rename index for cleaner labels
    labels = [c.split("_")[0].upper() for c in existing_day_cols]

    plt.figure(figsize=(10, 6))
    day_sums.plot(kind="bar", color="skyblue")
    plt.title("Total Sales by Day of Week")
    plt.xlabel("Day")
    plt.ylabel("Total Sales Amount")
    plt.xticks(range(len(labels)), labels, rotation=0)
    plt.savefig(EDA_OUTPUT_DIR / "bar_sales_by_day.png")
    plt.close()

# 6. Age Group Sales Analysis
age_cols = [
    "age_10_sales_amt",
    "age_20_sales_amt",
    "age_30_sales_amt",
    "age_40_sales_amt",
    "age_50_sales_amt",
    "age_60_over_sales_amt",
]
existing_age_cols = [c for c in age_cols if c in df.columns]
if existing_age_cols:
    age_sums = df[existing_age_cols].sum()
    labels = [
        c.replace("_sales_amt", "").replace("age_", "") for c in existing_age_cols
    ]

    plt.figure(figsize=(10, 6))
    age_sums.plot(kind="bar", color="lightgreen")
    plt.title("Total Sales by Age Group")
    plt.xlabel("Age Group")
    plt.ylabel("Total Sales Amount")
    plt.xticks(range(len(labels)), labels, rotation=0)
    plt.savefig(EDA_OUTPUT_DIR / "bar_sales_by_age.png")
    plt.close()

logger.info(f"EDA completed. Summary saved to {summary_file}")
