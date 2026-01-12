import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
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
EDA_OUTPUT_DIR = OUTPUT_DIR / "eda_workforce"

# Create output directory
EDA_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# 1. Load Data
logger.info("Connecting to database...")
try:
    conn = get_connection()
except Exception as e:
    logger.error(f"Failed to connect to DB: {e}")
    sys.exit(1)

query = "SELECT * FROM Dong_Workplace_Population"
try:
    df = pd.read_sql(query, conn)
except Exception as e:
    logger.error(f"Error reading from database: {e}")
    conn.close()
    sys.exit(1)
conn.close()

if df.empty:
    logger.warning("Warning: The table Dong_Workplace_Population is empty.")
    # Proceeding might fail, but let's try to handle gracefully
    sys.exit(0)

# Set Korean Font
system_name = platform.system()
if system_name == "Darwin":  # Mac
    plt.rcParams["font.family"] = "AppleGothic"
elif system_name == "Windows":
    plt.rcParams["font.family"] = "Malgun Gothic"
else:
    # Linux/Other - fallback properties
    pass
plt.rcParams["axes.unicode_minus"] = False

# Summary File
summary_file = EDA_OUTPUT_DIR / "eda_summary.txt"

with open(summary_file, "w") as f:
    f.write("# EDA Report: Dong Workplace Population\n\n")

    # 2. Missing Values & Outliers
    f.write("## 1. Missing Values\n")
    missing = df.isnull().sum()
    if missing.sum() == 0:
        f.write("No missing values found.\n")
    else:
        f.write(missing[missing > 0].to_string())
        f.write("\n")
    f.write("\n")

    # Outlier Detection (Total Pop)
    # Using IQR
    if "total_pop" in df.columns:
        Q1 = df["total_pop"].quantile(0.25)
        Q3 = df["total_pop"].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        outliers = df[(df["total_pop"] < lower_bound) | (df["total_pop"] > upper_bound)]
        f.write(
            f"## Outliers (Total Pop)\nFound {len(outliers)} outliers (using 1.5*IQR rule).\n"
        )
        if not outliers.empty:
            f.write(f"Top 5 outliers (by total_pop):\n")
            f.write(
                outliers.sort_values("total_pop", ascending=False)[
                    ["admin_dong_name", "total_pop"]
                ]
                .head(5)
                .to_string()
            )
        f.write("\n\n")

    # 3. Descriptive Statistics
    f.write("## 2. Descriptive Statistics\n")
    f.write(df.describe().to_string())
    f.write("\n\n")

    # 4. Correlation Analysis
    f.write("## 3. Correlation Analysis\n")
    numeric_df = df.select_dtypes(include=["number"])
    # Remove 'id' if present
    if "id" in numeric_df.columns:
        numeric_df = numeric_df.drop(columns=["id"])

    corr = numeric_df.corr()
    # Find high correlations with Total Pop (excluding itself and its components like male_pop if obvious)
    # Let's just list correlations with total_pop
    if "total_pop" in corr.columns:
        corr_with_total = corr["total_pop"].sort_values(ascending=False)
        f.write("Correlations with Total Pop:\n")
        f.write(corr_with_total.to_string())
    f.write("\n\n")

# -- Visualizations --
logger.info("Generating visualizations...")

# 1. Missing Pattern (Visual) - Optional, but simple heatmap is good if many missing
# Skip if no missing

# 2. Distribution of Total Population
if "total_pop" in df.columns:
    plt.figure(figsize=(10, 6))
    sns.histplot(df["total_pop"], kde=True, bins=30)
    plt.title("Distribution of Total Workplace Population by Dong")
    plt.xlabel("Population")
    plt.ylabel("Frequency")
    plt.savefig(EDA_OUTPUT_DIR / "dist_total_pop.png")
    plt.close()

    # 3. Boxplot for Outliers
    plt.figure(figsize=(8, 6))
    sns.boxplot(y=df["total_pop"])
    plt.title("Boxplot of Total Workplace Population")
    plt.savefig(EDA_OUTPUT_DIR / "boxplot_total_pop.png")
    plt.close()

    # 6. Scatter: Total Pop vs Age 30 (Key Workforce)
    if "age_30_pop" in df.columns:
        plt.figure(figsize=(10, 6))
        sns.scatterplot(data=df, x="total_pop", y="age_30_pop", alpha=0.6)
        plt.title("Total Population vs Age 30s Population")
        plt.xlabel("Total Population")
        plt.ylabel("Age 30s Population")
        # Add regression line
        try:
            sns.regplot(
                data=df, x="total_pop", y="age_30_pop", scatter=False, color="red"
            )
        except Exception as e:
            logger.warning(f"Could not plot regression line: {e}")
        plt.savefig(EDA_OUTPUT_DIR / "scatter_total_age30.png")
        plt.close()

# 4. Correlation Heatmap
if not numeric_df.empty:
    plt.figure(figsize=(12, 10))
    sns.heatmap(
        numeric_df.corr(), cmap="RdBu_r", center=0, annot=False
    )  # Annot false if too many vars
    plt.title("Correlation Heatmap of Variables")
    plt.tight_layout()
    plt.savefig(EDA_OUTPUT_DIR / "correlation_heatmap.png")
    plt.close()

# 5. Age Group Distribution (Summed across all dongs)
age_cols = [
    "age_10_pop",
    "age_20_pop",
    "age_30_pop",
    "age_40_pop",
    "age_50_pop",
    "age_60_over_pop",
]
# Ensure cols exist
age_cols = [c for c in age_cols if c in df.columns]
if age_cols:
    # Sum each column
    age_sums = df[age_cols].sum()
    plt.figure(figsize=(10, 6))
    age_sums.plot(kind="bar", color="skyblue")
    plt.title("Total Workforce Population by Age Group (All Dongs)")
    plt.xlabel("Age Group")
    plt.ylabel("Count")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(EDA_OUTPUT_DIR / "age_distribution.png")
    plt.close()

logger.info(f"EDA completed. Summary saved to {summary_file}")
