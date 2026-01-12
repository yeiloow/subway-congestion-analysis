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
    font_family = "AppleGothic"
elif system_name == "Windows":
    font_family = "Malgun Gothic"
else:
    font_family = "Malgun Gothic"  # Default fallback

plt.rcParams["font.family"] = font_family
plt.rcParams["axes.unicode_minus"] = False
sns.set(font=font_family, rc={"axes.unicode_minus": False})

# Summary File
summary_file = EDA_OUTPUT_DIR / "eda_summary.md"

with open(summary_file, "w", encoding="utf-8") as f:
    f.write("# 상권 추정 매출 데이터 분석 보고서 (Estimated Revenue)\n\n")

    # 2. Missing Values & Basic Info
    f.write("## 1. 기본 정보 및 결측치\n")
    f.write(f"총 행 수: {len(df)}\n")
    f.write(f"총 열 수: {len(df.columns)}\n\n")

    missing = df.isnull().sum()
    if missing.sum() == 0:
        f.write("결측치가 발견되지 않았습니다.\n")
    else:
        f.write("결측치 현황:\n")
        f.write(missing[missing > 0].to_string())
        f.write("\n")
    f.write("\n")

    # 3. Descriptive Statistics
    f.write("## 2. 기술 통계량 (매출 금액 및 건수)\n")
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
    f.write("## 3. 상관관계 분석\n")
    numeric_df = df.select_dtypes(include=["number"])
    if "id" in numeric_df.columns:
        numeric_df = numeric_df.drop(columns=["id"])

    corr = numeric_df.corr()
    if "month_sales_amt" in corr.columns:
        corr_with_sales = corr["month_sales_amt"].sort_values(ascending=False).head(10)
        f.write("월 매출 금액과 상관관계가 높은 상위 10개 변수:\n")
        f.write(corr_with_sales.to_string())
    f.write("\n\n")

# -- Visualizations --
logger.info("Generating visualizations...")

# 1. Distribution of Monthly Sales Amount
if "month_sales_amt" in df.columns:
    plt.figure(figsize=(10, 6))
    sns.histplot(df["month_sales_amt"], kde=True, bins=50)
    plt.title("월 매출 금액 분포")
    plt.xlabel("매출 금액")
    plt.ylabel("빈도")
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
    plt.title("상위 10개 업종별 월 매출 분포")
    plt.xticks(rotation=45)
    plt.xlabel("업종명")
    plt.ylabel("월 매출 금액")
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
    plt.title("주요 매출 변수 간 상관관계 히트맵")
    plt.tight_layout()
    plt.savefig(EDA_OUTPUT_DIR / "correlation_heatmap.png")
    plt.close()

# 4. Scatter Plot: Sales Amount vs Sales Count
if "month_sales_cnt" in df.columns and "month_sales_amt" in df.columns:
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x="month_sales_cnt", y="month_sales_amt", alpha=0.5)
    plt.title("월 매출 건수 vs 매출 금액")
    plt.xlabel("매출 건수")
    plt.ylabel("매출 금액")
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
    # mon_sales_amt -> 월요일
    day_map = {
        "mon_sales_amt": "월요일",
        "tue_sales_amt": "화요일",
        "wed_sales_amt": "수요일",
        "thu_sales_amt": "목요일",
        "fri_sales_amt": "금요일",
        "sat_sales_amt": "토요일",
        "sun_sales_amt": "일요일",
    }
    labels = [day_map.get(c, c) for c in existing_day_cols]

    plt.figure(figsize=(10, 6))
    day_sums.plot(kind="bar", color="skyblue")
    plt.title("요일별 총 매출 금액")
    plt.xlabel("요일")
    plt.ylabel("총 매출 금액")
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
        c.replace("_sales_amt", "").replace("age_", "") + "대"
        for c in existing_age_cols
    ]
    # fix 60_over -> 60대 이상
    labels = [l.replace("60_over대", "60대 이상") for l in labels]

    plt.figure(figsize=(10, 6))
    age_sums.plot(kind="bar", color="lightgreen")
    plt.title("연령대별 총 매출 금액")
    plt.xlabel("연령대")
    plt.ylabel("총 매출 금액")
    plt.xticks(range(len(labels)), labels, rotation=0)
    plt.savefig(EDA_OUTPUT_DIR / "bar_sales_by_age.png")
    plt.close()

logger.info(f"EDA completed. Summary saved to {summary_file}")
