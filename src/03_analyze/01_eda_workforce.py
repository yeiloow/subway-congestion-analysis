import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
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

    # Group by admin_dong_name to handle duplicates across quarters
    if not df.empty and "admin_dong_name" in df.columns:
        numeric_cols = df.select_dtypes(include=["number"]).columns
        df = df.groupby("admin_dong_name")[numeric_cols].mean().reset_index()

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
font_family = "Malgun Gothic"  # Default for Windows
if system_name == "Darwin":  # Mac
    font_family = "AppleGothic"
elif system_name == "Windows":
    font_family = "Malgun Gothic"

plt.rcParams["font.family"] = font_family
plt.rcParams["axes.unicode_minus"] = False
sns.set(font=font_family, rc={"axes.unicode_minus": False})

# Summary File
summary_file = EDA_OUTPUT_DIR / "eda_summary.md"

with open(summary_file, "w", encoding="utf-8") as f:
    f.write("# 직장인구 데이터 분석 보고서 (Workplace Population)\n\n")

    # 2. Missing Values & Outliers
    f.write("## 1. 결측치 확인\n")
    missing = df.isnull().sum()
    if missing.sum() == 0:
        f.write("결측치가 발견되지 않았습니다.\n")
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
            f"## 이상치 (총 직장인구)\n1.5*IQR 규칙에 따라 {len(outliers)} 개의 이상치가 발견되었습니다.\n"
        )
        if not outliers.empty:
            f.write("상위 5개 이상치 지역 (총 직장인구 기준):\n")
            f.write(
                outliers.sort_values("total_pop", ascending=False)[
                    ["admin_dong_name", "total_pop"]
                ]
                .head(5)
                .to_string(index=False)
            )
        f.write("\n\n")

    # Top 5 Populous Dongs
    if "total_pop" in df.columns:
        f.write("## 2. 직장인구 상위 5개 지역\n")
        top5 = df.sort_values("total_pop", ascending=False)[
            ["admin_dong_name", "total_pop"]
        ].head(5)
        f.write(top5.to_string(index=False))
        f.write("\n\n")

    # Top 5 Lowest Populous Dongs
    if "total_pop" in df.columns:
        f.write("## 3. 직장인구 하위 5개 지역\n")
        bottom5 = df.sort_values("total_pop", ascending=True)[
            ["admin_dong_name", "total_pop"]
        ].head(5)
        f.write(bottom5.to_string(index=False))
        f.write("\n\n")

    # 3. Descriptive Statistics
    f.write("## 4. 기술 통계량 (요약)\n")
    f.write(df.describe().to_string())
    f.write("\n\n")

# -- Visualizations --
logger.info("Generating visualizations...")

# 1. Top 10 Dongs by Total Population
if "total_pop" in df.columns:
    plt.figure(figsize=(12, 6))
    top10 = df.sort_values("total_pop", ascending=False).head(10)
    sns.barplot(data=top10, x="total_pop", y="admin_dong_name", palette="viridis")
    plt.title("직장인구 상위 10개 행정동")
    plt.xlabel("총 직장인구 수")
    plt.ylabel("행정동")
    plt.tight_layout()
    plt.savefig(EDA_OUTPUT_DIR / "top10_dongs.png")
    plt.close()

# 2. Distribution of Total Population
if "total_pop" in df.columns:
    plt.figure(figsize=(10, 6))
    sns.histplot(df["total_pop"], kde=True, bins=30)
    plt.title("행정동별 총 직장인구 분포")
    plt.xlabel("인구 수")
    plt.ylabel("빈도 (행정동 수)")
    plt.savefig(EDA_OUTPUT_DIR / "dist_total_pop.png")
    plt.close()

# 3. Population Pyramid (Age & Gender)
# Aggregating across all dongs
age_cols_male = [
    c for c in df.columns if "male" in c and "female" not in c and "age" in c
]
age_cols_female = [c for c in df.columns if "female" in c and "age" in c]

if age_cols_male and age_cols_female:
    # Summing up
    total_male = df[age_cols_male].sum()
    total_female = df[age_cols_female].sum()

    # Create summary DF for plotting
    # Assuming columns like male_age_10_pop, male_age_20_pop...
    # Extract age label '10대', '20대' etc.
    def extract_age_label(col_name):
        parts = col_name.split("_")
        for p in parts:
            if p.isdigit():  # 10, 20 ..
                return f"{p}대"
            if p == "over":  # 60_over
                return "60대 이상"
        return "기타"

    age_labels = [extract_age_label(c) for c in age_cols_male]

    # Create DataFrame
    pyramid_df = pd.DataFrame(
        {"Age": age_labels, "Male": total_male.values, "Female": total_female.values}
    )

    # Population Pyramid Plot
    fig, ax1 = plt.subplots(figsize=(10, 6))

    bar_plot = sns.barplot(
        x="Male", y="Age", data=pyramid_df, color="skyblue", label="남성"
    )
    bar_plot = sns.barplot(
        x=pyramid_df["Female"] * -1,
        y="Age",
        data=pyramid_df,
        color="lightpink",
        label="여성",
    )

    ax1.set_xlabel("인구 수 (여성 <-> 남성)")
    ax1.set_ylabel("연령대")
    ax1.set_title("전체 직장인구 인구 피라미드")

    # Format x-axis labels to be positive
    ticks = ax1.get_xticks()
    ax1.set_xticklabels([f"{int(abs(x))}" for x in ticks])

    plt.legend()
    plt.savefig(EDA_OUTPUT_DIR / "population_pyramid.png")
    plt.close()

# 4. Gender Ratio Pie Chart
if "male_pop" in df.columns and "female_pop" in df.columns:
    total_male_all = df["male_pop"].sum()
    total_female_all = df["female_pop"].sum()

    plt.figure(figsize=(6, 6))
    plt.pie(
        [total_male_all, total_female_all],
        labels=["남성", "여성"],
        autopct="%1.1f%%",
        colors=["skyblue", "lightpink"],
        startangle=90,
    )
    plt.title("전체 직장인구 성별 비율")
    plt.savefig(EDA_OUTPUT_DIR / "gender_ratio_pie.png")
    plt.close()

# 5. Correlation Heatmap
numeric_df = df.select_dtypes(include=["number"])
if "id" in numeric_df.columns:
    numeric_df = numeric_df.drop(columns=["id"])
if "quarter_code" in numeric_df.columns:
    numeric_df = numeric_df.drop(columns=["quarter_code"])
if "admin_dong_code" in numeric_df.columns:
    numeric_df = numeric_df.drop(columns=["admin_dong_code"])

if not numeric_df.empty:
    plt.figure(figsize=(12, 10))
    # Simplify: Only correlate Total and Major Summaries to avoid 40x40 grid mess
    # Identifying key summary columns
    key_cols = ["total_pop", "male_pop", "female_pop"] + [
        c for c in df.columns if "age_30" in c
    ]  # Focus on 30s as proxy for core workforce

    # Filter numeric_df to existing key_cols
    subset_cols = [c for c in key_cols if c in numeric_df.columns]

    if len(subset_cols) > 1:
        corr_df = numeric_df[subset_cols]
    else:
        corr_df = numeric_df  # Fallback to all if subset too small

    sns.heatmap(corr_df.corr(), cmap="RdBu_r", center=0, annot=True, fmt=".2f")
    plt.title("주요 변수 간 상관관계 히트맵")
    plt.tight_layout()
    plt.savefig(EDA_OUTPUT_DIR / "correlation_heatmap.png")
    plt.close()

logger.info(f"EDA completed. Summary saved to {summary_file}")
