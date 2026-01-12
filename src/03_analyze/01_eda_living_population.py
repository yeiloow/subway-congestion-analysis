import pandas as pd
import sys
import os
import plotly.express as px
import plotly.graph_objects as go
import logging
from plotly.subplots import make_subplots
from src.utils.db_util import get_engine, get_connection
from src.utils.config import OUTPUT_DIR, LOG_FORMAT, LOG_LEVEL

# Configure Logging
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

# Configuration
EDA_OUTPUT_DIR = OUTPUT_DIR / "eda_living_population"
EDA_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# 1. Load Data
logger.info("Connecting to database...")
try:
    engine = get_engine()

    # Check if we can get names
    # We'll use a raw connection to get names first or use pandas read_sql
    conn = get_connection()
    map_query = "SELECT DISTINCT admin_dong_code, admin_dong_name FROM Dong_Workplace_Population"
    try:
        df_map = pd.read_sql(map_query, conn)
    except Exception:
        logger.warning("Could not fetch Dong mapping. Using codes only.")
        df_map = pd.DataFrame()
    conn.close()

    query = "SELECT * FROM Dong_Living_Population"
    df = pd.read_sql(query, engine)

except Exception as e:
    logger.error(f"Error reading from database: {e}")
    sys.exit(1)

if df.empty:
    logger.warning("Warning: The table Dong_Living_Population is empty.")
    sys.exit(0)

# Merge Names if available
if not df_map.empty:
    df = pd.merge(df, df_map, on="admin_dong_code", how="left")
    df["admin_dong_name"] = df["admin_dong_name"].fillna(df["admin_dong_code"])
else:
    df["admin_dong_name"] = df["admin_dong_code"]

# Preprocessing
df.fillna(0, inplace=True)
df["total_long_term"] = (
    df["long_term_chinese_stay_pop"] + df["long_term_non_chinese_stay_pop"]
)
df["total_short_term"] = (
    df["short_term_chinese_stay_pop"] + df["short_term_non_chinese_stay_pop"]
)
df["total_living_pop"] = (
    df["local_total_living_pop"] + df["total_long_term"] + df["total_short_term"]
)

# Convert time_slot to numeric for sorting/plotting logic if needed
if "time_slot" in df.columns:
    df["time_slot"] = pd.to_numeric(df["time_slot"], errors="coerce")

# Summary File
summary_file = EDA_OUTPUT_DIR / "eda_summary.md"

with open(summary_file, "w", encoding="utf-8") as f:
    f.write("# 생활인구 데이터 분석 보고서 (Living Population)\n\n")

    # 2. Basic Info
    f.write("## 1. 기본 정보\n")
    f.write(f"총 행 수: {len(df)}\n")
    f.write(f"총 열 수: {len(df.columns)}\n")
    f.write(f"고유 행정동 수: {df['admin_dong_code'].nunique()}\n")
    f.write(f"데이터 기간: {df['base_date'].min()} ~ {df['base_date'].max()}\n\n")

    # 3. Aggregated Metrics
    f.write("## 2. 인구 구성 (평균)\n")
    mean_pops = df[
        [
            "local_total_living_pop",
            "total_long_term",
            "total_short_term",
            "total_living_pop",
        ]
    ].mean()
    f.write(mean_pops.to_string())
    f.write("\n\n")

    # Outlier Analysis (using Mean Total Pop per Dong)
    dong_summary = (
        df.groupby("admin_dong_name")["total_living_pop"].mean().reset_index()
    )
    Q1 = dong_summary["total_living_pop"].quantile(0.25)
    Q3 = dong_summary["total_living_pop"].quantile(0.75)
    IQR = Q3 - Q1
    outliers = dong_summary[
        (dong_summary["total_living_pop"] < (Q1 - 1.5 * IQR))
        | (dong_summary["total_living_pop"] > (Q3 + 1.5 * IQR))
    ]

    f.write("## 3. 이상치 (행정동 평균 총 생활인구 기준)\n")
    if not outliers.empty:
        f.write(f"1.5*IQR 규칙에 따라 {len(outliers)} 개의 이상치가 발견되었습니다.\n")
        f.write(
            outliers.sort_values("total_living_pop", ascending=False)
            .head(5)
            .to_string(index=False)
        )
    else:
        f.write("이상치가 발견되지 않았습니다.")
    f.write("\n\n")

    # 4. Top Dongs by Population
    f.write("## 4. 평균 총 생활인구 상위 10개 행정동\n")
    top_dongs = dong_summary.sort_values("total_living_pop", ascending=False).head(10)
    f.write(top_dongs.to_string(index=False))
    f.write("\n\n")

    # Bottom 5 Dongs
    f.write("## 5. 평균 총 생활인구 하위 5개 행정동\n")
    bottom_dongs = dong_summary.sort_values("total_living_pop", ascending=True).head(5)
    f.write(bottom_dongs.to_string(index=False))
    f.write("\n\n")


# -- Visualizations (Plotly) --
logger.info("Generating visualizations...")

# 1. Composition Pie Chart (Average)
labels = ["내국인", "장기체류 외국인", "단기체류 외국인"]
values = [
    df["local_total_living_pop"].mean(),
    df["total_long_term"].mean(),
    df["total_short_term"].mean(),
]

fig1 = px.pie(
    names=labels,
    values=values,
    title="평균 생활인구 구성 비율",
    color_discrete_sequence=px.colors.qualitative.Pastel,
)
fig1.write_html(EDA_OUTPUT_DIR / "composition_pie.html")

# 2. Time Slot Trends
time_trend = (
    df.groupby("time_slot")[
        ["local_total_living_pop", "total_long_term", "total_short_term"]
    ]
    .mean()
    .reset_index()
)

fig2 = make_subplots(specs=[[{"secondary_y": True}]])
fig2.add_trace(
    go.Scatter(
        x=time_trend["time_slot"],
        y=time_trend["local_total_living_pop"],
        name="내국인",
        mode="lines+markers",
    ),
    secondary_y=False,
)
fig2.add_trace(
    go.Scatter(
        x=time_trend["time_slot"],
        y=time_trend["total_long_term"],
        name="장기체류 외국인",
        mode="lines+markers",
    ),
    secondary_y=True,
)
fig2.add_trace(
    go.Scatter(
        x=time_trend["time_slot"],
        y=time_trend["total_short_term"],
        name="단기체류 외국인",
        mode="lines+markers",
    ),
    secondary_y=True,
)

fig2.update_layout(title_text="시간대별 평균 생활인구 변화")
fig2.update_yaxes(title_text="내국인 수", secondary_y=False)
fig2.update_yaxes(title_text="외국인 수", secondary_y=True)
fig2.write_html(EDA_OUTPUT_DIR / "trend_time_slot.html")


# 3. Local People Demographics (Population Pyramid)
age_cols_male = [c for c in df.columns if "local_male" in c]
age_cols_female = [c for c in df.columns if "local_female" in c]

male_sums = df[age_cols_male].mean()
female_sums = df[age_cols_female].mean()

age_labels = [
    c.replace("local_male_", "").replace("_pop", "").replace("age_", "") + "세"
    for c in age_cols_male
]

fig3 = go.Figure()
fig3.add_trace(
    go.Bar(
        y=age_labels,
        x=-male_sums.values,
        name="남성",
        orientation="h",
        marker=dict(color="cornflowerblue"),
        customdata=male_sums.values,
        hovertemplate="%{y} - 남성: %{customdata:.2f}<extra></extra>",
    )
)
fig3.add_trace(
    go.Bar(
        y=age_labels,
        x=female_sums.values,
        name="여성",
        orientation="h",
        marker=dict(color="lightpink"),
        hovertemplate="%{y} - 여성: %{x:.2f}<extra></extra>",
    )
)

fig3.update_layout(
    title="내국인 생활인구 피라미드 (평균)",
    barmode="relative",
    xaxis=dict(title="평균 인구 수"),
    yaxis=dict(title="연령대"),
    autosize=False,
    width=800,
    height=600,
)
fig3.write_html(EDA_OUTPUT_DIR / "local_demographics_pyramid.html")


# 4. Distribution of Total Living Population
fig4 = px.histogram(
    df,
    x="total_living_pop",
    nbins=50,
    title="시간/행정동별 총 생활인구 분포",
    labels={"total_living_pop": "생활인구 수"},
)
fig4.write_html(EDA_OUTPUT_DIR / "dist_total_living.html")

# 5. Correlation Heatmap
# Select numeric cols
numeric_cols = [
    "total_living_pop",
    "local_total_living_pop",
    "total_long_term",
    "total_short_term",
]
# Add some age comparisons if desired, e.g., 20s vs 30s
cols_20s = [c for c in df.columns if "age_20" in c]
cols_30s = [c for c in df.columns if "age_30" in c]
# Simplify to sums for correlation
df["local_20s"] = df[[c for c in cols_20s if "local" in c]].sum(axis=1)
df["local_30s"] = df[[c for c in cols_30s if "local" in c]].sum(axis=1)
numeric_cols.extend(["local_20s", "local_30s"])

corr = df[numeric_cols].corr()

fig5 = px.imshow(
    corr,
    text_auto=".2f",
    aspect="auto",
    title="주요 변수 상관관계 히트맵",
    color_continuous_scale="RdBu_r",
    origin="lower",
)
fig5.write_html(EDA_OUTPUT_DIR / "correlation_heatmap.html")

logger.info(f"EDA completed. Summary saved to {summary_file}")
logger.info("Plotly visualizations saved as HTML files.")
