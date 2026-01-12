import pandas as pd
import sys
import os
import plotly.express as px
import plotly.graph_objects as go
import logging
from plotly.subplots import make_subplots
from src.utils.db_util import get_engine
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
    query = "SELECT * FROM Dong_Living_Population"
    df = pd.read_sql(query, engine)
except Exception as e:
    logger.error(f"Error reading from database: {e}")
    sys.exit(1)

if df.empty:
    logger.warning("Warning: The table Dong_Living_Population is empty.")
    sys.exit(0)

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

# Summary File
summary_file = EDA_OUTPUT_DIR / "eda_summary.txt"

with open(summary_file, "w") as f:
    f.write("# EDA Report: Dong Living Population\n\n")

    # 2. Basic Info
    f.write("## 1. Basic Info\n")
    f.write(f"Total Rows: {len(df)}\n")
    f.write(f"Total Columns: {len(df.columns)}\n")
    f.write(f"Unique Dongs: {df['admin_dong_code'].nunique()}\n")
    f.write(f"Date Range: {df['base_date'].min()} to {df['base_date'].max()}\n\n")

    # 3. Aggregated Metrics
    f.write("## 2. Population composition (Means)\n")
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

    # 4. Top Dongs by Population
    f.write("## 3. Top 10 Dongs by Average Total Living Population\n")
    top_dongs = (
        df.groupby("admin_dong_code")["total_living_pop"]
        .mean()
        .sort_values(ascending=False)
        .head(10)
    )
    f.write(top_dongs.to_string())
    f.write("\n\n")


# -- Visualizations (Plotly) --
logger.info("Generating visualizations...")

# 1. Composition Pie Chart (Average)
labels = ["Local", "Long-term Foreigner", "Short-term Foreigner"]
values = [
    df["local_total_living_pop"].mean(),
    df["total_long_term"].mean(),
    df["total_short_term"].mean(),
]

fig1 = px.pie(
    names=labels,
    values=values,
    title="Average Living Population Composition",
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
        name="Local",
        mode="lines+markers",
    ),
    secondary_y=False,
)
fig2.add_trace(
    go.Scatter(
        x=time_trend["time_slot"],
        y=time_trend["total_long_term"],
        name="Long-term F",
        mode="lines+markers",
    ),
    secondary_y=True,
)
fig2.add_trace(
    go.Scatter(
        x=time_trend["time_slot"],
        y=time_trend["total_short_term"],
        name="Short-term F",
        mode="lines+markers",
    ),
    secondary_y=True,
)

fig2.update_layout(title_text="Average Population by Time Slot")
fig2.update_yaxes(title_text="Local Population", secondary_y=False)
fig2.update_yaxes(title_text="Foreigner Population", secondary_y=True)
fig2.write_html(EDA_OUTPUT_DIR / "trend_time_slot.html")


# 3. Local People Demographics (Population Pyramid)
age_cols_male = [c for c in df.columns if "local_male" in c]
age_cols_female = [c for c in df.columns if "local_female" in c]

male_sums = df[age_cols_male].mean()
female_sums = df[age_cols_female].mean()

age_labels = [
    c.replace("local_male_", "").replace("_pop", "").replace("age_", "")
    for c in age_cols_male
]

fig3 = go.Figure()
fig3.add_trace(
    go.Bar(
        y=age_labels,
        x=-male_sums.values,
        name="Male",
        orientation="h",
        marker=dict(color="cornflowerblue"),
        customdata=male_sums.values,
        hovertemplate="%{y} - Male: %{customdata:.2f}<extra></extra>",
    )
)
fig3.add_trace(
    go.Bar(
        y=age_labels,
        x=female_sums.values,
        name="Female",
        orientation="h",
        marker=dict(color="lightpink"),
        hovertemplate="%{y} - Female: %{x:.2f}<extra></extra>",
    )
)

fig3.update_layout(
    title="Local Living Population Pyramid (Average)",
    barmode="relative",
    xaxis=dict(title="Average Population Count"),
    yaxis=dict(title="Age Group"),
    autosize=False,
    width=800,
    height=600,
)
# Note: X-axis will show negative values for Male.
# This is standard for simple Plotly pyramids unless using complex tick mapping.
fig3.write_html(EDA_OUTPUT_DIR / "local_demographics_pyramid.html")


# 4. Distribution of Total Living Population
fig4 = px.histogram(
    df,
    x="total_living_pop",
    nbins=50,
    title="Distribution of Total Living Population per Dong/Time",
    labels={"total_living_pop": "Population"},
)
fig4.write_html(EDA_OUTPUT_DIR / "dist_total_living.html")

logger.info(f"EDA completed. Summary saved to {summary_file}")
logger.info("Plotly visualizations saved as HTML files.")
