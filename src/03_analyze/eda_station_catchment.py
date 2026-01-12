import pandas as pd
import sys
import os
import plotly.express as px
import plotly.graph_objects as go
import logging
from src.utils.db_util import get_engine
from src.utils.config import OUTPUT_DIR, LOG_FORMAT, LOG_LEVEL
from src.utils.visualization import apply_theme, save_plot

# Configure Logging
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

# Apply Theme
apply_theme()

# Configuration
EDA_OUTPUT_DIR = OUTPUT_DIR / "eda_station_catchment"
EDA_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def main():
    # 1. Load Data
    logger.info("Connecting to database...")
    try:
        engine = get_engine()

        query = """
        SELECT 
            s.station_name_kr,
            l.line_name,
            b.usage_type,
            b.total_area,
            b.total_households,
            b.total_families
        FROM Station_Catchment_Building_Stats b
        JOIN Stations s ON b.station_id = s.station_id
        JOIN Lines l ON b.line_id = l.line_id
        """
        df = pd.read_sql(query, engine)

    except Exception as e:
        logger.error(f"Error reading from database: {e}")
        sys.exit(1)

    if df.empty:
        logger.warning(
            "Warning: The table Station_Catchment_Building_Stats is empty or join failed."
        )
        sys.exit(0)

    logger.info(f"Loaded {len(df)} records.")

    # Summary File
    summary_file = EDA_OUTPUT_DIR / "eda_summary.md"

    with open(summary_file, "w", encoding="utf-8") as f:
        f.write(
            "# 역세권 건물 데이터 분석 보고서 (Station Catchment Building Stats)\n\n"
        )

        # 2. Basic Info
        f.write("## 1. 기본 정보\n")
        f.write(f"총 행 수: {len(df)}\n")
        f.write(f"총 열 수: {len(df.columns)}\n")
        f.write(f"고유 역 수: {df['station_name_kr'].nunique()}\n")
        f.write(f"고유 호선 수: {df['line_name'].nunique()}\n")
        f.write(f"건물 용도 유형 수: {df['usage_type'].nunique()}\n\n")

        # 3. Aggregated Metrics by Usage Type
        f.write("## 2. 건물 용도별 통계 (평균)\n")
        usage_stats = df.groupby("usage_type")[
            ["total_area", "total_households", "total_families"]
        ].mean()
        f.write(usage_stats.to_string())
        f.write("\n\n")

        # 4. Top Stations by Total Area
        f.write("## 3. 연면적 상위 10개 역\n")
        station_area = (
            df.groupby(["line_name", "station_name_kr"])["total_area"]
            .sum()
            .reset_index()
        )
        top_stations = station_area.sort_values("total_area", ascending=False).head(10)
        f.write(top_stations.to_string(index=False))
        f.write("\n\n")

    # -- Visualizations (Plotly) --
    logger.info("Generating visualizations...")

    # 1. Usage Type Count Bar Chart
    usage_counts = df["usage_type"].value_counts().reset_index()
    usage_counts.columns = ["usage_type", "count"]

    fig1 = px.bar(
        usage_counts,
        x="usage_type",
        y="count",
        title="건물 용도별 레코드 수 (데이터 분포)",
        labels={"usage_type": "건물 용도", "count": "레코드 수"},
        color="usage_type",
    )
    save_plot(fig1, EDA_OUTPUT_DIR / "usage_type_counts.html")

    # 2. Total Area by Usage Type
    usage_area = df.groupby("usage_type")["total_area"].sum().reset_index()

    fig2 = px.bar(
        usage_area,
        x="usage_type",
        y="total_area",
        title="건물 용도별 총 연면적 합계",
        labels={"usage_type": "건물 용도", "total_area": "총 연면적 (m²)"},
        color="usage_type",
    )
    save_plot(fig2, EDA_OUTPUT_DIR / "usage_type_total_area.html")

    # 3. Box Plot of Area by Usage Type (Log Scale)
    fig3 = px.box(
        df,
        x="usage_type",
        y="total_area",
        title="건물 용도별 연면적 분포 (로그 스케일)",
        labels={"usage_type": "건물 용도", "total_area": "연면적 (m²)"},
        color="usage_type",
        log_y=True,
    )
    save_plot(fig3, EDA_OUTPUT_DIR / "usage_type_area_boxplot.html")

    # 4. Households vs Families Scatter
    # Filter only rows having households or families > 0 to resolve log plot issues or just use linear if sparse
    # Typically households and families are highly correlated
    fig4 = px.scatter(
        df,
        x="total_households",
        y="total_families",
        color="usage_type",
        hover_data=["station_name_kr", "line_name"],
        title="총 세대 수 vs 총 가구 수 상관관계",
        labels={"total_households": "총 세대 수", "total_families": "총 가구 수"},
    )
    save_plot(fig4, EDA_OUTPUT_DIR / "households_vs_families.html")

    # 5. Top 20 Stations by Total Area (Bar Chart)
    top_20_stations = station_area.sort_values("total_area", ascending=False).head(20)
    # Create a combined name for clear labeling
    top_20_stations["station_label"] = (
        top_20_stations["line_name"] + " " + top_20_stations["station_name_kr"]
    )

    fig5 = px.bar(
        top_20_stations,
        x="station_label",
        y="total_area",
        title="연면적 합계 상위 20개 역",
        labels={"station_label": "역 (호선)", "total_area": "총 연면적 (m²)"},
        color="total_area",
    )
    fig5.update_layout(xaxis_tickangle=-45)
    save_plot(fig5, EDA_OUTPUT_DIR / "top_20_stations_area.html")

    logger.info(f"EDA completed. Summary saved to {summary_file}")
    logger.info("Plotly visualizations saved as HTML files.")


if __name__ == "__main__":
    main()
