import os
import sys
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from sqlalchemy import create_engine
from plotly.subplots import make_subplots

# Add src to path
# Add src to path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(project_root)

# Database connection
db_path = os.path.join(project_root, "db", "subway.db")
print(f"DEBUG: db_path resolved to: {db_path}")
if not os.path.exists(db_path):
    print(f"ERROR: Database file not found at {db_path}")
    # List files in the expected dir to debug
    db_dir = os.path.join(project_root, "db")
    print(
        f"DEBUG: Files in {db_dir}: {os.listdir(db_dir) if os.path.exists(db_dir) else 'Directory not found'}"
    )
    sys.exit(1)

engine = create_engine(f"sqlite:///{db_path}")


def get_quarter_code_vectorized(dates):
    """YYYYMMDD -> YYYYQ (Vectorized)"""
    dt = pd.to_datetime(dates)
    return dt.dt.year.astype(str) + ((dt.dt.month - 1) // 3 + 1).astype(str)


def get_day_type_vectorized(dates):
    """0: Weekday, 1: Saturday, 2: Sunday (Vectorized)"""
    dt = pd.to_datetime(dates)
    weekday = dt.dt.weekday
    # weekday is 0-6 (Mon-Sun)
    # 0-4: Weekday(0), 5: Sat(1), 6: Sun(2)
    # logic: if < 5 -> 0, elif == 5 -> 1, else -> 2
    return np.where(weekday < 5, 0, np.where(weekday == 5, 1, 2))


def load_data():
    print("Loading data...")

    # Load Station Information
    print("   - Loading Stations...")
    query_stations = """
    SELECT 
        sr.station_number, 
        s.station_name_kr as station_name,
        l.line_name
    FROM Station_Routes sr
    JOIN Stations s ON sr.station_id = s.station_id
    JOIN Lines l ON sr.line_id = l.line_id
    """
    df_stations = pd.read_sql(query_stations, engine)
    print(f"     -> Stations loaded: {len(df_stations)} rows")

    # Load Congestion Data
    print("   - Loading Congestion...")
    query_congestion = """
    SELECT 
        quarter_code,
        station_number,
        is_weekend,
        is_upline,
        time_slot,
        congestion_level
    FROM Station_Congestion
    """
    df_congestion = pd.read_sql(query_congestion, engine)
    print(f"     -> Congestion loaded: {len(df_congestion)} rows")

    # Load Daily Passengers Data
    print("   - Loading Passengers...")
    query_passengers = """
    SELECT 
        usage_date,
        line_name,
        station_name,
        boarding_count,
        alighting_count
    FROM Station_Daily_Passengers
    """
    df_passengers = pd.read_sql(query_passengers, engine)
    print(f"     -> Passengers loaded: {len(df_passengers)} rows")

    return df_stations, df_congestion, df_passengers


def process_data(df_stations, df_congestion, df_passengers):
    print("Processing data...")

    # 1. Process Passengers: Aggregate by Quarter, DayType
    print("   - Converting dates...")
    df_passengers["quarter_code"] = get_quarter_code_vectorized(
        df_passengers["usage_date"]
    )
    df_passengers["is_weekend"] = get_day_type_vectorized(df_passengers["usage_date"])

    print("   - Aggregating passengers...")
    # Aggregate mean daily passengers per quarter
    df_pass_agg = (
        df_passengers.groupby(
            ["line_name", "station_name", "quarter_code", "is_weekend"]
        )
        .agg({"boarding_count": "mean", "alighting_count": "mean"})
        .reset_index()
    )

    # 2. Process Congestion: Join with Station Info to get Names
    print("   - Merging congestion info...")
    df_cong_merged = pd.merge(
        df_congestion, df_stations, on="station_number", how="left"
    )

    # 3. Merge Congestion and Passengers
    print("   - Merging datasets...")
    # Keys: line_name, station_name, quarter_code, is_weekend
    df_analysis = pd.merge(
        df_cong_merged,
        df_pass_agg,
        on=["line_name", "station_name", "quarter_code", "is_weekend"],
        how="inner",
    )

    return df_analysis


def calculate_correlations(df):
    print("Calculating correlations...")

    results = []

    # Group by Station, Line, TimeSlot, Upline/Downline
    grouped = df.groupby(["line_name", "station_name", "time_slot", "is_upline"])

    # Use a faster way if possible, but loop is fine for reasonable group count
    # There are ~300 stations * 40 slots * 2 directions ~ 24000 groups. Might take a few seconds.
    count = 0
    total = len(grouped)

    for name, group in grouped:
        count += 1
        if count % 1000 == 0:
            print(f"   - Processed {count}/{total} groups...", end="\r")

        if len(group) < 3:  # Need at least 3 points for meaningful correlation
            continue

        line, station, time, upline = name

        try:
            corr_boarding = group["congestion_level"].corr(group["boarding_count"])
            corr_alighting = group["congestion_level"].corr(group["alighting_count"])

            # Filter NaN results (constant values)
            if pd.isna(corr_boarding) or pd.isna(corr_alighting):
                continue

            results.append(
                {
                    "line_name": line,
                    "station_name": station,
                    "time_slot": time,
                    "is_upline": upline,
                    "corr_boarding": corr_boarding,
                    "corr_alighting": corr_alighting,
                    "sample_size": len(group),
                }
            )
        except Exception:
            continue

    print(f"   - Processed {total}/{total} groups. Done.")
    return pd.DataFrame(results)


def visualize_results(df_corr, df_analysis):
    print("Visualizing results...")

    # Filter valid correlations
    df_corr = df_corr.dropna()

    if df_corr.empty:
        print("No sufficient data for correlation analysis.")
        return

    # 1. Distribution of Correlations
    fig_hist = go.Figure()
    fig_hist.add_trace(
        go.Histogram(
            x=df_corr["corr_boarding"], name="혼잡도 vs 승차인원 상관계수", opacity=0.75
        )
    )
    fig_hist.add_trace(
        go.Histogram(
            x=df_corr["corr_alighting"],
            name="혼잡도 vs 하차인원 상관계수",
            opacity=0.75,
        )
    )
    fig_hist.update_layout(
        title="지하철 혼잡도와 승하차 인원의 상관계수 분포",
        xaxis_title="상관계수 (Pearson)",
        yaxis_title="빈도",
        barmode="overlay",
        template="plotly_white",
    )
    # Save instead of show
    fig_hist.write_html("correlation_distribution.html")
    print("Saved correlation_distribution.html")

    # 2. Top Positive Correlations Table
    top_pos = df_corr.sort_values("corr_boarding", ascending=False).head(10)
    print("\n[상위 양의 상관관계 (혼잡도 vs 승차)]")
    print(
        top_pos[
            [
                "line_name",
                "station_name",
                "time_slot",
                "is_upline",
                "corr_boarding",
                "sample_size",
            ]
        ].to_string()
    )

    # 3. Example Visualization for the Highest Correlation
    if not top_pos.empty:
        best_case = top_pos.iloc[0]
        line = best_case["line_name"]
        station = best_case["station_name"]
        time = best_case["time_slot"]
        upline = best_case["is_upline"]

        subset = df_analysis[
            (df_analysis["line_name"] == line)
            & (df_analysis["station_name"] == station)
            & (df_analysis["time_slot"] == time)
            & (df_analysis["is_upline"] == upline)
        ].sort_values("quarter_code")

        fig_example = make_subplots(specs=[[{"secondary_y": True}]])

        fig_example.add_trace(
            go.Scatter(
                x=subset["quarter_code"],
                y=subset["congestion_level"],
                name="혼잡도",
                mode="lines+markers",
            ),
            secondary_y=False,
        )

        fig_example.add_trace(
            go.Scatter(
                x=subset["quarter_code"],
                y=subset["boarding_count"],
                name="승차인원",
                mode="lines+markers",
                line=dict(dash="dot"),
            ),
            secondary_y=True,
        )

        direction = "상행" if upline == 1 else "하행"

        fig_example.update_layout(
            title=f"시계열 추이 비교: {line} {station} ({direction}, TimeSlot {time})",
            template="plotly_white",
        )
        fig_example.update_yaxes(title_text="혼잡도 (%)", secondary_y=False)
        fig_example.update_yaxes(title_text="일평균 승차인원 (명)", secondary_y=True)

        fig_example.write_html("correlation_example.html")
        print("Saved correlation_example.html")


def main():
    df_stations, df_congestion, df_passengers = load_data()

    if df_congestion.empty or df_passengers.empty:
        print("Data unavailable.")
        return

    df_analysis = process_data(df_stations, df_congestion, df_passengers)

    print(f"Analysis Dataset Size: {len(df_analysis)} rows")

    df_corr = calculate_correlations(df_analysis)

    if df_corr is not None:
        print(f"Computed correlations for {len(df_corr)} station-time-direction pairs.")
        visualize_results(df_corr, df_analysis)


if __name__ == "__main__":
    main()
