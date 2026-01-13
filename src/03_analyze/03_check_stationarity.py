import pandas as pd
import logging
import plotly.graph_objects as go
from statsmodels.tsa.stattools import adfuller
from src.utils.db_util import get_connection
from src.utils.config import LOG_FORMAT, LOG_LEVEL

# Configure Logging
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)


def check_stationarity(series, name):
    """
    Perform ADF test on a time series.
    Returns a dictionary with test results.
    """
    try:
        result = adfuller(series.dropna())
        return {
            "name": name,
            "adf_statistic": result[0],
            "p_value": result[1],
            "critical_values": result[4],
            "is_stationary": result[1] < 0.05,
        }
    except Exception as e:
        logger.warning(f"ADF test failed for {name}: {e}")
        return None


def run_stationarity_analysis():
    conn = get_connection()

    # Load data
    query = """
    SELECT useage_date, line_name, station_name, boarding_count, alighting_count
    FROM Station_Daily_Passengers
    ORDER BY usage_date
    """
    # Note: Column in DB is 'usage_date', query uses 'useage_date' might be a typo in my thought?
    # Let me check schema.sql again in my memory...
    # Schema says: usage_date TEXT NOT NULL
    # So query should be 'usage_date'.

    query = """
    SELECT usage_date, line_name, station_name, boarding_count, alighting_count
    FROM Station_Daily_Passengers
    ORDER BY usage_date
    """

    try:
        df = pd.read_sql(query, conn)
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        conn.close()
        return

    conn.close()

    if df.empty:
        logger.warning("No data found in Station_Daily_Passengers.")
        return

    # Preprocessing
    df["usage_date"] = pd.to_datetime(df["usage_date"])
    df["station_id_str"] = df["line_name"] + " " + df["station_name"]

    # Aggregating duplicates if any (though schema has unique constraint on usage_date, line, station)
    # Just to be safe and easy to iterate
    stations = df["station_id_str"].unique()

    results = []

    # Analyze all stations
    logger.info(f"Analyzing {len(stations)} stations...")

    for station in stations:
        station_df = df[df["station_id_str"] == station].sort_values("usage_date")

        # Boarding
        res_boarding = check_stationarity(
            station_df["boarding_count"], f"{station} (승차)"
        )
        if res_boarding:
            results.append(res_boarding)

        # Alighting
        res_alighting = check_stationarity(
            station_df["alighting_count"], f"{station} (하차)"
        )
        if res_alighting:
            results.append(res_alighting)

    # Convert results to DataFrame
    results_df = pd.DataFrame(results)

    if results_df.empty:
        logger.warning("No valid results obtained.")
        return

    # print summary
    total_checks = len(results_df)
    stationary_count = results_df["is_stationary"].sum()
    logger.info("=== Stationarity Analysis Results ===")
    logger.info(f"Total Series Checked: {total_checks}")
    logger.info(
        f"Stationary Series (p < 0.05): {stationary_count} ({stationary_count / total_checks:.2%})"
    )

    # Visualization for Top 5 stations by total volume
    # Calculate volume
    df["total_volume"] = df["boarding_count"] + df["alighting_count"]
    top_stations = df.groupby("station_id_str")["total_volume"].sum().nlargest(5).index

    fig = go.Figure()

    for station in top_stations:
        station_df = df[df["station_id_str"] == station].sort_values("usage_date")

        # Get p-values for legend
        p_board = (
            results_df[results_df["name"] == f"{station} (승차)"]["p_value"].values[0]
            if not results_df[results_df["name"] == f"{station} (승차)"].empty
            else 1.0
        )
        p_alight = (
            results_df[results_df["name"] == f"{station} (하차)"]["p_value"].values[0]
            if not results_df[results_df["name"] == f"{station} (하차)"].empty
            else 1.0
        )

        fig.add_trace(
            go.Scatter(
                x=station_df["usage_date"],
                y=station_df["boarding_count"],
                mode="lines",
                name=f"{station} 승차 (p={p_board:.4f})",
            )
        )

        fig.add_trace(
            go.Scatter(
                x=station_df["usage_date"],
                y=station_df["alighting_count"],
                mode="lines",
                name=f"{station} 하차 (p={p_alight:.4f})",
                visible="legendonly",  # Hide by default to reduce clutter
            )
        )

    fig.update_layout(
        title="Top 5 Stations Daily Passengers & Stationarity (p-value)",
        xaxis_title="Date",
        yaxis_title="Passengers",
        template="plotly_white",
        font=dict(
            family="Malgun Gothic"
        ),  # Attempt to support Korean font if available on Windows
    )

    fig.show()

    # Save results to CSV for inspection
    # results_df.to_csv("stationarity_results.csv", index=False)
    # logger.info("Detailed results saved to stationarity_results.csv")


if __name__ == "__main__":
    run_stationarity_analysis()
