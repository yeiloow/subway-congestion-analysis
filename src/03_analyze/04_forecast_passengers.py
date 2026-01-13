import pandas as pd
import numpy as np
import logging
import plotly.graph_objects as go
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.holtwinters import ExponentialSmoothing

# from sklearn.metrics import mean_squared_error, mean_absolute_error # Removed to avoid dependency
from src.utils.db_util import get_connection
from src.utils.config import LOG_FORMAT, LOG_LEVEL
import warnings
from itertools import product

# Configure Logging
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

# Suppress warnings for cleaner output
warnings.filterwarnings("ignore")


def load_data():
    conn = get_connection()
    query = """
    SELECT usage_date, line_name, station_name, boarding_count, alighting_count
    FROM Station_Daily_Passengers
    ORDER BY usage_date
    """
    df = pd.read_sql(query, conn)
    conn.close()

    if df.empty:
        return None

    df["usage_date"] = pd.to_datetime(df["usage_date"])
    df["station_id_str"] = df["line_name"] + " " + df["station_name"]
    df["total_passengers"] = df["boarding_count"] + df["alighting_count"]
    return df


def get_top_station_series(df):
    """
    Returns the total passenger series for the station with the highest volume.
    """
    top_station = df.groupby("station_id_str")["total_passengers"].sum().idxmax()
    logger.info(f"Top Station by Volume: {top_station}")

    station_df = df[df["station_id_str"] == top_station].sort_values("usage_date")
    series = station_df.set_index("usage_date")["total_passengers"]

    # Fill missing dates if any
    idx = pd.date_range(start=series.index.min(), end=series.index.max(), freq="D")
    series = series.reindex(
        idx, fill_value=0
    )  # Assuming 0 for missing days or interpolate? 0 is safer for now.

    return top_station, series


def run_arima(train, test):
    """
    Runs ARIMA model with simple grid search for order.
    Returns best model fit, forecast, and order.
    Since grid search can be slow, we limit parameters.
    """
    logger.info("Running ARIMA Grid Search...")
    p_values = [1, 2, 7]  # 7 for weekly auto-regressive might be interesting
    d_values = [0, 1]
    q_values = [0, 1]

    best_score, best_cfg, best_model_fit = float("inf"), None, None

    for p, d, q in product(p_values, d_values, q_values):
        order = (p, d, q)
        try:
            model = ARIMA(train, order=order)
            model_fit = model.fit()
            aic = model_fit.aic
            if aic < best_score:
                best_score, best_cfg, best_model_fit = aic, order, model_fit
        except:
            continue

    logger.info(f"Best ARIMA Order: {best_cfg} AIC: {best_score}")

    # Forecast
    forecast = best_model_fit.forecast(steps=len(test))
    forecast.index = test.index

    return best_model_fit, forecast, best_cfg


def run_holt_winters(train, test):
    """
    Runs Holt-Winters Exponential Smoothing.
    """
    logger.info("Running Holt-Winters Exponential Smoothing...")
    # Additive trend and seasonality (weekly = 7)
    try:
        model = ExponentialSmoothing(
            train, trend="add", seasonal="add", seasonal_periods=7
        )
        model_fit = model.fit()
        forecast = model_fit.forecast(steps=len(test))
        forecast.index = test.index
        return model_fit, forecast
    except Exception as e:
        logger.error(f"Holt-Winters failed: {e}")
        return None, None


def evaluate_forecast(test, forecast, model_name):
    # Manual implementation of metrics
    errors = test - forecast
    mse = np.mean(errors**2)
    rmse = np.sqrt(mse)
    mae = np.mean(np.abs(errors))

    logger.info(f"[{model_name}] RMSE: {rmse:.2f}, MAE: {mae:.2f}")
    return rmse, mae


def plot_forecasts(
    train, test, arima_forecast, hw_forecast, station_name, arima_metrics, hw_metrics
):
    fig = go.Figure()

    # Train Data (Show last 90 days for clarity)
    train_subset = train[-90:]

    fig.add_trace(
        go.Scatter(
            x=train_subset.index,
            y=train_subset,
            mode="lines",
            name="Train Data (Last 90 Days)",
            line=dict(color="gray"),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=test.index,
            y=test,
            mode="lines",
            name="Test Data (Actual)",
            line=dict(color="black"),
        )
    )

    if arima_forecast is not None:
        fig.add_trace(
            go.Scatter(
                x=arima_forecast.index,
                y=arima_forecast,
                mode="lines",
                name=f"ARIMA Forecast (RMSE={arima_metrics[0]:.0f})",
                line=dict(dash="dash", color="blue"),
            )
        )

    if hw_forecast is not None:
        fig.add_trace(
            go.Scatter(
                x=hw_forecast.index,
                y=hw_forecast,
                mode="lines",
                name=f"Holt-Winters Forecast (RMSE={hw_metrics[0]:.0f})",
                line=dict(dash="dash", color="red"),
            )
        )

    fig.update_layout(
        title=f"Passenger Forecast for {station_name}",
        xaxis_title="Date",
        yaxis_title="Total Passengers",
        template="plotly_white",
        font=dict(family="Malgun Gothic"),
        hovermode="x unified",
    )

    fig.show()


def run_forecasting_pipeline():
    df = load_data()
    if df is None:
        logger.error("No data available.")
        return

    station_name, series = get_top_station_series(df)

    # Split Data (Last 30 days as test set)
    test_size = 30
    if len(series) < test_size * 2:
        logger.warning("Not enough data points for split.")
        return

    train = series[:-test_size]
    test = series[-test_size:]

    logger.info(f"Train Size: {len(train)}, Test Size: {len(test)}")

    # 1. ARIMA
    _, arima_forecast, arima_order = run_arima(train, test)
    arima_metrics = evaluate_forecast(test, arima_forecast, f"ARIMA {arima_order}")

    # 2. Holt-Winters
    _, hw_forecast = run_holt_winters(train, test)
    if hw_forecast is not None:
        hw_metrics = evaluate_forecast(test, hw_forecast, "Holt-Winters")
    else:
        hw_metrics = (0, 0)

    # Visualization
    plot_forecasts(
        train,
        test,
        arima_forecast, 
        hw_forecast,
        station_name,
        arima_metrics,
        hw_metrics,
    )


if __name__ == "__main__":
    run_forecasting_pipeline()
