import sqlite3
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.pipeline import Pipeline
import joblib
import os


def load_data(db_path):
    print("Loading data from database...")
    con = sqlite3.connect(db_path)

    # 1. Congestion Data with Station Info
    query_congestion = """
    SELECT 
        c.station_number, c.quarter_code, c.is_weekend, c.time_slot, c.congestion_level,
        r.line_id, r.administrative_dong
    FROM Station_Congestion c
    JOIN Station_Routes r ON c.station_number = r.station_number
    """
    df_congestion = pd.read_sql(query_congestion, con)
    print(f"Congestion Data Rows: {len(df_congestion)}")

    # 2. Mapping for Dong Code <-> Name (from Floating Population table)
    query_mapping = (
        "SELECT DISTINCT admin_dong_code, admin_dong_name FROM Dong_Floating_Population"
    )
    df_mapping = pd.read_sql(query_mapping, con)

    # 3. Living Population (Aggregated by Quarter, Dong Code)
    # Convert base_date (YYYYMMDD) to quarter_code (YYYY1, YYYY2...)
    # 20230101 -> 20231
    query_living = """
    SELECT 
        substr(base_date, 1, 4) || CASE 
            WHEN substr(base_date, 5, 2) BETWEEN '01' AND '03' THEN '1'
            WHEN substr(base_date, 5, 2) BETWEEN '04' AND '06' THEN '2'
            WHEN substr(base_date, 5, 2) BETWEEN '07' AND '09' THEN '3'
            ELSE '4' END as quarter_code,
        admin_dong_code,
        AVG(local_total_living_pop) as avg_living_pop
    FROM Dong_Living_Population
    GROUP BY 1, 2
    """
    df_living_raw = pd.read_sql(query_living, con)
    # Merge with mapping to get dong name
    df_living = pd.merge(df_living_raw, df_mapping, on="admin_dong_code", how="inner")

    # 4. Floating Population (Quarter, Dong Name)
    query_floating = """
    SELECT quarter_code, admin_dong_name, total_floating_pop
    FROM Dong_Floating_Population
    """
    df_floating = pd.read_sql(query_floating, con)

    # 5. Revenue (Quarter, Dong Name)
    query_revenue = """
    SELECT quarter_code, admin_dong_name, SUM(month_sales_amt) as total_revenue
    FROM Dong_Estimated_Revenue
    GROUP BY quarter_code, admin_dong_name
    """
    df_revenue = pd.read_sql(query_revenue, con)

    con.close()
    return df_congestion, df_living, df_floating, df_revenue


def merge_data(df_c, df_l, df_f, df_r):
    print("Merging datasets...")

    # Keys for merging: quarter_code, administrative_dong (matches admin_dong_name)
    # Note: df_c uses 'administrative_dong', others use 'admin_dong_name'

    # Merge Living
    df_merged = pd.merge(
        df_c,
        df_l,
        left_on=["quarter_code", "administrative_dong"],
        right_on=["quarter_code", "admin_dong_name"],
        how="inner",
    )  # Use inner to only get valid training data

    # Merge Floating
    df_merged = pd.merge(
        df_merged,
        df_f,
        left_on=["quarter_code", "administrative_dong"],
        right_on=["quarter_code", "admin_dong_name"],
        how="left",
    )

    # Merge Revenue
    df_merged = pd.merge(
        df_merged,
        df_r,
        left_on=["quarter_code", "administrative_dong"],
        right_on=["quarter_code", "admin_dong_name"],
        how="left",
    )

    # Drop redundant columns
    cols_to_drop = [
        c for c in df_merged.columns if "admin_dong_name" in c or "admin_dong_code" in c
    ]
    df_merged = df_merged.drop(columns=cols_to_drop)

    print(f"Merged Data Rows: {len(df_merged)}")
    return df_merged


def train_models(df):
    print("\nTraining Models...")

    # Features
    # Numeric: time_slot, avg_living_pop, total_floating_pop, total_revenue
    # Categorical: is_weekend, line_id, administrative_dong? (Maybe too many categories, let's keep line_id)
    # We will treat station_number as ID and not feature, or maybe categorical?
    # Let's use Line ID and Time Slot as main structural features, and Env features.

    features = [
        "time_slot",
        "is_weekend",
        "line_id",
        "avg_living_pop",
        "total_floating_pop",
        "total_revenue",
    ]
    target = "congestion_level"

    # Drop rows with NaN
    df_clean = df.dropna(subset=features + [target])
    print(f"Data for training after dropna: {len(df_clean)}")

    X = df_clean[features]
    y = df_clean[target]

    # Split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # Preprocessing
    numeric_features = [
        "time_slot",
        "avg_living_pop",
        "total_floating_pop",
        "total_revenue",
    ]
    categorical_features = [
        "line_id",
        "is_weekend",
    ]  # treat weekend as cat or num? 0/1, fine as is.

    # Pipeline
    # We can actually just treat is_weekend as numeric (binary).
    # Line ID should be OneHot.

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), numeric_features),
            (
                "cat",
                OneHotEncoder(handle_unknown="ignore"),
                ["line_id"],
            ),  # is_weekend can pass through or be cat
        ],
        remainder="passthrough",
    )

    models = {
        "Linear Regression": LinearRegression(),
        "Random Forest": RandomForestRegressor(
            n_estimators=50, max_depth=10, random_state=42, n_jobs=-1
        ),
        "Gradient Boosting": GradientBoostingRegressor(
            n_estimators=50, max_depth=5, random_state=42
        ),
    }

    results = {}
    best_score = -float("inf")
    best_model_name = ""
    best_pipeline = None

    for name, model in models.items():
        pipeline = Pipeline([("preprocessor", preprocessor), ("model", model)])

        pipeline.fit(X_train, y_train)
        y_pred = pipeline.predict(X_test)

        mse = mean_squared_error(y_test, y_pred)
        rmse = np.sqrt(mse)
        r2 = r2_score(y_test, y_pred)

        results[name] = {"RMSE": rmse, "R2": r2}
        print(f"{name} - RMSE: {rmse:.4f}, R2: {r2:.4f}")

        if r2 > best_score:
            best_score = r2
            best_model_name = name
            best_pipeline = pipeline

    print(f"\nBest Model: {best_model_name} (R2: {best_score:.4f})")

    # Save best model
    os.makedirs("output/models", exist_ok=True)
    joblib.dump(best_pipeline, "output/models/congestion_model.pkl")
    print("Best model saved to output/models/congestion_model.pkl")

    # Save results to text file
    with open("output/model_results.txt", "w") as f:
        f.write("Model Comparison Results\n")
        f.write("========================\n")
        for name, metrics in results.items():
            f.write(f"{name}: RMSE={metrics['RMSE']:.4f}, R2={metrics['R2']:.4f}\n")
        f.write(f"\nBest Model: {best_model_name}\n")

        # Feature Importance (if applicable)
        if hasattr(best_pipeline.named_steps["model"], "feature_importances_"):
            f.write("\nFeature Importance:\n")
            importances = best_pipeline.named_steps["model"].feature_importances_
            try:
                feature_names = best_pipeline.named_steps[
                    "preprocessor"
                ].get_feature_names_out()
                for name_feat, imp in zip(feature_names, importances):
                    f.write(f"{name_feat}: {imp:.4f}\n")
            except Exception as e:
                f.write(f"Could not extract feature names: {e}\n")
                f.write(f"Raw Importances: {importances}\n")

    return results


if __name__ == "__main__":
    db_path = "db/subway.db"
    if not os.path.exists(db_path):
        print(f"Error: {db_path} not found.")
    else:
        df_c, df_l, df_f, df_r = load_data(db_path)
        if len(df_c) > 0:
            df_merged = merge_data(df_c, df_l, df_f, df_r)
            if len(df_merged) > 100:
                train_models(df_merged)
            else:
                print("Not enough merged data to train.")
