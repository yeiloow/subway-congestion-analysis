import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score
import joblib
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Define paths
DATA_PATH = "data/02_processed/model_dataset.csv"
MODEL_DIR = "output/models"
PLOT_DIR = "output/plots"
MODEL_FILE = os.path.join(MODEL_DIR, "revenue_rf_model_v2.pkl")
PLOT_FILE = os.path.join(PLOT_DIR, "model_evaluation_v2.png")


def train_model():
    # Ensure directories exist
    os.makedirs(MODEL_DIR, exist_ok=True)
    os.makedirs(PLOT_DIR, exist_ok=True)

    print("Loading data...")
    if not os.path.exists(DATA_PATH):
        print(f"Error: Data file {DATA_PATH} not found. Run wrangling script first.")
        return

    df = pd.read_csv(DATA_PATH)
    print(f"Dataset Size: {len(df)}")

    # Feature Engineering
    # Features: congestion_level, time_slot, is_weekend, is_upline, total_floating_pop
    feature_cols = [
        "congestion_level",
        "time_slot",
        "is_weekend",
        "is_upline",
        "total_floating_pop",
    ]
    target_col = "total_estimated_revenue"

    # Drop NAs
    df = df.dropna(subset=feature_cols + [target_col])

    X = df[feature_cols]
    y = df[target_col]

    print(f"Training with {len(X)} samples...")
    print(f"Features: {feature_cols}")

    # Split Data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # Train Model
    print("Training Random Forest Regressor...")
    rf = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
    rf.fit(X_train, y_train)

    # Evaluate
    y_pred = rf.predict(X_test)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)

    print("Model Performance:")
    print(f"  RMSE: {rmse:,.2f}")
    print(f"  R2 Score: {r2:.4f}")

    # Feature Importance
    importances = rf.feature_importances_
    indices = np.argsort(importances)[::-1]
    print("\nFeature Importances:")
    for f in range(X.shape[1]):
        print(
            "%d. %s (%f)" % (f + 1, feature_cols[indices[f]], importances[indices[f]])
        )

    # Save Model
    print(f"Saving model to {MODEL_FILE}...")
    joblib.dump(rf, MODEL_FILE)

    # Visualize
    print("Generating evaluation plot...")
    plt.figure(figsize=(10, 6))
    sns.scatterplot(x=y_test, y=y_pred, alpha=0.5)
    plt.plot([y.min(), y.max()], [y.min(), y.max()], "r--")
    plt.xlabel("Actual Revenue")
    plt.ylabel("Predicted Revenue")
    plt.title(f"Actual vs Predicted Revenue (R2={r2:.2f})")
    plt.tight_layout()
    plt.savefig(PLOT_FILE)
    print(f"Plot saved to {PLOT_FILE}")
    print("Done.")


if __name__ == "__main__":
    train_model()
