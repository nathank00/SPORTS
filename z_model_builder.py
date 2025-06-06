import pandas as pd
import numpy as np
import joblib
import time
import os
from xata.client import XataClient
from xgboost import XGBClassifier
import matplotlib.pyplot as plt
from sklearn.model_selection import GridSearchCV, train_test_split
from sklearn.metrics import (
    accuracy_score, roc_auc_score, classification_report, confusion_matrix
)

# Load Xata credentials
xata = XataClient(api_key=os.getenv("XATA_API_KEY"), db_url=os.getenv("XATA_DB_URL"))

# Fetch all rows from the 'master' table
all_records = []
page = None
while True:
    query = {"columns": ["*"], "page": {"size": 1000}}
    if page:
        query["page"]["after"] = page
    resp = xata.data().query("master", query)
    if not resp.is_success():
        raise Exception("Failed to fetch data from Xata")
    records = resp.get("records", [])
    if not records:
        break
    all_records.extend(records)
    page = resp.get("meta", {}).get("page", {}).get("cursor")
    if not page:
        break

# Convert to DataFrame
df = pd.DataFrame(all_records)
df["game_date"] = pd.to_datetime(df["game_date"])

# Filter out push/noisy labels
df = df.dropna(subset=["label_over_under", "total_runs_scored", "runline"])
df = df[abs(df["total_runs_scored"] - df["runline"]) > 0.5]

# Prepare features
exclude_cols = [
    "label_over_under", "model_prediction", "prediction_confidence",
    "game_id", "game_date", "start_time" "id", "xata", "runs_home", "description",
    "runs_away", "total_runs_scored", "runline", "game_started", "game_complete"
]
feature_cols = [col for col in df.columns if col not in exclude_cols]
numeric_feature_cols = [col for col in feature_cols if df[col].dtype in ["int64", "float64", "bool"]]

X = df[numeric_feature_cols]
y = df["label_over_under"]

# Random split
X_train, X_val, y_train, y_val = train_test_split(
    X, y, test_size=0.3, random_state=42, stratify=y
)

# Compute scale_pos_weight
neg = sum(y_train == 0)
pos = sum(y_train == 1)
scale_pos_weight = neg / pos


# Grid search for best hyperparameters
param_grid = {
    "n_estimators": [100, 200],
    "max_depth": [3, 5, 7],
    "learning_rate": [0.01, 0.05, 0.1],
    "subsample": [0.7, 0.8, 1.0],
    "colsample_bytree": [0.7, 0.9, 1.0],
    'scale_pos_weight': [1, scale_pos_weight, scale_pos_weight * 1.5],
}

start = time.time()

grid_search = GridSearchCV(
    XGBClassifier(eval_metric="logloss", random_state=42, scale_pos_weight=scale_pos_weight),
    param_grid,
    cv=3,
    scoring="roc_auc",
    verbose=1,
    n_jobs=-1,
)
grid_search.fit(X_train, y_train)
model = grid_search.best_estimator_

# Predict
y_pred = model.predict(X_val)
y_proba = model.predict_proba(X_val)[:, 1]
prediction_confidence = np.maximum(y_proba, 1 - y_proba)

# Evaluate all predictions (not just confident ones)
accuracy = accuracy_score(y_val, y_pred)
roc_auc = roc_auc_score(y_val, y_proba)
print("\n--- Model Performance (All Predictions) ---")
print(f"Best Parameters: {grid_search.best_params_}")
print(f"Accuracy: {accuracy:.3f}")
print(f"ROC-AUC: {roc_auc:.3f}")
print("\nClassification Report:")
print(classification_report(y_val, y_pred))

joblib.dump((model, numeric_feature_cols), "z_model1.pkl")

end = time.time()

print(f"🔍 Prediction took {end - start:.2f} seconds.")

# ====================== CONFIDENT PREDICTION EVALUATION ==============================

# Create validation DataFrame with predictions

val_df = pd.concat([X_val.reset_index(drop=True), y_val.reset_index(drop=True)], axis=1)
val_df["model_prediction"] = y_pred
val_df["prediction_confidence"] = prediction_confidence

val_df["model_prediction"] = y_pred
val_df["prediction_confidence"] = prediction_confidence

thres = 0.65

# Filter for confident predictions
confident_df = val_df[val_df["prediction_confidence"] >= thres]

print(f"\n--- Performance for Confidence ≥ {thres} ---")
print(f"Total qualifying games: {len(confident_df)}")

if not confident_df.empty:
    confident_accuracy = accuracy_score(confident_df["label_over_under"], confident_df["model_prediction"])
    print(f"Accuracy: {confident_accuracy:.3f}")

    print("\nLabel Distribution (True Outcomes):")
    print(confident_df["label_over_under"].value_counts())

    print("\nPrediction Distribution:")
    print(confident_df["model_prediction"].value_counts())

    # ROI Simulation
    print(f"\n--- ROI Simulation (Confidence ≥ {thres}) ---")
    bet_unit = 100
    decimal_odds = 1.91
    wins = (confident_df["model_prediction"] == confident_df["label_over_under"]).sum()
    losses = len(confident_df) - wins
    profit = (wins * bet_unit * (decimal_odds - 1)) - (losses * bet_unit)
    roi = profit / (len(confident_df) * bet_unit)

    print(f"Total Bets: {len(confident_df)}")
    print(f"Wins: {wins}, Losses: {losses}")
    print(f"Profit: ${profit:.2f}")
    print(f"ROI: {roi * 100:.2f}%")
else:
    print("No confident predictions found.")


