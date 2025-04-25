import os
import joblib
import numpy as np
import pandas as pd
from xata.client import XataClient

# Load model and Xata credentials
model, expected_feature_order = joblib.load("z_model1.pkl")
xata = XataClient(api_key=os.getenv("XATA_API_KEY"), db_url=os.getenv("XATA_DB_URL"))

print("\n>> Fetching games from 'games' table...")

# Step 1: Fetch all games from the 'games' table
games_records = []
page = None
while True:
    query = {
        "columns": ["game_id"] + [f"{team}_{i}_id" for team in ["away", "home"] for i in range(1, 10)] +
                   ["game_started", "game_complete"],
        "page": {"size": 1000}
    }
    if page:
        query["page"]["after"] = page
    resp = xata.data().query("games", query)
    if not resp.is_success():
        raise Exception("❌ Failed to fetch data from Xata (games table)")
    games_records.extend(resp.get("records", []))
    page = resp.get("meta", {}).get("page", {}).get("cursor")
    records = resp.get("records", [])
    if not records:
        break
    if not page:
        break


games_df = pd.DataFrame(games_records)

# Step 2: Filter for games that are not started, not completed, and have full lineups
def has_full_lineup(row):
    return all(pd.notnull(row.get(f"{team}_{i}_id")) for team in ["away", "home"] for i in range(1, 10))

eligible_games_df = games_df[
    (games_df["game_started"] == False) &
    (games_df["game_complete"] == False) &
    games_df.apply(has_full_lineup, axis=1)
]

eligible_game_ids = eligible_games_df["game_id"].tolist()
print(f"[INFO] Found {len(eligible_game_ids)} eligible games.")

if not eligible_game_ids:
    print("⚠️ No eligible games to run model on.")
    exit()

# Step 3: Fetch corresponding rows from 'master'

query = {
    "filter": {
        "game_id": {"$any": eligible_game_ids}
    },
    "columns": ["*"]
}

resp = xata.data().query("master", query)

if not resp.is_success():
    print("❌ Error while fetching data from 'master' table:")
    print("Status Code:", resp.status_code)
    try:
        print("Response JSON:", resp.json())
    except Exception as e:
        print("Failed to parse response JSON:", e)
        print("Raw response text:", resp.text)
    raise Exception("❌ Failed to fetch data from Xata (master table)")

master_records = resp.get("records", [])


print(f"[INFO] Retrieved {len(master_records)} master records.")

# Step 4: Make predictions
df = pd.DataFrame(master_records)
exclude_cols = [
    "label_over_under", "model_prediction", "prediction_confidence",
    "game_id", "game_date", "start_time", "id", "xata","runs_home", "description",
    "runs_away", "total_runs_scored", "runline", "game_started", "game_complete"
]
feature_cols = [col for col in df.columns if col not in exclude_cols]
numeric_feature_cols = [
    col for col in feature_cols if df[col].dtype in ["int64", "float64", "bool"]
]

X = df[numeric_feature_cols]
X = df[expected_feature_order]
print("\n>> Running model predictions...")
y_pred = model.predict(X)
y_proba = model.predict_proba(X)[:, 1]
prediction_confidence = np.maximum(y_proba, 1 - y_proba)

# Step 5: Update master table (only if prediction and confidence exist)
print("\n>> Updating eligible game records in 'master' table...")
updates = 0
for i, row in df.iterrows():
    record_id = row.get("id")
    pred = y_pred[i]
    conf = round(float(prediction_confidence[i]), 3)

    # Safety check: Only update if still eligible and prediction was made
    if record_id is not None and pred is not None and conf is not None:
        xata.records().update("master", record_id, {
            "model_prediction": int(pred),
            "prediction_confidence": conf
        })
        updates += 1

print(f"\n✅ Finished - Updated {updates} eligible games with predictions.")
