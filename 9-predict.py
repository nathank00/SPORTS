import pandas as pd
from datetime import datetime
import joblib
import xgboost as xgb
import os
import pytz
import csv
from sklearn.base import BaseEstimator, TransformerMixin

print("\n---------- Now running 9-predict.py ----------\n")

# -------------------------------------------------------- REQUIRED FOR MODEL LOADING --------------------------------------------------------

class CorrelationFilter(BaseEstimator, TransformerMixin):
    def __init__(self, threshold=0.95):
        self.threshold = threshold
        self.to_drop = []

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        return X

# -------------------------------------------------------- LOAD & PREDICT --------------------------------------------------------

df = pd.read_parquet('model/currentdata.parquet')
df['game_date'] = pd.to_datetime(df['game_date'])
df.loc[df['over_under_runline'] == df['runs_total'], 'over_under_target'] = 1

local_tz = pytz.timezone("America/Los_Angeles")
today = datetime.now(local_tz).strftime('%Y-%m-%d')
now = datetime.now(local_tz)

todays_games = df[df['game_date'].dt.strftime('%Y-%m-%d') == today]

if todays_games.empty:
    print("No games found for today.")
else:
    # Load trained model and feature list
    xgb_model = joblib.load('model/xgb_model.pkl')
    with open("model/final_features.txt", "r") as f:
        expected_features = [line.strip() for line in f.readlines()]

    # Reindex input to ensure matching structure
    X_todays_games = todays_games.reindex(columns=expected_features, fill_value=0)

    # Predict
    predictions = xgb_model.predict(X_todays_games)
    todays_games['prediction'] = predictions
    todays_games['pick'] = todays_games['prediction'].map({1: 'Over', 0: 'Under'})

    output_df = todays_games[['game_id', 'home_name', 'away_name', 'over_under_runline', 'pick']].copy()
    output_df.columns = ['game_id', 'home_team', 'away_team', 'runline', 'pick']

    batter_columns = [f'Away_Batter{i}_Name' for i in range(1, 10)] + \
                     [f'Home_Batter{i}_Name' for i in range(1, 10)] + \
                     ['Away_SP_Name', 'Home_SP_Name']

    for col in batter_columns:
        output_df[col] = todays_games[col] if col in todays_games.columns else 'N/A'

    output_dir = 'mlb-app/src/app/api/picks'
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"{today}.csv")
    output_df.to_csv(output_file, index=False)

    with open('mlb-app/public/last_updated.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['last_updated'])
        writer.writerow([now])

    print(f"Predictions saved to {output_file}")
