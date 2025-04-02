import pandas as pd
import joblib
import os
import pytz
import csv
from datetime import datetime
from model.data_cleaner import clean_input_data

# Load model and features
model = joblib.load('model/xgb_model.pkl')
with open("model/final_features.txt", "r") as f:
    expected_features = [line.strip() for line in f.readlines()]

# Load and filter today's data
df = pd.read_parquet('model/currentdata.parquet')
df['game_date'] = pd.to_datetime(df['game_date'])

local_tz = pytz.timezone("America/Los_Angeles")
today = datetime.now(local_tz).strftime('%Y-%m-%d')
now = datetime.now(local_tz)

todays_games = df[df['game_date'].dt.strftime('%Y-%m-%d') == today]

if todays_games.empty:
    print("No games found for today.")
    exit()

# Clean (no drop_today)
X_cleaned, _ = clean_input_data(todays_games)
X_cleaned = X_cleaned[expected_features]

# Predict
predictions = model.predict(X_cleaned)

# Output results
todays_games['prediction'] = predictions
todays_games['pick'] = todays_games['prediction'].map({1: 'Over', 0: 'Under'})

output_df = todays_games[['game_id', 'home_name', 'away_name', 'over_under_runline', 'pick']].copy()
output_df.columns = ['game_id', 'home_team', 'away_team', 'runline', 'pick']

output_dir = 'mlb-app/src/app/api/picks'
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, f"{today}.csv")
output_df.to_csv(output_file, index=False)

# Timestamp file
with open('mlb-app/public/last_updated.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['last_updated'])
    writer.writerow([now])

print(f"Predictions saved to {output_file}")
