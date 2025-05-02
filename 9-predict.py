import pandas as pd
import joblib
import os
import pytz
import csv
from datetime import datetime
from model.data_cleaner import clean_input_data

# Load model and expected features
model = joblib.load('model/xgb_model.pkl')
with open("model/final_features.txt", "r") as f:
    expected_features = [line.strip() for line in f.readlines()]

# Load today's data
df = pd.read_parquet('model/currentdata.parquet')
df['game_date'] = pd.to_datetime(df['game_date'])

# Filter today's games
local_tz = pytz.timezone("America/Los_Angeles")
today = datetime.now(local_tz).strftime('%Y-%m-%d')
now = datetime.now(local_tz)
todays_games = df[df['game_date'].dt.strftime('%Y-%m-%d') == today]

if todays_games.empty:
    print("No games found for today.")
    exit()

# === Clean & Prepare Features ===
X_cleaned, _ = clean_input_data(todays_games)
X_cleaned = X_cleaned[expected_features]

# Diagnostics
print("\n✅ Feature Summary:")
print(X_cleaned.describe())

print("\n✅ Missing Values:")
print(X_cleaned.isnull().sum()[X_cleaned.isnull().sum() > 0])

non_numeric = X_cleaned.select_dtypes(exclude=['number']).columns.tolist()
if non_numeric:
    print("\n⚠️ Non-numeric columns detected:", non_numeric)

# Predict
filtered_games = todays_games.loc[X_cleaned.index].copy()
probas = model.predict_proba(X_cleaned)[:, 1]
predictions = model.predict(X_cleaned)

# Map predictions
filtered_games['prediction'] = predictions
filtered_games['prob_over'] = probas
filtered_games['pick'] = filtered_games['prediction'].map({1: 'Over', 0: 'Under'})

# Preview predictions
print("\n✅ Prediction Preview:")
print(filtered_games[['home_name', 'away_name', 'prediction', 'prob_over', 'pick']])

# Core output
output_df = filtered_games[['game_id', 'home_name', 'away_name', 'over_under_runline', 'pick']].copy()
output_df.columns = ['game_id', 'home_team', 'away_team', 'runline', 'pick']

# Add batter and pitcher names
batter_cols = (
    [f"Away_Batter{i}_Name" for i in range(1, 10)] +
    [f"Home_Batter{i}_Name" for i in range(1, 10)] +
    ['Away_SP_Name', 'Home_SP_Name']
)

for col in batter_cols:
    output_df[col] = filtered_games[col] if col in filtered_games.columns else 'N/A'

# Write prediction output files
for path in ['mlb-app/src/app/api/picks', 'mlb-app/public/data']:
    os.makedirs(path, exist_ok=True)
    output_file = os.path.join(path, f"{today}.csv")
    output_df.to_csv(output_file, index=False)

# Write last_updated timestamp
formatted_time = now.strftime('%Y-%m-%d %H:%M PST')
with open('mlb-app/public/last_updated.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['last_updated'])
    writer.writerow([formatted_time])

print(f"\n✅ Predictions saved to {output_file}")
