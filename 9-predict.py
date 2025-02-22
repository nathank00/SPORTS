import pandas as pd
from datetime import datetime
import joblib
import xgboost as xgb

# Load the dataset
df = pd.read_csv('model/currentdata.csv')

# Convert 'game_date' column to datetime objects
df['game_date'] = pd.to_datetime(df['game_date'])

# Change the 'push' games to equal 1.
df.loc[df['over_under_runline'] == df['runs_total'], 'over_under_target'] = 1

# Get today's date
today = datetime.today().strftime('%Y-%m-%d')

# Separate the data for today's games
todays_games = df[df['game_date'] == today]

# Check if there are any games today
if todays_games.empty:
    print("No games found for today.")
else:
    # Define the columns to drop
    columns_to_drop = [col for col in df.columns if 'Name' in col or 'ID' in col or '_P_' in col or '12' in col or '13' in col or '14' in col or '15'in col ]
    columns_to_drop.extend(['over_under_target', 'runs_total', 'game_date', 'runs_home', 'runs_away', 'game_id', 'home_name', 'away_name']) 

    # Drop the unnecessary columns
    X_todays_games = todays_games.drop(columns=columns_to_drop)

    # Load the trained model (assuming it's saved as 'model.pkl')
    xgb_model = joblib.load('model/xgb_model.pkl')

# Make predictions
predictions = xgb_model.predict(X_todays_games)

# Interpret and display the predictions
todays_games['prediction'] = predictions
for i, row in todays_games.iterrows():
    result = 'Over' if row['prediction'] == 1 else 'Under'
    print(f"Game {i + 2}: {result} the runline")

# ---------- CREATE CSV -----------

import os

# Map predictions to "Over" or "Under"
todays_games['pick'] = todays_games['prediction'].map({1: 'Over', 0: 'Under'})

# Create the output DataFrame
output_df = todays_games[['game_id', 'home_name', 'away_name', 'over_under_runline', 'pick']].copy()

# Rename columns to match required format
output_df.columns = ['game_id', 'home_team', 'away_team', 'runline', 'pick']

# Define the file path
output_dir = 'mlb-app/src/app/api/picks'
os.makedirs(output_dir, exist_ok=True)  # Ensure the directory exists

# Define the filename with today's date
output_file = os.path.join(output_dir, f"{today}.csv")

# Save to CSV
output_df.to_csv(output_file, index=False)

print(f"Predictions saved to {output_file}")
