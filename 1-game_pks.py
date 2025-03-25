import requests
import pandas as pd
from datetime import datetime

print("---------- Now running 1-game_pks.py ----------")

# MLB API endpoint for fetching schedule
MLB_SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"

# Get today's date
today = datetime.today().strftime("%Y-%m-%d")
current_year = datetime.today().year

# List to store game data
games_data = []

# Iterate over each season from 2021 to current year
for year in range(2021, current_year + 1):
    end_date = today if year == current_year else f"{year}-12-31"
    
    response = requests.get(MLB_SCHEDULE_URL, params={"sportId": 1, "startDate": f"{year}-01-01", "endDate": end_date})
    
    if response.status_code == 200:
        data = response.json()
        for date in data.get("dates", []):
            for game in date.get("games", []):
                games_data.append([
                    game["officialDate"],
                    str(game["gamePk"])[-6:],  # Ensure 6-digit game_id
                    game["teams"]["away"]["team"]["name"],
                    str(game["teams"]["away"]["team"]["id"]).zfill(3),  # Ensure 3-digit team ID
                    game["teams"]["home"]["team"]["name"],
                    str(game["teams"]["home"]["team"]["id"]).zfill(3)   # Ensure 3-digit team ID
                ])
    else:
        print(f"Failed to fetch data for {year}. Status Code: {response.status_code}")

# Convert to DataFrame
df = pd.DataFrame(games_data, columns=["game_date", "game_id", "away_name", "away_id", "home_name", "home_id"])

# Save to CSV (overwrite)
file = 'game_pks.csv'
df.to_csv(file, index=False)

print(f"\nSUCCESS - File 'game_pks.csv' successfully created on {today}.\n")
