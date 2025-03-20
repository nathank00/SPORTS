import requests
import pandas as pd
from pybaseball import playerid_reverse_lookup

# MLB API endpoint
MLB_PLAYERS_URL = "https://statsapi.mlb.com/api/v1/sports/1/players"

# Function to get active players for a given year
def get_players_by_year(season):
    response = requests.get(MLB_PLAYERS_URL, params={"season": season})
    
    if response.status_code == 200:
        players = response.json().get("people", [])
        print(f"Fetched {len(players)} players from {season}")
        return players
    else:
        print(f"Failed to fetch players for {season}. Status: {response.status_code}")
        return []

# Function to convert API response to DataFrame and separate Batters/Pitchers
def players_to_dataframe(players):
    batters, pitchers, twp_players = [], [], []

    for player in players:
        player_id = player.get("id") or player.get("mlbID")
        full_name = player.get("fullName") or player.get("Name")
        primary_position = player.get("primaryPosition", {}).get("abbreviation", player.get("Position", "UNK"))

        # If player is a batter, add to batters list
        if primary_position not in ["P", "SP", "RP"]:
            batters.append([player_id, full_name, primary_position])

        # If player is a pitcher, add to pitchers list
        if primary_position in ["P", "SP", "RP"]:
            pitchers.append([player_id, full_name, primary_position])

        # 🚨 FORCE DUPLICATION: If player is a TWP, explicitly add to both lists 🚨
        if primary_position == "TWP":
            twp_players.append([player_id, full_name, primary_position])
            pitchers.append([player_id, full_name, "TWP"])

    # Convert to DataFrames
    df_batters = pd.DataFrame(batters, columns=["mlbID", "Name", "Position"])
    df_pitchers = pd.DataFrame(pitchers, columns=["mlbID", "Name", "Position"])

    return df_batters, df_pitchers

# Dictionary to store unique players across all years
player_records = {}

# Fetch players for multiple years and store unique ones
for year in range(2021, 2026):  # 2021 to 2025
    year_players = get_players_by_year(year)
    
    for player in year_players:
        player_id = player["id"]
        full_name = player["fullName"]
        primary_position = player.get("primaryPosition", {}).get("abbreviation", "UNK")

        if player_id not in player_records:
            player_records[player_id] = {"mlbID": player_id, "Name": full_name, "Position": primary_position}

# Convert stored player data to DataFrame
df_players = pd.DataFrame.from_dict(player_records, orient="index")

# Separate batters and pitchers, ensuring TWP players are in both lists
df_batters, df_pitchers = players_to_dataframe(df_players.to_dict(orient="records"))


# Function to batch-fetch BBRef IDs before sorting
def update_bbref_ids(df, player_type, batch_size=100):
    print(f"Fetching BBRef IDs for {player_type} in batches of {batch_size}...")
    player_ids = df["mlbID"].tolist()
    
    df["key_bbref"] = None  # Initialize column before lookup
    
    for i in range(0, len(player_ids), batch_size):
        batch = player_ids[i:i + batch_size]
        try:
            bbref_data = playerid_reverse_lookup(batch, key_type="mlbam")
            for player_id in batch:
                match = bbref_data[bbref_data["key_mlbam"] == player_id]
                if not match.empty:
                    df.loc[df["mlbID"] == player_id, "key_bbref"] = match.iloc[0]["key_bbref"]
        except Exception as e:
            print(f"Error in batch {i // batch_size + 1}: {e}")

# Fetch BBRef IDs BEFORE sorting
update_bbref_ids(df_batters, "Batter")
update_bbref_ids(df_pitchers, "Pitcher")

# Sort by BBRef ID (A to Z)
df_batters = df_batters.sort_values(by="key_bbref")
df_pitchers = df_pitchers.sort_values(by="key_bbref")

# Save final output
df_batters.to_csv("batter_ids.csv", index=False)
df_pitchers.to_csv("pitcher_ids.csv", index=False)

print(f"\n✅ Final Output: {len(df_batters)} batters in 'batter_ids.csv'")
print(f"✅ Final Output: {len(df_pitchers)} pitchers in 'pitcher_ids.csv'")

# 🚨 **Final Verification: Read the saved files and search for Two-Way Players** 🚨
df_batters_final = pd.read_csv("batter_ids.csv")
df_pitchers_final = pd.read_csv("pitcher_ids.csv")

# Identify TWPs in batters
twp_batters = df_batters_final[df_batters_final["Position"] == "TWP"]
print("Batter TWPs:\n", twp_batters)

# Identify TWPs in pitchers
twp_pitchers = df_pitchers_final[df_pitchers_final["Position"] == "TWP"]
print("Pitcher TWPs:\n", twp_pitchers)