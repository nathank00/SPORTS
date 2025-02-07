import pandas as pd
from statsapi import player_stats
from pybaseball import (
    playerid_lookup,
    batting_stats_range,
    pitching_stats_range,
    playerid_reverse_lookup,
)
from datetime import datetime


def get_player_ids():
    start_date = "2021-01-01"
    today = datetime.now().strftime("%Y-%m-%d")

    # Fetch batting and pitching stats
    batting = batting_stats_range(start_date, today)
    pitching = pitching_stats_range(start_date, today)

    # Prepare batter data
    batter_ids = batting[["Name", "Tm", "mlbID"]].copy()
    batter_ids["key_bbref"] = None

    # Loop through each player ID and fetch their bbref ID
    for idx, row in batter_ids.iterrows():
        try:
            batterdata = playerid_reverse_lookup([row["mlbID"]], key_type="mlbam")
            if not batterdata.empty:
                batter_ids.at[idx, "key_bbref"] = batterdata.iloc[0]["key_bbref"]
            else:
                print(f"No BBref ID found for mlbID: {row['mlbID']}, {row['Name']} (Batter)")
        except Exception as e:
            print(f"Error processing mlbID: {row['mlbID']}, Error: {e}")

    # Prepare pitcher data
    pitching["mlbID"] = pitching["mlbID"].astype(int)
    pitcher_ids = pitching[["Name", "Tm", "mlbID"]].copy()
    pitcher_ids["key_bbref"] = None

    # Loop through each player ID and fetch their bbref ID
    for idx, row in pitcher_ids.iterrows():
        try:
            pitcherdata = playerid_reverse_lookup([row["mlbID"]], key_type="mlbam")
            if not pitcherdata.empty:
                pitcher_ids.at[idx, "key_bbref"] = pitcherdata.iloc[0]["key_bbref"]
            else:
                print(f"No BBref ID found for mlbID: {row['mlbID']}, {row['Name']} (Pitcher)")
        except Exception as e:
            print(f"Error processing mlbID: {row['mlbID']}, Error: {e}")

    # Count batters and pitchers
    num_batters = len(batter_ids)
    num_pitchers = len(pitcher_ids)

    # Save to CSV
    batter_ids.to_csv("batter_ids.csv", index=False)
    pitcher_ids.to_csv("pitcher_ids.csv", index=False)

    print(
        f"\n=================================================\n"
        f"All Player IDs Updated\n"
        f"Batters ({num_batters}): 'batter_ids.csv'\n"
        f"Pitchers ({num_pitchers}): 'pitcher_ids.csv'\n"
        f"=================================================\n"
    )


# Run the function
get_player_ids()
