import os
import re
import pandas as pd
from pybaseball import statcast_batter, statcast_pitcher, playerid_lookup, pitching_stats_range, batting_stats_range, schedule_and_record, team_game_logs, pybaseball
from datetime import timedelta, datetime
import statsapi
import pprint

today = datetime.now()
end_date = today.strftime('%Y-%m-%d')


season = today.year


def get_game_pks():
    
    desired_seasons = [2021, 2022, 2023, 2024, 2025] # Add Desired Seasons Here

    data_fields = ['game_date', 'game_id', 'away_name', 'away_id', 'home_name', 'home_id']

    ids_data = []

    today = datetime.now().strftime('%Y-%m-%d')

    total_count = 0 

    for year in desired_seasons:
        year_count = 0 
        if desired_seasons.index(year) < len(desired_seasons) - 1:
            schedule = statsapi.schedule(start_date=f'{year}-01-01', end_date=f'{year}-12-31')
        else:
            schedule = statsapi.schedule(start_date=f'{year}-01-01', end_date=today)

        print(f"\n===============================\nProcessing {year}\n===============================")
        for game in schedule:
            row = {field: game[field] for field in data_fields}
            ids_data.append(row)
            total_count += 1  
            year_count += 1

            #if total_count % 1000 == 0:  
            #    print(f"{total_count}")

        print(f"{year} Processed - {year_count} games. \nTotal Games: {total_count}")

    # Convert collected data to DataFrame
    ids = pd.DataFrame(ids_data, columns=data_fields)

    # Save to CSV
    file = 'game_pks.csv'
    ids.to_csv(file, index=False)

    print("\n\n-----------------------------------")
    print(f"Total games processed: {total_count}")
    print(f"Games outputted to '{file}'")
    print("SUCCESS")
    print("-----------------------------------\n")

get_game_pks()