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


    for year in desired_seasons:

        if desired_seasons.index(year) < len(desired_seasons)-1:
            schedule = statsapi.schedule(start_date=f'{year}-01-01', end_date=f'{year}-12-31')

            for game in schedule:
                row = {field: game[field] for field in data_fields}
                ids_data.append(row)

            ids = pd.DataFrame(ids_data, columns=data_fields)
        else:
            schedule = statsapi.schedule(start_date=f'{year}-01-01', end_date=today)

            for game in schedule:
                row = {field: game[field] for field in data_fields}
                ids_data.append(row)

            ids = pd.DataFrame(ids_data, columns=data_fields)

    ids.to_csv('game_pks.csv')

get_game_pks() 