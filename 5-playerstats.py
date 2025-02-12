import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
from datetime import date, timedelta
from pybaseball import playerid_reverse_lookup
import statsapi
import os
from dateutil import parser

def fetch_b_game_log(player_id, year):
    url = f'https://www.baseball-reference.com/players/gl.fcgi?id={player_id}&t=b&year={year}'
    response = requests.get(url)
    
    if response.status_code != 200:
        print(f" BAD - Failed to fetch data for batter {player_id} in {year}")
        return None

    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table', {'id': 'batting_gamelogs'})

    if table is None:
        print(f"No data found for batter {player_id} in {year} - OK")
        return None

    df = pd.read_html(str(table))[0]
    df = df[pd.to_numeric(df['Rk'], errors='coerce').notnull()]
    df['Date'] = df['Date'].apply(lambda x: f"{x}, {year}" if '(' not in x else x)
    df['dbl'] = df['Date'].str.extract(r'\((\d+)\)').astype(float)
    df.loc[df['dbl'].notnull(), 'Date'] = df['Date'] + ', ' + str(year)
    df['game_date'] = pd.to_datetime(df['Date'], errors='coerce').dt.strftime('%Y-%m-%d')

    return df

def fetch_p_game_log(player_id, year):
    url = f'https://www.baseball-reference.com/players/gl.fcgi?id={player_id}&t=p&year={year}'
    response = requests.get(url)

    if response.status_code != 200:
        print(f" BAD - Failed to fetch data for pitcher {player_id} in {year}")
        return None

    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table', {'id': 'pitching_gamelogs'})

    if table is None:
        print(f"No data found for pitcher {player_id} in {year} - OK")
        return None

    df = pd.read_html(str(table))[0]
    df = df[pd.to_numeric(df['Rk'], errors='coerce').notnull()]
    df['Date'] = df['Date'].apply(lambda x: f"{x}, {year}" if '(' not in x else x)
    df['dbl'] = df['Date'].str.extract(r'\((\d+)\)').astype(float)
    df.loc[df['dbl'].notnull(), 'Date'] = df['Date'] + ', ' + str(year)
    df['game_date'] = pd.to_datetime(df['Date'], errors='coerce').dt.strftime('%Y-%m-%d')

    return df

def run_player_stats_update(days_lookback=3):
    today = date.today()
    start_date = (today - timedelta(days=days_lookback)).strftime('%Y-%m-%d')
    end_date = today.strftime('%Y-%m-%d')

    print(f"INFO - Fetching active players from {start_date} to {end_date}")

    def get_active_player_ids(game_data):
        active_batters, active_pitchers = set(), set()

        for game in game_data:
            game_id = game['game_id']
            boxscore = statsapi.boxscore_data(game_id)

            for team_key in ['away', 'home']:
                if team_key in boxscore:
                    team_data = boxscore[team_key]
                    active_batters.update(team_data.get('batters', []))
                    active_pitchers.update(team_data.get('pitchers', []))

        return list(active_batters), list(active_pitchers)

    recent_games = statsapi.schedule(start_date=start_date, end_date=end_date)
    active_batter_ids, active_pitcher_ids = get_active_player_ids(recent_games)

    def get_bbref_ids(player_ids):
        player_data = playerid_reverse_lookup(player_ids, key_type='mlbam')
        return player_data[['key_mlbam', 'key_bbref']]

    active_batter_data = get_bbref_ids(active_batter_ids)
    active_pitcher_data = get_bbref_ids(active_pitcher_ids)

    active_batter_data.to_csv('active_batter_ids.csv', index=False)
    active_pitcher_data.to_csv('active_pitcher_ids.csv', index=False)

    active_batter_ids = pd.read_csv('active_batter_ids.csv')['key_bbref']
    active_pitcher_ids = pd.read_csv('active_pitcher_ids.csv')['key_bbref']
    game_pks = pd.read_csv('game_pks.csv')

    current_year = today.year

    def process_player_data(player_ids, player_type='batter'):
        fetch_game_log = fetch_b_game_log if player_type == 'batter' else fetch_p_game_log

        for id in player_ids:
            if not id or pd.isna(id):
                continue

            player_file_path = f'batters/{id}_batting.csv' if player_type == 'batter' else f'pitchers/{id}_pitching.csv'

            if os.path.exists(player_file_path) and os.path.getsize(player_file_path) > 0:
                player_df = pd.read_csv(player_file_path)
            else:
                player_df = pd.DataFrame()

            new_data_df = fetch_game_log(id, current_year)
            time.sleep(0.2)

            if new_data_df is None or new_data_df.empty:
                continue  

            new_data_df['game_date'] = new_data_df['Date'].apply(lambda date: parser.parse(date).strftime('%Y-%m-%d'))
            new_data_df['Date'] = new_data_df['game_date']

            new_data_df['Date'] = pd.to_datetime(new_data_df['Date'])
            game_pks['game_date'] = pd.to_datetime(game_pks['game_date'])

            new_data_df['game_id'] = None

            for index, row in new_data_df.iterrows():
                game_day_matches = game_pks[
                    (game_pks['game_date'] == row['Date']) &
                    (
                        ((game_pks['home_id'] == row['Tm']) & (game_pks['away_id'] == row['Opp'])) |
                        ((game_pks['home_id'] == row['Opp']) & (game_pks['away_id'] == row['Tm']))
                    )
                ]

                if not game_day_matches.empty:
                    game_id = game_day_matches.iloc[0]['game_id']
                    new_data_df.at[index, 'game_id'] = game_id
                else:
                    print(f"BAD - NO GAME MATCHES FOUND for {id} on {row['Date']}")

            combined_df = pd.concat([player_df, new_data_df]).drop_duplicates(subset=['game_id']) if not player_df.empty else new_data_df
            combined_df.to_csv(player_file_path, index=False)

        print(f'All {player_type} IDs processed and saved')

    process_player_data(active_batter_ids, player_type='batter')
    process_player_data(active_pitcher_ids, player_type='pitcher')

    print('\n=================================================\nPlayer stats updated. Now generating custom stats.\n=================================================')

#Run the function when script is executed
if __name__ == "__main__":
    # Change the number of lookback days here
    run_player_stats_update(days_lookback=105)
