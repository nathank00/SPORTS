import requests
from bs4 import BeautifulSoup
import pandas as pd
import pprint
import re 
from dateutil import parser
import time
from datetime import date, datetime, timedelta
from pybaseball import batting_stats_range, pitching_stats_range, playerid_reverse_lookup
import statsapi
import os

def fetch_b_game_log(player_id, year):
    # Construct the URL for the batter's game log for the given year
    url = f'https://www.baseball-reference.com/players/gl.fcgi?id={player_id}&t=b&year={year}'
    response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code != 200:
        print(f" BAD - Failed to fetch data for batter {player_id} in {year}")
        return None
    
    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')
    # Find the table containing the game logs
    table = soup.find('table', {'id': 'batting_gamelogs'})
    
    # Check if the table is found
    if table is None:
        print(f"No data found for batter {player_id} in {year} - OK")
        return None
    
    # Read the table into a pandas DataFrame
    df = pd.read_html(str(table))[0]
    
    # Remove rows where 'Rk' is not a number (header rows that repeat in the table)
    df = df[pd.to_numeric(df['Rk'], errors='coerce').notnull()]
    
    # Add the year to the 'Date' column if the year is not already present
    df['Date'] = df['Date'].apply(lambda x: f"{x}, {year}" if '(' not in x else x)
    
    # Extract the value from parentheses (if present) and assign it to a new column 'dbl'
    df['dbl'] = df['Date'].str.extract(r'\((\d+)\)').astype(float)
    
    # Add the year to the 'Date' column for doubleheader dates
    df.loc[df['dbl'].notnull(), 'Date'] = df['Date'] + ', ' + str(year)
    
    # Format 'Date' to 'game_date' in YYYY-MM-DD format
    df['game_date'] = pd.to_datetime(df['Date'], errors='coerce').dt.strftime('%Y-%m-%d')
    
    return df

def fetch_p_game_log(player_id, year):
    # Construct the URL for the pitcher's game log for the given year
    url = f'https://www.baseball-reference.com/players/gl.fcgi?id={player_id}&t=p&year={year}'
    response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code != 200:
        print(f" BAD - Failed to fetch data for pitcher {player_id} in {year}")
        return None
    
    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')
    # Find the table containing the game logs
    table = soup.find('table', {'id': 'pitching_gamelogs'})
    
    # Check if the table is found
    if table is None:
        print(f"No data found for pitcher {player_id} in {year} - OK")
        return None
    
    # Read the table into a pandas DataFrame
    df = pd.read_html(str(table))[0]
    
    # Remove rows where 'Rk' is not a number (header rows that repeat in the table)
    df = df[pd.to_numeric(df['Rk'], errors='coerce').notnull()]
    
    # Add the year to the 'Date' column if the year is not already present
    df['Date'] = df['Date'].apply(lambda x: f"{x}, {year}" if '(' not in x else x)
    
    # Extract the value from parentheses (if present) and assign it to a new column 'dbl'
    df['dbl'] = df['Date'].str.extract(r'\((\d+)\)').astype(float)
    
    # Add the year to the 'Date' column for doubleheader dates
    df.loc[df['dbl'].notnull(), 'Date'] = df['Date'] + ', ' + str(year)
    
    # Format 'Date' to 'game_date' in YYYY-MM-DD format
    df['game_date'] = pd.to_datetime(df['Date'], errors='coerce').dt.strftime('%Y-%m-%d')
    
    return df

# Function to clean and parse dates
def clean_date(date_str, year):
    try:
        # Replace invisible characters like U+00A0 with a space
        date_str = date_str.replace('\xa0', ' ')
        # Remove any null characters and non-printable characters
        date_str = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', date_str)
        # Remove unwanted characters and extra text like "(1)" or "susp"
        date_str = re.sub(r'\(.*?\)', '', date_str)  # Remove text inside parentheses
        date_str = ''.join(char for char in date_str if char.isalnum() or char.isspace() or char == ',')
        # Remove specific unwanted words like "susp"
        date_str = date_str.replace('susp', '').strip()
        # Parse the cleaned string to a date object
        parsed_date = parser.parse(date_str)
        # Force the year to be 2021
        parsed_date = parsed_date.replace(year=year)
        # Format the date to 'YYYY-MM-DD'
        #print(parsed_date)
        return parsed_date.strftime('%Y-%m-%d')
    except Exception as e:
        # Print the error for debugging purposes
        print(f"Error parsing date '{date_str}': {e}")
        # Handle any parsing errors by returning None
        return None

# Get today's date
today = date.today()

# Define end date
end_date = today.strftime('%Y-%m-%d')

def get_active_player_ids(game_data):
    active_batters = set()  # Use a set to avoid duplicates
    active_pitchers = set()  # Use a set to avoid duplicates
    
    for game in game_data:
        game_id = game['game_id']
        boxscore = statsapi.boxscore_data(game_id)
        
        for team_key in ['away', 'home']:
            if team_key in boxscore:
                team_data = boxscore[team_key]
                if 'batters' in team_data:
                    active_batters.update(team_data['batters'])
                if 'pitchers' in team_data:
                    active_pitchers.update(team_data['pitchers'])
                    
    return list(active_batters), list(active_pitchers)

# Get recent games
recent_games = statsapi.schedule(start_date=(today - timedelta(days=3)).strftime('%Y-%m-%d'), end_date=end_date) ##################################################### CHANGE THIS #################

# Get active batters and pitchers
active_batter_ids, active_pitcher_ids = get_active_player_ids(recent_games)

# Use playerid_reverse_lookup to get bbref_id
def get_bbref_ids(player_ids):
    player_data = playerid_reverse_lookup(player_ids, key_type='mlbam')
    return player_data[['key_mlbam', 'key_bbref']]

# Get bbref IDs for active batters and pitchers
active_batter_data = get_bbref_ids(active_batter_ids)
active_pitcher_data = get_bbref_ids(active_pitcher_ids)

# Save to CSV
active_batter_data.to_csv('active_batter_ids.csv', index=False)
active_pitcher_data.to_csv('active_pitcher_ids.csv', index=False)

# Load active player IDs
active_batter_ids = pd.read_csv('active_batter_ids.csv')['key_bbref']
active_pitcher_ids = pd.read_csv('active_pitcher_ids.csv')['key_bbref']

# Load game Pks
game_pks = pd.read_csv('game_pks.csv')

# Define the mapping from abbreviated team names to full team names
team_id_mapping = {
    'WSN': 120, 'MIA': 146, 'TBR': 139, 'ATL': 144, 'TEX': 140, 'HOU': 117,
    'SD': 135, 'SDP': 135, 'PHI': 143, 'BAL': 110, 'SEA': 136, 'NYM': 121,
    'ARI': 109, 'LAA': 108, 'OAK': 133, 'TOR': 141, 'CLE': 114, 'STL': 138,
    'MIN': 142, 'DET': 116, 'NYY': 147, 'SFG': 137, 'KCR': 118, 'CWS': 145,
    'CHW': 145, 'COL': 115, 'BOS': 111, 'LAD': 119, 'CHC': 112, 'MIL': 158,
    'CIN': 113, 'PIT': 134
}

# Define the current year
current_year = 2025

# Function to process and save player data
def process_player_data(player_ids, player_type='batter'):
    fetch_game_log = fetch_b_game_log if player_type == 'batter' else fetch_p_game_log
    
    for id in player_ids:
        if not id or pd.isna(id):
            continue

         # Load the existing player data if it exists
        player_file_path = f'{player_type}s/{id}_{player_type}ing.csv'
        if player_type == 'batter':
            player_file_path = f'batters/{id}_batting.csv'
        elif player_type == 'pitcher':
            player_file_path = f'pitchers/{id}_pitching.csv'
            
        if os.path.exists(player_file_path):
            player_df = pd.read_csv(player_file_path)
        else:
            player_df = pd.DataFrame()

        # Fetch data for the current year
        new_data_df = fetch_game_log(id, current_year)
        time.sleep(0.2)

        # Check if the fetched dataframe is None or empty
        if new_data_df is None or new_data_df.empty:
            continue  # Skip if no data available

        # Apply the function to the date_column and create a new column
        new_data_df['game_date'] = new_data_df['Date'].apply(lambda date: clean_date(date, current_year))
        new_data_df['Date'] = new_data_df['game_date']

        # Ensure the 'Date' column in new_data_df and 'game_date' column in game_pks are in datetime format
        new_data_df['Date'] = pd.to_datetime(new_data_df['Date'])
        game_pks['game_date'] = pd.to_datetime(game_pks['game_date'])

        # Map the team abbreviations to full team names
        new_data_df['team_id'] = new_data_df['Tm'].map(team_id_mapping)
        new_data_df['opp_id'] = new_data_df['Opp'].map(team_id_mapping)

        # Initialize a new column in new_data_df for game_id
        new_data_df['game_id'] = None

        # Iterate over the rows in new_data_df to find the corresponding game_id in game_pks
        for index, row in new_data_df.iterrows():
            # Filter the game_pks for the matching date and teams
            game_day_matches = game_pks[
                (game_pks['game_date'] == row['Date']) &
                (
                    ((game_pks['home_id'] == row['team_id']) & (game_pks['away_id'] == row['opp_id'])) |
                    ((game_pks['home_id'] == row['opp_id']) & (game_pks['away_id'] == row['team_id']))
                )
            ]

            # Check the 'dbl' column to assign the correct game_id
            if not game_day_matches.empty:
                if row['dbl'] == 1:
                    # For the first game of a double-header
                    game_id = game_day_matches.iloc[0]['game_id']
                elif row['dbl'] == 2:
                    # For the second game of a double-header
                    if len(game_day_matches) > 1:
                        game_id = game_day_matches.iloc[1]['game_id']
                    else:
                        game_id = game_day_matches.iloc[0]['game_id']
                else:
                    # For days without double-headers or unmarked double-headers, take the first game
                    game_id = game_day_matches.iloc[0]['game_id']
                new_data_df.at[index, 'game_id'] = game_id
            else:
                print(f"BAD - NO GAME MATCHES FOUND for {id} on {row['Date']}")

        # Concatenate the new data with the existing data, ensuring no duplicates
        if not player_df.empty:
            combined_df = pd.concat([player_df, new_data_df]).drop_duplicates(subset=['game_id'])
        else:
            combined_df = new_data_df

        # Save the updated player data to a CSV file
        combined_df.to_csv(player_file_path, index=False)

    print(f'All {player_type} IDs processed and saved')

# Process batter and pitcher data   
process_player_data(active_batter_ids, player_type='batter')
process_player_data(active_pitcher_ids, player_type='pitcher')

print('\nPlayer stats updated - SUCCESS\n')
