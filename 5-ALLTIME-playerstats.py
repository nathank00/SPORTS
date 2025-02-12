import requests
from bs4 import BeautifulSoup
import pandas as pd
import pprint
import re
from dateutil import parser
import time
import os


# ====================Fetch Batter Game Log Function======================

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
    table = soup.find('table', {'id': 'players_standard_batting'})
    
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


# ====================Fetch Pitcher Game Log Function======================

def fetch_p_game_log(player_id, year):
    # Construct the URL for the pitcher's game log for the given year
    url = f'https://www.baseball-reference.com/players/gl.fcgi?id={player_id}&t=p&year={year}'
    response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code != 200:
        print(f"\n\n\n\n\n\n\n BAD - Failed to fetch data for pitcher {player_id} in {year}\n\n\n\n\n\n\n\n\n\n\n")
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


# ===================== Clean and Parse Dates =============================
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
    

# ========================== FETCH BATTING ==============================

idlist = pd.read_csv('batter_ids.csv')
batter_ids = idlist.key_bbref

game_pks = pd.read_csv('game_pks.csv')

# Define the mapping from abbreviated team names to full team names
team_id_mapping = {
    'WSN': 120,
    'MIA': 146,
    'TBR': 139,
    'ATL' : 144,
    'TEX' : 140,
    'HOU' : 117,
    'SD' : 135,
    'SDP' : 135,
    'PHI' : 143,
    'BAL' : 110,
    'SEA' : 136,
    'NYM' : 121,
    'ARI' : 109,
    'LAA' : 108,
    'OAK' : 133,
    'TOR' : 141,
    'CLE' : 114,
    'STL' : 138,
    'MIN' : 142,
    'DET' : 116,
    'NYY' : 147,
    'SFG' : 137,
    'KCR' : 118,
    'CWS' : 145,
    'CHW' : 145,
    'COL' : 115,
    'BOS' : 111,
    'LAD' : 119,
    'CHC' : 112,
    'MIL' : 158,
    'CIN' : 113,
    'PIT' : 134
}

# Define the years you want to process
years = [2021, 2022, 2023, 2024, 2025]

# Loop through each batter ID
for id in batter_ids:
    # Initialize an empty dataframe for the player
    player_df = pd.DataFrame()

    # Loop through each year
    for year in years:
        # Fetch data for the player and year
        df = fetch_b_game_log(id, year)

        time.sleep(0.4)

        # Check if the fetched dataframe is None or empty
        if df is None or df.empty:
            continue  # Skip this year if no data available

        # Apply the function to the date_column and create a new column
        df['game_date'] = df['Date'].apply(lambda date : clean_date(date, year))
        df['Date'] = df['game_date']

        # Ensure the 'Date' column in df and 'game_date' column in game_pks are in datetime format
        df['Date'] = pd.to_datetime(df['Date'])
        game_pks['game_date'] = pd.to_datetime(game_pks['game_date'])

        # Map the team abbreviations to full team names
        df['team_id'] = df['Team'].map(team_id_mapping)
        df['opp_id'] = df['Opp'].map(team_id_mapping)

        # Initialize a new column in df for game_id
        df['game_id'] = None

        # Iterate over the rows in df to find the corresponding game_id in game_pks
        for index, row in df.iterrows():

            #print(f"Processing row {index}: Date={row['Date']}, Team={row['team_id']}, Opponent={row['opp_id']}")
            

            # Filter the game_pks for the matching date and teams
            game_day_matches = game_pks[
                (game_pks['game_date'] == row['Date']) &
                (
                    ((game_pks['home_id'] == row['team_id']) & (game_pks['away_id'] == row['opp_id'])) |
                    ((game_pks['home_id'] == row['opp_id']) & (game_pks['away_id'] == row['team_id']))
                )
            ]

            
            #print(f"Matches found: {len(game_day_matches)}")
            

            # Check the 'dbl' column to assign the correct game_id
            if not game_day_matches.empty:

                #print(f"Match details: {game_day_matches}")

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
                df.at[index, 'game_id'] = game_id
                #print(f"Assigned game_id: {game_id}")
            else:
                print("BAD - NO GAME MATCHES FOUND (batter).")

        # Append the data for the year to player_df
        player_df = pd.concat([player_df, df])

    # Save the player's data to a CSV file
    player_df.to_csv(f'batters/{id}_batting.csv', index=False)



# ==================== FETCH PITCHING ============================

idlist = pd.read_csv('pitcher_ids.csv')
pitcher_ids = idlist.key_bbref

game_pks = pd.read_csv('game_pks.csv')

# Define the mapping from abbreviated team names to full team names
team_id_mapping = {
    'WSN': 120,
    'MIA': 146,
    'TBR': 139,
    'ATL' : 144,
    'TEX' : 140,
    'HOU' : 117,
    'SD' : 135,
    'SDP' : 135,
    'PHI' : 143,
    'BAL' : 110,
    'SEA' : 136,
    'NYM' : 121,
    'ARI' : 109,
    'LAA' : 108,
    'OAK' : 133,
    'TOR' : 141,
    'CLE' : 114,
    'STL' : 138,
    'MIN' : 142,
    'DET' : 116,
    'NYY' : 147,
    'SFG' : 137,
    'KCR' : 118,
    'CWS' : 145,
    'CHW' : 145,
    'COL' : 115,
    'BOS' : 111,
    'LAD' : 119,
    'CHC' : 112,
    'MIL' : 158,
    'CIN' : 113,
    'PIT' : 134
}

# Define the years you want to process
years = [2021, 2022, 2023, 2024, 2025]

# Loop through each batter ID
for id in pitcher_ids:
    # Initialize an empty dataframe for the player
    player_df = pd.DataFrame()

    # Loop through each year
    for year in years:
        # Fetch data for the player and year
        df = fetch_p_game_log(id, year)

        time.sleep(1)

        # Check if the fetched dataframe is None or empty
        if df is None or df.empty:
            continue  # Skip this year if no data available

        # Apply the function to the date_column and create a new column
        df['game_date'] = df['Date'].apply(lambda date : clean_date(date, year))
        df['Date'] = df['game_date']

        # Ensure the 'Date' column in df and 'game_date' column in game_pks are in datetime format
        df['Date'] = pd.to_datetime(df['Date'])
        game_pks['game_date'] = pd.to_datetime(game_pks['game_date'])

        # Map the team abbreviations to full team names
        df['team_id'] = df['Tm'].map(team_id_mapping)
        df['opp_id'] = df['Opp'].map(team_id_mapping)

        # Initialize a new column in df for game_id
        df['game_id'] = None

        # Iterate over the rows in df to find the corresponding game_id in game_pks
        for index, row in df.iterrows():

            #print(f"Processing row {index}: Date={row['Date']}, Team={row['team_id']}, Opponent={row['opp_id']}")
            

            # Filter the game_pks for the matching date and teams
            game_day_matches = game_pks[
                (game_pks['game_date'] == row['Date']) &
                (
                    ((game_pks['home_id'] == row['team_id']) & (game_pks['away_id'] == row['opp_id'])) |
                    ((game_pks['home_id'] == row['opp_id']) & (game_pks['away_id'] == row['team_id']))
                )
            ]

            
            #print(f"Matches found: {len(game_day_matches)}")
            

            # Check the 'dbl' column to assign the correct game_id
            if not game_day_matches.empty:

                #print(f"Match details: {game_day_matches}")

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
                df.at[index, 'game_id'] = game_id
                #print(f"Assigned game_id: {game_id}")
            else:
                print("BAD - NO GAME MATCHES FOUND (pitcher).")

        # Append the data for the year to player_df
        player_df = pd.concat([player_df, df])

    # Save the player's data to a CSV file
    player_df.to_csv(f'pitchers/{id}_pitching.csv', index=False)


# ================ CHECK FOR EMPTY FILES =====================

idlist = pd.read_csv('pitcher_ids.csv')
pitcher_ids = idlist.key_bbref

game_pks = pd.read_csv('game_pks.csv')

bad_p_ids = []

# Loop through each pitcher ID
for id in pitcher_ids:
    # Skip empty or NaN IDs
    if not id or pd.isna(id):
        continue

    # Define the file path
    player_file_path = f'pitchers/{id}_pitching.csv'
    
    # Check if the file exists and is not empty
    if os.path.exists(player_file_path) and os.path.getsize(player_file_path) > 0:
        try:
            # Attempt to read the first few rows to ensure the file has content
            player_df = pd.read_csv(player_file_path, nrows=5)
            if player_df.empty:
                print(f"File for ID {id} is empty or improperly formatted.")
                player_df = pd.DataFrame()  # Ensure player_df is an empty DataFrame
                continue
            else:
                # Now read the entire file since it's confirmed to have content
                player_df = pd.read_csv(player_file_path)
        except pd.errors.EmptyDataError:
            print(f"File for ID {id} is empty.")
            player_df = pd.DataFrame()
            bad_p_ids.append(id)
            continue
        except pd.errors.ParserError:
            print(f"File for ID {id} is improperly formatted.")
            player_df = pd.DataFrame()
            bad_p_ids.append(id)
            continue

print(bad_p_ids)


# ================= Check Individual Data, Fetch Missing Data ===================

data = fetch_p_game_log('dolisra01', 2021)

pitcher_ids = bad_p_ids

game_pks = pd.read_csv('game_pks.csv')

# Define the mapping from abbreviated team names to full team names
team_id_mapping = {
    'WSN': 120,
    'MIA': 146,
    'TBR': 139,
    'ATL' : 144,
    'TEX' : 140,
    'HOU' : 117,
    'SD' : 135,
    'SDP' : 135,
    'PHI' : 143,
    'BAL' : 110,
    'SEA' : 136,
    'NYM' : 121,
    'ARI' : 109,
    'LAA' : 108,
    'OAK' : 133,
    'TOR' : 141,
    'CLE' : 114,
    'STL' : 138,
    'MIN' : 142,
    'DET' : 116,
    'NYY' : 147,
    'SFG' : 137,
    'KCR' : 118,
    'CWS' : 145,
    'CHW' : 145,
    'COL' : 115,
    'BOS' : 111,
    'LAD' : 119,
    'CHC' : 112,
    'MIL' : 158,
    'CIN' : 113,
    'PIT' : 134
}

# Define the years you want to process
years = [2021, 2022, 2023, 2024, 2025]

# Loop through each batter ID
for id in pitcher_ids:
    # Initialize an empty dataframe for the player
    player_df = pd.DataFrame()

    # Loop through each year
    for year in years:
        # Fetch data for the player and year
        df = fetch_p_game_log(id, year)

        time.sleep(1)

        # Check if the fetched dataframe is None or empty
        if df is None or df.empty:
            continue  # Skip this year if no data available

        # Apply the function to the date_column and create a new column
        df['game_date'] = df['Date'].apply(lambda date : clean_date(date, year))
        df['Date'] = df['game_date']

        # Ensure the 'Date' column in df and 'game_date' column in game_pks are in datetime format
        df['Date'] = pd.to_datetime(df['Date'])
        game_pks['game_date'] = pd.to_datetime(game_pks['game_date'])

        # Map the team abbreviations to full team names
        df['team_id'] = df['Tm'].map(team_id_mapping)
        df['opp_id'] = df['Opp'].map(team_id_mapping)

        # Initialize a new column in df for game_id
        df['game_id'] = None

        # Iterate over the rows in df to find the corresponding game_id in game_pks
        for index, row in df.iterrows():

            #print(f"Processing row {index}: Date={row['Date']}, Team={row['team_id']}, Opponent={row['opp_id']}")
            

            # Filter the game_pks for the matching date and teams
            game_day_matches = game_pks[
                (game_pks['game_date'] == row['Date']) &
                (
                    ((game_pks['home_id'] == row['team_id']) & (game_pks['away_id'] == row['opp_id'])) |
                    ((game_pks['home_id'] == row['opp_id']) & (game_pks['away_id'] == row['team_id']))
                )
            ]

            
            #print(f"Matches found: {len(game_day_matches)}")
            

            # Check the 'dbl' column to assign the correct game_id
            if not game_day_matches.empty:

                #print(f"Match details: {game_day_matches}")

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
                df.at[index, 'game_id'] = game_id
                #print(f"Assigned game_id: {game_id}")
            else:
                print("BAD - NO GAME MATCHES FOUND (pitcher).")

        # Append the data for the year to player_df
        player_df = pd.concat([player_df, df])

    # Save the player's data to a CSV file
    player_df.to_csv(f'pitchers/{id}_pitching.csv', index=False)