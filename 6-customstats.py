import pandas as pd
import numpy as np
import os

print("---------- Now running 6-customstats.py ----------")

idlist = pd.read_csv('active_batter_ids.csv')
batter_ids = idlist.key_bbref

game_pks = pd.read_csv('game_pks.csv')

# Define function to create an empty DataFrame with the correct structure
def create_empty_stats_df():
    columns = ['Rk', 'Gcar', 'Gtm', 'Date', 'Tm', 'Unnamed: 5', 'Opp', 'Rslt', 'Inngs', 'PA', 'AB', 'R', 'H', '2B', '3B', 'HR', 'RBI', 'BB', 'IBB', 'SO', 'HBP', 'SH', 'SF', 'ROE', 'GDP', 'SB', 'CS', 'BA', 'OBP', 'SLG', 'OPS', 'BOP', 'aLI', 'WPA', 'acLI', 'cWPA', 'RE24', 'DFS(DK)', 'DFS(FD)', 'Pos', 'dbl', 'game_date', 'team_id', 'opp_id', 'game_id']
    empty_df = pd.DataFrame(columns=columns)
    return empty_df


for id in batter_ids:

    if not id or pd.isna(id):
        continue

    file_path = f'batters/{id}_batting.csv'
    
    if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
        try:
            df = pd.read_csv(file_path)
        except pd.errors.EmptyDataError:
            print(f"File for ID {id} is empty.")
            continue
        except pd.errors.ParserError:
            print(f"File for ID {id} is improperly formatted.")
            continue
    else:
        print(f"File for ID {id} does not exist or is empty.")
        continue

    # Remove the irrelevant column 'Gtm'
    df = df.drop(columns=['Gtm'])

    # Ensure the 'game_date' column is in datetime format
    df['game_date'] = pd.to_datetime(df['game_date'])

    # Extract the year from the 'game_date' column
    df['season'] = df['game_date'].dt.year

    # Clean non-numeric values in numeric columns
    def clean_numeric(value):
        try:
            value = str(value).replace('\xa0', '').replace('(', '').replace(')', '').replace(',', '')
            return float(value)
        except ValueError:
            return np.nan

    # Define columns to convert to numeric
    numeric_columns = ['PA', 'AB', 'R', 'H', '2B', '3B', 'HR', 'RBI', 'SB', 'CS', 'BB', 'SO', 'BA', 'OBP', 'SLG', 'OPS', 'TB', 'GIDP', 'HBP', 'SH', 'SF', 'IBB', 'aLI', 'acLI', 'cWPA', 'RE24', 'BOP']

    # Apply cleaning function to numeric columns
    for col in numeric_columns:
        if col not in df.columns:
            df[col] = np.nan          # Create the column and fill with NaN
            
        df[col] = df[col].apply(clean_numeric)

    # Fill NaN values with 0 for numerical calculations
    df[numeric_columns] = df[numeric_columns].fillna(0)

    # Ensure columns are of correct numeric type
    df[numeric_columns] = df[numeric_columns].astype(float)

    # Define functions to calculate required statistics
    def calculate_avg(df):
        return df['H'] / df['AB']

    def calculate_obp(df):
        return (df['H'] + df['BB'] + df['HBP']) / (df['AB'] + df['BB'] + df['HBP'] + df['SF'])

    def calculate_slg(df):
        return (df['H'] + 2*df['2B'] + 3*df['3B'] + 4*df['HR']) / df['AB']

    def calculate_ops(df):
        return calculate_obp(df) + calculate_slg(df)

    def calculate_extra_base_hits(df):
        return df['2B'] + df['3B'] + df['HR']

    def calculate_total_bases(df):
        return df['H'] + df['2B'] + 2*df['3B'] + 3*df['HR']

    def calculate_rolling_stats(df, window, suffix):
        rolling_df = df.rolling(window=window, min_periods=1).sum()
        rolling_df['AVG'] = calculate_avg(rolling_df)
        rolling_df['OBP'] = calculate_obp(rolling_df)
        rolling_df['SLG'] = calculate_slg(rolling_df)
        rolling_df['OPS'] = calculate_ops(rolling_df)
        rolling_df['XB'] = calculate_extra_base_hits(rolling_df)
        rolling_df['TB'] = calculate_total_bases(rolling_df)
        rolling_df = rolling_df[['AVG', 'OBP', 'SLG', 'OPS', 'SB', 'CS', 'XB', 'TB', 'SO']]
        rolling_df.columns = [f'{col}_{suffix}' for col in rolling_df.columns]
        
        # Round the stats to 3 decimal points
        rolling_df = rolling_df.round(3)
        
        return rolling_df

    # Exclude non-numeric columns from rolling stats calculation
    rolling_df = df[numeric_columns].copy()

    # Calculate rolling stats for the last 20 games and shift by one row
    rolling_stats_20 = calculate_rolling_stats(rolling_df, 20, '20').shift(1).fillna(0)

    # Calculate rolling stats for the last 10 games and shift by one row
    rolling_stats_10 = calculate_rolling_stats(rolling_df, 10, '10').shift(1).fillna(0)

    # Calculate rolling stats for the last 5 games and shift by one row
    rolling_stats_5 = calculate_rolling_stats(rolling_df, 5, '5').shift(1).fillna(0)

    # Calculate rolling stats for the last 5 games and shift by one row
    rolling_stats_3 = calculate_rolling_stats(rolling_df, 3, '3').shift(1).fillna(0)

    # Calculate season-long stats for each year and shift by one row
    season_stats = pd.DataFrame()
    for year in range(2021, 2025):
        season_df = df[df['season'] == year][numeric_columns].copy()
        season_cumsum = season_df.cumsum().shift(1).fillna(0)
        season_cumsum['AVG'] = calculate_avg(season_cumsum)
        season_cumsum['OBP'] = calculate_obp(season_cumsum)
        season_cumsum['SLG'] = calculate_slg(season_cumsum)
        season_cumsum['OPS'] = calculate_ops(season_cumsum)
        season_cumsum['XB'] = calculate_extra_base_hits(season_cumsum)
        season_cumsum['TB'] = calculate_total_bases(season_cumsum)
        season_cumsum = season_cumsum[['AVG', 'OBP', 'SLG', 'OPS', 'SB', 'CS', 'XB', 'TB', 'SO']]
        season_cumsum.columns = [f'{col}_current' for col in season_cumsum.columns]
        season_stats = pd.concat([season_stats, season_cumsum])

    # Ensure the season_stats index aligns with the original dataframe
    season_stats.index = df.index

    # Combine all the stats into a single dataframe
    final_df = pd.concat([df, rolling_stats_20, rolling_stats_10, rolling_stats_5, rolling_stats_3, season_stats], axis=1)

    # Round the combined dataframe stats to 3 decimal points
    final_df = final_df.round(3)

    # Display the combined dataframe
    print(final_df.tail())

    # Save the combined stats to a CSV file
    final_df.to_csv(f'batters/{id}_stats_batting.csv', index=False)

    print(f"Generated stats for {id} and saved to CSV file.")

# Function to clean numeric values
def clean_numeric(value):
    try:
        value = str(value).replace('\xa0', '').replace('(', '').replace(')', '').replace(',', '')
        return float(value)
    except ValueError:
        return np.nan

# Function to convert IP notation to real numbers
def convert_ip_to_real(ip):
    if pd.isna(ip):
        return np.nan
    ip_str = str(ip)
    if '.' in ip_str:
        parts = ip_str.split('.')
        whole = int(parts[0])
        fraction = int(parts[1]) if len(parts) > 1 else 0
        if fraction == 1:
            return whole + 1/3
        elif fraction == 2:
            return whole + 2/3
        else:
            return whole
    return float(ip)

# Function to calculate ERA
def calculate_era(df):
    return (df['ER'] * 9) / df['IP_real']

# Function to calculate WHIP
def calculate_whip(df):
    return (df['H'] + df['BB']) / df['IP_real']

# Function to calculate extra base hits against
def calculate_extra_base_hits_against(df):
    return df['2B'] + df['3B'] + df['HR']

# Function to calculate total bases against
def calculate_total_bases_against(df):
    return df['H'] + df['2B'] + 2 * df['3B'] + 3 * df['HR']

# Function to calculate rolling stats
def calculate_rolling_stats(df, window, suffix):
    rolling_df = df.rolling(window=window, min_periods=1).sum()
    rolling_df['ERA'] = calculate_era(rolling_df)
    rolling_df['WHIP'] = calculate_whip(rolling_df)
    rolling_df['XB_against'] = calculate_extra_base_hits_against(rolling_df)
    rolling_df['TB_against'] = calculate_total_bases_against(rolling_df)
    rolling_df = rolling_df[['IP_real', 'H', 'BF', 'HR', 'R', 'ER', 'BB', 'SO', 'XB_against', 'TB_against', 'ERA', 'WHIP']]
    rolling_df.columns = [f'{col}_{suffix}' for col in rolling_df.columns]
    return rolling_df.round(3)

# Load pitcher IDs
idlist = pd.read_csv('active_pitcher_ids.csv')
pitcher_ids = idlist.key_bbref

# Load game PKs (if needed)
game_pks = pd.read_csv('game_pks.csv')

for id in pitcher_ids:

    if not id or pd.isna(id):
        continue

    file_path = f'pitchers/{id}_pitching.csv' 
    
    if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
        try:
            df = pd.read_csv(file_path)
        except pd.errors.EmptyDataError:
            print(f"File for ID {id} is empty.")
            continue
        except pd.errors.ParserError:
            print(f"File for ID {id} is improperly formatted.")
            continue
    else:
        print(f"File for ID {id} does not exist or is empty.")
        continue

    # Remove the irrelevant column 'Gtm'
    df = df.drop(columns=['Gtm'])

    # Ensure the 'game_date' column is in datetime format
    df['game_date'] = pd.to_datetime(df['game_date'])

    # Extract the year from the 'game_date' column
    df['season'] = df['game_date'].dt.year

    # Define columns to convert to numeric
    numeric_columns = ['IP', 'H', 'R', 'ER', 'BB', 'SO', 'HR', 'BF', '2B', '3B', 'IBB']

    # Apply cleaning function to numeric columns
    for col in numeric_columns:
        df[col] = df[col].apply(clean_numeric)

    # Fill NaN values with 0 for numerical calculations
    df[numeric_columns] = df[numeric_columns].fillna(0)

    # Ensure columns are of correct numeric type
    df[numeric_columns] = df[numeric_columns].astype(float)

    # Create the IP_real column
    df['IP_real'] = df['IP'].apply(convert_ip_to_real)

    # Exclude non-numeric columns from rolling stats calculation
    rolling_df = df[numeric_columns + ['IP_real']].copy()

    # Calculate rolling stats for the last 20 games and shift by one row
    rolling_stats_20 = calculate_rolling_stats(rolling_df, 20, '20').shift(1).fillna(0)

    # Calculate rolling stats for the last 20 games and shift by one row
    rolling_stats_10 = calculate_rolling_stats(rolling_df, 10, '10').shift(1).fillna(0)

    # Calculate rolling stats for the last 5 games and shift by one row
    rolling_stats_5 = calculate_rolling_stats(rolling_df, 5, '5').shift(1).fillna(0)

    # Calculate rolling stats for the last 20 games and shift by one row
    rolling_stats_3 = calculate_rolling_stats(rolling_df, 3, '3').shift(1).fillna(0)

    # Calculate season-long stats for each year and shift by one row
    season_stats = pd.DataFrame()
    for year in df['season'].unique():
        season_df = df[df['season'] == year][numeric_columns + ['IP_real']].copy()
        season_cumsum = season_df.cumsum().shift(1).fillna(0)
        season_cumsum['ERA'] = calculate_era(season_cumsum)
        season_cumsum['WHIP'] = calculate_whip(season_cumsum)
        season_cumsum['XB_against'] = calculate_extra_base_hits_against(season_cumsum)
        season_cumsum['TB_against'] = calculate_total_bases_against(season_cumsum)
        season_cumsum = season_cumsum[['IP_real', 'H', 'BF', 'HR', 'R', 'ER', 'BB', 'SO', 'XB_against', 'TB_against', 'ERA', 'WHIP']]
        season_cumsum.columns = [f'{col}_current' for col in season_cumsum.columns]
        season_stats = pd.concat([season_stats, season_cumsum])

    # Ensure the season_stats index aligns with the original dataframe
    season_stats.index = df.index

    # Combine all the stats into a single dataframe
    final_df = pd.concat([df, rolling_stats_20, rolling_stats_10, rolling_stats_5, rolling_stats_3, season_stats], axis=1)

    # Round the combined dataframe stats to 3 decimal points
    final_df = final_df.round(3)

    # Display the combined dataframe
    print(final_df.tail())

    # Save the combined stats to a CSV file
    final_df.to_csv(f'pitchers/{id}_stats_pitching.csv', index=False)

    print(f"Generated stats for {id} and saved to CSV file.")

import os
import pandas as pd

def get_player_stats(bbref_id, player_type, game_id):
    """
    Get the player's stats for the specific game_id. If not available, return the most recent stats.
    """
    stats_dir = 'batters' if player_type == 'batting' else 'pitchers'
    stats_file = os.path.join(stats_dir, f'{bbref_id}_stats_{player_type}.csv')
    
    if not os.path.exists(stats_file):
        print(f"Stats file for {bbref_id} not found ({player_type}).")
        return None
    
    stats_df = pd.read_csv(stats_file)
    game_stats = stats_df[stats_df['game_id'] == game_id]
    
    if not game_stats.empty:
        return game_stats.iloc[0]
    else:
        return stats_df.iloc[-1]

def process_game(game_id):
    # Read the gamelog file
    game_file = f'gamelogs/game_{game_id}.csv'
    if not os.path.exists(game_file):
        print(f"Gamelog file for game {game_id} not found.")
        return
    
    game_df = pd.read_csv(game_file)
    game_data = game_df.iloc[0].to_dict()
    
    # Define relevant columns for batters and pitchers
    batter_columns = ['AVG_20', 'OBP_20', 'SLG_20', 'OPS_20', 'SB_20', 'CS_20', 'XB_20', 'TB_20', 'SO_20',
                      'AVG_10', 'OBP_10', 'SLG_10', 'OPS_10', 'SB_10', 'CS_10', 'XB_10', 'TB_10', 'SO_10',
                      'AVG_5', 'OBP_5', 'SLG_5', 'OPS_5', 'SB_5', 'CS_5', 'XB_5', 'TB_5', 'SO_5',
                      'AVG_3', 'OBP_3', 'SLG_3', 'OPS_3', 'SB_3', 'CS_3', 'XB_3', 'TB_3', 'SO_3']
    pitcher_columns = ['IP_real_20', 'ERA', 'H_20', 'BF_20', 'HR_20', 'R_20', 'ER_20', 'BB_20', 'SO_20', 'XB_against_20',
                       'TB_against_20', 'ERA_20', 'WHIP_20', 'IP_real_10', 'H_10', 'BF_10', 'HR_10', 'R_10', 'ER_10', 'BB_10', 'SO_10', 'XB_against_10',
                       'TB_against_10', 'ERA_10', 'WHIP_10', 'IP_real_5', 'H_5', 'BF_5', 'HR_5', 'R_5', 'ER_5', 'BB_5',
                       'SO_5', 'XB_against_5', 'TB_against_5', 'ERA_5', 'WHIP_5', 'IP_real_3', 'H_3', 'BF_3', 'HR_3', 'R_3', 'ER_3', 'BB_3',
                       'SO_3', 'XB_against_3', 'TB_against_3', 'ERA_3', 'WHIP_3']
    
    # Fetch stats for each batter
    for i in range(1, 10):
        for team in ['Away', 'Home']:
            bbref_id = game_data.get(f'{team}_Batter{i}_bbrefID')
            if bbref_id:
                stats = get_player_stats(bbref_id, 'batting', game_id)
                if stats is not None:
                    for col in batter_columns:
                        game_data[f'{team}_Batter{i}_{col}'] = stats.get(col, '')
            else:
                print(f'missing bbrefID for game {game_id}')

    # Fetch stats for each pitcher
    for team in ['Away', 'Home']:
        for i in range(1, 11):
            role = 'SP' if i == 1 else f'P_{i}'
            bbref_id = game_data.get(f'{team}_{role}_bbrefID')
            if bbref_id:
                stats = get_player_stats(bbref_id, 'pitching', game_id)
                if stats is not None:
                    for col in pitcher_columns:
                        game_data[f'{team}_{role}_{col}'] = stats.get(col, '')

    # Fetch stats for each bullpen pitcher
    for team in ['Away', 'Home']:
        for i in range(1, 15):  # Adjust the range according to your maximum expected number of bullpen pitchers
            role = f'bullpen_{i}'
            bbref_id = game_data.get(f'{team}_{role}_bbrefID')
            if bbref_id:
                stats = get_player_stats(bbref_id, 'pitching', game_id)
                if stats is not None:
                    for col in pitcher_columns:
                        game_data[f'{team}_{role}_{col}'] = stats.get(col, '')
    
    # Create a DataFrame from the updated game data
    updated_game_df = pd.DataFrame([game_data])
    
    # Save the updated game data to a new CSV file
    output_file = f'gamelogs/gamestats_{game_id}.csv'
    updated_game_df.to_csv(output_file, index=False)
    print(f"Processed and saved game stats for game {game_id} to {output_file}")

def process_recent_games(num_recent_games):
    game_pks_file = 'game_pks.csv'
    if not os.path.exists(game_pks_file):
        print(f"{game_pks_file} not found.")
        return

    game_pks_df = pd.read_csv(game_pks_file)
    recent_game_pks = game_pks_df.tail(num_recent_games)['game_id'].tolist()
    
    for game_id in recent_game_pks:
        process_game(game_id)

# Input the number of most recent games to process
num_recent_games = 50     ########################################################### CHANGE THIS #################################################################
process_recent_games(num_recent_games)