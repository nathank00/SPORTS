import os
import pandas as pd

print("\n---------- Now running 7-currentdata.py ----------\n")

game_pks_path = 'game_pks.csv'
gamelogs_dir = 'gamelogs/'
output_path = 'model/unsorted_currentdata.csv'

# Read the game_pks.csv file
game_pks_df = pd.read_csv(game_pks_path).tail(100)
game_pks_list = game_pks_df['game_id'].tolist()

# Initialize an empty list to store DataFrames
dataframes = []

# Initialize a set to store all columns
all_columns = set()

# First pass: Collect all unique columns
for game_pk in game_pks_list:
    file_path = os.path.join(gamelogs_dir, f'gamestats_{game_pk}.csv')
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        all_columns.update(df.columns)
    else:
        print(f"BAD - File {file_path} not found.")

print("First pass: columns collected")

# Second pass: Read files and align columns
for game_pk in game_pks_list:
    file_path = os.path.join(gamelogs_dir, f'gamestats_{game_pk}.csv')
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        # Add missing columns with default value of NaN (handled by reindex)
        df = df.reindex(columns=all_columns)
        dataframes.append(df)

print("Second pass: data added.")

# Concatenate all the DataFrames
master_df = pd.concat(dataframes, ignore_index=True)

# Save the master DataFrame to a CSV file
master_df.to_csv(output_path, index=False)

print(f"Master dataset saved to {output_path}")

# ------------------------------------------------------ SORTING dataset -----------------------------------------------------------


import numpy as np

# Load the dataset
df = pd.read_csv('model/unsorted_currentdata.csv', low_memory=False)

pitcher_columns = [
    'IP_real_20', 'ERA', 'H_20', 'BF_20', 'HR_20', 'R_20', 'ER_20', 'BB_20', 'SO_20', 'XB_against_20',
    'TB_against_20', 'ERA_20', 'WHIP_20', 'IP_real_10', 'H_10', 'BF_10', 'HR_10', 'R_10', 'ER_10', 'BB_10', 'SO_10', 'XB_against_10',
    'TB_against_10', 'ERA_10', 'WHIP_10', 'IP_real_5', 'H_5', 'BF_5', 'HR_5', 'R_5', 'ER_5', 'BB_5',
    'SO_5', 'XB_against_5', 'TB_against_5', 'ERA_5', 'WHIP_5', 'IP_real_3', 'H_3', 'BF_3', 'HR_3', 'R_3', 'ER_3', 'BB_3',
    'SO_3', 'XB_against_3', 'TB_against_3', 'ERA_3', 'WHIP_3'
]
    

# Add 'over_under_runline' column right after 'runs_total'
if 'over_under_runline' in df.columns:
    columns = df.columns.tolist()
    runline_index = columns.index('over_under_runline')
    columns.insert(columns.index('runs_total') + 1, columns.pop(runline_index))
    df = df[columns]
else:
    print("Warning: 'over_under_runline' column not found.")

# Define a function to sort columns by player order and then alphabetically
def sort_columns(df):
    # List to store sorted column names
    sorted_columns = []
    
    # Ensure the specified order of the first few general columns
    first_columns = ['gamepk','game_id', 'game_date', 'home_name', 'away_name', 'runs_home', 'runs_away', 'runs_total', 'over_under_runline']
    for col in first_columns:
        if col in df.columns:
            sorted_columns.append(col)
    
    # Lists to categorize columns
    away_batter_columns = [[] for _ in range(9)]
    home_batter_columns = [[] for _ in range(9)]
    away_pitcher_columns = [[] for _ in range(9)]
    home_pitcher_columns = [[] for _ in range(9)]
    away_bullpen_columns = [[] for _ in range(15)]
    home_bullpen_columns = [[] for _ in range(15)]
    
    # Helper function to ensure the list is long enough
    def ensure_length(lst, index):
        while len(lst) <= index:
            lst.append([])
    
    # Helper function to sort player-specific columns
    def sort_player_columns(columns):
        player_columns = []
        other_columns = []
        for col in columns:
            if any(key in col for key in ['Name', 'ID', 'bbrefID']):
                player_columns.append(col)
            else:
                other_columns.append(col)
        return sorted(player_columns) + sorted(other_columns)
    
    # Categorize columns
    for col in df.columns:
        if col.startswith('Away_Batter'):
            try:
                num = int(col.split('_')[1][6]) - 1
                ensure_length(away_batter_columns, num)
                away_batter_columns[num].append(col)
            except (ValueError, IndexError):
                continue
        elif col.startswith('Home_Batter'):
            try:
                num = int(col.split('_')[1][6]) - 1
                ensure_length(home_batter_columns, num)
                home_batter_columns[num].append(col)
            except (ValueError, IndexError):
                continue
        elif col.startswith('Away_P_') or col.startswith('Away_SP'):
            try:
                if 'SP' in col:
                    num = 0
                else:
                    num = int(col.split('_')[2])
                ensure_length(away_pitcher_columns, num)
                away_pitcher_columns[num].append(col)
            except (ValueError, IndexError):
                continue
        elif col.startswith('Home_P_') or col.startswith('Home_SP'):
            try:
                if 'SP' in col:
                    num = 0
                else:
                    num = int(col.split('_')[2])
                ensure_length(home_pitcher_columns, num)
                home_pitcher_columns[num].append(col)
            except (ValueError, IndexError):
                continue
        elif col.startswith('Away_bullpen'):
            try:
                num = int(col.split('_')[2]) - 1
                ensure_length(away_bullpen_columns, num)
                away_bullpen_columns[num].append(col)
            except (ValueError, IndexError):
                continue
        elif col.startswith('Home_bullpen'):
            try:
                num = int(col.split('_')[2]) - 1
                ensure_length(home_bullpen_columns, num)
                home_bullpen_columns[num].append(col)
            except (ValueError, IndexError):
                continue

    # Sort each category
    for batter_columns in away_batter_columns:
        sorted_columns.extend(sort_player_columns(batter_columns))
    for batter_columns in home_batter_columns:
        sorted_columns.extend(sort_player_columns(batter_columns))
    for pitcher_columns in away_pitcher_columns:
        sorted_columns.extend(sort_player_columns(pitcher_columns))
    for pitcher_columns in home_pitcher_columns:
        sorted_columns.extend(sort_player_columns(pitcher_columns))
    for bullpen_columns in away_bullpen_columns:
        sorted_columns.extend(sort_player_columns(bullpen_columns))
    for bullpen_columns in home_bullpen_columns:
        sorted_columns.extend(sort_player_columns(bullpen_columns))
    
    return df[sorted_columns]

# Sort columns in the dataset
sorted_df = sort_columns(df)

# Save the sorted dataset
sorted_df.to_csv('model/currentdata.csv', index=False)


# Load the sorted dataset
sorted_df = pd.read_csv('model/currentdata.csv', low_memory=False)

# Function to determine if a column should be removed
def should_remove_column(col):
    if col.startswith('Home_bullpen_') or col.startswith('Away_bullpen_'):
        try:
            # Extract the number from the column name and check if it is 16 or higher
            number = int(col.split('_')[2])
            return number >= 16
        except ValueError:
            # If the suffix is not numeric, do not remove
            return False
    return False

# Remove columns that start with 'Home_bullpen_' or 'Away_bullpen_' and have a number 16 or higher
columns_to_remove = [col for col in sorted_df.columns if should_remove_column(col)]
sorted_df.drop(columns=columns_to_remove, inplace=True)


# Remove rows where runline is 'unknown'
#filtered_df = sorted_df[sorted_df['over_under_runline'] != 'unknown']

# Function to filter games based on date range
def filter_games_by_date(df):
    # Convert game_date to datetime
    df['game_date'] = pd.to_datetime(df['game_date'])
    
    # Filter out games before April 5 and after October 5
    #filtered_df = df[(df['game_date'].dt.month >= 4) & (df['game_date'].dt.month <= 10) &
                     #((df['game_date'].dt.month != 4) | (df['game_date'].dt.day >= 5)) &
                     #((df['game_date'].dt.month != 10) | (df['game_date'].dt.day <= 5))]
    
    return df

# Filter the games by date
#filtered_df = filter_games_by_date(filtered_df)
filtered_df = filter_games_by_date(sorted_df)

# Convert "unknown" values to 0 before converting to numeric
filtered_df['over_under_runline'] = filtered_df['over_under_runline'].replace("unknown", 0)

# Convert over_under_runline to numeric
filtered_df['over_under_runline'] = pd.to_numeric(filtered_df['over_under_runline'])

# Create binary target variable
filtered_df['over_under_target'] = (filtered_df['runs_total'] >= filtered_df['over_under_runline']).astype(int)

# Rearrange columns to move 'over_under_target' to the right of 'over_under_runline'
columns = list(filtered_df.columns)
over_under_runline_index = columns.index('over_under_runline')

# Insert 'over_under_target' right after 'over_under_runline'
columns.insert(over_under_runline_index + 1, columns.pop(columns.index('over_under_target')))
filtered_df = filtered_df[columns]


# Save the filtered dataset with the target variable
filtered_df.to_csv('model/currentdata.csv', index=False)


# -------------------------------------------------------- GENERATE Bullpen avg stats, remove bullpen individual stats --------------------------------------------

# Load the dataset
file_path = 'model/currentdata.csv'
df = pd.read_csv(file_path)

# List of pitcher stats columns to be averaged
pitcher_columns = [
    'IP_real_20', 'ERA', 'H_20', 'BF_20', 'HR_20', 'R_20', 'ER_20', 'BB_20', 'SO_20', 'XB_against_20',
    'TB_against_20', 'ERA_20', 'WHIP_20', 'IP_real_10', 'H_10', 'BF_10', 'HR_10', 'R_10', 'ER_10', 'BB_10', 'SO_10', 'XB_against_10',
    'TB_against_10', 'ERA_10', 'WHIP_10', 'IP_real_5', 'H_5', 'BF_5', 'HR_5', 'R_5', 'ER_5', 'BB_5',
    'SO_5', 'XB_against_5', 'TB_against_5', 'ERA_5', 'WHIP_5', 'IP_real_3', 'H_3', 'BF_3', 'HR_3', 'R_3', 'ER_3', 'BB_3',
    'SO_3', 'XB_against_3', 'TB_against_3', 'ERA_3', 'WHIP_3'
]

# Function to calculate average stats for bullpen pitchers
def calculate_bullpen_averages(team_prefix):
    for stat in pitcher_columns:
        stat_columns = [f"{team_prefix}_bullpen_{i}_{stat}" for i in range(1, 13)]
        # Convert columns to numeric, coercing errors to NaNs
        df[stat_columns] = df[stat_columns].apply(pd.to_numeric, errors='coerce')
        # Replace infinite values with NaNs
        df[stat_columns] = df[stat_columns].replace([np.inf, -np.inf], np.nan)
        df[f"{team_prefix}_bullpen_avg_{stat}"] = df[stat_columns].mean(axis=1)

# Calculate averages for home and away teams
calculate_bullpen_averages('Home')
calculate_bullpen_averages('Away')

# Save the updated dataframe back to the same file
df.to_csv(file_path, index=False)

print("Updated dataset saved successfully.")

# Load the dataset
file_path = 'model/currentdata.csv'
df = pd.read_csv(file_path)

# List of pitcher stats columns to be removed
pitcher_columns = [
    'IP_real_20', 'ERA', 'H_20', 'BF_20', 'HR_20', 'R_20', 'ER_20', 'BB_20', 'SO_20', 'XB_against_20',
    'TB_against_20', 'ERA_20', 'WHIP_20', 'IP_real_10', 'H_10', 'BF_10', 'HR_10', 'R_10', 'ER_10', 'BB_10', 'SO_10', 'XB_against_10',
    'TB_against_10', 'ERA_10', 'WHIP_10', 'IP_real_5', 'H_5', 'BF_5', 'HR_5', 'R_5', 'ER_5', 'BB_5',
    'SO_5', 'XB_against_5', 'TB_against_5', 'ERA_5', 'WHIP_5', 'IP_real_3', 'H_3', 'BF_3', 'HR_3', 'R_3', 'ER_3', 'BB_3',
    'SO_3', 'XB_against_3', 'TB_against_3', 'ERA_3', 'WHIP_3'
]

# Function to remove individual bullpen columns
def remove_bullpen_columns(team_prefix):
    for stat in pitcher_columns:
        stat_columns = [f"{team_prefix}_bullpen_{i}_{stat}" for i in range(1, 13)]
        df.drop(columns=stat_columns, inplace=True)

# Remove bullpen columns for home and away teams
remove_bullpen_columns('Home')
remove_bullpen_columns('Away')

# Save the updated dataframe back to the same file
df.to_csv(file_path, index=False)

print(f" {file_path} - DATA SET GENERATED - success.")