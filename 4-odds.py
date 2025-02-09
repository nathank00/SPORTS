import pandas as pd
from datetime import datetime

def fetch_over_under_runline(oddshark_id, game_date):
    """Fetches the Over/Under runline for a given team on a specific date."""
    year = game_date.year
    url = f"https://www.oddsshark.com/stats/gamelog/baseball/mlb/{oddshark_id}?season={year}"
    
    try:
        tables = pd.read_html(url)
        df = tables[0]
    except Exception as e:
        print(f"BAD - error for team {oddshark_id} on date {game_date}: {e}")
        return 'unknown', None, None, None
    
    if df.empty:
        print(f"BAD - No data in table for team {oddshark_id} on date {game_date}")
        return 'unknown', None, None, None
    
    df['Date'] = pd.to_datetime(df['Date'], format='%b %d, %Y')
    matching_rows = df[df['Date'] == game_date]

    if len(matching_rows) > 1:
        print(f"DOUBLEHEADER on {game_date}")
        return '', oddshark_id, year, game_date
    
    if matching_rows.empty:
        print(f"BAD - No matching date found for team {oddshark_id} on date {game_date}")
        return 'unknown', None, None, None
    
    over_under = matching_rows.iloc[0]['Total']
    return over_under, None, None, None

def update_gamelogs_with_over_under(game_pks_file, gamelogs_folder, num_games=50):
    """
    Updates game logs with Over/Under runline data from Oddshark.

    Parameters:
    - game_pks_file: Path to the game_pks CSV.
    - gamelogs_folder: Folder where game logs are stored.
    - num_games: Number of recent games to update (default: 50).
    """
    game_pks_df = pd.read_csv(game_pks_file)
    game_pks = game_pks_df['game_id'].tail(num_games)  # Now uses input variable

    duplicates = []
    count = 0
    
    for game_id in game_pks:
        try:
            gamelog_file = f'{gamelogs_folder}/game_{game_id}.csv'
            gamelog_df = pd.read_csv(gamelog_file)
            
            home_oddshark_id = gamelog_df.loc[0, 'home_oddshark_id']
            game_date_str = gamelog_df.loc[0, 'game_date']
            game_date = datetime.strptime(game_date_str, '%Y-%m-%d')
            
            over_under_runline, duplicate_id, duplicate_year, duplicate_date = fetch_over_under_runline(home_oddshark_id, game_date)
            
            if duplicate_id:
                duplicates.append((duplicate_id, duplicate_year, duplicate_date))
                
            gamelog_df['over_under_runline'] = over_under_runline
             
            gamelog_df.to_csv(gamelog_file, index=False)
            print(f"Updated {gamelog_file} with over/under runline.")
        except Exception as e:
            print(f"Error updating gamelog for game_id {game_id}: {e}")

        count += 1
        if count % 10 == 0:
            print(count)
    
    print("\nGames with duplicate dates:")
    for dup in duplicates:
        print(f"Team Oddshark ID: {dup[0]}, Year: {dup[1]}, Date: {dup[2].strftime('%Y-%m-%d')}")

# Example Usage:
game_pks_file = 'game_pks.csv'
gamelogs_folder = 'gamelogs'

# Run with a custom number of games if needed, otherwise default to 50
update_gamelogs_with_over_under(game_pks_file, gamelogs_folder, num_games=12000)
