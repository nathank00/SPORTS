import pandas as pd
import os
from datetime import datetime
from nba_api.stats.endpoints import playergamelog
from ratelimit import limits, sleep_and_retry
from tenacity import retry, stop_after_attempt, wait_exponential
from multiprocessing import Pool, cpu_count
from tqdm import tqdm

def get_current_season():
    """Returns the current NBA season in 'YYYY-YY' format."""
    current_year = datetime.now().year
    month = datetime.now().month
    season_start = current_year if month >= 10 else current_year - 1
    return f'{season_start}-{str(season_start+1)[-2:]}'

def get_season_years():
    """Generate a list of NBA seasons from 2020-21 to the current season."""
    start_year = 2020
    current_year = datetime.now().year
    current_season = get_current_season()
    seasons = [f'{year}-{str(year+1)[-2:]}' for year in range(start_year, current_year + 1)
               if f'{year}-{str(year+1)[-2:]}' <= current_season]
    return seasons

def get_existing_seasons_cache(players_folder, player_ids):
    """Cache seasons already present in Parquet files for all players."""
    cache = {}
    for player_id in player_ids:
        player_file = os.path.join(players_folder, f"{player_id}.parquet")
        if os.path.exists(player_file):
            try:
                df = pd.read_parquet(player_file)
                cache[player_id] = df['SEASON'].unique().tolist() if 'SEASON' in df.columns else []
            except:
                cache[player_id] = []
        else:
            cache[player_id] = []
    return cache

@sleep_and_retry
@limits(calls=25, period=60)  # NBA API rate limit: 25 calls per minute
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_gamelog(player_id, season):
    """Fetch player game log with rate-limiting and retries."""
    gamelog = playergamelog.PlayerGameLog(player_id=player_id, season=season)
    return gamelog.get_data_frames()[0]

def process_player_gamelog(args):
    """Wrapper for multiprocessing to fetch player game logs for a season."""
    player_id, player_name, player_slug, season, players_folder, existing_seasons = args
    player_file = os.path.join(players_folder, f"{player_slug}_{player_id}.parquet")

    if season in existing_seasons.get(player_id, []):
        return {'type': 'success', 'data': {'player_id': player_id, 'player_name': player_name, 'season': season, 'rows': 0}}

    try:
        df = fetch_gamelog(player_id, season)
        if not df.empty:
            df['SEASON'] = season
            df['Player_ID'] = player_id
            if os.path.exists(player_file):
                existing_df = pd.read_parquet(player_file)
                df = pd.concat([existing_df, df], ignore_index=True).drop_duplicates(subset=['Player_ID', 'Game_ID'])
            df.to_parquet(player_file, index=False)
            return {'type': 'success', 'data': {'player_id': player_id, 'player_name': player_name, 'season': season, 'rows': len(df)}}
        return {'type': 'success', 'data': {'player_id': player_id, 'player_name': player_name, 'season': season, 'rows': 0}}
    except Exception as e:
        return {'type': 'error', 'data': {'player_id': player_id, 'player_name': player_name, 'season': season, 'error': str(e)}}

def collect_player_gamelogs(players_file, players_folder, error_file='logs/player_gamelog_errors.parquet'):
    """Collects game logs for all unique players for seasons 2020-21 to current."""
    print(f"Loading player data from {players_file}...")
    players_df = pd.read_parquet(players_file)
    os.makedirs(players_folder, exist_ok=True)

    # Group by PERSON_ID to get unique players
    unique_players = players_df.groupby('PERSON_ID').agg({
        'DISPLAY_FIRST_LAST': 'first',
        'PLAYER_SLUG': 'first',
        'FROM_YEAR': 'min',
        'TO_YEAR': 'max'
    }).reset_index()

    print(f"Found {len(unique_players)} unique players by PERSON_ID.")

    # Cache existing seasons for all players
    print("Caching existing seasons...")
    existing_seasons = get_existing_seasons_cache(players_folder, unique_players['PERSON_ID'].astype(str))

    # Define seasons from 2020-21 to current
    seasons = get_season_years()

    args = []
    for _, row in unique_players.iterrows():
        player_id = str(row['PERSON_ID'])
        player_name = row['DISPLAY_FIRST_LAST']
        player_slug = row['PLAYER_SLUG']
        from_year = int(row['FROM_YEAR'])
        to_year = int(row['TO_YEAR'])
        # Limit to seasons within 2020-21 to current and player's active years
        player_seasons = [season for season in seasons if from_year <= int(season[:4]) <= to_year]
        for season in player_seasons:
            args.append((player_id, player_name, player_slug, season, players_folder, existing_seasons))

    num_processes = max(1, cpu_count() - 1)  # Use all cores minus one
    print(f"Processing game logs for {len(unique_players)} players across {len(args)} seasons with {num_processes} processes...")
    errors = []
    with Pool(processes=num_processes) as pool:
        results = list(tqdm(pool.imap_unordered(process_player_gamelog, args), total=len(args), desc="Processing player game logs"))

    for res in results:
        if res['type'] == 'error':
            errors.append(res['data'])

    if errors:
        pd.DataFrame(errors).to_parquet(error_file, index=False)
        print(f"Saved {len(errors)} errors to {error_file}")
    else:
        print("No errors encountered during player game log processing.")

if __name__ == "__main__":
    players_folder = 'players'
    collect_player_gamelogs('players.parquet', players_folder)
