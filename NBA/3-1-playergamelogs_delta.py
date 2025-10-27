import pandas as pd
import os
import time
import numpy as np
from datetime import datetime
from nba_api.stats.endpoints import playergamelog
from tenacity import retry, stop_after_attempt, wait_exponential
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
import logging

# Setup logging
os.makedirs('logs', exist_ok=True)  # Create logs directory if it doesn't exist
logging.basicConfig(level=logging.INFO, filename='logs/player_gamelog_delta.log', filemode='a', format='%(asctime)s - %(levelname)s - %(message)s')

def get_current_season():
    """Returns the current NBA season in 'YYYY-YY' format."""
    current_year = datetime.now().year
    month = datetime.now().month
    season_start = current_year if month >= 10 else current_year - 1
    return f'{season_start}-{str(season_start+1)[-2:]}'

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_gamelog(player_id, season):
    """Fetch player game log with retries and 1-second sleep."""
    time.sleep(1)  # 1-second delay between fetches
    logging.info(f"Fetching gamelog for player {player_id}, season {season}")
    gamelog = playergamelog.PlayerGameLog(player_id=player_id, season=season)
    return gamelog.get_data_frames()[0]

def process_player_update(args):
    """Wrapper for multiprocessing to update player game logs for current season."""
    player_id, player_slug, players_folder, current_season = args
    player_file = os.path.join(players_folder, f"{player_slug}_{player_id}.parquet")

    try:
        df_new = fetch_gamelog(player_id, current_season)
        if df_new.empty:
            logging.info(f"No data for player {player_id} in {current_season}")
            return {'type': 'success', 'data': {'player_id': player_id, 'rows': 0}}

        df_new['SEASON'] = current_season
        df_new['Player_ID'] = player_id

        if os.path.exists(player_file):
            df_existing = pd.read_parquet(player_file)
            # Remove existing current season rows
            df_existing = df_existing[df_existing['SEASON'] != current_season]
            # Concat new data and drop duplicates
            df_updated = pd.concat([df_existing, df_new], ignore_index=True).drop_duplicates(subset=['Player_ID', 'Game_ID'])
        else:
            df_updated = df_new.drop_duplicates(subset=['Player_ID', 'Game_ID'])

        df_updated.to_parquet(player_file, index=False)
        logging.info(f"Updated {player_file} with {len(df_updated)} rows")
        return {'type': 'success', 'data': {'player_id': player_id, 'rows': len(df_updated)}}
    except Exception as e:
        logging.error(f"Error updating player {player_id}: {e}")
        return {'type': 'error', 'data': {'player_id': player_id, 'error': str(e)}}

def update_recent_player_gamelogs(num_games, players_folder='players', gamelogs_file='gamelogs.parquet', players_file='players.parquet', error_file='player_update_errors.parquet'):
    """Update player game logs for players in the last num_games, overwriting current season data."""
    current_season = get_current_season()
    logging.info(f"Starting update for {num_games} recent games, current season {current_season}")

    # Validate input file
    if not os.path.exists(gamelogs_file):
        logging.error(f"{gamelogs_file} not found")
        raise FileNotFoundError(f"{gamelogs_file} not found. Ensure it exists with game_id, home_team_players, away_team_players.")

    # Load recent games
    game_data = pd.read_parquet(gamelogs_file)
    logging.info(f"Loaded {len(game_data)} games from {gamelogs_file}")
    if len(game_data) < num_games:
        logging.warning(f"Only {len(game_data)} games available, using all instead of {num_games}")
        print(f"Warning: Only {len(game_data)} games available, using all instead of {num_games}.")
        num_games = len(game_data)
    recent_games = game_data.tail(num_games)

    # Validate required columns
    required_cols = ['home_team_players', 'away_team_players']
    missing_cols = [col for col in required_cols if col not in game_data.columns]
    if missing_cols:
        logging.error(f"Missing columns in {gamelogs_file}: {missing_cols}")
        raise ValueError(f"Missing columns in {gamelogs_file}: {missing_cols}")

    # Extract unique player_ids from home and away players
    player_ids = set()
    for _, row in recent_games.iterrows():
        for col in ['home_team_players', 'away_team_players']:
            players_data = row[col]
            game_id = row.get('game_id', 'unknown')
            logging.debug(f"Processing {col} for game {game_id}")

            # Skip if data is None or empty
            if players_data is None or (isinstance(players_data, (list, np.ndarray)) and len(players_data) == 0):
                logging.warning(f"Empty or None {col} in game {game_id}")
                continue

            try:
                # Handle nested list or numpy array: [array([p1, p2, ...])] or [[p1, p2, ...]]
                if isinstance(players_data, (list, np.ndarray)) and len(players_data) > 0:
                    # Check if first element is a list or numpy array (nested)
                    if isinstance(players_data[0], (list, np.ndarray)):
                        players_data = players_data[0]  # Flatten the nested structure
                    players = [str(pid) for pid in players_data if str(pid).strip()]
                    player_ids.update(players)
                    logging.debug(f"Parsed {col} in game {game_id}: {players}")
                else:
                    logging.warning(f"Unexpected {col} format in game {game_id}: {type(players_data)}")
            except Exception as e:
                logging.error(f"Error parsing {col} in game {game_id}: {e}")

    if not player_ids:
        logging.error("No players found in recent games")
        print("No players found in recent games.")
        return

    logging.info(f"Found {len(player_ids)} unique player IDs in recent games")

    # Load players.parquet for slugs
    if not os.path.exists(players_file):
        logging.error(f"{players_file} not found")
        raise FileNotFoundError(f"{players_file} not found. Ensure it exists with PERSON_ID, PLAYER_SLUG.")

    players_df = pd.read_parquet(players_file)
    required_player_cols = ['PERSON_ID', 'PLAYER_SLUG']
    missing_player_cols = [col for col in required_player_cols if col not in players_df.columns]
    if missing_player_cols:
        logging.error(f"Missing columns in {players_file}: {missing_player_cols}")
        raise ValueError(f"Missing columns in {players_file}: {missing_player_cols}")

    player_slugs = {}
    missing_players = []
    for _, row in players_df.iterrows():
        pid = str(row['PERSON_ID'])
        if pid in player_ids:
            player_slugs[pid] = row['PLAYER_SLUG']
        else:
            missing_players.append(pid)

    if missing_players:
        logging.warning(f"{len(missing_players)} player IDs not found in players.parquet")

    # Filter to players with slugs
    player_ids = list(player_slugs.keys())
    if not player_ids:
        logging.error("No matching players found in players.parquet")
        print("No matching players found in players.parquet.")
        return

    print(f"Updating {len(player_ids)} players for current season {current_season}...")
    logging.info(f"Updating {len(player_ids)} players")

    # Ensure players folder exists
    os.makedirs(players_folder, exist_ok=True)

    num_processes = max(1, cpu_count() - 1)
    args = [(pid, player_slugs[pid], players_folder, current_season) for pid in player_ids]

    errors = []
    with Pool(processes=num_processes) as pool:
        results = list(tqdm(pool.imap_unordered(process_player_update, args), total=len(args), desc="Updating players"))

    for res in results:
        if res['type'] == 'error':
            errors.append(res['data'])

    if errors:
        pd.DataFrame(errors).to_parquet(error_file, index=False)
        print(f"Saved {len(errors)} errors to {error_file}")
        logging.info(f"Saved {len(errors)} errors to {error_file}")
    else:
        print("No errors encountered.")
        logging.info("No errors encountered.")

if __name__ == "__main__":
    num_games = 40
    update_recent_player_gamelogs(num_games)
