import pandas as pd
import os
from datetime import datetime
import pytz
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
import logging

# Define paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PLAYERS_FILE = os.path.join(SCRIPT_DIR, "players.parquet")
PLAYERS_FOLDER = os.path.join(SCRIPT_DIR, "players")
LOG_DIR = os.path.join(SCRIPT_DIR, "logs")
ERROR_LOG_FILE = os.path.join(LOG_DIR, "custom_playerstats_errors.log")
LOCAL_TZ = pytz.timezone("America/Los_Angeles")

# Setup logging
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(level=logging.INFO, filename=os.path.join(LOG_DIR, "custom_playerstats.log"), filemode='a', format='%(asctime)s - %(levelname)s - %(message)s')
# Configure separate handler for error log
error_handler = logging.FileHandler(ERROR_LOG_FILE, mode='a')
error_handler.setLevel(logging.ERROR)
error_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logging.getLogger('').addHandler(error_handler)

# Define stats and windows for rolling calculations
STATS = ['FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT',
         'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', 'PLUS_MINUS']
PCT_STATS = ['FG_PCT', 'FG3_PCT', 'FT_PCT']  # Stats to compute as rolling averages
RAW_STATS = [stat for stat in STATS if stat not in PCT_STATS]  # Stats to compute as rolling sums
WINDOWS = [10, 50]
ROLLING_COLS = [f'{stat}_{w}' for stat in STATS for w in WINDOWS]

def compute_rolling_stats(args):
    """Compute rolling statistics for a player, excluding current game, with sums for raw stats and averages for percentages."""
    player_id, player_slug = args
    player_file = os.path.join(PLAYERS_FOLDER, f"{player_slug}_{player_id}.parquet")
    try:
        if not os.path.exists(player_file):
            logging.error(f"Player file {player_file} not found")
            return {'type': 'error', 'data': {'player_id': player_id, 'error': f"Player file {player_file} not found"}}

        df = pd.read_parquet(player_file)
        if df.empty or 'Player_ID' not in df.columns or 'GAME_DATE' not in df.columns:
            logging.error(f"Empty or invalid data in {player_file}")
            return {'type': 'error', 'data': {'player_id': player_id, 'error': 'Empty or invalid player data'}}

        # Rename Player_ID to PLAYER_ID for consistency
        df = df.rename(columns={'Player_ID': 'PLAYER_ID'})
        required_cols = ['PLAYER_ID', 'Game_ID', 'GAME_DATE'] + STATS
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logging.error(f"Missing columns in {player_file}: {missing_cols}")
            return {'type': 'error', 'data': {'player_id': player_id, 'error': f"Missing columns: {missing_cols}"}}

        df = df[df['PLAYER_ID'] == player_id].sort_values('GAME_DATE')
        rolling_stats = {}
        for stat in STATS:
            for window in WINDOWS:
                col_name = f'{stat}_{window}'
                if stat in PCT_STATS:
                    # Rolling average for percentage stats
                    rolling_stats[col_name] = df[stat].rolling(window=window, min_periods=1).mean().shift(1).round(2)
                else:
                    # Rolling sum for raw numeric stats
                    rolling_stats[col_name] = df[stat].rolling(window=window, min_periods=1).sum().shift(1).round(2)
        rolling_df = pd.DataFrame(rolling_stats)
        rolling_df['GAME_ID'] = df['Game_ID'].values
        rolling_df['GAME_DATE'] = df['GAME_DATE'].values
        rolling_df['PLAYER_ID'] = player_id

        output_file = os.path.join(PLAYERS_FOLDER, f"{player_slug}_{player_id}_customstats.parquet")
        rolling_df.to_parquet(output_file, index=False)
        logging.info(f"Saved stats to {output_file}")
        return {'type': 'success', 'data': {'player_id': player_id, 'rows': len(rolling_df)}}
    except Exception as e:
        logging.error(f"Error computing stats for player {player_id}: {e}")
        return {'type': 'error', 'data': {'player_id': player_id, 'error': str(e)}}

def main():
    # Ensure directories exist
    os.makedirs(PLAYERS_FOLDER, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)

    logging.info(f"[{datetime.now(LOCAL_TZ)}] Loading players...")
    try:
        players_df = pd.read_parquet(PLAYERS_FILE)
    except Exception as e:
        logging.error(f"[{datetime.now(LOCAL_TZ)}] Error loading {PLAYERS_FILE}: {e}")
        return

    required_cols = ['PERSON_ID', 'PLAYER_SLUG']
    missing_cols = [col for col in required_cols if col not in players_df.columns]
    if missing_cols:
        logging.error(f"Missing columns in {PLAYERS_FILE}: {missing_cols}")
        return

    # Prepare arguments for parallel processing
    args = [(str(row['PERSON_ID']), row['PLAYER_SLUG']) for _, row in players_df.iterrows()]
    logging.info(f"[{datetime.now(LOCAL_TZ)}] Processing stats for {len(args)} players...")

    num_processes = max(1, cpu_count() - 1)
    errors = []
    with Pool(processes=num_processes) as pool:
        results = list(tqdm(pool.imap_unordered(compute_rolling_stats, args), total=len(args), desc="Processing player stats"))

    for res in results:
        if res['type'] == 'error':
            errors.append(res['data'])

    if errors:
        logging.info(f"[{datetime.now(LOCAL_TZ)}] Logged {len(errors)} errors to {ERROR_LOG_FILE}")
    else:
        logging.info(f"[{datetime.now(LOCAL_TZ)}] No errors encountered.")

if __name__ == "__main__":
    main()
