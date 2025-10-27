import os
import pandas as pd
import numpy as np
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
import logging

# Setup logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(level=logging.INFO, filename='logs/customgamelogs.log', filemode='a', format='%(asctime)s - %(levelname)s - %(message)s')

# Define stats and windows
STATS = ['FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT',
         'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', 'PLUS_MINUS']
WINDOWS = [10, 50]
ROLLING_COLS = [f'{stat}_{w}' for stat in STATS for w in WINDOWS]

def parse_player_ids(players_data, game_id='unknown', col='unknown'):
    """Parse player IDs from string, list, or nested array format."""
    logging.debug(f"Processing {col} for game {game_id}")
    if players_data is None or (isinstance(players_data, (list, np.ndarray)) and len(players_data) == 0):
        logging.warning(f"Empty or None {col} in game {game_id}")
        return []
    try:
        # Handle nested list/array: [array([p1, p2, ...])] or [[p1, p2, ...]]
        if isinstance(players_data, (list, np.ndarray)) and len(players_data) > 0 and isinstance(players_data[0], (list, np.ndarray)):
            players_data = players_data[0]  # Flatten nested structure
        # Handle direct list or array
        if isinstance(players_data, (list, np.ndarray)):
            parsed_ids = [str(pid) for pid in players_data if str(pid).strip()]
            logging.debug(f"Parsed {col} in game {game_id}: {parsed_ids}")
            return parsed_ids
        # Handle string format: "[1629631 1628381 ...]"
        elif isinstance(players_data, str) and players_data.strip():
            parsed_ids = [str(pid) for pid in players_data.strip('[]').split() if pid.strip()]
            logging.debug(f"Parsed {col} in game {game_id}: {parsed_ids}")
            return parsed_ids
        else:
            logging.warning(f"Unexpected {col} format in game {game_id}: {type(players_data)}")
            return []
    except Exception as e:
        logging.error(f"Error parsing {col} in game {game_id}: {e}")
        return []

def process_game(args):
    """Process a single game to compute team-averaged rolling stats with partial data."""
    game, id_to_slug, players_folder = args
    game_id = str(game['game_id'])
    game_id_padded = game_id.zfill(10)  # Try both padded and unpadded formats
    try:
        team_stats = {}
        for team_prefix, team_col in [('home', 'home_team_players'), ('away', 'away_team_players')]:
            player_ids = parse_player_ids(game[team_col], game_id, team_col)
            logging.info(f"Parsed {len(player_ids)} players for {team_prefix} in game {game_id}: {player_ids}")
            if not player_ids:
                logging.warning(f"No players parsed for {team_prefix} team in game {game_id}")
                team_stats.update({f'{team_prefix}_{col}': np.nan for col in ROLLING_COLS})
                continue
            team_data = []
            valid_players = 0
            for pid in player_ids:
                slug = id_to_slug.get(pid)
                if not slug:
                    logging.warning(f"No slug for player {pid} in game {game_id}")
                    continue
                file_path = os.path.join(players_folder, f"{slug}_{pid}_customstats.parquet")
                if not os.path.exists(file_path):
                    logging.info(f"No customstats file for player {pid} in game {game_id}")
                    continue
                try:
                    df = pd.read_parquet(file_path)
                    if 'GAME_ID' not in df.columns:
                        logging.warning(f"GAME_ID missing in {file_path}")
                        continue
                    # Try both padded and unpadded game_id
                    df['GAME_ID'] = df['GAME_ID'].astype(str)
                    game_id_variants = [game_id, game_id_padded]
                    player_row = None
                    for gid in game_id_variants:
                        if gid in df['GAME_ID'].values:
                            player_row = df[df['GAME_ID'] == gid][ROLLING_COLS].iloc[0]
                            break
                    if player_row is not None and not player_row.isna().all():
                        team_data.append(player_row)
                        valid_players += 1
                    else:
                        logging.info(f"No valid row or all NaN for player {pid} in game {game_id}")
                except Exception as e:
                    logging.error(f"Error loading {file_path} for game {game_id}: {e}")
            if team_data:
                team_avg = pd.DataFrame(team_data).mean().round(2)
                team_stats.update({f'{team_prefix}_{col}': team_avg.get(col, np.nan) for col in ROLLING_COLS})
                logging.info(f"Computed stats for {team_prefix} team in game {game_id} with {valid_players} valid players")
            else:
                logging.warning(f"No valid data for {team_prefix} team in game {game_id} with {valid_players} valid players")
                team_stats.update({f'{team_prefix}_{col}': np.nan for col in ROLLING_COLS})
        return {'type': 'success', 'data': {'game_id': game_id_padded, **team_stats}}
    except Exception as e:
        logging.error(f"Error processing game {game_id}: {e}")
        return {'type': 'error', 'data': {'game_id': game_id_padded, 'error': str(e)}}

def generate_customgamelogs(num_games, gamelogs_file='gamelogs.parquet', players_file='players.parquet', players_folder='players', output_file='customgamelogs.parquet'):
    """Generate or update customgamelogs.parquet with team-averaged rolling stats and winner column."""
    if not os.path.exists(gamelogs_file):
        raise FileNotFoundError(f"{gamelogs_file} not found")
    gamelogs = pd.read_parquet(gamelogs_file)
    logging.info(f"Loaded {len(gamelogs)} games from {gamelogs_file}")

    if len(gamelogs) < num_games:
        logging.warning(f"Only {len(gamelogs)} games available, using all")
        num_games = len(gamelogs)
    recent_games = gamelogs.tail(num_games).copy()

    required_cols = ['game_id', 'game_date', 'home_team_score', 'away_team_score', 'home_team_players', 'away_team_players']
    missing_cols = [col for col in required_cols if col not in recent_games.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in {gamelogs_file}: {missing_cols}")

    # Add winner column
    recent_games['winner'] = (recent_games['home_team_score'] > recent_games['away_team_score']).astype(int)

    # Initialize team stat columns
    for team_prefix in ['home', 'away']:
        for col in ROLLING_COLS:
            recent_games[f'{team_prefix}_{col}'] = np.nan

    # Load players.parquet for ID to slug mapping
    if not os.path.exists(players_file):
        raise FileNotFoundError(f"{players_file} not found")
    players_df = pd.read_parquet(players_file)
    required_cols = ['PERSON_ID', 'PLAYER_SLUG']
    missing_cols = [col for col in required_cols if col not in players_df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in {players_file}: {missing_cols}")
    id_to_slug = {str(row['PERSON_ID']): row['PLAYER_SLUG'] for _, row in players_df.iterrows()}

    # Process games in parallel
    args = [(row, id_to_slug, players_folder) for _, row in recent_games.iterrows()]
    num_processes = max(1, cpu_count() - 1)
    logging.info(f"Processing {len(args)} games with {num_processes} processes...")
    with Pool(processes=num_processes) as pool:
        results = list(tqdm(pool.imap_unordered(process_game, args), total=len(args), desc="Processing games"))

    # Update recent_games with team stats
    errors = []
    for res in results:
        if res['type'] == 'success':
            game_id = res['data']['game_id']
            idx = recent_games[recent_games['game_id'].astype(str).str.zfill(10) == game_id].index
            if not idx.empty:
                for col, value in res['data'].items():
                    if col != 'game_id':
                        recent_games.at[idx[0], col] = value
            else:
                logging.warning(f"Game {game_id} not found in recent_games")
        else:
            errors.append(res['data'])

    # Log errors
    if errors:
        logging.info(f"Logged {len(errors)} errors in processing")
        for err in errors:
            logging.error(f"Game {err['game_id']}: {err['error']}")

    # Append or update output
    if os.path.exists(output_file):
        existing = pd.read_parquet(output_file)
        existing['game_id'] = existing['game_id'].astype(str).str.zfill(10)
        existing = existing[~existing['game_id'].isin(recent_games['game_id'].astype(str).str.zfill(10))]
        updated = pd.concat([existing, recent_games], ignore_index=True)
    else:
        updated = recent_games

    updated.to_parquet(output_file, index=False)
    logging.info(f"Saved {len(updated)} games to {output_file} with columns: {list(updated.columns)}")

if __name__ == "__main__":
    num_games = 10000
    generate_customgamelogs(num_games)
