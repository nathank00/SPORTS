import pandas as pd
from nba_api.stats.endpoints import boxscoretraditionalv3, boxscoreadvancedv3
from time import sleep
import random
from multiprocessing import Pool
from tqdm import tqdm
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import logging
import functools
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Team mappings
team_id_to_name = {
    '1610612737': 'Atlanta Hawks', '1610612738': 'Boston Celtics', '1610612739': 'Cleveland Cavaliers',
    '1610612740': 'New Orleans Pelicans', '1610612741': 'Chicago Bulls', '1610612742': 'Dallas Mavericks',
    '1610612743': 'Denver Nuggets', '1610612744': 'Golden State Warriors', '1610612745': 'Houston Rockets',
    '1610612746': 'LA Clippers', '1610612747': 'Los Angeles Lakers', '1610612748': 'Miami Heat',
    '1610612749': 'Milwaukee Bucks', '1610612750': 'Minnesota Timberwolves', '1610612751': 'Brooklyn Nets',
    '1610612752': 'New York Knicks', '1610612753': 'Orlando Magic', '1610612754': 'Indiana Pacers',
    '1610612755': 'Philadelphia 76ers', '1610612756': 'Phoenix Suns', '1610612757': 'Portland Trail Blazers',
    '1610612758': 'Sacramento Kings', '1610612759': 'San Antonio Spurs', '1610612760': 'Oklahoma City Thunder',
    '1610612761': 'Toronto Raptors', '1610612762': 'Utah Jazz', '1610612763': 'Memphis Grizzlies',
    '1610612764': 'Washington Wizards', '1610612765': 'Detroit Pistons', '1610612766': 'Charlotte Hornets'
}

def create_session_with_retries():
    """Create a requests session with retry logic and exponential backoff."""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=2,  # Exponential backoff: 2s, 4s, 8s
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount('https://', adapter)
    return session

def patch_requests_get():
    """Monkey-patch requests.get to use a session with retries."""
    original_get = requests.get
    session = create_session_with_retries()

    @functools.wraps(original_get)
    def patched_get(*args, **kwargs):
        return session.get(*args, **kwargs)

    requests.get = patched_get
    return original_get

def restore_requests_get(original_get):
    """Restore the original requests.get function."""
    requests.get = original_get

def process_game(args):
    """
    Process a single NBA game to collect box score data.

    Args:
        args (tuple): (game_id, game_date, home_team_id, away_team_id, SEASON_ID)

    Returns:
        dict: {'type': 'success', 'data': pd.DataFrame} or {'type': 'error', 'data': dict}
    """
    game_id, game_date, home_team_id, away_team_id, SEASON_ID = args
    try:
        sleep(random.uniform(1.0, 2.0))  # Rate limit buffer

        # Fetch traditional and advanced box scores
        boxscore_trad = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id)
        trad_df = boxscore_trad.get_data_frames()[0]  # Player stats
        boxscore_adv = boxscoreadvancedv3.BoxScoreAdvancedV3(game_id=game_id)
        adv_df = boxscore_adv.get_data_frames()[0]  # Player advanced stats

        # Validate critical columns
        trad_required = ['teamId', 'personId', 'points']  # Relaxed: removed minutesCalculated
        adv_required = ['teamId', 'personId', 'offensiveRating', 'defensiveRating']
        trad_missing = [col for col in trad_required if col not in trad_df.columns]
        adv_missing = [col for col in adv_required if col not in adv_df.columns]
        if trad_missing or adv_missing:
            logger.warning(f"Missing critical columns for game {game_id} in season {SEASON_ID}: traditional {trad_missing}, advanced {adv_missing}. Skipping game.")
            return {'type': 'error', 'data': {'game_id': game_id, 'SEASON_ID': SEASON_ID, 'error': f"Missing columns: traditional {trad_missing}, advanced {adv_missing}"}}

        if trad_df.empty or adv_df.empty:
            logger.warning(f"No box score data for game {game_id} in season {SEASON_ID}.")
            return {'type': 'success', 'data': pd.DataFrame()}

        # Merge traditional and advanced stats
        trad_columns = ['personId', 'teamId', 'points', 'reboundsTotal', 'assists', 'steals', 'blocks', 'turnovers', 'fieldGoalsMade', 'fieldGoalsAttempted', 'threePointersMade', 'threePointersAttempted', 'freeThrowsMade', 'freeThrowsAttempted']
        if 'minutesCalculated' in trad_df.columns:
            trad_columns.append('minutesCalculated')
        player_stats = pd.merge(
            trad_df[trad_columns],
            adv_df[['personId', 'teamId', 'offensiveRating', 'defensiveRating', 'usagePercentage']],
            on=['personId', 'teamId'],
            how='inner'
        )

        # Aggregate game-level data
        home_team_score = trad_df[trad_df['teamId'] == int(home_team_id)]['points'].sum()
        away_team_score = trad_df[trad_df['teamId'] == int(away_team_id)]['points'].sum()
        total_points = home_team_score + away_team_score

        home_team_players = trad_df[trad_df['teamId'] == int(home_team_id)]['personId'].tolist()
        away_team_players = trad_df[trad_df['teamId'] == int(away_team_id)]['personId'].tolist()

        home_team_name = team_id_to_name.get(str(home_team_id), "Unknown")
        away_team_name = team_id_to_name.get(str(away_team_id), "Unknown")

        # Create game-level DataFrame
        game_info = {
            'game_id': game_id,
            'game_date': pd.to_datetime(game_date),
            'SEASON_ID': SEASON_ID,
            'home_team_name': home_team_name,
            'home_team_id': home_team_id,
            'away_team_name': away_team_name,
            'away_team_id': away_team_id,
            'home_team_score': home_team_score,
            'away_team_score': away_team_score,
            'total_points': total_points,
            'home_team_players': [home_team_players],
            'away_team_players': [away_team_players],
            'player_stats': [player_stats.to_dict('records')]
        }
        game_df = pd.DataFrame([game_info])

        return {'type': 'success', 'data': game_df}
    except Exception as e:
        logger.error(f"Error processing game {game_id} in season {SEASON_ID}: {str(e)}")
        return {'type': 'error', 'data': {'game_id': game_id, 'SEASON_ID': SEASON_ID, 'error': str(e)}}

def process_all_games(games_file, output_file, error_file='logs/gamelog_errors.parquet', all=True, num_games=10, batch_size=500, num_processes=8):
    """
    Processes games in the input file in parallel batches, saving game logs to a Parquet file.
    When all=False, appends new games and updates existing ones in output_file.

    Args:
        games_file (str): Path to input Parquet file with game data.
        output_file (str): Path to output Parquet file for game logs.
        error_file (str): Path to Parquet file for error logging.
        all (bool): If True, process all games (overwrite); if False, process recent num_games (append/update).
        num_games (int): Number of recent games to process when all=False.
        batch_size (int): Number of games per parallel batch.
        num_processes (int): Number of parallel processes per batch.
    """
    logger.info(f"Loading game data from {games_file}...")
    all_games = pd.read_parquet(games_file, engine='pyarrow')

    # Load existing gamelogs.parquet if all=False and file exists
    existing_logs = pd.DataFrame()
    if not all and os.path.exists(output_file):
        try:
            existing_logs = pd.read_parquet(output_file, engine='pyarrow')
            logger.info(f"Loaded {len(existing_logs)} existing game logs from {output_file}.")
        except Exception as e:
            logger.warning(f"Failed to load existing {output_file}: {str(e)}. Starting fresh.")

    # Select games based on all or num_games
    if all:
        selected_games = all_games
    else:
        if num_games <= 0:
            logger.error("num_games must be positive when all=False.")
            raise ValueError("num_games must be positive when all=False.")
        selected_games = all_games.sort_values('GAME_DATE').tail(num_games).reset_index(drop=True)

    total_games = len(selected_games)
    logger.info(f"Processing {total_games} games in parallel batches {'(all games, overwrite)' if all else f'(most recent {num_games} games, append/update)'}...")

    game_logs = []
    errors = []

    # Group games by season for season-based logging
    seasons = selected_games['SEASON_ID'].unique()
    seasons.sort()  # Ensure seasons are processed in order

    # Patch requests.get for retries
    original_get = patch_requests_get()
    try:
        for SEASON_ID in seasons:
            season_games = selected_games[selected_games['SEASON_ID'] == SEASON_ID]
            season_games_list = season_games.reset_index(drop=True)
            season_logs = []
            logger.info(f"Processing season {SEASON_ID} ({len(season_games)} games)...")

            # Process season in batches
            for batch_num in range(0, len(season_games), batch_size):
                batch_games = season_games_list.iloc[batch_num:batch_num + batch_size]
                batch_args = [(str(row['GAME_ID']), str(row['GAME_DATE']), str(row['HOME_ID']), str(row['AWAY_ID']), SEASON_ID)
                              for _, row in batch_games.iterrows()]

                logger.info(f"  Processing batch {batch_num // batch_size + 1} ({len(batch_games)} games)...")
                with Pool(processes=num_processes) as pool:
                    batch_results = list(tqdm(pool.imap_unordered(process_game, batch_args), total=len(batch_args), desc=f"Batch {batch_num // batch_size + 1}"))

                batch_logs = [res['data'] for res in batch_results if res['type'] == 'success' and not res['data'].empty]
                batch_errors = [res['data'] for res in batch_results if res['type'] == 'error']
                errors.extend(batch_errors)

                if batch_logs:
                    batch_df = pd.concat(batch_logs, ignore_index=True)
                    season_logs.append(batch_df)

            if season_logs:
                season_df = pd.concat(season_logs, ignore_index=True)
                game_logs.append(season_df)
                logger.info(f"Completed fetching game logs for season {SEASON_ID}: {len(season_df)} games.")
            else:
                logger.warning(f"Warning: No game logs for season {SEASON_ID}.")

        # Combine and save game logs
        if game_logs:
            new_logs = pd.concat(game_logs, ignore_index=True)
            if all:
                # Overwrite mode: use only new logs
                combined_logs = new_logs
            else:
                # Append/update mode: combine with existing logs, updating matching game_ids
                if not existing_logs.empty:
                    # Remove existing rows with matching game_ids
                    existing_logs = existing_logs[~existing_logs['game_id'].isin(new_logs['game_id'])]
                    # Append new logs
                    combined_logs = pd.concat([existing_logs, new_logs], ignore_index=True)
                else:
                    combined_logs = new_logs
            combined_logs.to_parquet(output_file, index=False, engine='pyarrow')
            logger.info(f"Saved {len(combined_logs)} game logs to {output_file} {'(overwritten)' if all else '(appended/updated)'}.")
        else:
            # No new logs: keep existing or save empty
            if all or existing_logs.empty:
                logger.warning(f"No game logs processed. Saving empty DataFrame to {output_file}.")
                pd.DataFrame(columns=['game_id', 'game_date', 'SEASON_ID', 'home_team_name', 'home_team_id',
                                      'away_team_name', 'away_team_id', 'home_team_score', 'away_team_score',
                                      'total_points', 'home_team_players', 'away_team_players', 'player_stats']).to_parquet(output_file, index=False, engine='pyarrow')
            else:
                logger.warning(f"No new game logs processed. Keeping existing {output_file} with {len(existing_logs)} logs.")
                combined_logs = existing_logs
                combined_logs.to_parquet(output_file, index=False, engine='pyarrow')

        # Save errors
        if errors:
            pd.DataFrame(errors).to_parquet(error_file, index=False, engine='pyarrow')
            logger.error(f"Saved {len(errors)} errors to {error_file}.")
        else:
            logger.info("No errors encountered during processing.")

        # Summary
        logger.info("Game log processing completed.")
        logger.info(f"Total game logs: {len(combined_logs) if 'combined_logs' in locals() else len(existing_logs)}")
        if errors:
            logger.info(f"Errors encountered: {len(errors)} (see {error_file})")

    finally:
        restore_requests_get(original_get)

if __name__ == "__main__":
    process_all_games('games.parquet', 'gamelogs.parquet', all=False, num_games=100)
