import pandas as pd
from nba_api.stats.endpoints import commonallplayers, leaguegamefinder
from datetime import datetime
import os
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import logging
import functools

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Team mappings
team_name_mapping = {
    "ATL": "Atlanta Hawks", "BOS": "Boston Celtics", "BKN": "Brooklyn Nets", "CHA": "Charlotte Hornets",
    "CHI": "Chicago Bulls", "CLE": "Cleveland Cavaliers", "DAL": "Dallas Mavericks", "DEN": "Denver Nuggets",
    "DET": "Detroit Pistons", "GSW": "Golden State Warriors", "HOU": "Houston Rockets", "IND": "Indiana Pacers",
    "LAC": "Los Angeles Clippers", "LAL": "Los Angeles Lakers", "MEM": "Memphis Grizzlies", "MIA": "Miami Heat",
    "MIL": "Milwaukee Bucks", "MIN": "Minnesota Timberwolves", "NOP": "New Orleans Pelicans", "NYK": "New York Knicks",
    "OKC": "Oklahoma City Thunder", "ORL": "Orlando Magic", "PHI": "Philadelphia 76ers", "PHX": "Phoenix Suns",
    "POR": "Portland Trail Blazers", "SAC": "Sacramento Kings", "SAS": "San Antonio Spurs", "TOR": "Toronto Raptors",
    "UTA": "Utah Jazz", "WAS": "Washington Wizards"
}

team_name_to_id_mapping = {
    "Atlanta Hawks": "1610612737", "Boston Celtics": "1610612738", "Brooklyn Nets": "1610612751",
    "Charlotte Hornets": "1610612766", "Chicago Bulls": "1610612741", "Cleveland Cavaliers": "1610612739",
    "Dallas Mavericks": "1610612742", "Denver Nuggets": "1610612743", "Detroit Pistons": "1610612765",
    "Golden State Warriors": "1610612744", "Houston Rockets": "1610612745", "Indiana Pacers": "1610612754",
    "Los Angeles Clippers": "1610612746", "Los Angeles Lakers": "1610612747", "Memphis Grizzlies": "1610612763",
    "Miami Heat": "1610612748", "Milwaukee Bucks": "1610612749", "Minnesota Timberwolves": "1610612750",
    "New Orleans Pelicans": "1610612740", "New York Knicks": "1610612752", "Oklahoma City Thunder": "1610612760",
    "Orlando Magic": "1610612753", "Philadelphia 76ers": "1610612755", "Phoenix Suns": "1610612756",
    "Portland Trail Blazers": "1610612757", "Sacramento Kings": "1610612758", "San Antonio Spurs": "1610612759",
    "Toronto Raptors": "1610612761", "Utah Jazz": "1610612762", "Washington Wizards": "1610612764"
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

def get_active_players(season):
    """Fetch active NBA players for a single season."""
    try:
        players = commonallplayers.CommonAllPlayers(is_only_current_season=0, season=season)
        df = players.get_data_frames()[0]
        if df.empty:
            logger.warning(f"Warning: No players found for season {season}.")
            return {'type': 'success', 'data': pd.DataFrame(columns=['SEASON', 'PERSON_ID', 'DISPLAY_FIRST_LAST', 'TEAM_ID', 'TEAM_NAME', 'FROM_YEAR', 'TO_YEAR', 'PLAYER_SLUG'])}

        required_columns = ['PERSON_ID', 'DISPLAY_FIRST_LAST', 'ROSTERSTATUS', 'TEAM_ID', 'TEAM_NAME', 'FROM_YEAR', 'TO_YEAR']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        active_players = df[df['ROSTERSTATUS'] == 1][required_columns + ['PLAYER_SLUG']].copy()
        active_players['SEASON'] = season
        logger.info(f"Completed fetching players for season {season}: {len(active_players)} players.")
        return {'type': 'success', 'data': active_players}
    except Exception as e:
        logger.error(f"Error fetching players for season {season}: {str(e)}")
        return {'type': 'error', 'data': {'season': season, 'error': str(e)}}

def get_season_schedule(season):
    """Fetch NBA season schedule using leaguegamefinder."""
    try:
        game_finder = leaguegamefinder.LeagueGameFinder(season_nullable=season, league_id_nullable='00')
        df = game_finder.get_data_frames()[0]
        if df.empty:
            logger.warning(f"Warning: No games found for season {season}.")
            return {'type': 'success', 'data': pd.DataFrame(columns=['SEASON_ID', 'GAME_DATE', 'GAME_ID', 'AWAY_NAME', 'HOME_NAME', 'AWAY_ID', 'HOME_ID'])}

        required_columns = ['SEASON_ID', 'GAME_DATE', 'GAME_ID', 'MATCHUP']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'], errors='coerce')
        df = df.sort_values('GAME_DATE').drop_duplicates(subset='GAME_ID')

        df['is_away'] = df['MATCHUP'].str.contains('@')
        df['HOME_NAME'] = df.apply(
            lambda x: x['MATCHUP'].split('@')[1].strip() if x['is_away'] else x['MATCHUP'].split('vs.')[0].strip(),
            axis=1
        )
        df['AWAY_NAME'] = df.apply(
            lambda x: x['MATCHUP'].split('@')[0].strip() if x['is_away'] else x['MATCHUP'].split('vs.')[1].strip(),
            axis=1
        )
        df['HOME_NAME'] = df['HOME_NAME'].map(team_name_mapping)
        df['AWAY_NAME'] = df['AWAY_NAME'].map(team_name_mapping)
        df['AWAY_ID'] = df['AWAY_NAME'].map(team_name_to_id_mapping)
        df['HOME_ID'] = df['HOME_NAME'].map(team_name_to_id_mapping)
        df = df[['SEASON_ID', 'GAME_DATE', 'GAME_ID', 'AWAY_NAME', 'HOME_NAME', 'AWAY_ID', 'HOME_ID']]
        logger.info(f"Completed fetching games for season {season}: {len(df)} games.")
        return {'type': 'success', 'data': df}
    except Exception as e:
        logger.error(f"Error fetching games for season {season}: {str(e)}")
        return {'type': 'error', 'data': {'season': season, 'error': str(e)}}

def process_data(start_season, end_season, players_file, games_file):
    """Fetch all players and games for all seasons sequentially and overwrite parquet files."""
    logger.info("Starting data fetch for all seasons...")
    engine = 'pyarrow'

    # Generate seasons, including current season
    today = datetime.today()
    current_year = today.year
    current_month = today.month
    start_year = int(start_season[:4])
    end_year = current_year if current_month >= 10 else current_year - 1
    seasons = [f"{year}-{str(year+1)[-2:]}" for year in range(start_year, end_year + 1)]
    logger.info(f"Processing seasons: {', '.join(seasons)}")

    # Patch requests.get for retries
    original_get = patch_requests_get()
    try:
        # Sequential processing for players
        player_results = []
        for season in seasons:
            result = get_active_players(season)
            player_results.append(result)

        # Sequential processing for games
        game_results = []
        for season in seasons:
            result = get_season_schedule(season)
            game_results.append(result)

        # Process player results
        player_dfs = [res['data'] for res in player_results if res['type'] == 'success' and not res['data'].empty]
        errors = [res['data'] for res in player_results if res['type'] == 'error']
        if player_dfs:
            combined_players = pd.concat(player_dfs, ignore_index=True).drop_duplicates(subset=['PERSON_ID', 'SEASON'])
            combined_players.to_parquet(players_file, index=False, engine=engine)
            logger.info(f"Saved {len(combined_players)} unique player records to {players_file}.")
        else:
            combined_players = pd.DataFrame(columns=['SEASON', 'PERSON_ID', 'DISPLAY_FIRST_LAST', 'TEAM_ID', 'TEAM_NAME', 'FROM_YEAR', 'TO_YEAR', 'PLAYER_SLUG'])
            combined_players.to_parquet(players_file, index=False, engine=engine)
            logger.warning(f"No player data fetched. Saved empty DataFrame to {players_file}.")

        # Process game results
        game_dfs = [res['data'] for res in game_results if res['type'] == 'success' and not res['data'].empty]
        errors.extend([res['data'] for res in game_results if res['type'] == 'error'])
        if game_dfs:
            combined_games = pd.concat(game_dfs, ignore_index=True).sort_values('GAME_DATE').drop_duplicates(subset='GAME_ID')
            combined_games.to_parquet(games_file, index=False, engine=engine)
            logger.info(f"Saved {len(combined_games)} game records to {games_file}.")
        else:
            combined_games = pd.DataFrame(columns=['SEASON_ID', 'GAME_DATE', 'GAME_ID', 'AWAY_NAME', 'HOME_NAME', 'AWAY_ID', 'HOME_ID'])
            combined_games.to_parquet(games_file, index=False, engine=engine)
            logger.warning(f"No game data fetched. Saved empty DataFrame to {games_file}.")

        # Log errors
        if errors:
            pd.DataFrame(errors).to_parquet('fetch_errors.parquet', index=False, engine=engine)
            logger.error(f"Saved {len(errors)} errors to fetch_errors.parquet")

        # Summary
        logger.info("Data fetch completed.")
        logger.info(f"Total players: {len(combined_players)}")
        logger.info(f"Total games: {len(combined_games)}")
        if errors:
            logger.info(f"Errors encountered: {len(errors)} (see fetch_errors.parquet)")

        return combined_players, combined_games
    finally:
        restore_requests_get(original_get)

if __name__ == "__main__":
    start_season = '2020-21'
    today = datetime.today()
    end_year = today.year if today.month >= 10 else today.year - 1
    end_season = f"{end_year}-{str(end_year + 1)[-2:]}"
    process_data(start_season, end_season, 'players.parquet', 'games.parquet')
