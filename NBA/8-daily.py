import pandas as pd
import os
import numpy as np
from datetime import datetime
import pytz
from nba_api.stats.endpoints import commonteamroster
from nba_api.live.nba.endpoints import scoreboard
import xgboost as xgb
import joblib
import logging
from tenacity import retry, stop_after_attempt, wait_exponential

# Constants
PLAYERS_FOLDER = "players"
PLAYERS_FILE = "players.parquet"
PREDICTIONS_FILE = "../mlb-app/public/data/predictions.csv"
MODEL_PATH = "xgboost_nba_winner_model.json"
SCALER_PATH = "scaler.pkl"
SELECTOR_PATH = "feature_selector.pkl"
LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "daily.log")
LOCAL_TZ = pytz.timezone("America/Los_Angeles")

# Setup logging
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(level=logging.INFO, filename=LOG_FILE, filemode='a', format='%(asctime)s - %(levelname)s - %(message)s')

# Define stats and windows
STATS = ['FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT',
         'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', 'PLUS_MINUS']
WINDOWS = [10, 50]
ROLLING_COLS = [f'{stat}_{w}' for stat in STATS for w in WINDOWS]
FEATURES = [f'{prefix}_{col}' for prefix in ['home', 'away'] for col in ROLLING_COLS]

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def get_team_roster_players(team_id, season):
    """Fetch team roster player IDs with retries."""
    logging.info(f"Fetching roster for team {team_id}, season {season}...")
    try:
        roster = commonteamroster.CommonTeamRoster(team_id=team_id, season=season).get_data_frames()[0]
        players = roster['PLAYER_ID'].astype(str).tolist()
        logging.info(f"Retrieved {len(players)} players for team {team_id}")
        return players
    except Exception as e:
        logging.error(f"Error fetching roster for team {team_id}, season {season}: {e}")
        raise

def fetch_team_roster_with_fallback(team_id, season):
    """Try fetching roster with fallback season format."""
    try:
        return get_team_roster_players(team_id, season)
    except Exception:
        # Try alternative season format (e.g., '2025' instead of '2025-26')
        alt_season = season.split('-')[0]
        logging.info(f"Fallback: Trying season {alt_season} for team {team_id}")
        try:
            return get_team_roster_players(team_id, alt_season)
        except Exception as e:
            logging.error(f"Fallback failed for team {team_id}, season {alt_season}: {e}")
            return []

def get_todays_games():
    """Fetch today's NBA games."""
    logging.info("Fetching today's games...")
    today = datetime.now(LOCAL_TZ).strftime('%Y-%m-%d')
    try:
        sb = scoreboard.ScoreBoard()
        games = sb.games.get_dict()
        game_data = []
        for game in games:
            gid = str(game['gameId']).zfill(10)
            season = f'20{gid[2:4]}-{int(gid[2:4]) + 1:02d}'
            game_info = {
                'game_id': gid,
                'game_date': pd.to_datetime(today),
                'away_team_name': game['awayTeam']['teamName'],
                'home_team_name': game['homeTeam']['teamName'],
                'away_team_id': str(game['awayTeam']['teamId']),
                'home_team_id': str(game['homeTeam']['teamId']),
                'season_id': season,
                'game_status': game['gameStatus']
            }
            game_data.append(game_info)
        df = pd.DataFrame(game_data)
        logging.info(f"Fetched {len(df)} games for {today}")
        return df
    except Exception as e:
        logging.error(f"Error fetching today's games: {e}")
        return pd.DataFrame()

def parse_player_ids(players_data):
    """Parse player IDs from string, list, or array."""
    if players_data is None or (isinstance(players_data, str) and not players_data.strip()):
        return []
    try:
        if isinstance(players_data, str):
            return [pid.strip() for pid in players_data.strip('[]').split() if pid.strip()]
        if isinstance(players_data, (list, np.ndarray)) and len(players_data) > 0 and isinstance(players_data[0], (list, np.ndarray)):
            players_data = players_data[0]
        return [str(pid) for pid in players_data if str(pid).strip()]
    except Exception as e:
        logging.error(f"Error parsing player IDs: {e}")
        return []

def main():
    # Step 1: Fetch games
    todays_games = get_todays_games()
    logging.info(f"Step 1: Fetched {len(todays_games)} games")
    print(f"Step 1: Fetched {len(todays_games)} games")
    print(todays_games[['game_id', 'away_team_name', 'home_team_name', 'game_status']])
    if todays_games.empty:
        logging.info("No games today. Exiting.")
        print("No games today. Exiting.")
        return

    # Step 2: Build in-memory gamelogs
    logging.info("Step 2: Building in-memory gamelogs...")
    todays_gamelogs = []
    season = '2025-26'
    for _, game in todays_games.iterrows():
        game_id = str(game['game_id']).zfill(10)
        logging.info(f"Processing game {game_id}...")
        home_players = fetch_team_roster_with_fallback(game['home_team_id'], season)
        away_players = fetch_team_roster_with_fallback(game['away_team_id'], season)
        if not home_players or not away_players:
            logging.warning(f"No valid players for game {game_id}: home={len(home_players)}, away={len(away_players)}")
            continue
        game_info = {
            'game_id': game_id,
            'game_date': game['game_date'],
            'season_id': game['season_id'],
            'home_team_name': game['home_team_name'],
            'home_team_id': game['home_team_id'],
            'away_team_name': game['away_team_name'],
            'away_team_id': game['away_team_id'],
            'home_team_score': np.nan,
            'away_team_score': np.nan,
            'total_points': np.nan,
            'home_team_players': [home_players],
            'away_team_players': [away_players],
            'player_stats': []
        }
        todays_gamelogs.append(game_info)
    if not todays_gamelogs:
        logging.info("Step 2: No gamelogs built (no valid game data).")
        print("Step 2: No gamelogs built (no valid game data).")
        return
    todays_gamelogs = pd.DataFrame(todays_gamelogs)
    logging.info("Step 2: Built in-memory gamelogs successfully.")
    print("Step 2: Built in-memory gamelogs successfully.")
    print(todays_gamelogs[['game_id', 'home_team_name', 'away_team_name']])

    # Step 3: Skip player updates
    logging.info("Step 3: Skipping player gamelog updates (handled by 3-1-playergamelogs_delta.py).")
    print("Step 3: Skipping player gamelog updates.")

    # Step 4: Skip custom stats computation
    logging.info("Step 4: Skipping custom player stats computation (handled by 4-customplayerstats.py).")
    print("Step 4: Skipping custom player stats computation.")

    # Step 5: Build in-memory custom gamelogs
    logging.info("Step 5: Building in-memory custom gamelogs...")
    todays_custom = todays_gamelogs.copy()
    for col in FEATURES:
        todays_custom[col] = np.nan

    logging.info("Loading player data...")
    if not os.path.exists(PLAYERS_FILE):
        logging.error(f"Error: {PLAYERS_FILE} not found. Exiting.")
        print(f"Error: {PLAYERS_FILE} not found. Exiting.")
        return
    players_df = pd.read_parquet(PLAYERS_FILE)
    required_cols = ['PERSON_ID', 'PLAYER_SLUG']
    missing_cols = [col for col in required_cols if col not in players_df.columns]
    if missing_cols:
        logging.error(f"Missing columns in {PLAYERS_FILE}: {missing_cols}")
        print(f"Error: Missing columns in {PLAYERS_FILE}: {missing_cols}")
        return
    id_to_slug = {str(row['PERSON_ID']): row['PLAYER_SLUG'] for _, row in players_df.iterrows()}

    player_dfs = {}
    for pid in set().union(*(parse_player_ids(row['home_team_players']) + parse_player_ids(row['away_team_players']) for _, row in todays_gamelogs.iterrows())):
        slug = id_to_slug.get(pid)
        if not slug:
            logging.warning(f"No slug for player {pid}")
            continue
        path = os.path.join(PLAYERS_FOLDER, f"{slug}_{pid}_customstats.parquet")
        if os.path.exists(path):
            try:
                df = pd.read_parquet(path)
                df['GAME_ID'] = df['GAME_ID'].astype(str).str.zfill(10)
                # Convert GAME_DATE to Timestamp
                try:
                    df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'])
                except Exception as e:
                    logging.warning(f"Error converting GAME_DATE for player {pid}: {e}")
                    continue
                player_dfs[pid] = df
                logging.info(f"Loaded stats for player {pid}")
            except Exception as e:
                logging.error(f"Error loading {path}: {e}")
        else:
            logging.warning(f"Missing custom stats for player {pid} ({slug})")

    for idx, game in todays_custom.iterrows():
        game_id = str(game['game_id']).zfill(10)
        logging.info(f"Computing team stats for game {game_id}...")
        for team_prefix, players_col in [('home', 'home_team_players'), ('away', 'away_team_players')]:
            pids = parse_player_ids(game[players_col])
            team_data = []
            valid_players = 0
            for pid in pids:
                if pid in player_dfs and not player_dfs[pid].empty:
                    try:
                        player_row = player_dfs[pid][player_dfs[pid]['GAME_DATE'] < game['game_date']][ROLLING_COLS].tail(1)
                        if not player_row.empty and not player_row.isna().all().all():
                            team_data.append(player_row.iloc[0])
                            valid_players += 1
                    except Exception as e:
                        logging.warning(f"Error processing stats for player {pid} in game {game_id}: {e}")
                else:
                    logging.info(f"No valid stats for player {pid} in game {game_id}")
            if team_data:
                team_avg = pd.DataFrame(team_data).mean().round(2)
                for col in ROLLING_COLS:
                    todays_custom.at[idx, f'{team_prefix}_{col}'] = team_avg.get(col, np.nan)
                logging.info(f"Computed {team_prefix} team stats for game {game_id} with {valid_players} valid players")
            else:
                logging.warning(f"No valid player data for {team_prefix} team in game {game_id}")

    logging.info("Step 5: Built in-memory custom gamelogs successfully.")
    print("Step 5: Built in-memory custom gamelogs successfully.")
    print(todays_custom[['game_id', 'home_team_name', 'away_team_name'] + FEATURES[:5] + ['home_PLUS_MINUS_50', 'away_PLUS_MINUS_50']])

    # Step 6: Run inference
    logging.info("Step 6: Running inference...")
    if not os.path.exists(MODEL_PATH):
        logging.error(f"Error: Model file {MODEL_PATH} not found. Exiting.")
        print(f"Error: Model file {MODEL_PATH} not found. Exiting.")
        return
    if not os.path.exists(SCALER_PATH):
        logging.error(f"Error: Scaler file {SCALER_PATH} not found. Exiting.")
        print(f"Error: Scaler file {SCALER_PATH} not found. Exiting.")
        return
    if not os.path.exists(SELECTOR_PATH):
        logging.error(f"Error: Feature selector file {SELECTOR_PATH} not found. Exiting.")
        print(f"Error: Feature selector file {SELECTOR_PATH} not found. Exiting.")
        return

    model = xgb.XGBClassifier()
    model.load_model(MODEL_PATH)
    scaler = joblib.load(SCALER_PATH)
    selector = joblib.load(SELECTOR_PATH)
    predictions = pd.read_csv(PREDICTIONS_FILE, dtype={'GAME_ID': str}) if os.path.exists(PREDICTIONS_FILE) else pd.DataFrame(columns=['GAME_ID', 'AWAY_NAME', 'HOME_NAME', 'PREDICTION', 'CONFIDENCE', 'TIMESTAMP'])

    new_preds = []
    for _, row in todays_custom.iterrows():
        game_id = str(row['game_id']).zfill(10)
        if game_id in predictions['GAME_ID'].astype(str).values:
            logging.info(f"Skipping game {game_id}: prediction already exists")
            print(f"Skipping game {game_id}: prediction already exists")
            continue
        X = row[FEATURES]
        if X.isna().any():
            logging.warning(f"Skipping game {game_id}: missing rolling stats")
            print(f"Skipping game {game_id}: missing rolling stats")
            continue
        X_df = pd.DataFrame([X], columns=FEATURES)
        X_scaled = scaler.transform(X_df)
        X_selected = selector.transform(X_scaled)
        pred = model.predict(X_selected)[0]
        confidence = model.predict_proba(X_selected)[0][1]  # Probability of home win
        new_preds.append({
            'GAME_ID': game_id,
            'AWAY_NAME': row['away_team_name'],
            'HOME_NAME': row['home_team_name'],
            'PREDICTION': int(pred),
            'CONFIDENCE': round(float(confidence), 4),
            'TIMESTAMP': datetime.now(LOCAL_TZ).isoformat()
        })
        logging.info(f"Generated prediction for game {game_id}: {pred} (confidence: {confidence:.4f})")
        print(f"Generated prediction for game {game_id}: {pred} (confidence: {confidence:.4f})")

    if new_preds:
        new_df = pd.DataFrame(new_preds)
        predictions = pd.concat([predictions, new_df], ignore_index=True)
        predictions.to_csv(PREDICTIONS_FILE, index=False)
        logging.info(f"Step 6: Saved {len(new_preds)} new predictions to {PREDICTIONS_FILE}")
        print("Step 6: Saved new predictions successfully.")
        print("New predictions:")
        print(new_df)
    else:
        logging.info("Step 6: No new predictions generated (all games predicted or missing data).")
        print("Step 6: No new predictions generated.")

if __name__ == "__main__":
    main()
