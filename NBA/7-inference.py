import pandas as pd
from datetime import datetime
import xgboost as xgb
import joblib
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_inference():
    """Run model inference on today's games if complete data exists."""
    # Column name for game ID in customgamelogs.parquet
    GAME_ID_CUSTOM = 'game_id'

    # Load today's games
    today = datetime.today().date()
    games = pd.read_parquet('games.parquet')
    today_games = games[games['GAME_DATE'].dt.date == today].copy()

    if today_games.empty:
        logger.info("No games scheduled for today.")
        return

    # Load custom game logs and model dependencies
    try:
        customgamelogs = pd.read_parquet('customgamelogs.parquet')
        model = xgb.XGBClassifier()
        model.load_model('model/xgboost_nba_winner_model.json')
        scaler = joblib.load('model/scaler.pkl')
    except FileNotFoundError as e:
        logger.error(f"Missing file: {str(e)}")
        raise FileNotFoundError(f"Missing file: {str(e)}")

    # Verify game ID column in customgamelogs
    if GAME_ID_CUSTOM not in customgamelogs.columns:
        logger.error(f"Game ID column '{GAME_ID_CUSTOM}' not found in customgamelogs.parquet. Available columns: {list(customgamelogs.columns)}")
        raise KeyError(f"Game ID column '{GAME_ID_CUSTOM}' not found in customgamelogs.parquet")

    # Get feature columns from training (same as in training script)
    feature_columns = [col for col in customgamelogs.columns if
                      (col.startswith('home_') or col.startswith('away_')) and
                      (col.endswith('_10') or col.endswith('_50'))]

    if not feature_columns:
        logger.error("No rolling stat features found in customgamelogs.parquet")
        raise ValueError("No rolling stat features found in customgamelogs.parquet")

    logger.info(f"Feature columns: {feature_columns}")

    # Load or initialize predictions file
    predictions_file = 'model/predictions.csv'
    os.makedirs('model', exist_ok=True)
    if os.path.exists(predictions_file):
        predictions = pd.read_csv(predictions_file)
    else:
        predictions = pd.DataFrame(columns=['GAME_ID', 'AWAY_NAME', 'HOME_NAME', 'PREDICTION', 'TIMESTAMP'])

    # Process each game
    for _, game in today_games.iterrows():
        game_id = game['GAME_ID']
        away = game['AWAY_NAME']
        home = game['HOME_NAME']
        print(f"Game: {away} @ {home} (ID: {game_id})")

        # Check if game data exists
        if game_id not in customgamelogs[GAME_ID_CUSTOM].values:
            print("Data: Incomplete (missing in customgamelogs)")
            logger.warning(f"Game {game_id} not found in customgamelogs.parquet")
            continue

        # Check for complete feature data
        row = customgamelogs[customgamelogs[GAME_ID_CUSTOM] == game_id].iloc[0]
        missing_features = row[feature_columns][row[feature_columns].isna()].index.tolist()
        if missing_features:
            print("Data: Incomplete (missing feature values)")
            logger.warning(f"Game {game_id} has missing features: {missing_features}")
            continue

        print("Data: Complete")
        # Prepare features for inference as a DataFrame to preserve column names
        X = pd.DataFrame([row[feature_columns]], columns=feature_columns)
        X_scaled = scaler.transform(X)  # Apply the same scaler used in training
        pred = model.predict(X_scaled)[0]
        pred_label = "Home win (1)" if pred == 1 else "Away win (0)"
        print(f"Prediction: {pred_label}")

        # Save new prediction
        if game_id not in predictions['GAME_ID'].values:
            new_row = pd.DataFrame({
                'GAME_ID': [game_id],
                'AWAY_NAME': [away],
                'HOME_NAME': [home],
                'PREDICTION': [pred],
                'TIMESTAMP': [datetime.now().isoformat()]
            })
            predictions = pd.concat([predictions, new_row], ignore_index=True)
            predictions.to_csv(predictions_file, index=False)
            logger.info(f"Saved prediction for game {game_id}")
        else:
            logger.info(f"Prediction for game {game_id} already exists; not overwriting")

if __name__ == "__main__":
    run_inference()
