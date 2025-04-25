from xata import XataClient
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import os
from tqdm import tqdm
import pytz
from z_master_builder import (
    fetch_game_metadata,
    compute_batter_team_aggregates,
    compute_pitcher_team_aggregates,
    assemble_master_row,
    clean_row_for_xata,
    load_schema_defaults
)

# Load environment variables
load_dotenv("xata-config.env")
xata = XataClient(api_key=os.getenv("XATA_API_KEY"), db_url=os.getenv("XATA_DATABASE_URL"))

def get_games_today():
    from datetime import datetime, timezone

    # Define Pacific timezone
    pacific = pytz.timezone("America/Los_Angeles")

    # Get now() in Pacific Time
    today_pacific = datetime.now(pacific).date()

    start = f"{today_pacific}T00:00:00Z"
    end = f"{today_pacific}T23:59:59Z"

    print(f"🔎 Querying for games between {start} and {end}")

    resp = xata.data().query("games", {
        "filter": {
            "game_date": {
                "$ge": start,
                "$le": end
            }
        },
        "sort": [{"game_date": "asc"}],
        "page": {"size": 1000}
    })

    if not resp.is_success():
        print("❌ Failed to query today's games from Xata.")
        print("📩 Error:", resp.get("message", "unknown error"))
        return []

    return resp.get("records", [])


def main():
    SCHEMA_DEFAULTS = load_schema_defaults("z_schema.json")
    games = get_games_today()
    tqdm.write(f">> Running delta update: {len(games)} games scheduled for today")

    updated = 0

    for game in tqdm(games, desc="Updating today's games"):
        try:
            game_id = str(game.get("game_id"))
            tqdm.write(f">> Processing game_id: {game_id}")

            meta = fetch_game_metadata(game)

            # Batter stats
            raw_home_stats = compute_batter_team_aggregates(meta["home_batters"], meta["game_date"], windows=[10, 20])
            raw_away_stats = compute_batter_team_aggregates(meta["away_batters"], meta["game_date"], windows=[10, 20])
            batter_stats_home = {f"home_{k}": v for k, v in raw_home_stats.items()}
            batter_stats_away = {f"away_{k}": v for k, v in raw_away_stats.items()}

            # Starting pitcher stats
            pitcher_stats_home_sp = compute_pitcher_team_aggregates([meta["home_sp_id"]], meta["game_date"], window=5)
            pitcher_stats_away_sp = compute_pitcher_team_aggregates([meta["away_sp_id"]], meta["game_date"], window=5)

            # Bullpen pitcher stats
            pitcher_stats_home_bp = compute_pitcher_team_aggregates(meta["home_bp_ids"], meta["game_date"], window=5)
            pitcher_stats_away_bp = compute_pitcher_team_aggregates(meta["away_bp_ids"], meta["game_date"], window=5)

            pitcher_stats_home_bp_20 = compute_pitcher_team_aggregates(meta["home_bp_ids"], meta["game_date"], window=20)
            pitcher_stats_away_bp_20 = compute_pitcher_team_aggregates(meta["away_bp_ids"], meta["game_date"], window=20)

            pitcher_stats_home_bp["ERA_20"] = pitcher_stats_home_bp_20["ERA"]
            pitcher_stats_home_bp["WHIP_20"] = pitcher_stats_home_bp_20["WHIP"]
            pitcher_stats_away_bp["ERA_20"] = pitcher_stats_away_bp_20["ERA"]
            pitcher_stats_away_bp["WHIP_20"] = pitcher_stats_away_bp_20["WHIP"]

            # Combined team pitching stats
            pitcher_ids_home_team = [meta["home_sp_id"]] + meta["home_bp_ids"]
            pitcher_ids_away_team = [meta["away_sp_id"]] + meta["away_bp_ids"]

            pitcher_stats_home_team_10 = compute_pitcher_team_aggregates(pitcher_ids_home_team, meta["game_date"], window=10)
            pitcher_stats_home_team_20 = compute_pitcher_team_aggregates(pitcher_ids_home_team, meta["game_date"], window=20)

            pitcher_stats_away_team_10 = compute_pitcher_team_aggregates(pitcher_ids_away_team, meta["game_date"], window=10)
            pitcher_stats_away_team_20 = compute_pitcher_team_aggregates(pitcher_ids_away_team, meta["game_date"], window=20)

            row = assemble_master_row(
                meta,
                batter_stats_home,
                batter_stats_away,
                pitcher_stats_home_sp,
                pitcher_stats_away_sp,
                pitcher_stats_home_bp,
                pitcher_stats_away_bp,
                pitcher_stats_home_team_10,
                pitcher_stats_home_team_20,
                pitcher_stats_away_team_10,
                pitcher_stats_away_team_20
            )

            # 🧼 Clean row
            cleaned_row = clean_row_for_xata(row, SCHEMA_DEFAULTS)

            # 🛡️ Preserve model prediction/confidence if already exists
            existing = xata.records().get("master", game_id)
            if existing.is_success():
                existing_data = existing.get("record", {})
                for key in ["model_prediction", "prediction_confidence"]:
                    if key in existing_data and existing_data[key] is not None:
                        cleaned_row[key] = existing_data[key]

            # 📤 Update row
            resp = xata.records().update("master", game_id, cleaned_row)
            if resp.is_success():
                tqdm.write(f"✅ Upserted {game_id}")
                updated += 1
            else:
                tqdm.write(f"❌ Failed to upsert {game_id}: {resp.status_code} - {resp.get('message', 'no message')}")
        except Exception as e:
            tqdm.write(f"❌ Error processing game {game_id}: {e}")

    tqdm.write(f"🎯 Done! Total upserts today: {updated}")

if __name__ == "__main__":
    main()
