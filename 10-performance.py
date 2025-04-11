import pandas as pd
from datetime import datetime
import pytz
import statsapi
import os
import glob

# Get current time in Pacific Timezone
pacific = pytz.timezone("America/Los_Angeles")
now_pacific = datetime.now(pacific)
today_str = now_pacific.strftime("%Y-%m-%d")


def get_game_metadata(game_pk: int) -> pd.DataFrame:
    try:
        game_data = statsapi.get("game", {"gamePk": game_pk})
        game_status = game_data["gameData"]["status"]["detailedState"]
        is_final = game_status in ["Final", "Game Over"]
        
        linescore = game_data.get("liveData", {}).get("linescore", {})
        away_score = linescore.get("teams", {}).get("away", {}).get("runs", 0)
        home_score = linescore.get("teams", {}).get("home", {}).get("runs", 0)
        
        start_time_utc = game_data["gameData"]["datetime"].get("dateTime", "")
        if start_time_utc:
            utc_dt = datetime.fromisoformat(start_time_utc.replace("Z", "+00:00"))
            pacific = pytz.timezone("America/Los_Angeles")
            start_time_local = utc_dt.astimezone(pacific).strftime("%H:%M")
        else:
            start_time_local = "Unknown"
        
        return pd.DataFrame([{
            "completed": int(is_final),
            "home_score": home_score,
            "away_score": away_score,
            "runs_total": home_score + away_score,
            "start_time": start_time_local
        }])
    except Exception as e:
        return pd.DataFrame([{"error": str(e)}])

# CREATE ENRICHED GAME FILES

def enrich_game_data(csv_file_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_file_path)
    enriched_rows = []

    for _, row in df.iterrows():
        game_pk = int(row['game_id'])
        meta_df = get_game_metadata(game_pk)

        if "error" in meta_df.columns:
            enriched_data = {
                "start_time": None,
                "runs_total": None,
                "prediction": None,
                "outcome": None,
                "completed": None
            }
        else:
            meta = meta_df.iloc[0]
            prediction = 1 if str(row['pick']).strip().lower() == "over" else 0

            try:
                runline = float(row['runline'])
                runs_total = meta["runs_total"]
                if runs_total > runline:
                    outcome = 1
                elif runs_total < runline:
                    outcome = 0
                else:
                    outcome = "push"
            except:
                outcome = None

            enriched_data = {
                "start_time": meta["start_time"],
                "runs_total": meta["runs_total"],
                "prediction": prediction,
                "outcome": outcome,
                "completed": meta["completed"]
            }

        row_dict = row.to_dict()
        col_keys = list(row_dict.keys())

        # Insert 'start_time' after 'game_id'
        gid_idx = col_keys.index("game_id") + 1
        front = {k: row_dict[k] for k in col_keys[:gid_idx]}
        back = {k: row_dict[k] for k in col_keys[gid_idx:]}
        row_with_start = {**front, "start_time": enriched_data.pop("start_time"), **back}

        # Now insert the rest of enriched data after 'pick'
        col_keys = list(row_with_start.keys())
        pick_idx = col_keys.index("pick") + 1
        front = {k: row_with_start[k] for k in col_keys[:pick_idx]}
        back = {k: row_with_start[k] for k in col_keys[pick_idx:]}
        final_row = {**front, **enriched_data, **back}

        enriched_rows.append(final_row)

    result_df = pd.DataFrame(enriched_rows)

    base, ext = os.path.splitext(csv_file_path)
    new_file_path = f"{base}_enriched{ext}"
    result_df.to_csv(new_file_path, index=False)

    print(f"Enriched CSV written to: {new_file_path}")
    return result_df

# Generates CUMULATIVE PERFORMANCE File

def cumulative_performance(folder_path: str) -> pd.DataFrame:
    csv_files = glob.glob(os.path.join(folder_path, "*_enriched.csv"))
    all_rows = []

    for file in csv_files:
        try:
            df = pd.read_csv(file)
            filename = os.path.basename(file)
            date_part = filename.replace("_enriched.csv", "")

            for _, row in df.iterrows():
                try:
                    completed = int(row.get("completed", 0))
                    runs_total = float(row.get("runs_total", 0))
                    runline = float(row.get("runline", 0))

                    if completed == 1 or runs_total > runline:
                        outcome = row.get("outcome", "")
                        if outcome == "push":
                            outcome = ""
                        new_row = {
                            "date": date_part,
                            "game_id": row.get("game_id"),
                            "home_team": row.get("home_team"),
                            "away_team": row.get("away_team"),
                            "runline": runline,
                            "runs_total": runs_total,
                            "prediction": row.get("prediction"),
                            "outcome": outcome
                        }
                        all_rows.append(new_row)
                except:
                    continue
        except Exception as e:
            print(f"Error reading {file}: {e}")
            continue

    result_df = pd.DataFrame(all_rows)
    output_path = os.path.join(folder_path, "cumulative_performance.csv")
    result_df.to_csv(output_path, index=False)
    print(f"Cumulative performance CSV saved to: {output_path}")
    return result_df


currentfile = f"mlb-app/src/app/api/picks/{today_str}.csv"
enrich_game_data(currentfile)

alternatefile = f"mlb-app/public/data/{today_str}.csv"
if not os.path.exists(alternatefile):
    os.makedirs(os.path.dirname(alternatefile), exist_ok=True)
    open(alternatefile, "w").close()  # Create an empty file so pd.read_csv() doesn't throw
enrich_game_data(alternatefile)

cumulative_performance("mlb-app/src/app/api/picks/")

cumulative_performance("mlb-app/public/data/")