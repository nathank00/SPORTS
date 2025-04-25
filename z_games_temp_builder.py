import os
import requests
import pandas as pd
import pytz
import statsapi
from datetime import datetime
from xata.client import XataClient
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

print("\n---------- Now running x1.py ----------\n")

load_dotenv("xata-config.env")
xata = XataClient()

team_to_oddshark_id = {
    120: 27017, 146: 27022, 139: 27003, 144: 27009, 140: 27002, 117: 27023,
    135: 26996, 143: 26995, 110: 27008, 136: 27011, 121: 27014, 109: 27007,
    108: 26998, 133: 27016, 141: 27010, 114: 27014, 138: 27019, 142: 27005,
    116: 26999, 147: 27001, 137: 26997, 118: 27006, 145: 27018, 115: 27004,
    111: 27021, 119: 27015, 112: 27020, 158: 27012, 113: 27000, 134: 27013
}

def enrich_game_metadata(game):
    game_pk = int(game["game_id"])
    try:
        game_data = statsapi.get("game", {"gamePk": game_pk})
        status = game_data["gameData"]["status"]["detailedState"]
        game_started = status not in ["Scheduled", "Pre-Game"]
        game_complete = status in ["Final", "Game Over"]

        linescore = game_data.get("liveData", {}).get("linescore", {})
        away_score = linescore.get("teams", {}).get("away", {}).get("runs", 0)
        home_score = linescore.get("teams", {}).get("home", {}).get("runs", 0)

        start_time_utc = game_data["gameData"]["datetime"].get("dateTime", "")
        if start_time_utc:
            utc_dt = datetime.fromisoformat(start_time_utc.replace("Z", "+00:00"))
            start_time_local = utc_dt.astimezone(pytz.timezone("America/Los_Angeles")).isoformat()
        else:
            start_time_local = None

        game.update({
            "start_time": start_time_local,
            "runs_away": away_score,
            "runs_home": home_score,
            "runs_total": away_score + home_score,
            "game_started": game_started,
            "game_complete": game_complete
        })
    except:
        game.update({
            "start_time": None,
            "runs_away": None,
            "runs_home": None,
            "runs_total": None,
            "game_started": None,
            "game_complete": None
        })
    return game

def fetch_runline(oddshark_id, game_date, game_id):
    try:
        url = f"https://www.oddsshark.com/stats/gamelog/baseball/mlb/{oddshark_id}?season={game_date.year}"
        tables = pd.read_html(url)
        df = tables[0]
        df["Date"] = pd.to_datetime(df["Date"], format="%b %d, %Y", errors="coerce")
        row = df[df["Date"] == game_date]
        if row.empty:
            print(f"MISSING RUNLINE (OK) - {game_id} - {game_date}")
        return float(row.iloc[0]["Total"]) if not row.empty else None
    except:
        print(f"API FAIL - {game_id}")
        return None

# === Clean Floats (Before Inserting to DB) ===
def clean_json_compat(d):
    for k, v in d.items():
        if isinstance(v, float) and (pd.isna(v) or pd.isnull(v)):
            d[k] = None
    return d

MLB_SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"
today = datetime.today().strftime("%Y-%m-%d")
current_year = datetime.today().year

games = []
for year in range(2021, current_year + 1):
    end_date = today if year == current_year else f"{year}-12-31"
    r = requests.get(MLB_SCHEDULE_URL, params={"sportId": 1, "startDate": f"{year}-01-01", "endDate": end_date})
    if r.status_code != 200:
        continue
    for d in r.json().get("dates", []):
        for g in d.get("games", []):
            games.append({
                "game_id": str(g["gamePk"]),
                "game_date": pd.to_datetime(g["officialDate"]),
                "home_id": str(g["teams"]["home"]["team"]["id"]).zfill(3),
                "home_name": g["teams"]["home"]["team"]["name"],
                "away_id": str(g["teams"]["away"]["team"]["id"]).zfill(3),
                "away_name": g["teams"]["away"]["team"]["name"]
            })

games = list({g["game_id"]: g for g in games}.values())

# === Add Oddshark IDs ===
for game in games:
    game["home_oddshark_id"] = str(team_to_oddshark_id.get(int(game["home_id"]), ""))
    game["away_oddshark_id"] = str(team_to_oddshark_id.get(int(game["away_id"]), ""))

# === Threaded game metadata enrichment with progress
enriched_games = []
with ThreadPoolExecutor(max_workers=12) as executor:
    futures = {executor.submit(enrich_game_metadata, game): game for game in games}
    for i, future in enumerate(as_completed(futures)):
        enriched_games.append(future.result())
        if i % 500 == 0:
            print(f"Metadata processed: {i} games")

# === Threaded runline scraping
def fetch_for_game(game):
    if not game["home_oddshark_id"]:
        print(f"UNKNOWN TEAM ID (OK) - {game['game_id']} (no oddshark ID)")
        return game["game_id"], None
    month_day = (game["game_date"].month, game["game_date"].day)
    if month_day < (3, 15):  # Before May 15
        print(f"SKIP GAME (OK) - {game['game_id']} (spring training)")
        return game["game_id"], None
    return game["game_id"], fetch_runline(int(game["home_oddshark_id"]), game["game_date"], game["game_id"])

runlines = {}
with ThreadPoolExecutor(max_workers=12) as executor:
    futures = {executor.submit(fetch_for_game, g): g for g in enriched_games}
    for future in as_completed(futures):
        gid, rl = future.result()
        runlines[gid] = rl

# === Insert to Xata ===
for i, game in enumerate(enriched_games):
    game_id = game["game_id"]
    record = {
        "game_id": game_id,
        "game_date": game["game_date"].strftime("%Y-%m-%dT00:00:00Z"),
        "start_time": game.get("start_time"),
        "home_name": game["home_name"],
        "home_id": game["home_id"],
        "home_oddshark_id": game["home_oddshark_id"],
        "away_name": game["away_name"],
        "away_id": game["away_id"],
        "away_oddshark_id": game["away_oddshark_id"],
        "runs_home": game.get("runs_home"),
        "runs_away": game.get("runs_away"),
        "runs_total": game.get("runs_total"),
        "runline": runlines.get(game_id),
        "prediction": None,
        "outcome": None,
        "game_started": game.get("game_started"),
        "game_complete": game.get("game_complete")
    }
    resp = xata.records().insert_with_id("games_temp", game_id, clean_json_compat(record))
    if i % 100 == 0:
        print(f"Inserted {i} games...")

print(f"\n ALL {i} GAMES INSERTED - SUCCESS\n")