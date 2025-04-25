import os
import requests
import pandas as pd
import pytz
import statsapi
from datetime import datetime
from xata.client import XataClient
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

print("\n---------- Running x1-sync.py ----------\n")

load_dotenv("xata-config.env")
xata = XataClient()

team_to_oddshark_id = {
    120: 27017, 146: 27022, 139: 27003, 144: 27009, 140: 27002, 117: 27023,
    135: 26996, 143: 26995, 110: 27008, 136: 27011, 121: 27014, 109: 27007,
    108: 26998, 133: 27016, 141: 27010, 114: 27014, 138: 27019, 142: 27005,
    116: 26999, 147: 27001, 137: 26997, 118: 27006, 145: 27018, 115: 27004,
    111: 27021, 119: 27015, 112: 27020, 158: 27012, 113: 27000, 134: 27013
}

def clean_json_compat(d):
    for k, v in d.items():
        if isinstance(v, float) and (pd.isna(v) or pd.isnull(v)):
            d[k] = None
    return d

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

def get_all_xata_game_ids():
    print(">> Fetching existing game IDs from Xata...")
    api_key = os.getenv("XATA_API_KEY")
    workspace = os.getenv("WORKSPACE")
    region = os.getenv("REGION")
    db = os.getenv("DB")
    branch = os.getenv("BRANCH")
    table = os.getenv("GAMES_TABLE")
    url = f"https://{workspace}.{region}.xata.sh/db/{db}:{branch}/tables/{table}/query"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    page_size = 1000
    cursor = None
    all_ids = set()

    while True:
        payload = {"columns": ["id"], "page": {"size": page_size}}
        if cursor:
            payload["page"]["after"] = cursor
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code != 200:
            break
        data = response.json()
        records = data.get("records", [])
        if not records:
            break
        cursor = data.get("meta", {}).get("page", {}).get("cursor")
        all_ids.update(r["id"] for r in records)
        if not cursor:
            break
    print(f">> Retrieved {len(all_ids)} game IDs from Xata.")
    return all_ids

existing_ids = get_all_xata_game_ids()

MLB_SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"
today = datetime.today().strftime("%Y-%m-%d")
current_year = datetime.today().year

print(">> Fetching full MLB schedule...")

games = []
for year in range(2021, current_year + 1):
    end_date = today if year == current_year else f"{year}-12-31"
    r = requests.get(MLB_SCHEDULE_URL, params={
        "sportId": 1,
        "startDate": f"{year}-01-01",
        "endDate": end_date
    })
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

print(f">> Retrieved {len(games)} total games from MLB.")

games = list({g["game_id"]: g for g in games}.values())
new_games = [g for g in games if g["game_id"] not in existing_ids]
print(f">> Identified {len(new_games)} new games to insert.")

for g in new_games:
    g["home_oddshark_id"] = str(team_to_oddshark_id.get(int(g["home_id"]), ""))
    g["away_oddshark_id"] = str(team_to_oddshark_id.get(int(g["away_id"]), ""))

print(">> Starting metadata enrichment...")
with ThreadPoolExecutor(max_workers=12) as executor:
    futures = {executor.submit(enrich_game_metadata, game): game for game in new_games}
    new_games = [future.result() for future in as_completed(futures)]
print(">> Metadata enrichment complete.")

def fetch_for_game(game):
    if not game["home_oddshark_id"]:
        return game["game_id"], None
    month_day = (game["game_date"].month, game["game_date"].day)
    if month_day < (5, 15):
        return game["game_id"], None
    return game["game_id"], fetch_runline(int(game["home_oddshark_id"]), game["game_date"], game["game_id"])

print(">> Starting runline scraping...")
runlines = {}
with ThreadPoolExecutor(max_workers=12) as executor:
    futures = {executor.submit(fetch_for_game, g): g for g in new_games}
    for future in as_completed(futures):
        gid, rl = future.result()
        runlines[gid] = rl
print(">> Runline scraping complete.")

print(">> Inserting new games into Xata...")
for i, game in enumerate(new_games):
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
    resp = xata.records().insert_with_id("games", game_id, clean_json_compat(record))
    if resp.is_success():
        print(f"Inserted game {game_id}")
    else:
        print(f"FAILED to insert game {game_id} - {resp.status_code} - {resp.get('message', 'no message')}")


print("\n>> Patching missing runlines for current season...")
year_str = str(current_year)
missing_runline_query = {
    "filter": {
        "runline": {"$isNull": True},
        "game_date": {
            "$ge": f"{year_str}-01-01T00:00:00Z",
            "$lt": f"{year_str}-12-31T23:59:59Z"
        }
    },
    "columns": ["id", "game_date", "home_oddshark_id"]
}
resp = xata.data().query("games", missing_runline_query)
records = resp.get("records", [])

for r in records:
    gid = r["id"]
    date = pd.to_datetime(r["game_date"])
    oid = r.get("home_oddshark_id", "")
    if not oid:
        continue
    if (date.month, date.day) < (5, 15):
        continue
    new_rl = fetch_runline(int(oid), date, gid)
    if new_rl is not None:
        xata.records().update("games", gid, {"runline": new_rl})
        print(f"Updated runline for {gid}")
