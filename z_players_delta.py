import os
import requests
import pandas as pd
from dotenv import load_dotenv
from xata.client import XataClient
from pybaseball import playerid_reverse_lookup
from datetime import datetime
from tqdm import tqdm

print("\n---------- Running x2-sync.py ----------\n")

load_dotenv("xata-config.env")
xata = XataClient()

MLB_PLAYERS_URL = "https://statsapi.mlb.com/api/v1/sports/1/players"

def clean_json_compat(d):
    for k, v in d.items():
        if isinstance(v, float) and (pd.isna(v) or pd.isnull(v)):
            d[k] = None
    return d

def get_players_by_year(season):
    response = requests.get(MLB_PLAYERS_URL, params={"season": season})
    if response.status_code == 200:
        return response.json().get("people", [])
    return []

# Fetch players across all seasons
today = datetime.today().strftime("%Y-%m-%d")
current_year = datetime.today().year
player_records = {}
for year in range(2021, current_year):
    for player in get_players_by_year(year):
        pid = player["id"]
        if pid not in player_records:
            player_records[pid] = {
                "player_id": pid,
                "player_name": player["fullName"]
            }

players_df = pd.DataFrame(player_records.values())
players_df["bbref_id"] = None

# Add BBRef IDs
print(">> Mapping BBRef IDs...")
batch_size = 100
for i in tqdm(range(0, len(players_df), batch_size)):
    batch = players_df.iloc[i:i+batch_size]
    try:
        mapped = playerid_reverse_lookup(batch["player_id"].tolist(), key_type="mlbam")
        for _, row in mapped.iterrows():
            players_df.loc[players_df["player_id"] == row["key_mlbam"], "bbref_id"] = row["key_bbref"]
    except Exception as e:
        print(f"!! ERROR at batch {i // batch_size + 1}: {e}")

# Remove players with no name or ID
players_df.dropna(subset=["player_id", "player_name"], inplace=True)

# Fetch existing IDs from Xata
def get_existing_player_ids():
    print(">> Fetching existing player IDs from Xata...")
    ids = set()
    page = None
    while True:
        query = {"columns": ["id"], "page": {"size": 1000}}
        if page: query["page"]["after"] = page
        resp = xata.data().query("players", query)
        if not resp.is_success():
            break
        records = resp.get("records", [])
        if not records:
            break
        ids.update(r["id"] for r in records)
        page = resp.get("meta", {}).get("page", {}).get("cursor")
        if not page:
            break
    return ids

existing_ids = get_existing_player_ids()

# Insert new players
print(">> Inserting new players...")
for _, row in tqdm(players_df.iterrows(), total=len(players_df)):
    pid = str(row["player_id"])
    if pid in existing_ids:
        continue
    record = {
        "player_id": pid,
        "player_name": row["player_name"],
        "bbref_id": row.get("bbref_id")
    }
    resp = xata.records().insert_with_id("players", pid, clean_json_compat(record))
    if not resp.is_success():
        print(f"Failed to insert {pid}: {resp.status_code} - {resp.get('message', 'no message')}")

print(" Player sync complete.")
