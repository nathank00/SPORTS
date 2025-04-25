
import os
import requests
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

# Load credentials
load_dotenv("xata-config.env")
API_KEY = os.getenv("XATA_API_KEY")
WORKSPACE = os.getenv("WORKSPACE")
REGION = os.getenv("REGION")
DB = os.getenv("DB")
BRANCH = os.getenv("BRANCH")

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

BATTING_DIR = "batters"
PITCHING_DIR = "pitchers"

def safe_float(val):
    try:
        return float(val)
    except (ValueError, TypeError):
        return None

def safe_int(val):
    try:
        return int(val)
    except (ValueError, TypeError):
        return None

def fetch_table_records(table_name, columns):
    url = f"https://{WORKSPACE}.{REGION}.xata.sh/db/{DB}:{BRANCH}/tables/{table_name}/query"
    all_records = []
    cursor = None
    while True:
        payload = {"columns": columns, "page": {"size": 1000}}
        if cursor:
            payload["page"]["after"] = cursor
        resp = requests.post(url, headers=HEADERS, json=payload)
        if not resp.ok:
            raise Exception(f"Error fetching {table_name}: {resp.status_code} {resp.text}")
        data = resp.json()
        records = data.get("records", [])
        if not records:
            break
        all_records.extend(records)
        cursor = data.get("meta", {}).get("page", {}).get("cursor")
        if not cursor:
            break
    return all_records

def clean_dict(record):
    return {k: None if isinstance(v, float) and pd.isna(v) else v for k, v in record.items()}

def send_to_xata(table_name, records):
    url = f"https://{WORKSPACE}.{REGION}.xata.sh/db/{DB}:{BRANCH}/tables/{table_name}/bulk"
    payload = {"records": [clean_dict(r) for r in records]}
    response = requests.post(url, headers=HEADERS, json=payload)
    if not response.ok:
        print(f"❌ Failed to insert batch into {table_name}: {response.status_code}, {response.text}")
    return response.ok

def build_game_date_lookup(games):
    return {g["game_id"]: g["game_date"] for g in games if "game_id" in g and "game_date" in g}

def parse_batter_csv(path, player_id, date_lookup):
    df = pd.read_csv(path)
    records = []
    for _, row in df.iterrows():
        game_id = str(row["game_id"])
        game_date = date_lookup.get(game_id)
        if not game_date:
            print(f"⚠️ Skipping: Missing game_date for game_id={game_id}")
            continue
        record = {
            "player_id": player_id,
            "game_id": game_id,
            "game_date": game_date,
            "team_id": str(row["Team"]),
            "opponent_id": str(row["Opp"]),
            "lineup_spot": None,
            "is_starting": None,
            "handedness": None,
            "b_pa": safe_int(row.get("PA")), "b_ab": safe_int(row.get("AB")), "b_h": safe_int(row.get("H")),
            "b_2b": safe_int(row.get("2B")), "b_3b": safe_int(row.get("3B")), "b_hr": safe_int(row.get("HR")),
            "b_rbi": safe_int(row.get("RBI")), "b_bb": safe_int(row.get("BB")), "b_so": safe_int(row.get("SO")),
            "b_sb": safe_int(row.get("SB")), "b_cs": safe_int(row.get("CS")), "b_hbp": safe_int(row.get("HBP")),
            "b_sh": safe_int(row.get("SH")), "b_sf": safe_int(row.get("SF")), "b_ibb": safe_int(row.get("IBB")),
            "b_tb": safe_int(row.get("TB")), "b_ba": safe_float(row.get("BA")), "b_obp": safe_float(row.get("OBP")),
            "b_slg": safe_float(row.get("SLG")), "b_ops": safe_float(row.get("OPS"))
        }
        records.append(record)
    return records

def parse_pitcher_csv(path, player_id, date_lookup):
    df = pd.read_csv(path)
    records = []
    for _, row in df.iterrows():
        game_id = str(row["game_id"])
        game_date = date_lookup.get(game_id)
        if not game_date:
            print(f"⚠️ Skipping: Missing game_date for game_id={game_id}")
            continue
        record = {
            "player_id": player_id,
            "game_id": game_id,
            "game_date": game_date,
            "team_id": str(row["Team"]),
            "opponent_id": str(row["Opp"]),
            "is_starting": None,
            "handedness": None,
            "role": None,
            "p_ip": safe_float(row.get("IP")), "p_h": safe_int(row.get("H")),
            "p_r": safe_int(row.get("R")), "p_er": safe_int(row.get("ER")),
            "p_bb": safe_int(row.get("BB")), "p_so": safe_int(row.get("SO")),
            "p_hr": safe_int(row.get("HR")), "p_hbp": safe_int(row.get("HBP")),
            "p_era": safe_float(row.get("ERA")), "p_bf": safe_int(row.get("BF")),
            "p_pit": safe_int(row.get("Pit")), "p_str": safe_int(row.get("Str")),
            "p_stl": safe_int(row.get("StL")), "p_sts": safe_int(row.get("StS")),
            "p_fip": safe_float(row.get("FIP")), "p_sb": safe_int(row.get("SB")),
            "p_cs": safe_int(row.get("CS")), "p_ab": safe_int(row.get("AB")),
            "p_2b": safe_int(row.get("2B")), "p_3b": safe_int(row.get("3B")),
            "p_ibb": safe_int(row.get("IBB")), "p_whip": None
        }
        records.append(record)
    return records

def main():
    print(">> Fetching players and games_temp from Xata...")
    players = fetch_table_records("players", ["player_id", "bbref_id"])
    games_temp = fetch_table_records("games_temp", ["game_id", "game_date"])
    game_lookup = build_game_date_lookup(games_temp)

    for p in tqdm(players, desc="Processing players"):
        player_id = p["player_id"]
        bbref_id = p.get("bbref_id")
        if not bbref_id:
            continue

        batting_file = os.path.join(BATTING_DIR, f"{bbref_id}_batting.csv")
        pitching_file = os.path.join(PITCHING_DIR, f"{bbref_id}_pitching.csv")

        if os.path.exists(batting_file):
            try:
                batter_records = parse_batter_csv(batting_file, player_id, game_lookup)
                if batter_records:
                    send_to_xata("batter_gamelogs", batter_records)
            except Exception as e:
                print(f"❌ Error parsing batter CSV for {bbref_id}: {e}")

        if os.path.exists(pitching_file):
            try:
                pitcher_records = parse_pitcher_csv(pitching_file, player_id, game_lookup)
                if pitcher_records:
                    send_to_xata("pitcher_gamelogs", pitcher_records)
            except Exception as e:
                print(f"❌ Error parsing pitcher CSV for {bbref_id}: {e}")

    print("\n>> All Pitchers and Batters Inserted Successfully.\n")

if __name__ == "__main__":
    main()
