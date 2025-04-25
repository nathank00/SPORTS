import os
import pandas as pd
from dotenv import load_dotenv
from pybaseball import playerid_reverse_lookup
from datetime import datetime
from tqdm import tqdm
from xata.client import XataClient
import requests

# === Load Xata credentials ===
load_dotenv("xata-config.env")
API_KEY = os.getenv("XATA_API_KEY")
XATA_DATABASE_URL = os.getenv("XATA_DATABASE_URL")
season = datetime.now().year

xata = XataClient(api_key=API_KEY, db_url=XATA_DATABASE_URL)

# === Safe conversions ===
def safe_float(val):
    try: return float(val)
    except (ValueError, TypeError): return None

def safe_int(val):
    try: return int(val)
    except (ValueError, TypeError): return None

def clean_record(record):
    return {k: v for k, v in record.items() if v is not None}

# === Fetch players ===
MLB_PLAYERS_URL = "https://statsapi.mlb.com/api/v1/sports/1/players"

def get_players_by_year(year):
    r = requests.get(MLB_PLAYERS_URL, params={"season": year})
    if r.status_code == 200:
        return r.json().get("people", [])
    return []

def build_player_lists():
    player_records = {}
    for year in range(2021, season+1):
        players = get_players_by_year(year)
        for p in players:
            pid = p["id"]
            if pid not in player_records:
                player_records[pid] = {
                    "mlbID": pid,
                    "Name": p["fullName"],
                    "Position": p.get("primaryPosition", {}).get("abbreviation", "UNK")
                }
    df_all = pd.DataFrame.from_dict(player_records, orient="index")
    batters = df_all[~df_all["Position"].isin(["P", "SP", "RP"])]
    pitchers = df_all[df_all["Position"].isin(["P", "SP", "RP"])]

    def enrich_bbref_ids(df, label):
        df = df.copy()
        df["key_bbref"] = None
        ids = df["mlbID"].tolist()
        for i in range(0, len(ids), 100):
            batch = ids[i:i+100]
            try:
                bbref_data = playerid_reverse_lookup(batch, key_type="mlbam")
                for pid in batch:
                    match = bbref_data[bbref_data["key_mlbam"] == pid]
                    if not match.empty:
                        df.loc[df["mlbID"] == pid, "key_bbref"] = match.iloc[0]["key_bbref"]
            except Exception as e:
                print(f"BBRef ID lookup failed for {label} batch {i//100 + 1}: {e}")
        return df.dropna(subset=["key_bbref"])

    return enrich_bbref_ids(batters, "batters"), enrich_bbref_ids(pitchers, "pitchers")

# === Game log fetcher ===
def get_schedule(team_id):
    url = "https://statsapi.mlb.com/api/v1/schedule"
    res = requests.get(url, params={"teamId": team_id, "season": season, "sportId": 1}, timeout=10).json()
    sched = {}
    for date in res.get('dates', []):
        for g in date['games']:
            gid = int(g['gamePk'])
            away = g['teams']['away']['team']['id']
            home = g['teams']['home']['team']['id']
            opp = home if away == team_id else away
            sched[gid] = {'Team': team_id, 'Opp': opp}
    return sched

def fetch_current_log(player_id, group, stat_map, existing_ids):
    url = f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats"
    params = {"stats": "gameLog", "group": group, "season": season}
    res = requests.get(url, params=params, timeout=10).json()
    if not res['stats'] or not res['stats'][0]['splits']:
        return pd.DataFrame()

    all_rows = []
    for game in res['stats'][0]['splits']:
        gid = int(game['game']['gamePk'])
        if gid in existing_ids:
            continue  # Already stored — skip
        all_rows.append((gid, game))

    rows = []
    for gid, game in all_rows:
        status_url = f"https://statsapi.mlb.com/api/v1.1/game/{gid}/feed/live"
        status_res = requests.get(status_url).json()
        game_status = status_res.get("gameData", {}).get("status", {}).get("abstractGameState")
        if game_status != "Final":
            continue

        raw = game['stat']
        row = {stat_map[k]: v for k, v in raw.items() if k in stat_map}
        team_id = game['team']['id']
        sched = get_schedule(team_id)
        info = sched.get(gid, {})
        row.update({
            'game_id': gid,
            'game_date': game['date'],
            'Team': info.get('Team'),
            'Opp': info.get('Opp')
        })
        rows.append(row)

    return pd.DataFrame(rows)


# === Xata helpers ===
def get_existing_game_ids(player_id, table):
    seen = set()
    cursor = None
    while True:
        result = xata.data().query(table, {
            "filter": {"player_id": str(player_id)},
            "columns": ["game_id"],
            "page": {"size": 1000, **({"after": cursor} if cursor else {})}
        })
        records = result.get("records", [])
        if not records:
            break
        for r in records:
            seen.add(int(r["game_id"]))
        cursor = result.get("meta", {}).get("nextCursor")
        if not cursor:
            break
    return seen

def insert_new_rows(table, rows):
    if not rows:
        return
    try:
        resp = xata.records().bulk_insert(table, {"records": rows})
        if not resp.is_success():
            raise Exception(f"Xata insert failed ({resp.status_code}): {resp.json()}")
    except Exception as e:
        raise Exception(f"Bulk insert failed for table '{table}': {e}")


# === Column maps ===
batter_col_map = {
    'atBats': 'AB', 'hits': 'H', 'runs': 'R', 'doubles': '2B', 'triples': '3B', 'homeRuns': 'HR',
    'rbi': 'RBI', 'baseOnBalls': 'BB', 'strikeOuts': 'SO', 'stolenBases': 'SB', 'caughtStealing': 'CS',
    'hitByPitch': 'HBP', 'sacBunts': 'SH', 'sacFlies': 'SF', 'intentionalWalks': 'IBB',
    'totalBases': 'TB', 'avg': 'BA', 'obp': 'OBP', 'slg': 'SLG', 'ops': 'OPS', 'plateAppearances': 'PA'
}

pitcher_map = {
    'inningsPitched': 'IP', 'hits': 'H', 'runs': 'R', 'earnedRuns': 'ER', 'baseOnBalls': 'BB',
    'strikeOuts': 'SO', 'homeRuns': 'HR', 'hitByPitch': 'HBP', 'era': 'ERA', 'fip': 'FIP',
    'battersFaced': 'BF', 'pitchesThrown': 'Pit', 'strikes': 'Str', 'strikesLooking': 'StL',
    'strikesSwinging': 'StS', 'stolenBases': 'SB', 'caughtStealing': 'CS', 'atBats': 'AB',
    'doubles': '2B', 'triples': '3B', 'intentionalWalks': 'IBB'
}

# === Record builders ===
def build_batter_record(row, player_id):
    return {
        "player_id": str(player_id),
        "game_id": str(row["game_id"]),
        "game_date": pd.to_datetime(row["game_date"]).strftime("%Y-%m-%dT00:00:00Z"),
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

def build_pitcher_record(row, player_id):
    return {
        "player_id": str(player_id),
        "game_id": str(row["game_id"]),
        "game_date": pd.to_datetime(row["game_date"]).strftime("%Y-%m-%dT00:00:00Z"),
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

def process_players(df, group, table, col_map, record_builder):
    for _, row in tqdm(df.iterrows(), total=len(df), desc=f"Updating {table}"):
        player_id = int(row["mlbID"])
        tqdm.write(f"🔍 Checking player {player_id}")

        # Fetch player's full game log
        url = f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats"
        params = {"stats": "gameLog", "group": group, "season": season}
        res = requests.get(url, params=params, timeout=10).json()

        # Defensive check against empty stats list
        stats = res.get("stats", [])
        if not stats or not stats[0].get("splits"):
            tqdm.write(f"ℹ️ No games returned from API for player {player_id}")
            continue

        splits = stats[0]["splits"]
        existing_ids = get_existing_game_ids(player_id, table)

        new_rows = []
        for game in splits:
            gid = int(game['game']['gamePk'])
            if gid in existing_ids:
                continue

            # Check game status
            status_url = f"https://statsapi.mlb.com/api/v1.1/game/{gid}/feed/live"
            status_res = requests.get(status_url).json()
            game_status = status_res.get("gameData", {}).get("status", {}).get("abstractGameState")
            if game_status != "Final":
                continue

            # Build record
            raw = game['stat']
            row_data = {col_map[k]: v for k, v in raw.items() if k in col_map}
            team_id = game['team']['id']
            sched = get_schedule(team_id)
            info = sched.get(gid, {})
            row_data.update({
                'game_id': gid,
                'game_date': game['date'],
                'Team': info.get('Team'),
                'Opp': info.get('Opp')
            })

            record = clean_record(record_builder(pd.Series(row_data), player_id))
            new_rows.append(record)

        if new_rows:
            try:
                resp = xata.records().bulk_insert(table, {"records": new_rows})
                if not resp.is_success():
                    raise Exception(f"Xata insert failed ({resp.status_code}): {resp.status()}")
                tqdm.write(f"✅ Inserted {len(new_rows)} new rows for player {player_id}")
            except Exception as e:
                tqdm.write(f"❌ Failed to insert rows for player {player_id}: {e}")
        else:
            tqdm.write(f"✔️ No new rows to insert for player {player_id}")



# === Main ===
if __name__ == "__main__":
    df_batters, df_pitchers = build_player_lists()
    process_players(df_batters, "hitting", "batter_gamelogs", batter_col_map, build_batter_record)
    process_players(df_pitchers, "pitching", "pitcher_gamelogs", pitcher_map, build_pitcher_record)
