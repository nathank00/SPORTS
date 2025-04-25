import os
import time
import requests
from dotenv import load_dotenv
from xata.client import XataClient
from xata.helpers import BulkProcessor
from more_itertools import chunked
from tqdm import tqdm

# === INIT ===
load_dotenv("xata-config.env")
xata = XataClient()
bp = BulkProcessor(xata, batch_size=25)
BATCH_SIZE = 10
MAX_WORKERS = 4

batter_map = {
    "atBats": "b_ab", "hits": "b_h", "doubles": "b_2b", "triples": "b_3b", "homeRuns": "b_hr",
    "rbi": "b_rbi", "baseOnBalls": "b_bb", "strikeOuts": "b_so", "stolenBases": "b_sb", "caughtStealing": "b_cs",
    "hitByPitch": "b_hbp", "sacBunts": "b_sh", "sacFlies": "b_sf", "intentionalWalks": "b_ibb", "totalBases": "b_tb",
    "avg": "b_ba", "obp": "b_obp", "slg": "b_slg", "ops": "b_ops", "plateAppearances": "b_pa"
}
pitcher_map = {
    "inningsPitched": "p_ip", "hits": "p_h", "runs": "p_r", "earnedRuns": "p_er", "baseOnBalls": "p_bb",
    "strikeOuts": "p_so", "homeRuns": "p_hr", "hitByPitch": "p_hbp", "era": "p_era", "fip": "p_fip",
    "battersFaced": "p_bf", "pitchesThrown": "p_pit", "strikes": "p_str", "strikesLooking": "p_stl",
    "strikesSwinging": "p_sts", "stolenBases": "p_sb", "caughtStealing": "p_cs", "atBats": "p_ab",
    "doubles": "p_2b", "triples": "p_3b", "intentionalWalks": "p_ibb", "whip": "p_whip"
}

int_fields = {*batter_map.values(), *pitcher_map.values()} - {"b_ba", "b_obp", "b_slg", "b_ops", "p_era", "p_fip", "p_whip", "p_ip"}
float_fields = {"b_ba", "b_obp", "b_slg", "b_ops", "p_era", "p_fip", "p_whip", "p_ip"}

def fetch_all_game_ids_from_xata():
    ids, cursor = [], None
    while True:
        query = {"columns": ["id"], "page": {"size": 200}}
        if cursor:
            query["page"]["after"] = cursor
        resp = xata.data().query("games", query)
        if not resp.is_success():
            raise Exception("Xata query failed")
        records = resp.get("records", [])
        if not records:
            break
        ids.extend(r["id"] for r in records)
        cursor = resp.get("meta", {}).get("page", {}).get("cursor")
        if not cursor:
            break
    return list(set(ids))

def fetch_boxscore(game_id):
    url = f"https://statsapi.mlb.com/api/v1.1/game/{game_id}/feed/live"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.json()

def parse_player_stats(game_id, box):
    entries = []
    teams = box["liveData"]["boxscore"]["teams"]

    for side in ["home", "away"]:
        team_players = teams[side]["players"]
        lineup = teams[side].get("battingOrder", [])
        bullpen = teams[side].get("bullpen", [])
        pitchers = teams[side].get("pitchers", [])
        seen = set()

        for pid, full in team_players.items():
            player_id = int(pid.replace("ID", ""))
            if player_id in seen:
                continue
            seen.add(player_id)

            stats = full.get("stats", {})
            record = {
                "id": f"{game_id}_{player_id}",
                "game_id": game_id,
                "player_id": str(player_id),
                "position": None,
                "lineup_spot": None
            }

            if player_id in pitchers:
                record["position"] = "SP" if pitchers[0] == player_id else "RP"
            elif player_id in bullpen:
                record["position"] = "BP"
            if player_id in lineup:
                record["lineup_spot"] = lineup.index(player_id) + 1

            for src, tgt in batter_map.items():
                val = stats.get("batting", {}).get(src)
                if val is not None:
                    record[tgt] = val

            for src, tgt in pitcher_map.items():
                val = stats.get("pitching", {}).get(src)
                if val is not None:
                    record[tgt] = val

            for key in list(record.keys()):
                if key in int_fields:
                    try:
                        record[key] = int(record[key])
                    except:
                        record[key] = None
                elif key in float_fields:
                    try:
                        record[key] = float(record[key])
                    except:
                        record[key] = None

            entries.append(record)
    return entries

def run_all():
    game_ids = fetch_all_game_ids_from_xata()
    print(f"✅ Fetched {len(game_ids)} unique game IDs from Xata.")
    batches = list(chunked(game_ids, BATCH_SIZE))
    total_inserted = 0
    for batch in tqdm(batches, desc="Processing game batches"):
        for gid in batch:
            try:
                box = fetch_boxscore(gid)
                records = parse_player_stats(gid, box)
                bp.put_records("game_players", records)
                total_inserted += len(records)
            except Exception as e:
                print(f"❌ Game {gid} failed: {e}")
    print("⏳ Flushing queue...")
    bp.flush_queue()
    print(f"✅ Done. Enqueued {total_inserted} records to BulkProcessor.")

if __name__ == "__main__":
    run_all()
