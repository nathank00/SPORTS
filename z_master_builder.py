from xata import XataClient
from datetime import datetime
from dotenv import load_dotenv
import os
import numpy as np
import pandas as pd
from tqdm import tqdm
import json
from time import sleep


# Load env vars from .env config
load_dotenv("xata-config.env")
xata = XataClient(api_key=os.getenv("XATA_API_KEY"), db_url=os.getenv("XATA_DB_URL"))

# Helper functions

def normalize_game_date(date_str):
    """
    Ensures the game_date is in ISO 8601 format without milliseconds.
    Converts e.g. '2021-04-01T00:00:00.000Z' → '2021-04-01T00:00:00Z'
    """
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "").split(".")[0])
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception as e:
        print(f"[ERROR] Invalid date format: {date_str} — {e}")
        return date_str

def safe_round(value, digits=3):
    """
    Rounds a value and checks for NaN or inf. Returns None if value is not finite.
    """
    if value is None:
        return None
    try:
        val = round(value, digits)
        if not np.isfinite(val):
            return None
        return val
    except:
        return None

def convert_ip(ip_raw):
    """Convert MLB-style IP (e.g., 6.1, 6.2) to actual float value (e.g., 6.333, 6.667)"""
    if ip_raw is None:
        return 0.0
    ip_int = int(ip_raw)
    ip_decimal = round(ip_raw - ip_int, 1)
    if ip_decimal == 0.1:
        return ip_int + (1 / 3)
    elif ip_decimal == 0.2:
        return ip_int + (2 / 3)
    return float(ip_raw)

def parse_ip(ip_val):
    try:
        if isinstance(ip_val, float):
            int_part = int(ip_val)
            decimal_part = ip_val - int_part
            if decimal_part == 0.1:
                return int_part + 1/3
            elif decimal_part == 0.2:
                return int_part + 2/3
            else:
                return float(ip_val)
        return float(ip_val)
    except:
        return 0.0

def safe_stat(value):
    try:
        return float(value)
    except:
        return 0.0

def round_or_default(value, digits=3):
    try:
        return round(float(value), digits)
    except:
        return 0.0

def avg_or_default(lst, default=0.0):
    if not lst:
        return default
    return sum(lst) / len(lst)

def load_schema_defaults(schema_file: str, table_name: str = "master") -> dict:
    """
    Loads the schema from a JSON file and returns default fallbacks for the specified table.
    """
    with open(schema_file, "r") as f:
        schema_data = json.load(f)

    defaults = {}
    for table in schema_data:
        if isinstance(table, dict) and table.get("name") == table_name:
            for col in table.get("columns", []):
                col_name = col["name"]
                col_type = col["type"]

                # Define fallbacks for common types
                if col_type == "float":
                    defaults[col_name] = 0.0
                elif col_type == "int":
                    defaults[col_name] = 0
                elif col_type == "bool":
                    defaults[col_name] = False
                elif col_type in {"text", "string"}:
                    defaults[col_name] = ""
                elif col_type == "datetime":
                    defaults[col_name] = "1970-01-01T00:00:00Z"
            break


    return defaults

def clean_row_for_xata(row: dict, schema_defaults: dict) -> dict:
    return {
        k: (v if v is not None else schema_defaults.get(k))
        for k, v in row.items()
    }


# Core functions

def get_games_to_process():
    """
    Returns a list of all games not yet present in the 'master' table.
    Uses cursor-based pagination to pull all game records.
    """

    # Step 1: Get all existing game IDs from the master table
    existing = set()
    page = None
    while True:
        query = {"columns": ["game_id"], "page": {"size": 1000}}
        if page:
            query["page"]["after"] = page
        resp = xata.data().query("master", query)
        if not resp.is_success():
            break
        records = resp.get("records", [])
        if not records:
            break
        for r in records:
            gid = r.get("game_id")
            if gid:
                existing.add(str(gid))
        page = resp.get("meta", {}).get("page", {}).get("cursor")
        if not page:
            break

    # Step 2: Pull all game records (paginated)
    all_games = []
    page = None
    while True:
        query = {"page": {"size": 1000}}
        if page:
            query["page"]["after"] = page
        resp = xata.data().query("games", query)
        if not resp.is_success():
            break
        records = resp.get("records", [])
        if not records:
            break
        for r in records:
            if r.get("game_id") not in existing:
                all_games.append(r)
        page = resp.get("meta", {}).get("page", {}).get("cursor")
        if not page:
            break

    tqdm.write(f"[INFO] Total unprocessed games: {len(all_games)}")
    return all_games

def fetch_game_metadata(game: dict) -> dict:
    """
    Returns a dictionary of metadata for a given game, correctly pulling all player IDs.
    """


    game_id = str(game["game_id"])

    # Batters
    home_batters = [game.get(f"home_{i}_id") for i in range(1, 10)]
    away_batters = [game.get(f"away_{i}_id") for i in range(1, 10)]

    # Starting Pitchers
    home_sp_id = game.get("home_sp_id")
    away_sp_id = game.get("away_sp_id")

    # Bullpen Pitchers
    home_bp_ids = [game.get(f"home_bp_{i}_id") for i in range(1, 14) if game.get(f"home_bp_{i}_id")]
    away_bp_ids = [game.get(f"away_bp_{i}_id") for i in range(1, 14) if game.get(f"away_bp_{i}_id")]

    return {
        "game_id": game_id,
        "game_date": game["game_date"],
        "start_time": game["start_time"],
        "home_id": game["home_id"],
        "home_oddshark_id": game["home_oddshark_id"],
        "home_name": game["home_name"],
        "stadium": game.get("stadium"),
        "away_id": game["away_id"],
        "away_oddshark_id": game["away_oddshark_id"],
        "away_name": game["away_name"],
        "is_night_game": game.get("is_night_game", False),
        "is_dome": game.get("is_dome", False),
        "game_started": game["game_started"],
        "game_complete": game["game_complete"],
        "description": game["description"],
        "temperature": game.get("temperature"),
        "wind_speed": game.get("wind_speed"),
        "wind_direction": game.get("wind_direction"),
        "humidity": game.get("humidity"),
        "elevation": game.get("elevation"),
        "runline": game.get("runline"),
        "runs_home": game.get("runs_home"),
        "runs_away": game.get("runs_away"),
        "total_runs_scored": game.get("runs_total"),
        

        "home_batters": home_batters,
        "away_batters": away_batters,
        "home_sp_id": home_sp_id,
        "away_sp_id": away_sp_id,
        "home_bp_ids": home_bp_ids,
        "away_bp_ids": away_bp_ids,
    }

def fetch_recent_batter_stats(player_id: str, before_date: str, window: int = 10) -> list:
    """
    Fetches the most recent batter game logs for a player up to (but not including) before_date.
    Returns a list of game records (dicts) sorted descending by date.
    """
    recent_logs = []

    result = xata.data().query("batter_gamelogs", {
        "filter": {
            "player_id": player_id,
            "game_date": { "$lt": before_date }
        },
        "sort": [{ "game_date": "desc" }],
        "page": { "size": window }
    })

    while True:
        records = result.get("records", [])
        if not records:
            break
        recent_logs.extend(records)
        if len(recent_logs) >= window or not result.get("next"):
            break
        result = xata.data().query("batter_gamelogs", {
            "filter": {
                "player_id": player_id,
                "game_date": { "$lt": before_date }
            },
            "sort": [{ "game_date": "desc" }],
            "page": { "size": window, "after": result["meta"]["page"]["last"] }
        })

    return recent_logs[:window]

def fetch_recent_pitcher_stats(player_id: str, before_date: str, window: int = 5) -> list:
    """
    Fetches the most recent pitcher game logs for a player up to (but not including) before_date.
    Returns a list of game records (dicts) sorted descending by date.
    """
    recent_logs = []

    result = xata.data().query("pitcher_gamelogs", {
        "filter": {
            "player_id": player_id,
            "game_date": { "$lt": before_date }
        },
        "sort": [{ "game_date": "desc" }],
        "page": { "size": window }
    })

    while True:
        records = result.get("records", [])
        if not records:
            break
        recent_logs.extend(records)
        if len(recent_logs) >= window or not result.get("next"):
            break
        result = xata.data().query("pitcher_gamelogs", {
            "filter": {
                "player_id": player_id,
                "game_date": { "$lt": before_date }
            },
            "sort": [{ "game_date": "desc" }],
            "page": { "size": window, "after": result["meta"]["page"]["last"] }
        })

    return recent_logs[:window]

def compute_batter_team_aggregates(batter_ids: list[str], game_date: str, windows=[10, 20]) -> dict:
    from datetime import datetime

    result = {}
    cutoff = datetime.fromisoformat(game_date.replace("Z", "")).isoformat() + "Z"

    for window in windows:
        prefix = "10" if window == 10 else "20"

        team_obp, team_slg, team_ops, team_rpg = [], [], [], []
        top5_obp, top5_ops, top5_xbh = [], [], []

        
        for pid in batter_ids:
            logs = xata.data().query("batter_gamelogs", {
                "filter": {
                    "player_id": str(pid),
                    "game_date": {"$lt": cutoff}
                },
                "sort": {"game_date": "desc"},
                "page": {"size": window}
            }).get("records", [])

            
            if not logs:
                continue

            # Initialize cumulative stat counters
            H, BB, HBP, AB, SF, TB = 0, 0, 0, 0, 0, 0
            XBH = 0
            TB_for_rpg = 0

            for g in logs:
                try:
                    H += safe_stat(g.get("b_h"))
                    BB += safe_stat(g.get("b_bb"))
                    HBP += safe_stat(g.get("b_hbp"))
                    AB += safe_stat(g.get("b_ab"))
                    SF += safe_stat(g.get("b_sf"))
                    TB += safe_stat(g.get("b_tb"))
                    TB_for_rpg += safe_stat(g.get("b_tb"))
                    XBH += (
                        safe_stat(g.get("b_2b")) +
                        safe_stat(g.get("b_3b")) +
                        safe_stat(g.get("b_hr"))
                    )
                except Exception as e:
                    print(f"❌ Error parsing stats for batter {pid}: {e}")
                    continue

            obp = (H + BB + HBP) / max((AB + BB + HBP + SF), 1)
            slg = TB / max(AB, 1)
            ops = obp + slg
            rpg = TB_for_rpg / 4 / len(logs)

        
            team_obp.append(obp)
            team_slg.append(slg)
            team_ops.append(ops)
            team_rpg.append(rpg)
            top5_obp.append(obp)
            top5_ops.append(ops)
            top5_xbh.append(XBH)

        top5_obp = sorted(top5_obp, reverse=True)[:5]
        top5_ops = sorted(top5_ops, reverse=True)[:5]
        top5_xbh = sorted(top5_xbh, reverse=True)[:5]

        result[f"team_OBP_{prefix}"] = round_or_default(avg_or_default(team_obp))
        result[f"team_SLG_{prefix}"] = round_or_default(avg_or_default(team_slg))
        result[f"team_OPS_{prefix}"] = round_or_default(avg_or_default(team_ops))
        result[f"team_RPG_{prefix}"] = round_or_default(sum(team_rpg))

        result[f"top5_OBP_{prefix}"] = round_or_default(avg_or_default(top5_obp))
        result[f"top5_OPS_{prefix}"] = round_or_default(avg_or_default(top5_ops))
        result[f"top5_XBH_{prefix}"] = int(sum(top5_xbh))

    return result

def compute_pitcher_team_aggregates(pitcher_ids: list[str], game_date: str, window: int) -> dict:
    """
    Computes average ERA, WHIP, IP/game, SO/game, HR/game, and BAA over a rolling window for a group of pitchers.
    """
    from datetime import datetime

    stats = {
        "ERA": [],
        "WHIP": [],
        "IP": [],
        "SO": [],
        "HR": [],
        "H": [],
        "BF": [],
    }

    # Normalize to ISO format
    cutoff = datetime.fromisoformat(game_date.replace("Z", "").split(".")[0]).strftime("%Y-%m-%dT%H:%M:%SZ")


    for pid in pitcher_ids:
        try:
            logs = xata.data().query("pitcher_gamelogs", {
                "filter": {
                    "player_id": str(pid),
                    "game_date": {"$lt": cutoff}
                },
                "sort": {"game_date": "desc"},
                "page": {"size": window}
            }).get("records", [])


            for g in logs:
                ip = parse_ip(g.get("p_ip"))
                stats["IP"].append(ip)
                stats["SO"].append(safe_stat(g.get("p_so")))
                stats["HR"].append(safe_stat(g.get("p_hr")))
                stats["H"].append(safe_stat(g.get("p_h")))
                stats["BF"].append(safe_stat(g.get("p_bf")))
                stats["ERA"].append(safe_stat(g.get("p_era")))
                stats["WHIP"].append(safe_stat(g.get("p_whip")))
                bb = safe_stat(g.get("p_bb"))
                h = safe_stat(g.get("p_h"))
                ip = parse_ip(g.get("p_ip"))
                if ip > 0:
                    stats["WHIP"].append((bb + h) / ip)

        except:
            continue

    n = len(stats["IP"]) if stats["IP"] else 1
    total_ip = sum(stats["IP"])

    return {
        "ERA": round_or_default(sum(stats["ERA"]) / n),
        "WHIP": round_or_default(sum(stats["WHIP"]) / n),
        "IP_per_game": round_or_default(total_ip / n),
        "SO_per_game": round_or_default(sum(stats["SO"]) / n),
        "HR_allowed": round_or_default(sum(stats["HR"]) / n),
        "BAA": round_or_default((sum(stats["H"]) / sum(stats["BF"])) if sum(stats["BF"]) > 0 else 0.0)
    }


def assemble_master_row(
    metadata: dict,
    batter_home: dict,
    batter_away: dict,
    pitcher_home_sp: dict,
    pitcher_away_sp: dict,
    pitcher_home_bp: dict,
    pitcher_away_bp: dict,
    pitcher_home_team_10: dict,
    pitcher_home_team_20: dict,
    pitcher_stats_away_team_10: dict,
    pitcher_stats_away_team_20: dict
) -> dict:
    row = {
        "game_id": metadata["game_id"],
        "game_date": metadata["game_date"],
        "start_time": metadata["start_time"],
        "home_id": metadata["home_id"],
        "home_oddshark_id": metadata["home_oddshark_id"],
        "home_name": metadata["home_name"],
        "away_id": metadata["away_id"],
        "away_oddshark_id": metadata["away_oddshark_id"],
        "away_name": metadata["away_name"],
        "stadium": metadata["stadium"],
        "is_night_game": metadata["is_night_game"],
        "is_dome": metadata["is_dome"],
        "game_started": metadata["game_started"],
        "game_complete": metadata["game_complete"],
        "description": metadata["description"],
        "temperature": metadata["temperature"],
        "wind_speed": metadata["wind_speed"],
        "wind_direction": metadata["wind_direction"],
        "elevation": metadata["elevation"],
        "humidity": metadata["humidity"],
        "runs_home": metadata["runs_home"],
        "runs_away": metadata["runs_away"],
        "total_runs_scored": metadata["total_runs_scored"],
        "runline": metadata["runline"],
        "label_over_under": (
            1 if metadata.get("runline") is not None and metadata["total_runs_scored"] > metadata["runline"]
            else 0 if metadata.get("runline") is not None
            else None
        )
    }

    row.update(batter_home)
    row.update(batter_away)
    row.update({
        "home_SP_ERA_5": pitcher_home_sp.get("ERA"),
        "home_SP_WHIP_5": pitcher_home_sp.get("WHIP"),
        "home_SP_IP_per_game_5": pitcher_home_sp.get("IP_per_game"),
        "home_SP_SO_per_game_5": pitcher_home_sp.get("SO_per_game"),
        "home_SP_HR_allowed_5": pitcher_home_sp.get("HR_allowed"),

        "away_SP_ERA_5": pitcher_away_sp.get("ERA"),
        "away_SP_WHIP_5": pitcher_away_sp.get("WHIP"),
        "away_SP_IP_per_game_5": pitcher_away_sp.get("IP_per_game"),
        "away_SP_SO_per_game_5": pitcher_away_sp.get("SO_per_game"),
        "away_SP_HR_allowed_5": pitcher_away_sp.get("HR_allowed"),

        "home_bullpen_ERA_5": pitcher_home_bp.get("ERA"),
        "home_bullpen_WHIP_5": pitcher_home_bp.get("WHIP"),
        "home_bullpen_ERA_20": pitcher_home_bp.get("ERA_20"),
        "home_bullpen_WHIP_20": pitcher_home_bp.get("WHIP_20"),

        "away_bullpen_ERA_5": pitcher_away_bp.get("ERA"),
        "away_bullpen_WHIP_5": pitcher_away_bp.get("WHIP"),
        "away_bullpen_ERA_20": pitcher_away_bp.get("ERA_20"),
        "away_bullpen_WHIP_20": pitcher_away_bp.get("WHIP_20"),

        # TEAM pitching stats (SP + BP)
        "home_team_ERA_10": pitcher_home_team_10.get("ERA"),
        "home_team_WHIP_10": pitcher_home_team_10.get("WHIP"),
        "home_team_BAA_10": pitcher_home_team_10.get("BAA"),
        "home_team_ERA_20": pitcher_home_team_20.get("ERA"),
        "home_team_WHIP_20": pitcher_home_team_20.get("WHIP"),
        "home_team_BAA_20": pitcher_home_team_20.get("BAA"),

        "away_team_ERA_10": pitcher_stats_away_team_10.get("ERA"),
        "away_team_WHIP_10": pitcher_stats_away_team_10.get("WHIP"),
        "away_team_BAA_10": pitcher_stats_away_team_10.get("BAA"),
        "away_team_ERA_20": pitcher_stats_away_team_20.get("ERA"),
        "away_team_WHIP_20": pitcher_stats_away_team_20.get("WHIP"),
        "away_team_BAA_20": pitcher_stats_away_team_20.get("BAA"),
    })

    return row

def insert_row_into_master(row: dict) -> bool:
    """
    Inserts a single assembled row into the 'master' table.
    Returns True if successful, False otherwise.
    """
    try:
        xata.records().insert("master", row)
        print(f"✅ Inserted game {row['game_id']} into master")
        return True
    except Exception as e:
        print(f"❌ Failed to insert game {row['game_id']} into master: {e}")
        return False

def main():
    """
    Full end-to-end pipeline to populate the master table.
    - Gathers games not in master
    - Computes all stats
    - Assembles rows
    - Inserts with game_id as the row ID
    """
    SCHEMA_DEFAULTS = load_schema_defaults("z_schema.json")
    games = get_games_to_process()
    tqdm.write(f">> Starting master build: {len(games)} games to process")

    success_count = 0

    for idx, game in enumerate(tqdm(games, desc="Building master table")):
        try:
            tqdm.write(f">> Processing game {idx + 1}/{len(games)} — ID: {game.get('game_id')}")


            meta = fetch_game_metadata(game)

            # Batter stats (10 and 20-game windows)
            raw_home_stats = compute_batter_team_aggregates(meta["home_batters"], meta["game_date"], windows=[10, 20])
            raw_away_stats = compute_batter_team_aggregates(meta["away_batters"], meta["game_date"], windows=[10, 20])
            batter_stats_home = {f"home_{k}": v for k, v in raw_home_stats.items()}
            batter_stats_away = {f"away_{k}": v for k, v in raw_away_stats.items()}

            # Starting pitcher stats (5-game window)
            pitcher_stats_home_sp = compute_pitcher_team_aggregates([meta["home_sp_id"]], meta["game_date"], window=5)
            pitcher_stats_away_sp = compute_pitcher_team_aggregates([meta["away_sp_id"]], meta["game_date"], window=5)

            # Bullpen stats (5-game and 20-game windows)
            pitcher_stats_home_bp = compute_pitcher_team_aggregates(meta["home_bp_ids"], meta["game_date"], window=5)
            pitcher_stats_away_bp = compute_pitcher_team_aggregates(meta["away_bp_ids"], meta["game_date"], window=5)

            pitcher_stats_home_bp_20 = compute_pitcher_team_aggregates(meta["home_bp_ids"], meta["game_date"], window=20)
            pitcher_stats_away_bp_20 = compute_pitcher_team_aggregates(meta["away_bp_ids"], meta["game_date"], window=20)

            pitcher_stats_home_bp["ERA_20"] = pitcher_stats_home_bp_20["ERA"]
            pitcher_stats_home_bp["WHIP_20"] = pitcher_stats_home_bp_20["WHIP"]
            pitcher_stats_away_bp["ERA_20"] = pitcher_stats_away_bp_20["ERA"]
            pitcher_stats_away_bp["WHIP_20"] = pitcher_stats_away_bp_20["WHIP"]

            # Full team pitching stats (SP + bullpen)
            pitcher_ids_home_team = [meta["home_sp_id"]] + meta["home_bp_ids"]
            pitcher_ids_away_team = [meta["away_sp_id"]] + meta["away_bp_ids"]

            pitcher_stats_home_team_10 = compute_pitcher_team_aggregates(pitcher_ids_home_team, meta["game_date"], window=10)
            pitcher_stats_home_team_20 = compute_pitcher_team_aggregates(pitcher_ids_home_team, meta["game_date"], window=20)

            pitcher_stats_away_team_10 = compute_pitcher_team_aggregates(pitcher_ids_away_team, meta["game_date"], window=10)
            pitcher_stats_away_team_20 = compute_pitcher_team_aggregates(pitcher_ids_away_team, meta["game_date"], window=20)

            # Assemble row
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

            cleaned_row = clean_row_for_xata(row, SCHEMA_DEFAULTS)

            # Insert with custom ID (game_id)
            game_id = str(cleaned_row["game_id"])
            resp = xata.records().insert_with_id("master", game_id, cleaned_row)
            if resp.is_success():
                tqdm.write(f"✅ Inserted {game_id}")
                success_count += 1
            else:
                tqdm.write(f"❌ FAILED insert for {game_id}: {resp.status_code} - {resp.get('message', 'no message')}")

        except Exception as e:
            tqdm.write(f"❌ Error processing game {game.get('game_id')}: {e}")

    tqdm.write(f"🎯 Done! Total inserted: {success_count} games")

#Script Runner

if __name__ == "__main__":
    main()