import statsapi
import requests
from dateutil import parser
from zoneinfo import ZoneInfo
from datetime import timezone
from datetime import timedelta
from datetime import datetime
from timezonefinder import TimezoneFinder
import pytz
import os
import pandas as pd
from dotenv import load_dotenv
from xata import XataClient
from time import sleep

# === Load Xata credentials ===
load_dotenv("xata-config.env")
API_KEY = os.getenv("XATA_API_KEY")
XATA_DATABASE_URL = os.getenv("XATA_DATABASE_URL")
xata = XataClient(api_key=API_KEY, db_url=XATA_DATABASE_URL)

# === API Key ===
VISUAL_CROSSING_API_KEY = "2XX3SRE3JERPZ8P2T37K4JNKT"  

# === Mapping/stadium data ===

team_to_oddshark_id = {
    120: 27017, 146: 27022, 139: 27003, 144: 27009, 140: 27002, 117: 27023,
    135: 26996, 143: 26995, 110: 27008, 136: 27011, 121: 27014, 109: 27007,
    108: 26998, 133: 27016, 141: 27010, 114: 27014, 138: 27019, 142: 27005,
    116: 26999, 147: 27001, 137: 26997, 118: 27006, 145: 27018, 115: 27004,
    111: 27021, 119: 27015, 112: 27020, 158: 27012, 113: 27000, 134: 27013
}

team_timezones = {
    108: "America/Los_Angeles", 109: "America/Phoenix", 110: "America/New_York",
    111: "America/New_York", 112: "America/Chicago", 113: "America/New_York",
    114: "America/New_York", 115: "America/Denver", 116: "America/Detroit",
    117: "America/Chicago", 118: "America/Chicago", 119: "America/Los_Angeles",
    120: "America/New_York", 121: "America/Chicago", 133: "America/Los_Angeles",
    134: "America/New_York", 135: "America/Los_Angeles", 136: "America/Los_Angeles",
    137: "America/New_York", 138: "America/New_York", 139: "America/New_York",
    140: "America/Chicago", 141: "America/Toronto", 142: "America/Chicago",
    143: "America/New_York", 144: "America/New_York", 145: "America/Chicago",
    146: "America/New_York", 147: "America/New_York", 158: "America/Chicago"
}

stadium_coordinates = {
    "Angel Stadium": (33.8003, -117.8827),
    "Chase Field": (33.4456, -112.0667),
    "Oriole Park at Camden Yards": (39.2839, -76.6218),
    "Fenway Park": (42.3467, -71.0972),
    "Wrigley Field": (41.9484, -87.6553),
    "Great American Ball Park": (39.0978, -84.5064),
    "Progressive Field": (41.4962, -81.6852),
    "Coors Field": (39.7559, -104.9942),
    "Comerica Park": (42.3390, -83.0485),
    "Minute Maid Park": (29.7573, -95.3555),
    "Daikin Park": (29.7573, -95.3555),
    "Kauffman Stadium": (39.0516, -94.4805),
    "Dodger Stadium": (34.0739, -118.2400),
    "loanDepot park": (25.7781, -80.2198),
    "American Family Field": (43.0280, -87.9712),
    "Target Field": (44.9817, -93.2789),
    "Citi Field": (40.7571, -73.8458),
    "Yankee Stadium": (40.8296, -73.9262),
    "Oakland Coliseum": (37.7516, -122.2005),
    "Sutter Health Park": (38.5804, -121.5134),
    "George M. Steinbrenner Field": (27.98028, -82.50667),
    "Citizens Bank Park": (39.9050, -75.1665),
    "PNC Park": (40.4469, -80.0057),
    "Tropicana Field": (27.7683, -82.6534),
    "Oracle Park": (37.7786, -122.3893),
    "T-Mobile Park": (47.5914, -122.3325),
    "Busch Stadium": (38.6226, -90.1928),
    "Globe Life Field": (32.7513, -97.0820),
    "Rogers Centre": (43.6414, -79.3894),
    "Nationals Park": (38.8729, -77.0074),
    "Guaranteed Rate Field": (41.8299, -87.6338),
    "Rate Field": (41.8299, -87.6338),
    "Truist Park": (33.8908, -84.4678),
    "Petco Park": (32.7076, -117.1570)
}

def to_iso_z(dt_str):
    try:
        dt = parser.isoparse(dt_str)
        return dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    except:
        return ""

def is_night_game_in_local_time(start_time_str, home_team_id):
    try:
        if home_team_id not in team_timezones:
            return False
        tz = ZoneInfo(team_timezones[home_team_id])
        dt_utc = parser.isoparse(start_time_str)
        dt_local = dt_utc.astimezone(tz)
        return dt_local.hour >= 18
    except Exception:
        return False


def get_local_hour_string(lat, lon, dt_utc):
    tf = TimezoneFinder()
    tz_str = tf.timezone_at(lat=lat, lng=lon)
    if tz_str is None:
        print(f"[DEBUG] No timezone found for coordinates {lat},{lon}")
        return None, None
    tz = pytz.timezone(tz_str)
    local_dt = dt_utc.astimezone(tz)

    # Round to nearest hour
    if local_dt.minute >= 30:
        local_dt = local_dt.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    else:
        local_dt = local_dt.replace(minute=0, second=0, microsecond=0)

    return local_dt.strftime('%H:00:00'), local_dt.strftime('%Y-%m-%d')

def is_future_game(start_time_str, stadium_name):
    try:
        if stadium_name not in stadium_coordinates:
            return False  # If we can't geolocate, err on the side of inclusion

        lat, lon = stadium_coordinates[stadium_name]
        tf = TimezoneFinder()
        tz_str = tf.timezone_at(lat=lat, lng=lon)
        if not tz_str:
            return False

        local_tz = pytz.timezone(tz_str)
        now_local = datetime.now(local_tz)
        game_time = parser.isoparse(start_time_str).astimezone(local_tz)

        return game_time > now_local
    except Exception:
        return False



def fetch_over_under_runline(oddshark_id, game_date):
    year = game_date.year
    url = f"https://www.oddsshark.com/stats/gamelog/baseball/mlb/{oddshark_id}?season={year}"

    try:
        tables = pd.read_html(url)
        df = tables[0]
    except Exception as e:
        print(f"[ODDS ERROR] Could not fetch runline for team {oddshark_id} on {game_date.date()}: {e}")
        return None

    if df.empty or 'Date' not in df.columns or 'Total' not in df.columns:
        print(f"[ODDS ERROR] Malformed or empty table for {oddshark_id}")
        return None

    df['Date'] = pd.to_datetime(df['Date'], format='%b %d, %Y', errors='coerce')
    match = df[df['Date'] == game_date]

    if len(match) == 1:
        total = match.iloc[0]['Total']
        try:
            return float(total)
        except:
            return None
    elif len(match) > 1:
        print(f"[ODDS WARNING] Multiple matches found for team {oddshark_id} on {game_date.date()}")
    else:
        print(f"[ODDS WARNING] No match found for team {oddshark_id} on {game_date.date()}")
    
    return None

def clean_float(value):
    try:
        f = float(value)
        if not (f == f and abs(f) != float("inf")):  # check for NaN and inf
            return ""
        return f
    except:
        return ""

def get_description(gamepk):
    """
    Fetches a compact game state description for a given gamepk.
    Format: 'inning: T9, outs: 2, runners: (1,2,3)'
    """
    try:
        url = f"https://statsapi.mlb.com/api/v1.1/game/{gamepk}/feed/live"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        linescore = data.get("liveData", {}).get("linescore", {})
        inning_half = linescore.get("inningHalf")
        current_inning = linescore.get("currentInning")
        outs = linescore.get("outs")

        offense = linescore.get("offense", {})

        # Build runners list
        bases_occupied = []
        if offense.get("first"):
            bases_occupied.append(1)
        if offense.get("second"):
            bases_occupied.append(2)
        if offense.get("third"):
            bases_occupied.append(3)

        bases_occupied.sort()

        if inning_half and current_inning is not None and outs is not None:
            inning_code = f"{inning_half[0]}{current_inning}"  # T1, B5, etc.
            runners_str = f"({','.join(str(b) for b in bases_occupied)})" if bases_occupied else "()"
            return f"inning: {inning_code}, outs: {outs}, runners: {runners_str}"
        else:
            detailed_state = data.get("gameData", {}).get("status", {}).get("detailedState", "Unknown")
            return detailed_state

    except Exception as e:
        print(f"[GAME STATE ERROR] Game {gamepk}: {e}")
        return None




def get_weather_by_coords(lat, lon, dt_iso):
    try:
        dt_utc = parser.isoparse(dt_iso)

        # Get local timezone
        tf = TimezoneFinder()
        tz_str = tf.timezone_at(lat=lat, lng=lon)
        if tz_str is None:
            print(f"[DEBUG] No timezone found for {lat},{lon}")
            return {"temperature": "", "humidity": "", "wind_speed": ""}
        
        tz = pytz.timezone(tz_str)
        dt_local = dt_utc.astimezone(tz)

        # Round to nearest hour
        if dt_local.minute >= 30:
            dt_local = dt_local.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        else:
            dt_local = dt_local.replace(minute=0, second=0, microsecond=0)

        # Construct ISO local datetime string
        local_datetime_str = dt_local.strftime("%Y-%m-%dT%H:00:00")

        # Make the request for only that hour
        url = (
            f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
            f"{lat},{lon}/{local_datetime_str}"
            f"?key={VISUAL_CROSSING_API_KEY}&include=current&elements=temp,humidity,windspeed,winddir,elevation"
        )


        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        current = data.get("currentConditions", {})
        return {
            "temperature": current.get("temp", ""),
            "humidity": current.get("humidity", ""),
            "wind_speed": current.get("windspeed", ""),
            "wind_direction": current.get("winddir", ""),
            "elevation": data.get("elevation", "")
        }

    except Exception as e:
        print(f"Weather API error for {lat},{lon} at {dt_iso}: {e}")
        return {"temperature": "", "humidity": "", "wind_speed": ""}


def insert_new_rows(table, rows):
    if not rows:
        return
    try:
        # Set the record ID explicitly using the game_id field
        for row in rows:
            row["id"] = row["game_id"]

        resp = xata.records().bulk_insert(table, {"records": rows})
        if not resp.is_success():
            raise Exception(f"Xata insert failed ({resp.status_code}): {resp.json()}")
    except Exception as e:
        raise Exception(f"Bulk insert failed for table '{table}': {e}")

# === Fetch all regular season game IDs since 2021 ===

def fetch_regular_season_game_ids():
    print(">> Fetching game IDs from MLB schedule...")
    MLB_SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"
    today = datetime.today().strftime("%Y-%m-%d")
    current_year = datetime.today().year
    game_ids = []

    for year in range(2021, current_year + 1):
        end_date = today if year == current_year else f"{year}-12-31"
        response = requests.get(MLB_SCHEDULE_URL, params={
            "sportId": 1,
            "startDate": f"{year}-01-01",
            "endDate": end_date
        })

        if response.status_code == 200:
            data = response.json()
            for date in data.get("dates", []):
                for game in date.get("games", []):
                    if game.get("gameType") == "R":
                        game_ids.append(game["gamePk"])
        else:
            print(f"[ERROR] Failed to fetch schedule for {year}. Status Code: {response.status_code}")

    print(f"[INFO] Found {len(game_ids)} regular season games")
    return game_ids

def extract_player_ids(gamepk):
    try:
        url = f"https://statsapi.mlb.com/api/v1.1/game/{gamepk}/feed/live"
        response = requests.get(url, timeout=10)
        data = response.json()

        boxscore = data["liveData"]["boxscore"]
        output = {}

        for side in ["home", "away"]:
            team_data = boxscore["teams"][side]
            
            # Batting order
            batting_order = team_data.get("battingOrder", [])
            for i, player_id in enumerate(batting_order[:9]):
                output[f"{side}_{i+1}_id"] = str(player_id)

            # Starting pitcher
            pitchers = team_data.get("pitchers", [])
            if pitchers:
                output[f"{side}_sp_id"] = str(pitchers[0])

            # Bullpen pitchers — this works even if game hasn't started
            bullpen = team_data.get("bullpen", [])
            for i, player_id in enumerate(bullpen[:13], start=1):  # Limit to 10
                output[f"{side}_bp_{i}_id"] = str(player_id)

        return output

    except Exception as e:
        print(f"[PLAYER ERROR] Game {gamepk}: {e}")
        return {}



def get_existing_game_ids():
    print(">> Fetching existing game IDs from Xata...")
    seen = set()
    page = None
    while True:
        query = {"columns": ["game_id"], "page": {"size": 1000}}
        if page:
            query["page"]["after"] = page
        resp = xata.data().query("games", query)
        if not resp.is_success():
            break
        records = resp.get("records", [])
        if not records:
            break
        for r in records:
            game_id = r.get("game_id")
            if game_id:
                seen.add(str(game_id))
        page = resp.get("meta", {}).get("page", {}).get("cursor")
        if not page:
            break
    print(f"[INFO] Found {len(seen)} existing game_ids in Xata")
    sleep(10)
    return seen



# === Main bulk insert logic ===
def run_full_game_insert():
    all_game_ids = fetch_regular_season_game_ids()
    existing_game_ids = get_existing_game_ids()
    batch_size = 20
    batch = []

    for i, gamepk in enumerate(all_game_ids, 1):
        if str(gamepk) in existing_game_ids:
            print(f"[{i}/{len(all_game_ids)}] Skipping game {gamepk} (already in Xata)")
            continue

        print(f"[{i}/{len(all_game_ids)}] Processing game {gamepk}")
        data = get_game_data(gamepk)
        if data:
            batch.append(data)

        if len(batch) >= batch_size or i == len(all_game_ids):
            try:
                insert_new_rows("games", batch)
                print(f"[INFO] Inserted {len(batch)} games into Xata")
            except Exception as e:
                print(f"[ERROR] Batch insert failed: {e}")
            batch = []
            sleep(1)

def get_game_data(gamepk):
    try:
        game = statsapi.get("game", {"gamePk": gamepk})["gameData"]
        box = statsapi.boxscore_data(gamepk)

        game_date_raw = game.get("datetime", {}).get("originalDate", "")
        start_time_raw = game.get("datetime", {}).get("dateTime", "")
        game_date = to_iso_z(game_date_raw + "T00:00:00") if game_date_raw else ""
        start_time = to_iso_z(start_time_raw) if start_time_raw else ""

        stadium = game.get("venue", {}).get("name", "")
        status_code = game.get("status", {}).get("abstractGameCode", "")

        # Skip future games
        if is_future_game(start_time, stadium):
            print(f"[SKIP] Game {gamepk} is in the future. Skipping.")
            return None

        home = game.get("teams", {}).get("home", {})
        away = game.get("teams", {}).get("away", {})

        home_id = home.get("id", "")
        home_name = home.get("name", "")
        away_id = away.get("id", "")
        away_name = away.get("name", "")

        home_score = box.get("home", {}).get("teamStats", {}).get("batting", {}).get("runs", "")
        away_score = box.get("away", {}).get("teamStats", {}).get("batting", {}).get("runs", "")
        runs_total = (
            float(home_score) + float(away_score)
            if home_score != "" and away_score != ""
            else ""
        )

        # Weather
        if stadium_coordinates.get(stadium):
            lat, lon = stadium_coordinates[stadium]
            weather = get_weather_by_coords(lat, lon, start_time)
        else:
            weather = {}

        # Runline
        runline = None
        home_oddshark_id = team_to_oddshark_id.get(home_id, "")
        if home_oddshark_id and game_date:
            try:
                game_date_dt = datetime.strptime(game_date, "%Y-%m-%dT%H:%M:%S.000Z")
                runline_val = fetch_over_under_runline(int(home_oddshark_id), game_date_dt)
                runline = clean_float(runline_val)
            except Exception as e:
                print(f"[ODDS EXCEPTION] {e}")

        # Build safe record
        record = {
            "game_id": str(gamepk),
            "game_date": game_date,
            "start_time": start_time,
            "home_id": str(home_id),
            "home_oddshark_id": str(team_to_oddshark_id.get(int(home_id), "")),
            "home_name": home_name,
            "away_id": str(away_id),
            "away_oddshark_id": str(team_to_oddshark_id.get(away_id, "")),
            "away_name": away_name,
            "runs_home": int(home_score) if home_score != "" else "",
            "runs_away": int(away_score) if away_score != "" else "",
            "runs_total": clean_float(runs_total),
            "stadium": stadium,
            "is_night_game": is_night_game_in_local_time(start_time_raw, home_id),
            "is_dome": False,
            "game_started": status_code not in ["S", "P"],
            "game_complete": status_code == "F"
        }

        #Add description field
        description = get_description(gamepk)
        if description:
            record["description"] = description


        # Only include float fields if valid
        if runline != "":
            record["runline"] = runline

        for field in ["temperature", "wind_speed", "wind_direction", "humidity", "elevation"]:
            value = clean_float(weather.get(field, ""))
            if value != "":
                record[field] = value

        record.update(extract_player_ids(gamepk))

        return record

    except Exception as e:
        print(f"Error fetching game {gamepk}: {e}")
        return None


if __name__ == "__main__":
    run_full_game_insert()