import statsapi
import requests
from datetime import datetime, timedelta
from dateutil import parser
from zoneinfo import ZoneInfo
from timezonefinder import TimezoneFinder
import pytz
import os
from dotenv import load_dotenv
from xata import XataClient
import re
import time
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

wagertalk_team_map = {
    "NYY": "New York Yankees", "NYM": "New York Mets", "LAD": "Los Angeles Dodgers", "LAA": "Los Angeles Angels",
    "CHC": "Chicago Cubs", "CHW": "Chicago White Sox", "ATL": "Atlanta Braves", "ARI": "Arizona Diamondbacks",
    "BAL": "Baltimore Orioles", "BOS": "Boston Red Sox", "CIN": "Cincinnati Reds", "CLE": "Cleveland Guardians",
    "COL": "Colorado Rockies", "DET": "Detroit Tigers", "HOU": "Houston Astros", "KC": "Kansas City Royals",
    "MIL": "Milwaukee Brewers", "MIN": "Minnesota Twins", "ATH": "Oakland Athletics", "PHI": "Philadelphia Phillies",
    "PIT": "Pittsburgh Pirates", "SD": "San Diego Padres", "SF": "San Francisco Giants", "SEA": "Seattle Mariners",
    "STL": "St. Louis Cardinals", "TB": "Tampa Bay Rays", "TEX": "Texas Rangers", "TOR": "Toronto Blue Jays",
    "WAS": "Washington Nationals", "MIA": "Miami Marlins"
}

def fetch_live_runlines():
    from bs4 import BeautifulSoup
    import platform
    import re
    import pandas as pd
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    from webdriver_manager.chrome import ChromeDriverManager

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")

    if platform.system() == "Darwin":
        options.binary_location = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    elif platform.system() == "Windows":
        options.binary_location = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.get("https://www.wagertalk.com/odds")
    time.sleep(5)

    soup = BeautifulSoup(driver.page_source, "lxml")
    tables = pd.read_html(str(soup), flavor="lxml")
    driver.quit()

    if not tables:
        return {}

    df = tables[0]
    df.reset_index(drop=True, inplace=True)

    section_titles = ["AMERICAN LEAGUE", "NATIONAL LEAGUE", "INTERLEAGUE"]
    section_starts = []

    for i, row in df.iterrows():
        row_str = str(row.values).upper()
        if "[-]" in row_str:
            for section in section_titles:
                if section in row_str:
                    section_starts.append((i, section))
                    break

    section_ranges = []
    for idx, (start_idx, _) in enumerate(section_starts):
        end_idx = section_starts[idx + 1][0] if idx + 1 < len(section_starts) else len(df)
        section_ranges.append((start_idx, end_idx))

    mlb_sections = [df.iloc[start:end] for start, end in section_ranges]
    mlb_odds_df = pd.concat(mlb_sections, ignore_index=True)

    if mlb_odds_df.empty:
        return {}

    def extract_runline(consensus_str):
        if not isinstance(consensus_str, str) or consensus_str.lower() == "unknown":
            return None
        match = re.findall(r'\b(\d{1,2}½?)\s*(?=[ou]|\s|$)', consensus_str)
        if match:
            return float(match[0].replace("½", ".5"))
        return None

    def extract_teams(teams_str):
        team_name_mapping = {
            "NYY": "New York Yankees", "NYM": "New York Mets", "LAD": "Los Angeles Dodgers", "LAA": "Los Angeles Angels",
            "CHC": "Chicago Cubs", "CHW": "Chicago White Sox", "ATL": "Atlanta Braves", "ARI": "Arizona Diamondbacks",
            "BAL": "Baltimore Orioles", "BOS": "Boston Red Sox", "CIN": "Cincinnati Reds", "CLE": "Cleveland Guardians",
            "COL": "Colorado Rockies", "DET": "Detroit Tigers", "HOU": "Houston Astros", "KC": "Kansas City Royals",
            "MIL": "Milwaukee Brewers", "MIN": "Minnesota Twins", "ATH": "Athletics", "PHI": "Philadelphia Phillies",
            "PIT": "Pittsburgh Pirates", "SD": "San Diego Padres", "SF": "San Francisco Giants", "SEA": "Seattle Mariners",
            "STL": "St. Louis Cardinals", "TB": "Tampa Bay Rays", "TEX": "Texas Rangers", "TOR": "Toronto Blue Jays",
            "WAS": "Washington Nationals", "MIA": "Miami Marlins"
        }

        if not isinstance(teams_str, str):
            return None, None
        try:
            first_period = teams_str.index('.')
            second_period = teams_str.index('.', first_period + 1)
            pitcher1_initial_index = first_period - 1
            team1_raw = teams_str[:pitcher1_initial_index]
            team1 = team1_raw.strip()
            pitcher2_initial_index = second_period - 1
            pre_pitcher2 = teams_str[pitcher2_initial_index - 3:pitcher2_initial_index]
            if pre_pitcher2 in team_name_mapping:
                team2 = pre_pitcher2
            else:
                pre_pitcher2_alt = pre_pitcher2[-2:]
                team2 = pre_pitcher2_alt if pre_pitcher2_alt in team_name_mapping else None
            if team1 not in team_name_mapping or team2 not in team_name_mapping:
                return None, None
            return team_name_mapping[team1], team_name_mapping[team2]
        except:
            return None, None

    runline_map = {}
    for _, row in mlb_odds_df.iterrows():
        if "Consensus" in row and "Teams" in row:
            runline = extract_runline(row["Consensus"])
            away, home = extract_teams(row["Teams"])
            if runline and home and away:
                runline_map[(home, away)] = runline

    return runline_map


# === Load Xata credentials ===
load_dotenv("xata-config.env")
API_KEY = os.getenv("XATA_API_KEY")
XATA_DATABASE_URL = os.getenv("XATA_DATABASE_URL")
xata = XataClient(api_key=API_KEY, db_url=XATA_DATABASE_URL)

VISUAL_CROSSING_API_KEY = "2XX3SRE3JERPZ8P2T37K4JNKT"

# === Stadium + Mapping Data ===
from z_games_builder import team_to_oddshark_id, team_timezones, stadium_coordinates, clean_float, is_night_game_in_local_time, fetch_over_under_runline, extract_player_ids, to_iso_z


def fetch_today_game_ids():
    today = datetime.now().strftime("%Y-%m-%d")
    resp = requests.get("https://statsapi.mlb.com/api/v1/schedule", params={
        "sportId": 1,
        "startDate": today,
        "endDate": today
    })
    data = resp.json()
    game_ids = []
    for date in data.get("dates", []):
        for game in date.get("games", []):
            if game.get("gameType") == "R":
                game_ids.append(game["gamePk"])
    print(f"[INFO] Found {len(game_ids)} games for today")
    return game_ids

def get_description(gamepk):
    """
    Fetches the current game status description for a given gamepk.
    """
    try:
        url = f"https://statsapi.mlb.com/api/v1.1/game/{gamepk}/feed/live"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        description = data.get("liveData", {}).get("plays", {}).get("currentPlay", {}).get("about", {}).get("description")
        if not description:
            description = data.get("gameData", {}).get("status", {}).get("detailedState", "Unknown")
        
        return description
    except Exception as e:
        print(f"[DESCRIPTION ERROR] Game {gamepk}: {e}")
        return None


def is_future_game(start_time_str, stadium):
    try:
        if stadium not in stadium_coordinates:
            return False
        lat, lon = stadium_coordinates[stadium]
        tf = TimezoneFinder()
        tz_str = tf.timezone_at(lat=lat, lng=lon)
        if not tz_str:
            return False
        local_tz = pytz.timezone(tz_str)
        now_local = datetime.now(local_tz)
        game_time = parser.isoparse(start_time_str).astimezone(local_tz)
        return game_time > now_local
    except:
        return False


def get_weather_forecast(lat, lon, dt_iso):
    try:
        dt_utc = parser.isoparse(dt_iso)
        tf = TimezoneFinder()
        tz_str = tf.timezone_at(lat=lat, lng=lon)
        tz = pytz.timezone(tz_str)
        dt_local = dt_utc.astimezone(tz)

        if dt_local.minute >= 30:
            dt_local += timedelta(hours=1)
        dt_local = dt_local.replace(minute=0, second=0, microsecond=0)

        date_str = dt_local.strftime("%Y-%m-%d")
        hour_index = dt_local.hour

        url = (
            f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
            f"{lat},{lon}/{date_str}?key={VISUAL_CROSSING_API_KEY}"
            f"&include=hours&elements=temp,humidity,windspeed,winddir,elevation"
        )
        r = requests.get(url)
        r.raise_for_status()
        data = r.json()
        hours = data.get("days", [])[0].get("hours", [])

        if 0 <= hour_index < len(hours):
            hour_data = hours[hour_index]
            return {
                "temperature": hour_data.get("temp", ""),
                "humidity": hour_data.get("humidity", ""),
                "wind_speed": hour_data.get("windspeed", ""),
                "wind_direction": hour_data.get("winddir", ""),
                "elevation": data.get("elevation", "")
            }
        return {}
    except Exception as e:
        raise RuntimeError(f"[WEATHER FORECAST ERROR] {lat},{lon} at {dt_iso}: {e}")


def get_weather_historical(lat, lon, dt_iso):
    try:
        url = (
            f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
            f"{lat},{lon}/{dt_iso}?key={VISUAL_CROSSING_API_KEY}&include=current&elements=temp,humidity,windspeed,winddir,elevation"
        )
        r = requests.get(url)
        r.raise_for_status()
        data = r.json().get("currentConditions", {})
        return {
            "temperature": data.get("temp", ""),
            "humidity": data.get("humidity", ""),
            "wind_speed": data.get("windspeed", ""),
            "wind_direction": data.get("winddir", ""),
            "elevation": r.json().get("elevation", "")
        }
    except Exception as e:
        raise RuntimeError(f"[WEATHER HISTORICAL ERROR] {lat},{lon} at {dt_iso}: {e}")


def get_game_data(gamepk, runline_map):
    url = f"https://statsapi.mlb.com/api/v1.1/game/{gamepk}/feed/live"
    response = requests.get(url, timeout=10)
    data = response.json()

    game = data["gameData"]
    home_name_dbg = game["teams"]["home"]["name"]
    away_name_dbg = game["teams"]["away"]["name"]
    #print(f"[DEBUG] Looking for runline: ({home_name_dbg}, {away_name_dbg})")

    live = data["liveData"]

    game_date_raw = game.get("datetime", {}).get("originalDate", "")
    start_time_raw = game.get("datetime", {}).get("dateTime", "")
    game_date = to_iso_z(game_date_raw + "T00:00:00")
    start_time = to_iso_z(start_time_raw)

    stadium = game.get("venue", {}).get("name", "")
    status_code = game.get("status", {}).get("abstractGameCode", "")

    home = game.get("teams", {}).get("home", {})
    away = game.get("teams", {}).get("away", {})
    home_id = str(home.get("id"))
    away_id = str(away.get("id"))

    box = live.get("linescore", {})
    home_score = box.get("teams", {}).get("home", {}).get("runs", "")
    away_score = box.get("teams", {}).get("away", {}).get("runs", "")
    if home_score != "" and away_score != "":
        home_score_val = int(home_score)
        away_score_val = int(away_score)
        runs_total = float(home_score_val + away_score_val)
    else:
        runs_total = 0


    lat, lon = stadium_coordinates.get(stadium, (None, None))
    if lat is not None:
        if is_future_game(start_time_raw, stadium):
            weather = get_weather_forecast(lat, lon, start_time)
        else:
            weather = get_weather_historical(lat, lon, start_time)
    else:
        weather = {}

    runline = runline_map.get((home.get("name"), away.get("name")))


    record = {
        "game_id": str(gamepk),
        "game_date": game_date or None,
        "start_time": start_time or None,
        "home_id": home_id,
        "home_oddshark_id": str(team_to_oddshark_id.get(int(home_id), "")),
        "home_name": home.get("name"),
        "away_id": away_id,
        "away_oddshark_id": str(team_to_oddshark_id.get(int(away_id), "")),
        "away_name": away.get("name"),
        "runs_home": int(home_score) if home_score != "" else None,
        "runs_away": int(away_score) if away_score != "" else None,
        "runs_total": clean_float(runs_total),
        "stadium": stadium,
        "is_night_game": is_night_game_in_local_time(start_time_raw, int(home_id)),
        "is_dome": False,
        "game_started": status_code not in ["S", "P"],
        "game_complete": status_code == "F",
        "runline": runline if runline != "" else None,
        "temperature": clean_float(weather.get("temperature", "")) or None,
        "wind_speed": clean_float(weather.get("wind_speed", "")) or None,
        "wind_direction": clean_float(weather.get("wind_direction", "")) or None,
        "humidity": clean_float(weather.get("humidity", "")) or None,
        "elevation": clean_float(weather.get("elevation", "")) or None
    }

    description = get_description(gamepk)
    if description:
        record["description"] = description


    record.update(extract_player_ids(gamepk))
    for k, v in record.items():
        if "_id" in k and v == "":
            record[k] = None

    return record


def run_delta_upsert():
    game_ids = fetch_today_game_ids()

    runline_map = fetch_live_runlines()
    #print(f"[DEBUG] Runline keys: {list(runline_map.keys())}")

    for i, gamepk in enumerate(game_ids, 1):
        print(f"[{i}/{len(game_ids)}] Processing game {gamepk}")
        try:
            record = get_game_data(gamepk, runline_map)
            #print(f"[DEBUG] Record preview for game {record['game_id']}")
            resp = xata.records().upsert("games", record["game_id"], record)

            if not resp.is_success():
                raise RuntimeError(f"[XATA ERROR] Upsert failed: {resp.status_code} - {resp.json()}")
            print(f"[UPSERT] Game {record['game_id']} upserted")
        except Exception as e:
            print(f"[ERROR] Game {gamepk} failed: {e}")
            import traceback
            traceback.print_exc()
            raise SystemExit("[FATAL] Exiting due to failure.")


if __name__ == "__main__":
    run_delta_upsert()
