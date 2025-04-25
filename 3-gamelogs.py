import requests
import pandas as pd
from datetime import datetime
from pybaseball import playerid_reverse_lookup
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

print("\n---------- Now running 3-gamelogs.py  ----------\n")

# Load game_pks.csv to get team names and IDs
game_pks_df = pd.read_csv('game_pks.csv')

team_to_oddshark_id = {
    120: 27017, 146: 27022, 139: 27003, 144: 27009, 140: 27002, 117: 27023,
    135: 26996, 143: 26995, 110: 27008, 136: 27011, 121: 27014, 109: 27007,
    108: 26998, 133: 27016, 141: 27010, 114: 27024, 138: 27019, 142: 27005,
    116: 26999, 147: 27001, 137: 26997, 118: 27006, 145: 27018, 115: 27004,
    111: 27021, 119: 27015, 112: 27020, 158: 27012, 113: 27000, 134: 27013
}

errors = []
errors_lock = Lock()

def get_game_data(gamepk):
    url = f"https://statsapi.mlb.com/api/v1.1/game/{gamepk}/feed/live"
    response = requests.get(url, timeout=10)
    return response.json()

def get_bbref_id(mlbam_id):
    try:
        lookup_df = playerid_reverse_lookup([mlbam_id], key_type='mlbam')
        return lookup_df.loc[lookup_df['key_mlbam'] == mlbam_id, 'key_bbref'].values[0]
    except IndexError:
        return 'unknown'

def extract_starting_lineup(game_data, team_side, gamepk):
    lineup = {}
    team = game_data['liveData']['boxscore']['teams'][team_side]['players']
    for player_id, player_info in team.items():
        try:
            if 'battingOrder' in player_info and int(player_info['battingOrder']) % 100 == 0:
                order = int(player_info['battingOrder']) // 100
                mlbam_id = player_info['person']['id']
                bbref_id = get_bbref_id(mlbam_id)
                lineup[order] = {
                    'name': player_info['person']['fullName'],
                    'mlbam_id': mlbam_id,
                    'bbref_id': bbref_id
                }
        except Exception as e:
            with errors_lock:
                errors.append((gamepk, f"Error processing player {player_id} in {team_side} lineup: {str(e)}"))
    return [lineup.get(i, {'name': '', 'mlbam_id': '', 'bbref_id': ''}) for i in range(1, 10)]

def get_pitchers(game_data, team_side, gamepk):
    team = game_data['liveData']['boxscore']['teams'][team_side]
    pitchers = []
    for idx, pitcher_id in enumerate(team['pitchers']):
        try:
            pitcher = team['players'][f'ID{pitcher_id}']
            mlbam_id = pitcher['person']['id']
            bbref_id = get_bbref_id(mlbam_id)
            pitchers.append({
                'name': pitcher['person']['fullName'],
                'mlbam_id': mlbam_id,
                'bbref_id': bbref_id,
                'order': idx + 1
            })
        except Exception as e:
            with errors_lock:
                errors.append((gamepk, f"Error processing pitcher {pitcher_id} in {team_side} team: {str(e)}"))
    return pitchers

def get_bullpen(game_data, team_side, gamepk):
    team = game_data['liveData']['boxscore']['teams'][team_side]
    bullpen = []
    if team['pitchers']:
        targets = team['bullpen'] + team['pitchers'][1:]
    else:
        targets = team['bullpen']
    for pitcher_id in targets:
        try:
            pitcher = team['players'][f'ID{pitcher_id}']
            mlbam_id = pitcher['person']['id']
            bbref_id = get_bbref_id(mlbam_id)
            bullpen.append({
                'name': pitcher['person']['fullName'],
                'mlbam_id': mlbam_id,
                'bbref_id': bbref_id
            })
        except Exception as e:
            with errors_lock:
                errors.append((gamepk, f"Error processing bullpen pitcher {pitcher_id} in {team_side} team: {str(e)}"))
    return bullpen

def create_game_dataframe(gamepk):
    try:
        game_data = get_game_data(gamepk)
        home_lineup = extract_starting_lineup(game_data, 'home', gamepk)
        away_lineup = extract_starting_lineup(game_data, 'away', gamepk)
        home_bullpen = get_bullpen(game_data, 'home', gamepk)
        away_bullpen = get_bullpen(game_data, 'away', gamepk)
        home_pitchers = get_pitchers(game_data, 'home', gamepk)
        away_pitchers = get_pitchers(game_data, 'away', gamepk)
    except Exception as e:
        with errors_lock:
            errors.append((gamepk, f"Error during game parsing: {str(e)}"))
        return pd.DataFrame()

    game_info = game_pks_df[game_pks_df['game_id'] == gamepk].iloc[0]
    game_record = {
        'game_id': gamepk,
        'game_date': game_info.get('game_date', 'unknown'),
        'runs_home': game_data.get('liveData', {}).get('linescore', {}).get('teams', {}).get('home', {}).get('runs', 0),
        'runs_away': game_data.get('liveData', {}).get('linescore', {}).get('teams', {}).get('away', {}).get('runs', 0),
        'home_id': game_info['home_id'],
        'home_name': game_info['home_name'],
        'away_id': game_info['away_id'],
        'away_name': game_info['away_name'],
        'home_oddshark_id': team_to_oddshark_id.get(game_info['home_id'], 'unknown'),
        'away_oddshark_id': team_to_oddshark_id.get(game_info['away_id'], 'unknown')
    }
    game_record['runs_total'] = game_record['runs_home'] + game_record['runs_away']

    for i, player in enumerate(away_lineup, start=1):
        game_record[f'Away_Batter{i}_Name'] = player['name']
        game_record[f'Away_Batter{i}_ID'] = player['mlbam_id']
        game_record[f'Away_Batter{i}_bbrefID'] = player['bbref_id']
    for i, player in enumerate(home_lineup, start=1):
        game_record[f'Home_Batter{i}_Name'] = player['name']
        game_record[f'Home_Batter{i}_ID'] = player['mlbam_id']
        game_record[f'Home_Batter{i}_bbrefID'] = player['bbref_id']
    if home_pitchers:
        game_record['Home_SP_Name'] = home_pitchers[0]['name']
        game_record['Home_SP_ID'] = home_pitchers[0]['mlbam_id']
        game_record['Home_SP_bbrefID'] = home_pitchers[0]['bbref_id']
    if away_pitchers:
        game_record['Away_SP_Name'] = away_pitchers[0]['name']
        game_record['Away_SP_ID'] = away_pitchers[0]['mlbam_id']
        game_record['Away_SP_bbrefID'] = away_pitchers[0]['bbref_id']
    for i, pitcher in enumerate(home_bullpen, start=1):
        game_record[f'Home_bullpen_{i}_Name'] = pitcher['name']
        game_record[f'Home_bullpen_{i}_ID'] = pitcher['mlbam_id']
        game_record[f'Home_bullpen_{i}_bbrefID'] = pitcher['bbref_id']
    for i, pitcher in enumerate(away_bullpen, start=1):
        game_record[f'Away_bullpen_{i}_Name'] = pitcher['name']
        game_record[f'Away_bullpen_{i}_ID'] = pitcher['mlbam_id']
        game_record[f'Away_bullpen_{i}_bbrefID'] = pitcher['bbref_id']
    for i, pitcher in enumerate(home_pitchers[1:], start=2):
        game_record[f'Home_P_{i}_Name'] = pitcher['name']
        game_record[f'Home_P_{i}_ID'] = pitcher['mlbam_id']
        game_record[f'Home_P_{i}_bbrefID'] = pitcher['bbref_id']
    for i, pitcher in enumerate(away_pitchers[1:], start=2):
        game_record[f'Away_P_{i}_Name'] = pitcher['name']
        game_record[f'Away_P_{i}_ID'] = pitcher['mlbam_id']
        game_record[f'Away_P_{i}_bbrefID'] = pitcher['bbref_id']

    return pd.DataFrame([game_record])

def save_game_to_csv(gamepk):
    df = create_game_dataframe(gamepk)
    if not df.empty:
        df.to_csv(f'gamelogs/game_{gamepk}.csv', index=False)

# --- Main parallel execution ---
gamepks = pd.read_csv('game_pks.csv').game_id.tail(100)  # CHANGE TO RECENT GAMES

with ThreadPoolExecutor(max_workers=8) as executor:
    futures = [executor.submit(save_game_to_csv, gamepk) for gamepk in gamepks]
    for _ in tqdm(as_completed(futures), total=len(futures), desc="Generating gamelogs", unit="games"):
        pass

# --- Error output ---
if errors:
    print("\nErrors encountered:")
    for e in errors:
        print(e)
else:
    print("No errors encountered creating gamelogs.")

print("\n=================================================")
print("Game logs updated.\nLocation: ~/gamelogs/game_<gamepk>.csv\nSUCCESS")
print("=================================================")
