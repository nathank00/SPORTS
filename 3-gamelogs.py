import requests
import pandas as pd
from datetime import datetime
from pybaseball import playerid_reverse_lookup

# Load game_pks.csv to get team names and IDs
game_pks_df = pd.read_csv('game_pks.csv')

# Mapping of team 3-digit IDs to oddshark 5-digit IDs 
team_to_oddshark_id = {
    120: 27017, 146: 27022, 139: 27003, 144: 27009, 140: 27002, 117: 27023,
    135: 26996, 143: 26995, 110: 27008, 136: 27011, 121: 27014, 109: 27007,
    108: 26998, 133: 27016, 141: 27010, 114: 27014, 138: 27019, 142: 27005,
    116: 26999, 147: 27001, 137: 26997, 118: 27006, 145: 27018, 115: 27004,
    111: 27021, 119: 27015, 112: 27020, 158: 27012, 113: 27000, 134: 27013
}

errors = []

def get_game_data(gamepk):
    url = f"https://statsapi.mlb.com/api/v1.1/game/{gamepk}/feed/live"
    response = requests.get(url)
    return response.json()

def get_bbref_id(mlbam_id):
    try:
        lookup_df = playerid_reverse_lookup([mlbam_id], key_type='mlbam')
        return lookup_df.loc[lookup_df['key_mlbam'] == mlbam_id, 'key_bbref'].values[0]
    except IndexError:
        return 'unknown'

def extract_starting_lineup(game_data, team_side):
    lineup = {}
    team = game_data['liveData']['boxscore']['teams'][team_side]['players']
    
    for player_id, player_info in team.items():
        try:
            if 'battingOrder' in player_info and int(player_info['battingOrder']) % 100 == 0:
                order = int(player_info['battingOrder']) // 100
                mlbam_id = player_info['person']['id']
                lineup[order] = {
                    'name': player_info['person']['fullName'],
                    'mlbam_id': mlbam_id,
                    'bbref_id': get_bbref_id(mlbam_id)
                }
        except Exception as e:
            errors.append((gamepk, f"Error processing player {player_id} in {team_side} lineup: {str(e)}"))
    
    return [lineup.get(i, {'name': '', 'mlbam_id': '', 'bbref_id': ''}) for i in range(1, 10)]

def get_pitchers(game_data, team_side):
    team = game_data['liveData']['boxscore']['teams'][team_side]
    pitchers = []
    for idx, pitcher_id in enumerate(team['pitchers']):
        try:
            pitcher = team['players'][f'ID{pitcher_id}']
            mlbam_id = pitcher['person']['id']
            pitchers.append({
                'name': pitcher['person']['fullName'],
                'mlbam_id': mlbam_id,
                'bbref_id': get_bbref_id(mlbam_id),
                'order': idx + 1
            })
        except Exception as e:
            errors.append((gamepk, f"Error processing pitcher {pitcher_id} in {team_side} team: {str(e)}"))
    return pitchers

def get_bullpen(game_data, team_side):
    team = game_data['liveData']['boxscore']['teams'][team_side]
    bullpen = []
    for pitcher_id in team.get('bullpen', []):
        try:
            pitcher = team['players'][f'ID{pitcher_id}']
            mlbam_id = pitcher['person']['id']
            bullpen.append({
                'name': pitcher['person']['fullName'],
                'mlbam_id': mlbam_id,
                'bbref_id': get_bbref_id(mlbam_id)
            })
        except Exception as e:
            errors.append((gamepk, f"Error processing bullpen pitcher {pitcher_id} in {team_side} team: {str(e)}"))
    return bullpen

def create_game_dataframe(gamepk):
    game_data = get_game_data(gamepk)
    
    try:
        home_lineup = extract_starting_lineup(game_data, 'home')
        away_lineup = extract_starting_lineup(game_data, 'away')
        home_bullpen = get_bullpen(game_data, 'home')
        away_bullpen = get_bullpen(game_data, 'away')
        home_pitchers = get_pitchers(game_data, 'home')
        away_pitchers = get_pitchers(game_data, 'away')
    except Exception as e:
        errors.append((gamepk, f"Error extracting data: {str(e)}"))
        return pd.DataFrame()
    
    game_info = game_pks_df[game_pks_df['game_id'] == gamepk].iloc[0]
    
    game_record = {
        'game_id': gamepk,
        'game_date': game_info.get('game_date', 'unknown'),
        'runs_home': game_data['liveData']['linescore']['teams'].get('home', {}).get('runs', 0),
        'runs_away': game_data['liveData']['linescore']['teams'].get('away', {}).get('runs', 0),
        'home_id': game_info['home_id'],
        'home_name': game_info['home_name'],
        'away_id': game_info['away_id'],
        'away_name': game_info['away_name'],
        'home_oddshark_id': team_to_oddshark_id.get(game_info['home_id'], 'unknown'),
        'away_oddshark_id': team_to_oddshark_id.get(game_info['away_id'], 'unknown')
    }

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
    
    return pd.DataFrame([game_record])

def save_game_to_csv(gamepk):
    game_df = create_game_dataframe(gamepk)
    if not game_df.empty:
        game_df.to_csv(f'gamelogs/game_{gamepk}.csv', index=False)

def process_games(num_games=100):
    all_games = pd.read_csv('game_pks.csv').game_id
    gamepks = all_games.tail(num_games)

    count = 0
    for gamepk in gamepks:
        save_game_to_csv(gamepk)
        count += 1
        if count % 50 == 0:
            print(f"{count} games processed.")

    if errors:
        print("Errors encountered:")
        for error in errors:
            print(error)
    else:
        print("No errors encountered.")

    print("\n=================================================")
    print("Game logs updated.\nLocation: ~/gamelogs/game_{game_pk}.csv\nSUCCESS")
    print("=================================================")

# Call the function with the desired number of games
process_games(12000)
