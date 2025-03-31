import pandas as pd
import numpy as np
import os

print("\n---------- Now running 6-customstats.py ----------\n")

# --- UTILS ---
def clean_numeric(value):
    try:
        value = str(value).replace('\xa0', '').replace('(', '').replace(')', '').replace(',', '')
        return float(value)
    except ValueError:
        return np.nan

def convert_ip_to_real(ip):
    if pd.isna(ip): return np.nan
    ip_str = str(ip)
    if '.' in ip_str:
        whole, fraction = map(int, ip_str.split('.'))
        return whole + (1/3 if fraction == 1 else 2/3 if fraction == 2 else 0)
    return float(ip)

# --- BATTER ROLLING CALC ---
def batter_rolling(df, window, suffix):
    roll = df.rolling(window=window, min_periods=1).sum()
    roll['AVG'] = roll['H'] / roll['AB']
    roll['OBP'] = (roll['H'] + roll['BB'] + roll['HBP']) / (roll['AB'] + roll['BB'] + roll['HBP'] + roll['SF'])
    roll['SLG'] = (roll['H'] + roll['2B'] + 2*roll['3B'] + 3*roll['HR']) / roll['AB']
    roll['OPS'] = roll['OBP'] + roll['SLG']
    roll['XB'] = roll['2B'] + roll['3B'] + roll['HR']
    roll['TB'] = roll['H'] + roll['2B'] + 2*roll['3B'] + 3*roll['HR']
    out = roll[['AVG','OBP','SLG','OPS','SB','CS','XB','TB','SO']]
    return out.add_suffix(f"_{suffix}").round(3)

# --- BATTER STATS ---
print("\n----- BATTER PROCESSING -----\n")
batter_ids = pd.read_csv('active_batter_ids.csv').key_bbref

for bbref_id in batter_ids:
    if not bbref_id or pd.isna(bbref_id): continue
    path = f'batters/{bbref_id}_batting.csv'
    if not os.path.exists(path): continue
    try: df = pd.read_csv(path)
    except: continue

    df['game_date'] = pd.to_datetime(df['game_date'])
    df['season'] = df['game_date'].dt.year

    numeric = ['PA','AB','R','H','2B','3B','HR','RBI','SB','CS','BB','SO','HBP','SH','SF','IBB','TB']
    for col in numeric:
        df[col] = df.get(col, 0).apply(clean_numeric).fillna(0).astype(float)

    # Rolling stats
    rolling = pd.concat([
        batter_rolling(df[numeric], 20, '20').shift(1).fillna(0),
        batter_rolling(df[numeric], 10, '10').shift(1).fillna(0),
        batter_rolling(df[numeric], 5,  '5').shift(1).fillna(0),
        batter_rolling(df[numeric], 3,  '3').shift(1).fillna(0),
    ], axis=1)

    # Season-to-date
    seasonal = []
    for y in df['season'].unique():
        partial = df[df['season'] == y][numeric].cumsum().shift(1).fillna(0)
        partial['AVG'] = partial['H'] / partial['AB']
        partial['OBP'] = (partial['H'] + partial['BB'] + partial['HBP']) / (partial['AB'] + partial['BB'] + partial['HBP'] + partial['SF'])
        partial['SLG'] = (partial['H'] + partial['2B'] + 2*partial['3B'] + 3*partial['HR']) / partial['AB']
        partial['OPS'] = partial['OBP'] + partial['SLG']
        partial['XB'] = partial['2B'] + partial['3B'] + partial['HR']
        partial['TB'] = partial['H'] + partial['2B'] + 2*partial['3B'] + 3*partial['HR']
        seasonal.append(partial[['AVG','OBP','SLG','OPS','SB','CS','XB','TB','SO']].add_suffix("_current"))
    seasonal_df = pd.concat(seasonal).reindex(df.index)

    final = pd.concat([df, rolling, seasonal_df], axis=1).round(3)
    final.to_csv(f'batters/{bbref_id}_stats_batting.csv', index=False)
    print(f"Batter {bbref_id} stats saved.")

# --- PITCHER ROLLING CALC ---
def pitcher_rolling(df, window, suffix):
    roll = df.rolling(window=window, min_periods=1).sum()
    roll['ERA'] = (roll['ER'] * 9) / roll['IP_real']
    roll['WHIP'] = (roll['H'] + roll['BB']) / roll['IP_real']
    roll['XB_against'] = roll['2B'] + roll['3B'] + roll['HR']
    roll['TB_against'] = roll['H'] + roll['2B'] + 2*roll['3B'] + 3*roll['HR']
    out = roll[['IP_real','H','BF','HR','R','ER','BB','SO','XB_against','TB_against','ERA','WHIP']]
    return out.add_suffix(f"_{suffix}").round(3)

# --- PITCHER STATS ---
print("\n----- PITCHER PROCESSING -----\n")
pitcher_ids = pd.read_csv('active_pitcher_ids.csv').key_bbref

for bbref_id in pitcher_ids:
    if not bbref_id or pd.isna(bbref_id): continue
    path = f'pitchers/{bbref_id}_pitching.csv'
    if not os.path.exists(path): continue
    try: df = pd.read_csv(path)
    except: continue

    df['game_date'] = pd.to_datetime(df['game_date'])
    df['season'] = df['game_date'].dt.year

    numeric = ['IP','H','R','ER','BB','SO','HR','BF','2B','3B','IBB']
    for col in numeric:
        df[col] = df.get(col, 0).apply(clean_numeric).fillna(0).astype(float)
    df['IP_real'] = df['IP'].apply(convert_ip_to_real)

    # Rolling stats
    rolling = pd.concat([
        pitcher_rolling(df[numeric + ['IP_real']], 20, '20').shift(1).fillna(0),
        pitcher_rolling(df[numeric + ['IP_real']], 10, '10').shift(1).fillna(0),
        pitcher_rolling(df[numeric + ['IP_real']], 5,  '5').shift(1).fillna(0),
        pitcher_rolling(df[numeric + ['IP_real']], 3,  '3').shift(1).fillna(0),
    ], axis=1)

    # Season-to-date
    seasonal = []
    for y in df['season'].unique():
        part = df[df['season'] == y][numeric + ['IP_real']].cumsum().shift(1).fillna(0)
        part['ERA'] = (part['ER'] * 9) / part['IP_real']
        part['WHIP'] = (part['H'] + part['BB']) / part['IP_real']
        part['XB_against'] = part['2B'] + part['3B'] + part['HR']
        part['TB_against'] = part['H'] + part['2B'] + 2*part['3B'] + 3*part['HR']
        seasonal.append(part[['IP_real','H','BF','HR','R','ER','BB','SO','XB_against','TB_against','ERA','WHIP']].add_suffix("_current"))
    seasonal_df = pd.concat(seasonal).reindex(df.index)

    final = pd.concat([df, rolling, seasonal_df], axis=1).round(3)
    final.to_csv(f'pitchers/{bbref_id}_stats_pitching.csv', index=False)
    print(f"Pitcher {bbref_id} stats saved.")


# --- [PART 2] Enrich game logs with latest player stats ---


def get_player_stats(bbref_id, player_type, game_id):
    stats_dir = 'batters' if player_type == 'batting' else 'pitchers'
    stats_file = os.path.join(stats_dir, f'{bbref_id}_stats_{player_type}.csv')
    if not os.path.exists(stats_file):
        print(f"Stats file for {bbref_id} not found ({player_type}).")
        return None
    stats_df = pd.read_csv(stats_file)
    if 'game_id' not in stats_df:
        return None
    stats_df['game_id'] = stats_df['game_id'].astype(int)
    game_stats = stats_df[stats_df['game_id'] == game_id]
    return game_stats.iloc[0] if not game_stats.empty else stats_df.iloc[-1]

def process_game(game_id):
    game_file = f'gamelogs/game_{game_id}.csv'
    if not os.path.exists(game_file):
        print(f"Gamelog file for game {game_id} not found.")
        return
    
    game_df = pd.read_csv(game_file)
    game_data = game_df.iloc[0].to_dict()

    batter_cols = [f"{stat}_{window}" for window in [20, 10, 5, 3] for stat in ['AVG','OBP','SLG','OPS','SB','CS','XB','TB','SO']]
    pitcher_cols = [f"{stat}_{window}" for window in [20, 10, 5, 3] for stat in ['IP_real','H','BF','HR','R','ER','BB','SO','XB_against','TB_against','ERA','WHIP']]

    # Batter stats
    for i in range(1, 10):
        for team in ['Away', 'Home']:
            bbref_id = game_data.get(f'{team}_Batter{i}_bbrefID')
            if bbref_id:
                stats = get_player_stats(bbref_id, 'batting', game_id)
                if stats is not None:
                    for col in batter_cols:
                        game_data[f'{team}_Batter{i}_{col}'] = stats.get(col, '')

    # Pitcher stats
    for team in ['Away', 'Home']:
        for i in range(1, 11):
            role = 'SP' if i == 1 else f'P_{i}'
            bbref_id = game_data.get(f'{team}_{role}_bbrefID')
            if bbref_id:
                stats = get_player_stats(bbref_id, 'pitching', game_id)
                if stats is not None:
                    for col in pitcher_cols:
                        game_data[f'{team}_{role}_{col}'] = stats.get(col, '')

    # Bullpen stats
    for team in ['Away', 'Home']:
        for i in range(1, 15):
            role = f'bullpen_{i}'
            bbref_id = game_data.get(f'{team}_{role}_bbrefID')
            if bbref_id:
                stats = get_player_stats(bbref_id, 'pitching', game_id)
                if stats is not None:
                    for col in pitcher_cols:
                        game_data[f'{team}_{role}_{col}'] = stats.get(col, '')

    # Save enriched game
    output = pd.DataFrame([game_data])
    output_file = f'gamelogs/gamestats_{game_id}.csv'
    output.to_csv(output_file, index=False)
    print(f"Processed and saved game stats for game {game_id} -> {output_file}")

def process_recent_games(n=50):
    game_pks_file = 'game_pks.csv'
    if not os.path.exists(game_pks_file):
        print(f"{game_pks_file} not found.")
        return

    game_ids = pd.read_csv(game_pks_file).tail(n)['game_id'].astype(int).tolist()
    for gid in game_ids:
        process_game(gid)

# --- CHANGE THIS TO CONTROL HOW MANY GAMES TO PROCESS ---
process_recent_games(50)
