import requests
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import os
from tqdm import tqdm

# --- CONFIG ---
season = datetime.now().year
schedule_cache = {}

os.makedirs("batters", exist_ok=True)
os.makedirs("pitchers", exist_ok=True)

batter_df = pd.read_csv("batter_ids.csv")
pitcher_df = pd.read_csv("pitcher_ids.csv")

# --- Schedule cache ---
def get_schedule(team_id):
    if team_id in schedule_cache:
        return schedule_cache[team_id]

    url = "https://statsapi.mlb.com/api/v1/schedule"
    params = {"teamId": team_id, "season": season, "sportId": 1}
    res = requests.get(url, params=params, timeout=10).json()
    sched = {}
    for date in res['dates']:
        for g in date['games']:
            gid = int(g['gamePk'])
            away = g['teams']['away']['team']['id']
            home = g['teams']['home']['team']['id']
            opp = home if away == team_id else away
            sched[gid] = {'Team': team_id, 'Opp': opp, 'away_id': away, 'home_id': home}
    schedule_cache[team_id] = sched
    return sched

# --- Doubleheader logic ---
def flag_doubleheaders(df):
    df['game_date'] = pd.to_datetime(df['game_date'])
    df = df.sort_values(['game_date', 'game_id'])
    dbl_counts = df.groupby('game_date').cumcount() + 1
    dbl_flags = df.groupby('game_date')['game_id'].transform('count')
    df['dbl'] = dbl_flags.where(dbl_flags > 1, None)
    df.loc[df['dbl'].notnull(), 'dbl'] = dbl_counts[df['dbl'].notnull()].astype(float)
    return df

# --- Field mappings ---
batter_col_map = {
    'atBats': 'AB', 'hits': 'H', 'runs': 'R', 'doubles': '2B', 'triples': '3B',
    'homeRuns': 'HR', 'rbi': 'RBI', 'baseOnBalls': 'BB', 'strikeOuts': 'SO',
    'stolenBases': 'SB', 'caughtStealing': 'CS', 'hitByPitch': 'HBP',
    'sacBunts': 'SH', 'sacFlies': 'SF', 'intentionalWalks': 'IBB',
    'totalBases': 'TB', 'avg': 'BA', 'obp': 'OBP', 'slg': 'SLG', 'ops': 'OPS',
    'plateAppearances': 'PA'
}

pitcher_map = {
    'inningsPitched': 'IP', 'hits': 'H', 'runs': 'R', 'earnedRuns': 'ER', 'baseOnBalls': 'BB',
    'strikeOuts': 'SO', 'homeRuns': 'HR', 'hitByPitch': 'HBP', 'era': 'ERA', 'fip': 'FIP',
    'battersFaced': 'BF', 'pitchesThrown': 'Pit', 'strikes': 'Str', 'strikesLooking': 'StL',
    'strikesSwinging': 'StS', 'groundOuts': 'GB', 'flyOuts': 'FB',
    'inheritedRunners': 'IR', 'inheritedRunnersScored': 'IS', 'stolenBases': 'SB',
    'caughtStealing': 'CS', 'pickoffs': 'PO', 'atBats': 'AB', 'doubles': '2B',
    'triples': '3B', 'intentionalWalks': 'IBB', 'groundIntoDoublePlay': 'GDP',
    'sacFlies': 'SF', 'reachedOnError': 'ROE'
}

# --- Shared log fetcher ---
def fetch_current_log(player_id, group, stat_map):
    url = f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats"
    params = {"stats": "gameLog", "group": group, "season": season}
    res = requests.get(url, params=params, timeout=10).json()
    if not res['stats'] or not res['stats'][0]['splits']:
        return None

    rows = []
    for game in res['stats'][0]['splits']:
        raw = game['stat']
        row = {stat_map[k]: v for k, v in raw.items() if k in stat_map}
        gid = int(game['game']['gamePk'])
        team_id = game['team']['id']
        row.update({
            'game_id': gid,
            'game_date': game['date'],
            'team_id': team_id,
            'opp_id': game['opponent']['id'],
        })

        sched = get_schedule(team_id)
        info = sched.get(gid, {})
        row.update({
            'Team': info.get('Team'),
            'Opp': info.get('Opp'),
            'away_id': info.get('away_id'),
            'home_id': info.get('home_id')
        })

        rows.append(row)

    df = pd.DataFrame(rows)
    if df.empty:
        return None

    df['game_id'] = df['game_id'].astype(int)
    df = flag_doubleheaders(df)
    return df

# --- Incremental update logic ---
def update_player(df_row, group, stat_map, folder, suffix):
    player_id = int(df_row['mlbID'])
    bbref_id = df_row['key_bbref']
    path = f"{folder}/{bbref_id}_{suffix}.csv"

    try:
        existing = pd.read_csv(path) if os.path.exists(path) else pd.DataFrame()
        existing_ids = set(existing['game_id'].astype(int)) if 'game_id' in existing else set()

        new_df = fetch_current_log(player_id, group, stat_map)
        if new_df is None:
            return None

        new_df = new_df[~new_df['game_id'].isin(existing_ids)]

        if not new_df.empty:
            combined = pd.concat([existing, new_df], ignore_index=True)
            combined['game_date'] = pd.to_datetime(combined['game_date'])
            combined = combined.sort_values(['game_date', 'game_id'])

            for col in set(existing.columns).union(new_df.columns):
                if col not in combined.columns:
                    combined[col] = None

            combined.to_csv(path, index=False)
        return None

    except Exception as e:
        return f"Error processing {bbref_id}: {e}"

# --- Thread Runners ---
def run_updates(df, label, group, stat_map, folder, suffix):
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {
            executor.submit(update_player, row, group, stat_map, folder, suffix): i
            for i, row in df.iterrows()
        }
        try:
            for f in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc=f"Updating {label}", unit=label):
                result = f.result()
                if result:
                    print(result)
        except KeyboardInterrupt:
            print(f"\nKeyboardInterrupt detected during {label}. Shutting down...")
            executor.shutdown(wait=False, cancel_futures=True)
            raise

# --- GO ---
print("\n---------- Now Running 5-playerstats.py -----------\n")

run_updates(batter_df, "batters", "hitting", batter_col_map, "batters", "batting")
run_updates(pitcher_df, "pitchers", "pitching", pitcher_map, "pitchers", "pitching")

print('\n')
