import requests
import pandas as pd
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import concurrent.futures
import os
from tqdm import tqdm

# --- CONFIG ---
current_year = datetime.now().year
years = list(range(2021, current_year + 1))
schedule_cache = {}

os.makedirs("batters", exist_ok=True)
os.makedirs("pitchers", exist_ok=True)

# --- Load IDs ---
batter_df = pd.read_csv("batter_ids.csv")
pitcher_df = pd.read_csv("pitcher_ids.csv")

# --- Schedule Cache ---
def get_schedule(team_id, season):
    key = (team_id, season)
    if key in schedule_cache:
        return schedule_cache[key]

    url = "https://statsapi.mlb.com/api/v1/schedule"
    params = {"teamId": team_id, "season": season, "sportId": 1}
    res = requests.get(url, params=params).json()
    sched = {}
    for date in res['dates']:
        for g in date['games']:
            gid = int(g['gamePk'])
            away = g['teams']['away']['team']['id']
            home = g['teams']['home']['team']['id']
            opp = home if away == team_id else away
            sched[gid] = {'Team': team_id, 'Opp': opp, 'away_id': away, 'home_id': home}
    schedule_cache[key] = sched
    return sched

# --- BATTER FIELDS ---
batter_col_map = {
    'atBats': 'AB', 'hits': 'H', 'runs': 'R', 'doubles': '2B', 'triples': '3B',
    'homeRuns': 'HR', 'rbi': 'RBI', 'baseOnBalls': 'BB', 'strikeOuts': 'SO',
    'stolenBases': 'SB', 'caughtStealing': 'CS', 'hitByPitch': 'HBP',
    'sacBunts': 'SH', 'sacFlies': 'SF', 'intentionalWalks': 'IBB',
    'totalBases': 'TB', 'avg': 'BA', 'obp': 'OBP', 'slg': 'SLG', 'ops': 'OPS',
    'plateAppearances': 'PA'
}

# --- PITCHER FIELDS ---
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

# --- Universal doubleheader logic ---
def flag_doubleheaders(df):
    df['game_date'] = pd.to_datetime(df['game_date'])
    df = df.sort_values(['game_date', 'game_id'])
    dbl_counts = df.groupby('game_date').cumcount() + 1
    dbl_flags = df.groupby('game_date')['game_id'].transform('count')
    df['dbl'] = dbl_flags.where(dbl_flags > 1, None)
    df.loc[df['dbl'].notnull(), 'dbl'] = dbl_counts[df['dbl'].notnull()].astype(float)
    return df

# --- Player fetchers ---
def fetch_log(player_id, group, stat_map):
    all_rows = []
    for season in years:
        url = f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats"
        params = {"stats": "gameLog", "group": group, "season": season}
        res = requests.get(url, params=params).json()
        if not res['stats'] or not res['stats'][0]['splits']:
            continue

        rows, team_ids = [], set()
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
                'season': season
            })
            rows.append(row)
            team_ids.add(team_id)

        df = pd.DataFrame(rows)
        df['game_id'] = df['game_id'].astype(int)

        for col in ['Team', 'Opp', 'away_id', 'home_id']:
            df[col] = None

        for tid in team_ids:
            sched = get_schedule(tid, season)
            for gid, row in df[df['team_id'] == tid].iterrows():
                match = sched.get(row['game_id'], {})
                df.at[gid, 'Team'] = match.get('Team')
                df.at[gid, 'Opp'] = match.get('Opp')
                df.at[gid, 'away_id'] = match.get('away_id')
                df.at[gid, 'home_id'] = match.get('home_id')

        df = flag_doubleheaders(df)
        all_rows.append(df)

    return pd.concat(all_rows) if all_rows else None

# --- Threaded runners ---
def process_batter(row):
    pid, bbref = int(row['mlbID']), row['key_bbref']
    try:
        df = fetch_log(pid, 'hitting', batter_col_map)
        if df is not None:
            ordered = ['game_date', 'game_id', 'Team', 'Opp', 'away_id', 'home_id'] + list(batter_col_map.values()) + ['dbl']
            for col in ordered:
                if col not in df: df[col] = None
            df = df[ordered]
            df.to_csv(f"batters/{bbref}_batting.csv", index=False)
        return None
    except Exception as e:
        return f"Error batter {bbref}: {e}"

def process_pitcher(row):
    pid, bbref = int(row['mlbID']), row['key_bbref']
    try:
        df = fetch_log(pid, 'pitching', pitcher_map)
        if df is not None:
            ordered = ['game_date', 'game_id', 'Team', 'Opp', 'away_id', 'home_id'] + list(pitcher_map.values()) + ['dbl']
            for col in ordered:
                if col not in df: df[col] = None
            df = df[ordered]
            df.to_csv(f"pitchers/{bbref}_pitching.csv", index=False)
        return None
    except Exception as e:
        return f"Error pitcher {bbref}: {e}"


# --- Fire Threads ---
def run_threads(df, processor, label):
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(processor, row): i for i, row in df.iterrows()}
        try:
            for f in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc=f"Processing {label}"):
                result = f.result()
                if result:
                    print(result)
        except KeyboardInterrupt:
            print("\nKeyboardInterrupt detected. Shutting down threads...")
            executor.shutdown(wait=False, cancel_futures=True)
            raise

# --- Execute both groups ---

print("\n---------- Now Running 5-ALLTIME-playerstats.py -----------\n")

run_threads(batter_df, process_batter, "batters")
run_threads(pitcher_df, process_pitcher, "pitchers")

print('\n')