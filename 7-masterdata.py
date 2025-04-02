import os
import pandas as pd
import numpy as np
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import warnings

warnings.filterwarnings("ignore", message=".*DataFrame is highly fragmented.*")

gamelogs_dir = 'gamelogs/'
output_path = 'model/masterdata.parquet'
game_pks_df = pd.read_csv('game_pks.csv')
game_pks_list = game_pks_df['game_id'].tolist()

print("\n\nBUILDING MASTERDATA \n")

# --- SCHEMA COLLECTION ---
print("Scanning column schema...\n")
all_columns = set()
bad_files = []

def read_schema(game_pk):
    path = os.path.join(gamelogs_dir, f'gamestats_{game_pk}.csv')
    if os.path.exists(path):
        try:
            df = pd.read_csv(path, nrows=1)
            return set(df.columns), None
        except Exception as e:
            return set(), (path, str(e))
    return set(), None

with ThreadPoolExecutor(max_workers=8) as executor:
    futures = [executor.submit(read_schema, pk) for pk in game_pks_list]
    for f in tqdm(as_completed(futures), total=len(futures), desc="Schema scan", unit="file"):
        cols, err = f.result()
        all_columns.update(cols)
        if err:
            bad_files.append(err)

all_columns = sorted(all_columns)

# --- INGEST ROWS ---
print("\nReading game logs...\n")
row_dicts = []

def read_game_file(game_pk):
    path = os.path.join(gamelogs_dir, f'gamestats_{game_pk}.csv')
    if not os.path.exists(path):
        return [], None
    try:
        df = pd.read_csv(path)
        df = df.reindex(columns=all_columns, fill_value=pd.NA)
        return df.to_dict(orient='records'), None
    except Exception as e:
        return [], (path, str(e))

with ThreadPoolExecutor(max_workers=8) as executor:
    futures = [executor.submit(read_game_file, pk) for pk in game_pks_list]
    for f in tqdm(as_completed(futures), total=len(futures), desc="Building rows", unit="file"):
        rows, err = f.result()
        row_dicts.extend(rows)
        if err:
            bad_files.append(err)

df = pd.DataFrame(row_dicts, columns=all_columns)

# --- BULLPEN AVERAGING ---
print("\nAveraging bullpen stats (first 8 pitchers only)...\n")

bullpen_stats = [
    'IP_real_20', 'ERA', 'H_20', 'BF_20', 'HR_20', 'R_20', 'ER_20', 'BB_20', 'SO_20', 'XB_against_20',
    'TB_against_20', 'ERA_20', 'WHIP_20', 'IP_real_10', 'H_10', 'BF_10', 'HR_10', 'R_10', 'ER_10', 'BB_10',
    'SO_10', 'XB_against_10', 'TB_against_10', 'ERA_10', 'WHIP_10', 'IP_real_5', 'H_5', 'BF_5', 'HR_5', 'R_5',
    'ER_5', 'BB_5', 'SO_5', 'XB_against_5', 'TB_against_5', 'ERA_5', 'WHIP_5', 'IP_real_3', 'H_3', 'BF_3',
    'HR_3', 'R_3', 'ER_3', 'BB_3', 'SO_3', 'XB_against_3', 'TB_against_3', 'ERA_3', 'WHIP_3', 'BB', 'BF', 'ER',
    'H', 'HR', 'IP_real', 'R', 'SO', 'TB_against', 'WHIP', 'XB_against'
]

for team in ['Home', 'Away']:
    for stat in bullpen_stats:
        cols = [f'{team}_bullpen_{i}_{stat}' for i in range(1, 9) if f'{team}_bullpen_{i}_{stat}' in df.columns]
        if cols:
            df[cols] = df[cols].apply(pd.to_numeric, errors='coerce').replace([np.inf, -np.inf], np.nan)
            df[f'{team}_bullpen_avg_{stat}'] = df[cols].mean(axis=1)

# --- DROP INDIVIDUAL BULLPEN PITCHERS ---
print("\nDropping individual bullpen pitcher columns...\n")

for team in ['Home', 'Away']:
    for stat in bullpen_stats:
        drop_cols = [f'{team}_bullpen_{i}_{stat}' for i in range(1, 16) if f'{team}_bullpen_{i}_{stat}' in df.columns]
        df.drop(columns=drop_cols, inplace=True)

# --- SANITIZE, FILTER, ADD TARGET ---
print("\nSanitizing and filtering...\n")

df.replace('-.--', pd.NA, inplace=True)
df['game_date'] = pd.to_datetime(df['game_date'], errors='coerce')
df = df[df['game_date'].notna()]

df = df[
    ((df['game_date'].dt.month > 3) & (df['game_date'].dt.month < 10)) |
    ((df['game_date'].dt.month == 3) & (df['game_date'].dt.day >= 25)) |
    ((df['game_date'].dt.month == 10) & (df['game_date'].dt.day <= 5))
]

df['over_under_runline'] = pd.to_numeric(df['over_under_runline'], errors='coerce')
df = df[df['over_under_runline'].notna()]
df['over_under_target'] = (df['runs_total'] >= df['over_under_runline']).astype(int)

cols = df.columns.tolist()
if 'over_under_target' in cols and 'over_under_runline' in cols:
    tgt = cols.pop(cols.index('over_under_target'))
    idx = cols.index('over_under_runline') + 1
    cols.insert(idx, tgt)
    df = df[cols]

# --- TYPE NORMALIZATION ---
for col in df.columns:
    if df[col].dtype == 'object':
        try:
            df[col] = pd.to_numeric(df[col], errors='raise')
        except:
            df[col] = df[col].astype('string')

# --- SAVE TO PARQUET ---
print("\nSaving to Parquet...\n")
os.makedirs(os.path.dirname(output_path), exist_ok=True)
df.to_parquet(output_path, index=False, compression='snappy')
print(f"MASTERDATA BUILD COMPLETE — {df.shape[0]} rows, {df.shape[1]} columns — saved to {output_path}")


# --- OUTPUT FEATURE LIST ---
feature_list_path = 'model/masterdata_features.txt'
with open(feature_list_path, 'w') as f:
    for col in df.columns:
        f.write(col + '\n')

print(f"Feature list saved to {feature_list_path}")
