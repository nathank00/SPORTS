import pandas as pd
import numpy as np
from datetime import datetime

def clean_input_data(df: pd.DataFrame):
    df = df.copy()
    df = df.tail(len(df) - 255) if len(df) > 255 else df
    df['game_date'] = pd.to_datetime(df['game_date'])

    drop_cols = [col for col in df.columns if 'Name' in col or 'ID' in col or 'bbrefID' in col or '_P_' in col or 'id' in col]
    df.drop(columns=drop_cols, inplace=True, errors='ignore')

    # Drop rows only if those columns are present
    required_cols = [col for col in ['over_under_runline', 'over_under_target'] if col in df.columns]
    df.dropna(subset=required_cols, inplace=True)

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
            drop_cols = [f'{team}_bullpen_{i}_{stat}' for i in range(1, 16) if f'{team}_bullpen_{i}_{stat}' in df.columns]
            df.drop(columns=drop_cols, inplace=True, errors='ignore')

    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    # Always return X and y (None if not present)
    y = df['over_under_target'] if 'over_under_target' in df.columns else None
    X = df.drop(columns=['over_under_target', 'runs_total', 'game_date', 'runs_home', 'runs_away', 'game_id', 'home_name', 'away_name'], errors='ignore')
    return X, y
