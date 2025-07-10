import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from threading import Lock

print("\n---------- Now running 4-odds.py ----------\n")

duplicates = []
errors = []
lock = Lock()

def fetch_over_under_runline(oddshark_id, game_date):
    year = game_date.year
    url = f"https://www.oddsshark.com/stats/gamelog/baseball/mlb/{oddshark_id}?season={year}"

    try:
        tables = pd.read_html(url)
        df = tables[0]
    except Exception as e:
        return f"Error fetching for team {oddshark_id} on {game_date.date()}: {e}", None

    if df.empty:
        return f"No data for team {oddshark_id} on {game_date.date()}", None

    df['Date'] = pd.to_datetime(df['Date'], format='%b %d, %Y')
    matching_rows = df[df['Date'] == game_date]

    if len(matching_rows) > 1:
        return "", (oddshark_id, year, game_date)

    if matching_rows.empty:
        return f"No match found for team {oddshark_id} on {game_date.date()}", None

    return matching_rows.iloc[0]['Total'], None

def update_single_game(game_id, gamelogs_folder):
    try:
        gamelog_file = f'{gamelogs_folder}/game_{game_id}.csv'
        gamelog_df = pd.read_csv(gamelog_file)

        home_oddshark_id = gamelog_df.loc[0, 'home_oddshark_id']
        game_date_str = gamelog_df.loc[0, 'game_date']
        game_date = datetime.strptime(game_date_str, '%Y-%m-%d')

        result, dup = fetch_over_under_runline(home_oddshark_id, game_date)

        if dup:
            with lock:
                duplicates.append(dup)

        if isinstance(result, str) and result.startswith("Error"):
            with lock:
                errors.append(f"{result} (game {game_id})")
            return

        gamelog_df['over_under_runline'] = result
        gamelog_df.to_csv(gamelog_file, index=False)

    except Exception as e:
        with lock:
            errors.append(f"Error updating game {game_id}: {e}")

def update_gamelogs_with_over_under(game_pks_file, gamelogs_folder, num_games=50):
    game_pks_df = pd.read_csv(game_pks_file)
    game_ids = game_pks_df['game_id'].tail(num_games).tolist()

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(update_single_game, gid, gamelogs_folder) for gid in game_ids]
        for _ in tqdm(as_completed(futures), total=len(futures), desc="Updating odds", unit="games"):
            pass

    if duplicates:
        print("\nGames with duplicate dates (doubleheaders):")
        for dup in duplicates:
            print(f"Team Oddshark ID: {dup[0]}, Year: {dup[1]}, Date: {dup[2].strftime('%Y-%m-%d')}")

    if errors:
        print("\nErrors encountered:")
        for e in errors:
            print(e)
    else:
        print("No errors encountered.")

# Run
update_gamelogs_with_over_under('game_pks.csv', 'gamelogs', num_games=100)   #CHANGE TO RECENT GAMES
