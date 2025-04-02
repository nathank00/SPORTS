import pandas as pd
import time
import re
import os
import platform
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

print("---------- Now running 8-scrape-odds.py ----------")

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

url = "https://www.wagertalk.com/odds"
driver.get(url)
time.sleep(5)

soup = BeautifulSoup(driver.page_source, "lxml")
tables = pd.read_html(str(soup), flavor="lxml")
driver.quit()

if not tables:
    raise ValueError("No tables found on the webpage.")

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
    raise ValueError("No MLB sections (AL/NL/Interleague) found in the scraped data.")

mlb_odds_df.reset_index(drop=True, inplace=True)

def parse_date(time_str):
    if not isinstance(time_str, str) or len(time_str) < 5:
        return None
    date_part = time_str[:5]
    try:
        return datetime.strptime(date_part, "%m/%d").replace(year=2025).strftime("%Y-%m-%d")
    except ValueError:
        return None

if "Time" in mlb_odds_df.columns:
    mlb_odds_df["Date"] = mlb_odds_df["Time"].apply(parse_date)

def extract_runline(consensus_str):
    if not isinstance(consensus_str, str) or consensus_str.lower() == "unknown":
        return 0
    match = re.findall(r'\b(\d{1,2}½?)\s*(?=[ou]|\s|$)', consensus_str)
    if match:
        return float(match[0].replace("½", ".5"))
    return 0

if "Consensus" in mlb_odds_df.columns:
    mlb_odds_df["Runline"] = mlb_odds_df["Consensus"].apply(extract_runline)

team_name_mapping = {
    "NYY": "New York Yankees", "NYM": "New York Mets", "LAD": "Los Angeles Dodgers", "LAA": "Los Angeles Angels",
    "CHC": "Chicago Cubs", "CHW": "Chicago White Sox", "ATL": "Atlanta Braves", "ARI": "Arizona Diamondbacks",
    "BAL": "Baltimore Orioles", "BOS": "Boston Red Sox", "CIN": "Cincinnati Reds", "CLE": "Cleveland Guardians",
    "COL": "Colorado Rockies", "DET": "Detroit Tigers", "HOU": "Houston Astros", "KC": "Kansas City Royals",
    "MIL": "Milwaukee Brewers", "MIN": "Minnesota Twins", "OAK": "Athletics", "PHI": "Philadelphia Phillies",
    "PIT": "Pittsburgh Pirates", "SD": "San Diego Padres", "SF": "San Francisco Giants", "SEA": "Seattle Mariners",
    "STL": "St. Louis Cardinals", "TB": "Tampa Bay Rays", "TEX": "Texas Rangers", "TOR": "Toronto Blue Jays",
    "WAS": "Washington Nationals", "MIA": "Miami Marlins"
}

def extract_teams(teams_str):
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
            #print(f"[BAD PARSE] Raw: {teams_str} | T1: {team1} | T2: {team2}")
            return None, None
        #print(f"[PARSED] Raw: {teams_str} | T1: {team1} | T2: {team2}")
        return team_name_mapping[team1], team_name_mapping[team2]
    except Exception as e:
        #print(f"[ERROR] {teams_str} | {e}")
        return None, None

if "Teams" in mlb_odds_df.columns:
    mlb_odds_df[["Away Team", "Home Team"]] = mlb_odds_df["Teams"].apply(lambda x: pd.Series(extract_teams(x)))

input_path = "model/currentdata.parquet"
output_path = "model/currentdata.parquet"

if not os.path.exists(input_path):
    raise FileNotFoundError(f"File not found: {input_path}")

current_df = pd.read_parquet(input_path)

# Ensure runline column is float-compatible
if "over_under_runline" not in current_df.columns:
    current_df["over_under_runline"] = pd.NA
current_df["over_under_runline"] = pd.to_numeric(current_df["over_under_runline"], errors="coerce")

current_df["game_date"] = pd.to_datetime(current_df["game_date"]).dt.date
mlb_odds_df["Date"] = pd.to_datetime(mlb_odds_df["Date"]).dt.date

double_headers = {}

for index, row in mlb_odds_df.iterrows():
    matches = current_df[
        (current_df["game_date"] == row["Date"]) &
        (current_df["home_name"] == row["Home Team"]) &
        (current_df["away_name"] == row["Away Team"])
    ]
    if not matches.empty:
        if len(matches) == 1:
            current_df.at[matches.index[0], "over_under_runline"] = row["Runline"]
        else:
            key = (row["Date"], row["Home Team"], row["Away Team"])
            if key not in double_headers:
                double_headers[key] = list(matches.index)
            if double_headers[key]:
                match_index = double_headers[key].pop(0)
                current_df.at[match_index, "over_under_runline"] = row["Runline"]
    else:
        #print(f"[NO MATCH] {row['Away Team']} @ {row['Home Team']} on {row['Date']}")
        #print(current_df[current_df["game_date"] == row["Date"]][["home_name", "away_name", "game_date"]].head())
        continue



current_df.to_parquet(output_path, index=False)
print("\nSUCCESS - Live runlines merged into currentdata.parquet\n")
