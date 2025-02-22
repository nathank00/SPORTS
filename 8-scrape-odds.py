import pandas as pd
import time
import re
import os
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

### Setup Selenium WebDriver ###
options = Options()
options.add_argument("--headless")  # Run in headless mode
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.binary_location = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"  # Update path if needed

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)

### Load the odds page ###
url = "https://www.wagertalk.com/odds"
driver.get(url)

# Wait for JavaScript to load content
time.sleep(5)  # Adjust if needed

# Parse the page source using BeautifulSoup
soup = BeautifulSoup(driver.page_source, "lxml")

# Extract tables using pandas with "lxml" parser
tables = pd.read_html(str(soup), flavor="lxml")

# Close Selenium session
driver.quit()

# Ensure at least one table was found
if not tables:
    raise ValueError("No tables found on the webpage.")

# Select the correct table (only one found, so index 0)
df = tables[0]

# Reset index for easier processing
df.reset_index(drop=True, inplace=True)

### Find the start & end indexes for MLB games ###
start_index = None
for i, row in df.iterrows():
    if "[-]" in str(row.values) and "MLB" in str(row.values):
        start_index = i
        break

end_index = None
if start_index is not None:
    for i in range(start_index + 1, len(df)):
        if "[-]" in str(df.iloc[i].values):  # Next sport found
            end_index = i
            break

# Slice the dataframe to only include MLB odds
if start_index is not None and end_index is not None:
    mlb_odds_df = df.iloc[start_index:end_index]
elif start_index is not None:  # If MLB is the last sport, take all remaining rows
    mlb_odds_df = df.iloc[start_index:]
else:
    raise ValueError("MLB section not found in the scraped data.")

# Reset index for clean output
mlb_odds_df.reset_index(drop=True, inplace=True)

### Extract and Format Date from "Time" Column ###
def parse_date(time_str):
    if not isinstance(time_str, str) or len(time_str) < 5:
        return None
    
    date_part = time_str[:5]  # Extract MM/DD

    try:
        formatted_date = datetime.strptime(date_part, "%m/%d").replace(year=2025)  # Assume year 2025
        return formatted_date.strftime("%Y-%m-%d")
    except ValueError:
        return None

if "Time" in mlb_odds_df.columns:
    mlb_odds_df["Date"] = mlb_odds_df["Time"].apply(parse_date)

### Extract Runline from "Consensus" Column ###
def extract_runline(consensus_str):
    if not isinstance(consensus_str, str) or consensus_str.lower() == "unknown":
        return 0  # Default to 0 if "unknown" or not a string
    
    # Regex: Find a 1 or 2-digit number (optionally with ½) that comes BEFORE any "o" or "u" (or nothing)
    match = re.findall(r'\b(\d{1,2}½?)\s*(?=[ou]|\s|$)', consensus_str)

    if match:
        runline_str = match[0]  # Take the FIRST valid match (ignoring betting odds and anything after "o/u")
        runline = float(runline_str.replace("½", ".5"))  # Convert ½ to .5
        return runline
    
    return 0  # Default to 0 if no match is found

if "Consensus" in mlb_odds_df.columns:
    mlb_odds_df["Runline"] = mlb_odds_df["Consensus"].apply(extract_runline)

### Extract and Normalize Team Names ###
team_name_mapping = {
    "NY Yankees": "New York Yankees", "NY Mets": "New York Mets",
    "LA Dodgers": "Los Angeles Dodgers", "LA Angels": "Los Angeles Angels",
    "Chi. Cubs": "Chicago Cubs", "Chi. White Sox": "Chicago White Sox",
    "Atlanta": "Atlanta Braves", "Arizona": "Arizona Diamondbacks",
    "Baltimore": "Baltimore Orioles", "Boston": "Boston Red Sox",
    "Cincinnati": "Cincinnati Reds", "Cleveland": "Cleveland Guardians",
    "Colorado": "Colorado Rockies", "Detroit": "Detroit Tigers",
    "Houston": "Houston Astros", "Kansas City": "Kansas City Royals",
    "Milwaukee": "Milwaukee Brewers", "Minnesota": "Minnesota Twins",
    "Oakland": "Oakland Athletics", "Philadelphia": "Philadelphia Phillies",
    "Pittsburgh": "Pittsburgh Pirates", "San Diego": "San Diego Padres",
    "San Francisco": "San Francisco Giants", "Seattle": "Seattle Mariners",
    "St. Louis": "St. Louis Cardinals", "Tampa Bay": "Tampa Bay Rays",
    "Texas": "Texas Rangers", "Toronto": "Toronto Blue Jays",
    "Washington": "Washington Nationals", "Miami": "Miami Marlins"
}

def extract_teams(teams_str):
    if not isinstance(teams_str, str):
        return None, None

    home_team = None
    for key in team_name_mapping.keys():
        if teams_str.endswith(key):
            home_team = key
            break

    if home_team:
        away_team = teams_str.replace(home_team, "").strip()
        away_team = team_name_mapping.get(away_team, away_team)
        home_team = team_name_mapping[home_team]
        return away_team, home_team
    else:
        return None, None

if "Teams" in mlb_odds_df.columns:
    mlb_odds_df[["Away Team", "Home Team"]] = mlb_odds_df["Teams"].apply(
        lambda x: pd.Series(extract_teams(x))
    )

### Load and Merge with currentdata.csv ###
current_data_path = "model/currentdata.csv"

if not os.path.exists(current_data_path):
    raise FileNotFoundError(f"File not found: {current_data_path}")

current_df = pd.read_csv(current_data_path)

# Convert date columns to datetime format for proper matching
current_df["game_date"] = pd.to_datetime(current_df["game_date"])
mlb_odds_df["Date"] = pd.to_datetime(mlb_odds_df["Date"])

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
            if (row["Date"], row["Home Team"], row["Away Team"]) not in double_headers:
                double_headers[(row["Date"], row["Home Team"], row["Away Team"])] = list(matches.index)

            if double_headers[(row["Date"], row["Home Team"], row["Away Team"])]:
                match_index = double_headers[(row["Date"], row["Home Team"], row["Away Team"])].pop(0)
                current_df.at[match_index, "over_under_runline"] = row["Runline"]

current_df.to_csv(current_data_path, index=False)

print("Live runlines successfully merged into currentdata.csv!")
