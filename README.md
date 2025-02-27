#### Environment Setup
* Set up a python virtual environment by running 'python -m venv environment-name'
* Activate virtual environment by running 'source environment-name/bin/activate'
* (deactivate at any time by running 'deactivate')
* Run 'pip install -r requirements.txt'  to install all necessary packages

*** The use of a VPN (Virtual Private Network) located somewhere outside the Continental US is recommended, as the US appears to be a red-flagged zone for repetitive API queries ***


# SCRIPTS OVERVIEW

### 1-game_pks.py    (Frequency - once daily)    (Est. time: 2 min.)

The function 'get_game_pks()' fetches all game identifiers (game_pk), using the statsapi MLB API wrapper for games that exist within a predefined time range. 
* The default data timeframe for the entire system is 2021-today.

On the day that the file is executed, the file will fetch all games that are scheduled to occur that day, serving as the basis for building the machine learning model to predict the outcomes of these games that have not yet occurred. 

Output file: ~/game_pks.csv


### 2-player_ids.py    (Frequency - once daily)    (Est time: 1 min.)

The function 'get_player_ids()' fetches all player identifiers using several APIs and compiles the pitcher identifiers into a "pitchers" file and batters into a "batters" file. 
* The default data timeframe is 01-01-2021 to today.

Output file (pitchers): ~/pitcher_ids.csv
Output file (batters): ~/batter_ids.csv


### 3-gamelogs.py    (Frequency - within 1 hour of each game)    (Est time: 3 min.)

The function 'process_games(num_games=100)' allows user to edit the number of recent games they want to generate new gamelogs for. This value defaults to 100 because this file is going to be run many times throughout the day in an attempt to provide quasi-real-time gamelog information so that the model can make real-time predictions for upcoming games. 
* The pybaseball.playerid_reverse_lookup function is used to fetch player id data which we add to the relevant gamelog file.

Output files: ~/gamelogs/game_{gamepk}.csv


### 4-odds.py    (Frequency - within 1 hour of each game)    (Est time: 5 min.)

The function 'update_gamelogs_with_over_under(game_pks_file, gamelogs_folder, num_games=50) fetches the over/under runline for the number of specified recent games in the current game_pks.csv file. This function obtains odds data from oddshark and then appends the existing gamelogs file for that game with the runline as a new column.

Output files: ~/gamelogs/game_{gamepk}.csv


### 5-ALLTIME-playerstats.py    (ONCE AND NEVER AGAIN)    (Est time: 8 hours)

The functions 'fetch_p_game_log' and 'fetch_p_game_log' use the statsapi.schedule package from MLB to fetch the relevant game-by-game data for pitchers and batters, respectively. The script proceeds to *generate* (if no data exists), or *concatenate* (if current data exists) the gamelogs file for each pitcher/batter, in which their entire historical data for pitching/batting will be located. 
- There is no need to run this file more than one time to obtain all players' historical datasets - to continually fetch new data on a day-by-day basis, use the file:
* '5-playerstats.py'

To edit the start/end dates of the data that this script will fetch for all players:
- edit on line 167 (batting)
- edit on line 293 (pitching)

Output files: ~/pitchers/{playerid}_pitching.csv
and ~/batters/{playerid}_batting.csv


### 5-playerstats.py    (Frequency - Daily)    (Est time: 30 min.)

The functions 'fetch_p_game_log' and 'fetch_p_game_log' use the statsapi.schedule package from MLB to fetch the relevant game-by-game data for ACTIVE pitchers and batters, respectively.
* This script is looping through the 'active_batter_ids.csv' and 'active_pitcher_ids.csv' lists because only active players will have new data.
 The script proceeds to *generate* (if no data exists), or *concatenate* (if current data exists) the gamelogs file for each pitcher/batter, in which their entire historical data for pitching/batting will be located. 

 NOTE: line 143 there is the ability to define what an "active" pitcher/batter is (the default definition is that the player played within the last 3 days).

Output files: ~/pitchers/{playerid}_pitching.csv
and ~/batters/{playerid}_batting.csv


### 6-customstats.py  (Frequency - within 1 hour of each game) (Est. time: 5 min.)

the function 'process_recent_games({num_recent_games})' takes the input of "num_recent_games', which specifies the number of recent games to process. The function then sequentially, game by game, fetches the stats files for each of the players that exist in a game's "gamelogs/' file. The function then processes all of these files, which includes:
* generating custom stats (batting average over last 10 games, 5 games, etc.)
* pushing this new, custom dataset for each player to:
    * '~/batters/{playerid}_stats_batting.csv'
    * '~/pitchers/{playerid}_stats_pitching.csv'

* creating a new gamelogs file for each game:
    * '~/gamelogs/gamestats_{gamepk}.csv'

These new gamelogs files with custom stats for all the players will be used to generate the master dataset used for model training/testing.

### 7-masterdata.py   (Frequency - whenever you want to have current data to train a new model)  (Est. time: 3 hours.)

This script builds the entire dataset from 2021-now, which we use to train/tune the model. Loops through the game_pks.csv list and concatenates all the 'gamestats_{gamepk}.csv' files into a single file called 'unsorted_masterdata.csv'. The script then organizes the columns in a sensible structure and removes all games that meet any of these criteria:
- Any empty data
- Preseason games (before April of any year)
- Postseason games (after October 5 of any year)

Output file: ~/model/masterdata.csv


### 7-currentdata.py   (Frequency - within 1 hour of each game)  (Est. time: 1 min.)

This script is functionally the same as the '7-masterdata.py' script, but this script only generates a dataset for the last 100 games, making it much more efficient to run on a game-by-game basis. This dataset is used to feed into the model to make live predictions, and is also used to push game predictions and game lineup data to the web app.

Output file: ~/model/currentdata.csv

### 8-scrape-odds.py    (Frequency - within 1 hour of each game)  (Est time: 3 min.)

This script scrapes the wagertalk.com site for live runline and odds data for the current day's games. Then, the script appends this runline to the appropriate game in the currentdata.csv dataset. 
- This ensures that each game has a runline that is accurate in real-time before making a prediction for that game. 
- This script also allows the user to avoid having to manually input runline data.

Updated file: ~/model/currentdata.csv

### 9-predict.py   (Frequency - within 1 hour of each game)  (Est. time: 2 min.)

This script uses the model located at '~/model/xgb_model.pkl' to make the predictions on all of today's games (data fetched from the currentdata.csv dataset) and then updates the currentdata.csv file with the prediction (0 = under, 1 = over) in the 'over_under_target' column. The script then generates a new file that serves the web app, where this live data is rendered for the end user.

Output file: ~/mlb-app/src/app/api/picks/{today's date}.csv


### model.py  (Frequency - to train a new model)  (Est. time: 3 hours)

This script uses xgboost to build a binary classifier for predicting an MLB game's runline result of "Over" or "Under" based on individual player stats and the current runline obtained from a betting sportsbook. 

Output file: ~/model/xgb_model.pkl

# BUILD DATASET + MODEL FROM SCRATCH - Order of Operations

1-game_pks.py  -  fetches all game pks through current date(today)
2-player_ids.py  -  fetches all batter and pitcher ids that have played at any time from 2021-now
3-gamelogs.py  -  builds basic gamelogs
* REMOVE THE .tail(50) for initial build of the entire dataset
4-odds.py - fetches and adds runline to each gamelog file
* CHANGE num_games=50 to num_games=15000 for initial build of the entire dataset
5-ALLTIME-playerstats.py - builds all individual player (pitcher+batter) historical stats datasets
6-customstats.py - generates custom player stats and adds them to the gamelog files
* line 5 - change 'active_batter_ids.csv' to 'batter_ids.csv'
* line 205 - change 'active_pitcher_ids.csv' to 'pitcher_ids.csv'
* line 399 - change num_recent_games = 50 to num_recent_games = 15000
7-masterdata.py - generates master dataset with over 12,000 games since 2021
model.py - creates a new model using the masterdata.csv file



# DAILY - Order of Operations

1-game_pks.py
2-player_ids.py
3-gamelogs.py
4-odds.py
5-playerstats.py
6-customstats.py
7-currentdata.py
8-scrape-odds.py
9-predict.py



# GAME-BY-GAME - Order of Operations

3-gamelogs.py
4-odds.py
6-customstats.py
7-currentdata.py
8-scrape-odds.py
9-predict.py
