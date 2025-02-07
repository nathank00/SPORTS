#### Environment Setup
* Set up a python virtual environment by running 'python -m venv environment-name'
* Activate virtual environment by running 'source environment-name/bin/activate'
* (deactivate at any time by running 'deactivate')
* Run 'pip install -r requirements.txt'  to install all necessary packages


# Manual Order of Operations

### 1-game_pks.py    (Frequency - once daily)    (Est. time: 1 min.)

The function 'get_game_pks()' fetches all game identifiers (game_pk), using the statsapi MLB API wrapper for games that exist within a predefined time range. 
* The default data timeframe for the entire system is 2021-today.

On the day that the file is executed, the file will fetch all games that are scheduled to occur that day, serving as the basis for building the machine learning model to predict the outcomes of these games that have not yet occurred. 

Output file: ~/game_pks.csv


### 2-player_ids.py    (Frequency - once daily)    (Est time: 1 min.)

The function 'get_player_ids()' fetches all player identifiers using several APIs and compiles the pitcher identifiers into a "pitchers" file and batters into a "batters" file. 
* The default data timeframe is 01-01-2021 to today.

Output file (pitchers): ~/pitcher_ids.csv
Output file (batters): ~/batter_ids.csv

### 2-player_ids.py    (Frequency - within 1 hour of each game)    (Est time: 3 min.)

The function 'process_games(num_games=100)' allows user to edit the number of recent games they want to generate new gamelogs for. This value defaults to 100 because this file is going to be run many times throughout the day in an attempt to provide quasi-real-time gamelog information so that the model can make real-time predictions for upcoming games. 
* The pybaseball.playerid_reverse_lookup function is used to fetch player id data to add to the gamelog data.

Ourput files: ~/gamelogs/game_{gamepk}.csv
