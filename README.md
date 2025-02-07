#### Environment Setup
* Set up a python virtual environment by running 'python -m venv <environment name>'
* Activate virtual environment by running 'source <environment name>/bin/activate'
* (deactivate at any time by running 'deactivate')
* Run 'pip install -r requirements.txt'  to install all necessary packages


## Manual Order of Operations

### 1-game_pks.py   (Frequency - once daily)   

The function 'get_game_pks()' fetches all game identifiers (game_pk), using the statsapi MLB API wrapper for games that exist within a predefined time range. 
* The default data timeframe for the entire system is 2021-today.

On the day that the file is executed, the file will fetch all games that are scheduled to occur that day, serving as the basis for building the machine learning model to predict the outcomes of these games that have not yet occurred. 
