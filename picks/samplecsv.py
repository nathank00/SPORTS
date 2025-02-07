import pandas as pd
import os
from datetime import datetime
import subprocess

# Simulate your model output
data = [
    {"game_id": 12345, "home_team": "Yankees", "away_team": "Red Sox", "runline": 9.5, "pick": "Over"},
    {"game_id": 67890, "home_team": "Dodgers", "away_team": "Giants", "runline": 8.5, "pick": "Under"},
]

# Convert to DataFrame
df = pd.DataFrame(data)

# Generate filename for today's games
today = datetime.now().strftime("%Y-%m-%d")
filename = f"{today}.csv"

# Save CSV
df.to_csv(filename, index=False)
