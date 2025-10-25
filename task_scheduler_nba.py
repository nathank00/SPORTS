import os
import subprocess
from datetime import datetime
import pytz
import sys

# Define paths and virtual environment
REPO_PATH = "/Users/njk/Development/SPORTS"
NBA_PATH = os.path.join(REPO_PATH, "NBA")
VENV_PYTHON = "/Users/njk/venvs/NBA/bin/python"
LOG_FILE = os.path.join(NBA_PATH, "nba_task_scheduler.log")
LOCAL_TZ = pytz.timezone("America/Los_Angeles")

# NBA scripts to run sequentially
NBA_SCRIPTS = [
    "1-games_players_ids.py",
    "2-gamelogs.py",
    "3-1-playergamelogs.py",
    "4-custom_playerstats.py",
    "5-customgamelogs.py",
    "8-daily.py"
]

def get_local_time():
    return datetime.now().astimezone(LOCAL_TZ)

def log_message(message):
    with open(LOG_FILE, "a") as f:
        f.write(f"[{get_local_time()}] {message}\n")

def run_scripts(scripts):
    for script in scripts:
        script_path = os.path.join(NBA_PATH, script)
        print(f"[{get_local_time()}] Running {script}...")
        log_message(f"Running {script}...")
        try:
            subprocess.run([VENV_PYTHON, script_path], check=True)
            print(f"[{get_local_time()}] {script} completed successfully.")
            log_message(f"{script} completed successfully.")
        except subprocess.CalledProcessError as e:
            print(f"[{get_local_time()}] [ERROR] {script} failed: {e}")
            log_message(f"[ERROR] {script} failed: {e}")

def git_commit_and_push():
    try:
        print(f"[{get_local_time()}] Committing and pushing changes to GitHub...")
        log_message("[INFO] Committing and pushing changes to GitHub...")
        os.chdir(REPO_PATH)
        subprocess.run(["git", "fetch", "origin"], check=True)
        subprocess.run(["git", "rebase", "--autostash", "origin/main"], check=True)
        subprocess.run(["git", "add", "."], check=True)
        subprocess.run(["git", "commit", "-m", f"NBA pipeline update {get_local_time()}"], check=True)
        subprocess.run(["git", "push", "origin", "nba-pipeline", "--force"], check=True)
        subprocess.run(["git", "checkout", "main"], check=True)
        subprocess.run(["git", "merge", "nba-pipeline"], check=True)
        subprocess.run(["git", "push", "origin", "main"], check=True)
        print(f"[{get_local_time()}] Git operations completed successfully.")
        log_message("[INFO] Git operations completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"[{get_local_time()}] [ERROR] Git operation failed: {e}")
        log_message(f"[ERROR] Git operation failed: {e}")

def main():
    print(f"[{get_local_time()}] Starting NBA pipeline at {get_local_time()}")
    log_message(f"[INFO] Starting NBA pipeline at {get_local_time()}")
    run_scripts(NBA_SCRIPTS)
    git_commit_and_push()
    print(f"[{get_local_time()}] NBA pipeline completed.")
    log_message("[INFO] NBA pipeline completed.")

if __name__ == "__main__":
    main()
