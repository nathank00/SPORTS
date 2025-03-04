import os
import time
import subprocess
from datetime import datetime
import pytz

INITIAL_SCRIPTS = ["1-game_pks.py", "2-player_ids.py", "3-gamelogs.py", "4-odds.py", "5-playerstats.py", "6-customstats.py", "7-currentdata.py", "8-scrape-odds.py", "9-predict.py"]
PERIODIC_SCRIPTS = ["3-gamelogs.py", "4-odds.py", "6-customstats.py", "7-currentdata.py", "8-scrape-odds.py", "9-predict.py"]
REPO_PATH = "/Users/natekessell/Desktop/development/MLB-Analytics"
VENV_PATH = "/Users/natekessell/Desktop/development/MLB-Analytics/mlb/bin/activate"
STOP_FILE = "/Users/natekessell/Desktop/development/MLB-Analytics/stop_scheduler"
LOG_FILE = "/Users/natekessell/Desktop/development/MLB-Analytics/task_scheduler.log"
LOCAL_TZ = pytz.timezone("America/Los_Angeles")  # Pacific time zone

def get_local_time():
    return datetime.now().astimezone(LOCAL_TZ)

def log_message(message):
    """Logs messages to a file with timestamps."""
    with open(LOG_FILE, "a") as f:
        f.write(f"[{get_local_time()}] {message}\n")

def should_stop():
    """Check if the stop file exists."""
    return os.path.exists(STOP_FILE)

def run_scripts(scripts):
    """Executes a sequential list of scripts."""
    for script in scripts:
        print(f"[{get_local_time()}] Running {script}...")
        log_message(f" Running {script}...")
        subprocess.run(f"source {VENV_PATH} && python {script}", shell=True, executable="/bin/zsh", check=True)


def git_commit_and_push():
    """Commits and pushed changes to GitHub to update web app."""
    print(f"[{get_local_time()}] Committing and pushing changes to Github...")
    log_message(f" [INFO] Committing and pushing changes to Github...")
    os.chdir(REPO_PATH)
    subprocess.run(["git", "add", "."], check=True)
    subprocess.run(["git", "commit", "-m", f"Auto-update {get_local_time()}"], check=True)
    subprocess.run(["git", "push"], check=True)


def main():
    """Main execution loop."""
    print(f"[INFO] Starting process at {get_local_time()}")
    log_message(f"[INFO] Starting process at {get_local_time()}")
    # Run initial scripts at 6:30 am

    if should_stop():
        print("[INFO] Stop file detected. Exiting before execution.")
        log_message("[INFO] Stop file detected. Exiting before execution.")
        return

    run_scripts(INITIAL_SCRIPTS)

    # Loop to execute periodic scripts every 30 min until midnight
    while True:
        now = get_local_time()

        if now.hour == 0 or should_stop(): 
            print(f"[INFO] Stopping execution at {get_local_time()}")
            log_message(f"[INFO] Stopping execution at {get_local_time()}")
            break

        run_scripts(PERIODIC_SCRIPTS)
        git_commit_and_push()

        print(f"[{get_local_time()}] Sleeping for 30 minutes")
        log_message(f" Sleeping for 30 minutes")
        time.sleep(1800)  # 30 min sleep


if __name__ == "__main__":
    main()