import os
import time
import subprocess
from datetime import datetime
import pytz
import sys
print(sys.executable)


# Define paths
INITIAL_SCRIPTS = ["1-game_pks.py", "2-player_ids.py", "3-gamelogs.py", "4-odds.py", "5-playerstats.py", "6-customstats.py", "7-currentdata.py", "8-scrape-odds.py", "9-predict.py"]
PERIODIC_SCRIPTS = ["3-gamelogs.py", "4-odds.py", "6-customstats.py", "7-currentdata.py", "8-scrape-odds.py", "9-predict.py"]
REPO_PATH = "/Users/natekessell/Desktop/development/MLB-Analytics"
STOP_FILE = "/Users/natekessell/Desktop/development/MLB-Analytics/stop_scheduler"
LOG_FILE = "/Users/natekessell/Desktop/development/MLB-Analytics/task_scheduler.log"
LOCAL_TZ = pytz.timezone("America/Los_Angeles")

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
        script_path = os.path.join(REPO_PATH, script)
        print(f"[{get_local_time()}] Running {script}...")
        log_message(f" Running {script}...")

        # Corrected subprocess call
        try:
            subprocess.run([
            "/Users/natekessell/Desktop/development/MLB-Analytics/mlb/bin/python",
            script_path
            ], check=True)
        except subprocess.CalledProcessError as e:
            log_message(f" [ERROR] Script {script} failed: {e}")

def git_commit_and_push():
    """Commits and pushes changes to GitHub."""
    print(f"[{get_local_time()}] Committing and pushing changes to GitHub...")
    log_message(f" [INFO] Committing and pushing changes to GitHub...")
    os.chdir(REPO_PATH)
    subprocess.run(["git", "add", "."], check=True)
    subprocess.run(["git", "commit", "-m", f"Auto-update {get_local_time()}"], check=True)
    subprocess.run(["git", "push"], check=True)

def main():
    """Main execution loop."""
    print(f"[INFO] Starting process at {get_local_time()}")
    log_message(f"[INFO] Starting process at {get_local_time()}")

    if should_stop():
        print("[INFO] Stop file detected. Exiting before execution.")
        log_message("[INFO] Stop file detected. Exiting before execution.")
        return

    run_scripts(INITIAL_SCRIPTS)

    while True:
        #STOP RUNNING PERIODIC SCRIPTS ONCE IT HITS 9 PM
        now = get_local_time()
        nine_pm = now.replace(hour=21, minute=0, second=0, microsecond=0)
        ten_pm = nine_pm.replace(hour=22)

        if should_stop() or now.hour >= 21:
            print(f"[INFO] Stopping execution at {get_local_time()}")
            log_message(f"[INFO] Stopping execution at {get_local_time()}")
            break

        run_scripts(PERIODIC_SCRIPTS)
        git_commit_and_push()

        print(f"[{get_local_time()}] Sleeping for 15 minutes")
        log_message(f" Sleeping for 15 minutes")
        time.sleep(900)  # 15 min sleep

if __name__ == "__main__":
    main()
