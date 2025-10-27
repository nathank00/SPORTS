# NBA/task_scheduler_nba.py
import os
import subprocess
from datetime import datetime
import pytz

# Define paths relative to NBA/ folder
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))  # NBA/
REPO_PATH = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))  # SPORTS/
VENV_PYTHON = os.path.join(REPO_PATH, "env-sports/bin/python")  # ../env-sports/bin/python
LOG_DIR = os.path.join(SCRIPT_DIR, "logs")  # NBA/logs
LOG_FILE = os.path.join(LOG_DIR, "nba_task_scheduler.log")
APP_DATA_DIR = os.path.join(REPO_PATH, "mlb-app/public/data")  # ../mlb-app/public/data
LOCAL_TZ = pytz.timezone("America/Los_Angeles")

# Ensure directories exist
def ensure_directories():
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        os.makedirs(APP_DATA_DIR, exist_ok=True)
        print(f"Ensured directories: {os.path.abspath(LOG_DIR)}, {os.path.abspath(APP_DATA_DIR)}")
    except Exception as e:
        print(f"Error creating directories: {e}")
        raise

# Call directory creation immediately
try:
    ensure_directories()
except Exception as e:
    print(f"Failed to initialize directories: {e}")
    exit(1)

# Verify VENV_PYTHON exists
if not os.path.exists(VENV_PYTHON):
    print(f"Error: Virtual environment Python not found at {os.path.abspath(VENV_PYTHON)}")
    exit(1)

# NBA scripts to run sequentially
NBA_SCRIPTS = [
    "1-games_players_ids.py",
    "2-gamelogs.py",
    "3-1-playergamelogs_delta.py",
    "4-customplayerstats.py",
    "5-customgamelogs.py",
    "8-daily.py"
]

def get_local_time():
    return datetime.now().astimezone(LOCAL_TZ)

def log_message(message):
    try:
        with open(LOG_FILE, "a") as f:
            f.write(f"[{get_local_time()}] {message}\n")
    except Exception as e:
        print(f"Error writing to log file {os.path.abspath(LOG_FILE)}: {e}")
        raise

def run_scripts(scripts):
    critical_scripts = ["1-games_players_ids.py", "2-gamelogs.py", "3-1-playergamelogs_delta.py"]
    for script in scripts:
        script_path = os.path.join(SCRIPT_DIR, script)
        if not os.path.exists(script_path):
            print(f"[{get_local_time()}] [ERROR] Script not found: {script_path}")
            log_message(f"[ERROR] Script not found: {script_path}")
            if script in critical_scripts:
                raise FileNotFoundError(f"Critical script {script} not found")
            continue
        print(f"[{get_local_time()}] Running {script}...")
        try:
            log_message(f"Running {script}...")
            subprocess.run([VENV_PYTHON, script_path], check=True)
            print(f"[{get_local_time()}] {script} completed successfully.")
            log_message(f"{script} completed successfully.")
        except subprocess.CalledProcessError as e:
            print(f"[{get_local_time()}] [ERROR] {script} failed: {e}")
            log_message(f"[ERROR] {script} failed: {e}")
            if script in critical_scripts:
                raise
            continue
        except FileNotFoundError as e:
            print(f"[{get_local_time()}] [ERROR] Python executable not found: {e}")
            log_message(f"[ERROR] Python executable not found: {e}")
            raise

def git_commit_and_push():
    current_dir = os.getcwd()
    try:
        print(f"[{get_local_time()}] Committing and pushing changes to GitHub...")
        log_message("[INFO] Committing and pushing changes to GitHub...")
        os.chdir(REPO_PATH)  # Change to SPORTS/
        subprocess.run(["git", "add", "."], check=True)  # Trust .gitignore
        result = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
        if result.stdout.strip():
            subprocess.run(["git", "commit", "-m", f"NBA pipeline update {get_local_time()}"], check=True)
            subprocess.run(["git", "push", "origin", "data-feed"], check=True)
            print(f"[{get_local_time()}] Git operations completed successfully.")
            log_message("[INFO] Git operations completed successfully.")
        else:
            print(f"[{get_local_time()}] No changes to commit.")
            log_message("[INFO] No changes to commit.")
    except subprocess.CalledProcessError as e:
        print(f"[{get_local_time()}] [ERROR] Git operation failed: {e}")
        log_message(f"[ERROR] Git operation failed: {e}")
    except FileNotFoundError as e:
        print(f"[{get_local_time()}] [ERROR] Git executable not found: {e}")
        log_message(f"[ERROR] Git executable not found: {e}")
    finally:
        os.chdir(current_dir)

def main():
    print(f"[{get_local_time()}] Starting NBA pipeline")
    log_message(f"[INFO] Starting NBA pipeline")
    run_scripts(NBA_SCRIPTS)
    git_commit_and_push()
    print(f"[{get_local_time()}] NBA pipeline completed.")
    log_message(f"[INFO] NBA pipeline completed.")

if __name__ == "__main__":
    main()
