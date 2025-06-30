from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time
import random

# Set up credentials
USERNAME = "monkeyking1379"
PASSWORD = "Sycamore2000!"
TWEET_ID = "1939755377350078927"

# List of random replies
REPLIES = [
    "Just testing out my reply bot",
    "This is a test reply. Hello, world!",
    "Automated comment using Selenium",
    "Lets cook - bot testing post",
    "Let’s gooo #Test"
]

def reply_to_tweet():
    driver = webdriver.Chrome()
    driver.get("https://twitter.com/login")
    time.sleep(3)

    # Enter username
    username_field = driver.find_element(By.NAME, "text")
    username_field.send_keys(USERNAME)
    username_field.send_keys(Keys.RETURN)
    time.sleep(3)

    # Enter password
    password_field = driver.find_element(By.NAME, "password")
    password_field.send_keys(PASSWORD)
    password_field.send_keys(Keys.RETURN)
    time.sleep(5)

    # Go to tweet
    tweet_url = f"https://twitter.com/i/web/status/{TWEET_ID}"
    driver.get(tweet_url)
    time.sleep(5)

    # Click reply box
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    # Wait for reply box to load
    wait = WebDriverWait(driver, 10)
    reply_box = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-testid='tweetTextarea_0']")))
    reply = random.choice(REPLIES)
    reply_box.send_keys(reply)
    time.sleep(1)

    # Post it
    reply_box.send_keys(Keys.COMMAND, Keys.RETURN)  # Use Keys.CONTROL on Windows

    reply_box.click()
    time.sleep(1)

    reply = random.choice(REPLIES)
    reply_box.send_keys(reply)
    time.sleep(1)

    # Press Ctrl+Enter (Windows) or Cmd+Enter (Mac) to post
    reply_box.send_keys(Keys.CONTROL, Keys.RETURN)  # Use Keys.COMMAND if on Mac

    print(f"SUCCESS - Replied to tweet {TWEET_ID} with: {reply}")
    time.sleep(5)
    driver.quit()

if __name__ == "__main__":
    reply_to_tweet()
