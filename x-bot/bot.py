from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import re
import random

# Credentials
USERNAME = "monkeyking1379"
PASSWORD = "Sycamore2000!"

SEARCH_QUERY = "MLB parlay OR MLB betting OR 'lock of the day'"
MIN_LIKES = 10
SCROLL_COUNT = 5


# List of random replies
REPLIES = [
    "Facts. Been thinking the same.",
    "Hard to disagree with this",
    "Cant argue with that",
    "Underrated take right here.",
    "People arent talking about this enough.",
    "Cooking",
    "Real ones know.",
    "This needs more attention.",
    "Yup. Been seeing it play out just like this.",
    "More people need to realize this."
]


def login(driver):
    driver.get("https://twitter.com/login")
    time.sleep(3)

    driver.find_element(By.NAME, "text").send_keys(USERNAME + Keys.RETURN)
    time.sleep(2)
    driver.find_element(By.NAME, "password").send_keys(PASSWORD + Keys.RETURN)
    time.sleep(5)

def extract_numeric_from_tweet(tweet):
    try:
        footer = tweet.find_element(By.XPATH, ".//div[@role='group']")
        spans = footer.find_elements(By.TAG_NAME, "span")
        numbers = [int(span.text.replace(",", "")) for span in spans if span.text.replace(",", "").isdigit()]
        return max(numbers) if numbers else 0
    except:
        return 0

def scrape_tweet_ids(driver):
    tweet_ids = set()
    search_url = f"https://twitter.com/search?q={SEARCH_QUERY}&f=top"
    driver.get(search_url)
    time.sleep(5)

    for _ in range(SCROLL_COUNT):
        tweets = driver.find_elements(By.XPATH, "//article[@data-testid='tweet']")
        for tweet in tweets:
            likes = extract_numeric_from_tweet(tweet)
            if likes >= MIN_LIKES:
                try:
                    tweet_url_elem = tweet.find_element(By.XPATH, ".//a[contains(@href, '/status/')]")
                    tweet_url = tweet_url_elem.get_attribute("href")
                    tweet_id = tweet_url.split("/")[-1]
                    tweet_ids.add(tweet_id)
                except:
                    continue

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)

        if len(tweet_ids) >= 10:
            break

    return list(tweet_ids)[:10]

def reply_to_tweet(driver, tweet_id):
    tweet_url = f"https://twitter.com/i/web/status/{tweet_id}"
    driver.get(tweet_url)
    time.sleep(5)

    try:
        wait = WebDriverWait(driver, 10)
        reply_box = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-testid='tweetTextarea_0']")))

        reply = random.choice(REPLIES)
        safe_reply = ''.join(c for c in reply if ord(c) <= 0xFFFF)
        reply_box.send_keys(safe_reply)
        time.sleep(1)
        reply_box.send_keys(Keys.COMMAND, Keys.RETURN)  # Use Keys.CONTROL on Windows
        print(f"✅ Replied to tweet {tweet_id} with: {safe_reply}")
        time.sleep(3)
    except Exception as e:
        print(f"❌ Failed to reply to {tweet_id}: {e}")

def run_session():
    driver = webdriver.Chrome()
    login(driver)

    tweet_ids = scrape_tweet_ids(driver)
    print(f"\n🟢 Found {len(tweet_ids)} tweet(s) to reply to.")

    for tweet_id in tweet_ids:
        reply_to_tweet(driver, tweet_id)

    driver.quit()

if __name__ == "__main__":
    run_session()