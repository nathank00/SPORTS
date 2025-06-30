from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time
import re

SEARCH_QUERY = "MLB parlay OR betting OR 'lock of the day'"
MIN_LIKES = 10
SCROLL_COUNT = 5

def login(driver, username, password):
    driver.get("https://twitter.com/login")
    time.sleep(3)

    driver.find_element(By.NAME, "text").send_keys(username + Keys.RETURN)
    time.sleep(2)

    try:
        driver.find_element(By.NAME, "password").send_keys(password + Keys.RETURN)
    except:
        print("🔒 Extra verification required. Login halted.")
        return False

    time.sleep(4)
    return True

def extract_numeric_from_tweet(tweet):
    try:
        footer = tweet.find_element(By.XPATH, ".//div[@role='group']")
        spans = footer.find_elements(By.TAG_NAME, "span")
        numbers = []

        for span in spans:
            text = span.text.strip().replace(",", "")
            if re.fullmatch(r"\d+", text):
                numbers.append(int(text))

        return max(numbers) if numbers else 0
    except:
        return 0

def scrape_tweet_ids(username, password):
    driver = webdriver.Chrome()
    if not login(driver, username, password):
        driver.quit()
        return []

    search_url = f"https://twitter.com/search?q={SEARCH_QUERY}&f=top"
    driver.get(search_url)
    time.sleep(5)

    tweet_ids = set()

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

    driver.quit()
    return list(tweet_ids)[:10]

if __name__ == "__main__":
    USERNAME = "monkeyking1379"
    PASSWORD = "Sycamore2000!"

    candidates = scrape_tweet_ids(USERNAME, PASSWORD)

    print("\n🟢 Top tweet candidates:")
    for tid in candidates:
        print(tid)
