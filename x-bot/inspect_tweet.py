from selenium import webdriver
from selenium.webdriver.common.by import By
import time

USERNAME = "monkeyking1379"
PASSWORD = "Sycamore2000!"
TWEET_ID = "1939797921127354848"

def login(driver):
    driver.get("https://twitter.com/login")
    time.sleep(3)
    driver.find_element(By.NAME, "text").send_keys(USERNAME + "\n")
    time.sleep(2)
    driver.find_element(By.NAME, "password").send_keys(PASSWORD + "\n")
    time.sleep(5)

def inspect_likes(tweet_id):
    driver = webdriver.Chrome()
    login(driver)

    url = f"https://twitter.com/i/web/status/{tweet_id}"
    print(f"\n🔍 Inspecting tweet: {url}")
    driver.get(url)
    time.sleep(5)

    spans = driver.find_elements(By.XPATH, "//article[@data-testid='tweet']//div[@role='group']//span")

    # Extract numeric text and get first 4 unique values
    seen = set()
    values = []
    for span in spans:
        text = span.text.strip().replace(",", "")
        if text.isdigit():
            num = int(text)
            if num not in seen:
                seen.add(num)
                values.append(num)
            if len(values) == 4:
                break

    labels = ["Comments", "Retweets", "Likes", "Bookmarks"]
    for label, val in zip(labels, values):
        print(f"{label}: {val}")

    driver.quit()

if __name__ == "__main__":
    inspect_likes(TWEET_ID)
