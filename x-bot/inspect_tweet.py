from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time

USERNAME = "monkeyking1379"
PASSWORD = "Sycamore2000!"

def login(driver):
    driver.get("https://twitter.com/login")
    time.sleep(3)

    driver.find_element(By.NAME, "text").send_keys(USERNAME + Keys.RETURN)
    time.sleep(2)

    driver.find_element(By.NAME, "password").send_keys(PASSWORD + Keys.RETURN)
    time.sleep(5)

def inspect_tweet_footer(driver, tweet_id):
    tweet_url = f"https://twitter.com/i/web/status/{tweet_id}"
    driver.get(tweet_url)
    time.sleep(5)

    print(f"\n🔍 Inspecting tweet: {tweet_url}\n")

    try:
        footer = driver.find_element(By.XPATH, "//div[@role='group']")
        spans = footer.find_elements(By.XPATH, ".//div[@aria-label]")

        for span in spans:
            label = span.get_attribute("aria-label")
            if label:
                print(label)
    except Exception as e:
        print(f"❌ Failed to inspect tweet footer: {e}")


if __name__ == "__main__":
    TWEET_ID = 1939797921127354848

    driver = webdriver.Chrome()
    login(driver)
    inspect_tweet_footer(driver, TWEET_ID)
    driver.quit()
