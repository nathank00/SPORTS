import requests
from bs4 import BeautifulSoup

# Baseball Reference URL
url = "https://www.baseball-reference.com/leagues/MLB/bat.shtml"

# Use a real browser User-Agent to avoid getting blocked
headers = {"User-Agent": "Mozilla/5.0"}

# Make the request
response = requests.get(url, headers=headers)

# Check if the request was successful
if response.status_code == 200:
    soup = BeautifulSoup(response.text, "html.parser")
    print(soup.prettify()[:2000])  # Print first 2000 characters of the response
else:
    print(f"Failed to fetch page. Status Code: {response.status_code}")
