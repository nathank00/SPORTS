import tweepy
from config import BEARER_TOKEN

client = tweepy.Client(bearer_token=BEARER_TOKEN)

# Example search query
query = "bet"

# Search recent tweets
tweets = client.search_recent_tweets(query=query, max_results=20, tweet_fields=["public_metrics"])

for tweet in tweets.data:
    likes = tweet.public_metrics["like_count"]
    retweets = tweet.public_metrics["retweet_count"]
    if likes >= 5 or retweets >= 2:
        print(f"{tweet.id} | Likes: {likes} | Text: {tweet.text}")
