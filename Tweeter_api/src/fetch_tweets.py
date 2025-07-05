import tweepy
import configparser
import pandas as pd

def fetch_tweets():
    # Step 1: Load Bearer Token from config.ini
    config = configparser.RawConfigParser()
    config.read('D:/PythonTutorial/config.ini')
    bearer_token = config['TwitterAPI']['bearer_token']

    # Step 2: Create Tweepy client
    client = tweepy.Client(bearer_token=bearer_token)

    # Step 3: Define search query
    query = "Trump lang:en"

    # Step 4: Fetch tweets
    response = client.search_recent_tweets(
        query=query,
        max_results=20,
        tweet_fields=["id", "text", "lang", "public_metrics"]
    )

    # Step 5: Extract to list of dicts
    if response.data:
        tweet_rows = []
        for tweet in response.data:
            metrics = tweet.data["public_metrics"]
            tweet_rows.append({
                "id": tweet.id,
                "text": tweet.text,
                "lang": tweet.lang,
                "retweet_count": metrics["retweet_count"],
                "reply_count": metrics["reply_count"],
                "like_count": metrics["like_count"],
                "quote_count": metrics["quote_count"]
            })

        # Convert to DataFrame
        df = pd.DataFrame(tweet_rows)
        return df 

