from src.fetch_tweets import fetch_tweets
from src.sentimental_analysis import analyze_sentiment
from src.load_to_ssms import load_to_ssms

fetched_tweets_data = fetch_tweets()
analyzed_data = analyze_sentiment(fetched_tweets_data)
load_to_ssms(analyzed_data)

