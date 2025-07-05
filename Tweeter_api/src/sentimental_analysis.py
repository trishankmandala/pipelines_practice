from nltk.sentiment import SentimentIntensityAnalyzer
import nltk
import pandas as pd

nltk.download('vader_lexicon')

def analyze_sentiment(df: pd.DataFrame) -> pd.DataFrame:
    sia = SentimentIntensityAnalyzer()
    
    def get_sentiment(text):
        score = sia.polarity_scores(str(text))['compound']
        if score > 0.05:
            return "Positive"
        elif score < -0.05:
            return "Negative"
        else:
            return "Neutral"
    
    df["sentiment"] = df["text"].apply(get_sentiment)
    return df
