import os
import pandas as pd
import requests
import json

from common.bigquery_utils import read_bq, write_bq
from common.gcs_utils import store_in_gcs_as_parquet
from common.proxy import getProxy

TWEETS_TABLE = os.getenv("AIRFLOW_VAR_SENTIMENT_TABLE")
API_URL = "https://us-central1-akee-376111.cloudfunctions.net/infer-twitter-models"
TWITTER_BOT_DETECTOR_API = (
    "https://us-central1-akee-376111.cloudfunctions.net/twitter_bot_detector"
)


def infer_sentiment(texts):
    headers = {"Content-Type": "application/json"}
    data = {"data": texts}
    response = requests.post(API_URL, headers=headers, json=data)

    if response.status_code == 200:
        result = response.json()

        sentiments = []

        for sentiment in result["sentiment_predictions"]:
            sentiments.append(sentiment[0])

        return sentiments
    else:
        raise Exception(f"API request failed with status code {response.status_code}")


def infer_bot(data):
    headers = {"Content-Type": "application/json"}

    tweets = []
    for d in data:
        tweets.append(
            {
                "screen_name": d["username"],
                "content": d["content"],
            }
        )

    payload = {
        "data": tweets,
    }

    response = requests.post(TWITTER_BOT_DETECTOR_API, headers=headers, json=payload)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API request failed with status code {response.status_code}")


def process_sentiment_analysis(tweets_df):
    # Infer sentiment for each tweet
    sentiments = infer_sentiment(tweets_df["content"].tolist())
    tweets_df["sentiment"] = sentiments

    bot_predictions = infer_bot(tweets_df.to_dict("records"))

    # Add bot probabilities to the dataframe
    tweets_df["BotValue"] = bot_predictions

    # Convert the "date" column to a datetime object
    tweets_df["date"] = pd.to_datetime(tweets_df["date"])

    # Create a new column with the date and hour information
    tweets_df["date_hour"] = tweets_df["date"].dt.strftime("%Y-%m-%d %H:00:00")

    # Calculate tweet count per date_hour
    tweet_count_df = (
        tweets_df.groupby("date_hour").size().reset_index(name="num_tweets")
    )

    # Calculate average sentiment per date_hour
    aggregated_sentiment_df = (
        tweets_df.groupby("date_hour")["sentiment"].mean().reset_index()
    )

    # Calculate sentiment_no_bot and sentiment_with_bot
    tweets_df["sentiment_no_bot"] = tweets_df["sentiment"] * (1 - tweets_df["BotValue"])
    tweets_df["sentiment_with_bot"] = tweets_df["sentiment"] * tweets_df["BotValue"]
    no_bot_df = tweets_df.groupby("date_hour")["sentiment_no_bot"].mean().reset_index()
    with_bot_df = (
        tweets_df.groupby("date_hour")["sentiment_with_bot"].mean().reset_index()
    )

    # Merge the dataframes
    result_df = pd.merge(aggregated_sentiment_df, tweet_count_df, on="date_hour")
    result_df = pd.merge(result_df, no_bot_df, on="date_hour")
    result_df = pd.merge(result_df, with_bot_df, on="date_hour")

    # Add the "keyword" column (assuming the keyword is present in the input dataframe)
    result_df["keyword"] = tweets_df["keyword"].iloc[0]

    # Add the "platform" column and hardcode its value to "twitter"
    result_df["platform"] = "twitter"

    result_df["date_hour"] = pd.to_datetime(tweets_df["date_hour"])

    # Format the "date" column
    result_df["date"] = result_df["date_hour"].dt.strftime("%Y-%m-%dT%H:%M:%S")

    # Save raw sentiment and bot data to GCS
    # store_in_gcs(tweets_df, "path/to/gcs/sentiment/sentiment_analysis_data.csv")

    # Write aggregated sentiment data to BigQuery
    write_bq(
        result_df,
        TWEETS_TABLE,
    )
