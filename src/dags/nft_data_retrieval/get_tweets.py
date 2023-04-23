import json
import os
import pandas as pd
import requests
import snscrape.modules.twitter as sntwitter

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from common.bigquery_utils import read_bq
from common.gcs_utils import store_in_gcs_as_parquet
from sentiment_analysis.sentiment_analysis import process_sentiment_analysis


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 1, 1),
    "retries": 1,
}

dag = DAG(
    "twitter_extract",
    default_args=default_args,
    description="twitter_extract DAG",
    schedule_interval="@daily",
    catchup=True,
)


def store_and_process_tweets(tweets, filename):
    tweets_df = pd.DataFrame().from_dict(tweets)

    store_in_gcs_as_parquet(tweets, filename)
    process_sentiment_analysis(tweets_df)


def get_keywords():
    nfts = read_bq(os.getenv("AIRFLOW_VAR_NFT_TABLE"))

    if nfts.empty:
        return None

    print(nfts)

    keyword_list = []

    for index, nft in nfts.iterrows():
        if nft["twitter"] is not None and nft["twitter"] != "":
            keyword_list.append("@" + nft["twitter"].split("twitter.com/")[1])

    print(keyword_list)

    # Remove duplicates
    keyword_list = list(dict.fromkeys(keyword_list))
    print(keyword_list)

    return keyword_list


def scrape_twitter_day(topic: str, execution_date: str, **kwargs):
    print(f"Getting tweets for {topic} on {execution_date}")

    since = (
        datetime.strptime(execution_date, "%Y-%m-%d") - timedelta(days=1)
    ).strftime("%Y-%m-%d")

    until = since

    # (datetime.strptime(execution_date, "%Y-%m-%d")).strftime("%Y-%m-%d")

    # Use the mock Twitter API
    mock_api_url = "https://neat-geese-rescue-27-125-178-129.loca.lt/search"
    response = requests.get(
        mock_api_url, params={"q": topic, "start_date": since, "end_date": until}
    )

    if response.status_code != 200:
        print(f"Error fetching tweets: {response.status_code} - {response.text}")
        return

    myscraper = response.json()
    tweets = []
    i = 0

    # Process and store tweets
    if len(myscraper) > 0:
        for tweet in myscraper:
            tweets.append(
                {
                    "id": str(tweet["id"]),
                    "keyword": topic,
                    "date": tweet["Date"],
                    "content": tweet["content"],
                    "username": tweet["username"],
                    "source": "twitter",
                }
            )

            i += 1

            # store in batches of 500
            if i % 500 == 0:
                filename = f"twitter_{topic}_{since}-{until}-{i}.json"
                store_and_process_tweets(tweets, filename)
                tweets = []

        filename = f"twitter_{topic}_{since}-{until}-{i}.json"

        store_and_process_tweets(tweets, filename)


# Fetch keyword_list
keyword_list = get_keywords()

if keyword_list is None:
    keyword_list = []

for keyword in keyword_list:
    scrape_twitter_task = PythonOperator(
        task_id=f"scrape_twitter_{keyword.replace('@','')}",
        python_callable=scrape_twitter_day,
        provide_context=True,
        op_args=[keyword],
        op_kwargs={
            "execution_date": "{{ ds }}",
        },
        dag=dag,
    )
