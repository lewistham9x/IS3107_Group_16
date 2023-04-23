import asyncio
import cloudscraper
import json
import os
import pandas as pd
import requests
from datetime import datetime
from dateutil.rrule import rrule, MONTHLY
from dateutil.relativedelta import relativedelta
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from common.bigquery_utils import read_bq, write_bq

import concurrent.futures

from dotenv import load_dotenv

from common.proxy import getProxy

load_dotenv()


# def is_first_time():
#     table_exists = read_bq(f"SELECT COUNT(*) FROM {os.getenv('AIRFLOW_VAR_NFT_TABLE')}")

#     return table_exists is not None


def nft_list():
    payload = {}

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:105.0) Gecko/20100101 Firefox/105.0",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.5",
        # 'Accept-Encoding': 'gzip, deflate, br',
        "Origin": "https://nftgo.io",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "TE": "trailers",
        "Cookie": "__cf_bm=DJhCHidhfwbfkfADrlYe4AZOx6vAICxqeCV8A.fZoME-1665975767-0-AXc0wY50eSVouByg907FB4ZacIe8vKayES6WNMKKbzWM7xwzhG0ZgdlO0I1uyiH8n78p0YUJVEkdI2y9+tSRelw=",
    }

    # top 5 only
    url = "https://api.nftgo.io/api/v2/ranking/collections?offset=0&limit=5&by=volumeEth&asc=-1&rarity=-1&keyword=&fields=id,marketCap,marketCapEth,marketCapEthRanking,marketCapEthChange24h,buyerNum24h,buyerNum24hChange24h,sellerNum24h,sellerNum24hChange24h,liquidity24h,liquidity24hChange24h,saleNum24h,saleNum24hChange24h,volume24h,volumeEth24h,volumeEth24hChange24h,traderNum24h,traderNum24hChange24h,holderNum,holderNumChange24h,whaleNum,whaleNumChange24h,orderAvgPriceETH24h,orderAvgPriceETH24hChange24h,orderAvgPrice24h,orderAvgPrice24hChange24h,floorPrice,floorPriceChange24h,blueChipHolderNum,blueChipHolderNumChange24h,blueChipHolderRatio,whaleRatio"
    scraper = cloudscraper.create_scraper(
        browser={"browser": "firefox", "platform": "darwin", "desktop": True}
    )
    response = scraper.get(url, headers=headers, data=payload)

    # parse json response and convert to a list of dicts
    resjson = json.loads(response.content.decode("utf-8"))
    nfts = []

    for nft in resjson["data"]["list"]:
        nft["twitter"] = nft["medias"].get("twitter")
        nft["discord"] = nft["medias"].get("discord")

        if nft["twitter"] != None:
            nft["twitter"] = nft["twitter"].lower()

        if nft["discord"] != None:
            nft["discord"] = nft["discord"].lower()

        nft["floorPrice"] = nft["floorPrice"]["usdPrice"]

        # only include schema properties inside nft object
        nft = {
            k: v
            for k, v in nft.items()
            if k
            in [
                "id",
                "twitter",
                "discord",
                "name",
            ]
        }

        nfts.append(nft)

    write_bq(nfts, os.getenv("AIRFLOW_VAR_NFT_TABLE"), no_dupes="id")

    return nfts


def nft_price_part(nft_id, date):
    start_date = date
    end_date = start_date + timedelta(days=1)
    if check_price_data_exists(nft_id, start_date):
        return []

    scraper = cloudscraper.create_scraper(
        browser={"browser": "firefox", "platform": "darwin", "desktop": True}
    )

    url = f"https://api.nftgo.io/api/v1/collection-new/data/{nft_id}/chart/marketcap-volume-V2?cid={nft_id}&from={int(datetime.strptime(start_date,'%Y-%m-%d').timestamp() * 1000)}&to={int(datetime.strptime(end_date, '%Y-%m-%d').timestamp()*1000)}"

    payload = {}
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:105.0) Gecko/20100101 Firefox/105.0",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.5",
        "Origin": "https://nftgo.io",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "TE": "trailers",
    }

    res = scraper.get(url, headers=headers, data=payload)

    resjson = res.json()

    prices = []

    for i in range(len(resjson["data"]["marketCap"]["values"]["x"])):
        prices.append(
            {
                "date": pd.to_datetime(
                    resjson["data"]["marketCap"]["values"]["x"][i],
                    utc=True,
                    unit="ms",
                ).strftime("%Y-%m-%d %H:%M:%S"),
                "marketCap": resjson["data"]["marketCap"]["values"]["y"][i],
                "volume": resjson["data"]["volume"]["values"]["y"][i],
                "marketCapEth": resjson["data"]["marketCapEth"]["values"]["y"][i],
                "volumeEth": resjson["data"]["volumeEth"]["values"]["y"][i],
                "nft_id": nft_id,
            }
        )

    return prices


def check_price_data_exists(nft_id, date):
    try:
        query = f"SELECT COUNT(*) as count FROM {os.getenv('AIRFLOW_VAR_PROJECT_ID')}.{os.getenv('AIRFLOW_VAR_DATASET_NAME')}.{os.getenv('AIRFLOW_VAR_NFT_PRICE_TABLE')} WHERE nft_id = '{nft_id}' AND date = '{date}'"
        result = read_bq("", manual=query)
        return result.at[0, "count"] > 0
    except:
        return False


def nft_price_history(nft, execution_date):
    date = datetime.strptime(execution_date, "%Y-%m-%d") - timedelta(days=1)
    nft_price_data = nft_price_part(nft["id"], date)
    write_bq(nft_price_data, os.getenv("AIRFLOW_VAR_NFT_PRICE_TABLE"))
    return nft_price_data


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "nft_dag",
    default_args=default_args,
    description="Scrapes NFT data and stores it in BigQuery",
    schedule_interval="@daily",
    catchup=True,
)

with dag:

    def _nft_list():
        return nft_list()

    nft_list_task = PythonOperator(
        task_id="nft_list",
        python_callable=_nft_list,
    )

    def _nft_price_history(nft, execution_date):
        return nft_price_history(nft, execution_date)

    nft_price_history_tasks = []

    for nft in nft_list():
        task_id = f"nft_price_history_{nft['id']}"
        task = PythonOperator(
            task_id=task_id,
            python_callable=_nft_price_history,
            op_kwargs={"nft": nft, "execution_date": "{{ ds }}"},
        )
        nft_price_history_tasks.append(task)

    nft_list_task >> nft_price_history_tasks
