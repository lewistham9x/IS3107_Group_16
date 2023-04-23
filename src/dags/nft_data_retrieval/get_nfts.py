# import os
# from pandas import DataFrame
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
# from common.bigquery_utils import write_bq
# from common.gcs_utils import store_in_gcs
# from nft_data_retrieval.tasks.nft import nft_list

# from dotenv import load_dotenv

# load_dotenv()


# def get_nfts():
#     nft_data = nft_list()
    
#     write_bq(nft_data, os.getenv("AIRFLOW_VAR_NFT_TABLE"))
    
#     return nft_data


# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "start_date": datetime(2023, 4, 20),
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5),
# }

# dag = DAG(
#     "nft_data_retrieval_dag",
#     default_args=default_args,
#     description="Scrapes NFT data and stores it in BigQuery",
#     schedule_interval="@daily")

# with dag:

#     get_nfts_task = PythonOperator(
#         task_id="get_nfts_task",
#         python_callable=get_nfts,
#     )

#     get_nfts_task 