# import os

# import pandas as pd
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
# from common.bigquery_utils import read_bq, write_bq
# from nft_data_retrieval.tasks.nft import nft_price_history
# from nft_data_retrieval.get_nfts import get_nfts_task
# from airflow.sensors.external_task import ExternalTaskSensor


# def get_price_history_of_nfts():
#     nfts = read_bq(os.getenv("AIRFLOW_VAR_NFT_TABLE"))

#     # iterate dataframe
#     for index, nft in nfts.iterrows():
#         # calls nft_price_history
#         write_bq(
#             pd.DataFrame().from_dict(nft_price_history(nft)),
#             os.getenv("AIRFLOW_VAR_NFT_PRICE_TABLE"),
#         )


# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "start_date": datetime(2023, 4, 20),
#     "email_on_failure": False,
#     "email_on_retry": False,
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5),
# }

# dag = DAG(
#     "nft_price_data_retrieval_dag",
#     default_args=default_args,
#     description="Retrieve NFT price history data",
#     schedule_interval=timedelta(days=1),
#     catchup=False,
# )

# get_price_history_task = PythonOperator(
#     task_id="get_price_history_task",
#     python_callable=get_price_history_of_nfts,
#     dag=dag,
# )

# # # this allows you to wait for an external dag
# # wait_for_get_nfts_task = ExternalTaskSensor(
# #     task_id="wait_for_get_nfts_task",
# #     external_dag_id="nft_data_retrieval_dag",
# #     external_task_id="get_nfts_task",
# #     mode="poke",
# #     timeout=60 * 60 * 24, # Timeout after 24 hours
# #     poke_interval=60 * 5, # Check every 5 minutes
# #     dag=dag,
# # )

# # wait_for_get_nfts_task >> 
# get_price_history_task
