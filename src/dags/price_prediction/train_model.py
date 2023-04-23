import os
import pandas as pd
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from sklearn.model_selection import train_test_split
from common.bigquery_utils import read_bq
from price_prediction.model import (
    train_model,
    save_model,
    upload_model_to_gcs,
    create_model_version,
    deploy_model_version,
    get_merged_data,
    get_gcp_predictions,
    transform_data,
    predictors,
    market_cap_columns,
    predicted_column,
    test_interval,
)
from dotenv import load_dotenv

load_dotenv()

# Set default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2023, 4, 20),
}

# Instantiate the DAG
dag = DAG(
    "train_model",
    default_args=default_args,
    description="Train a model to predict NFT prices",
    schedule_interval=timedelta(weeks=1),
    catchup=False,
)


def get_all_merged_data(execution_date):
    nfts = read_bq(os.getenv("AIRFLOW_VAR_NFT_TABLE"))
    all_merged_data = []

    # Iterate through the NFTs DataFrame
    for index, nft in nfts.iterrows():
        try:
            nft_id = nft["id"]
            merged_data = get_merged_data(nft_id)
            all_merged_data.append(merged_data)
        except Exception as e:
            print(e)

    # Check if all_merged_data is not empty
    if all_merged_data:
        # Combine all merged DataFrames into a single DataFrame
        df = pd.concat(all_merged_data, ignore_index=True)
        # json_df = combined_data.to_json(orient="split", index=False)
        
        X, y = transform_data(
            predictors,
            predicted_column,
            window_size="60D",
            start_date=execution_date,
            df=df,
        )
        return train_test_split(X, y, test_size=0.2, random_state=42)
    else:
        print("No objects to concatenate")


get_all_merged_data_task = PythonOperator(
    task_id="get_all_merged_data",
    python_callable=get_all_merged_data,
    op_kwargs={"execution_date": "{{ ds }}"},
    provide_context=True,
    dag=dag,
)


train_model_task = PythonOperator(
    task_id="train_model",
    python_callable=train_model,
    provide_context=True,
    dag=dag,
)

# save_model_task = PythonOperator(
#     task_id="save_model",
#     python_callable=save_model,
#     op_args=["model.joblib"],
#     provide_context=True,
#     dag=dag,
# )

# upload_model_to_gcs_task = PythonOperator(
#     task_id="upload_model_to_gcs",
#     python_callable=upload_model_to_gcs,
#     op_args=["model.joblib", os.getenv("AIRFLOW_VAR_GCS_BUCKET"), "model.joblib"],
#     provide_context=True,
#     dag=dag,
# )

# create_model_version_task = PythonOperator(
#     task_id="create_model_version",
#     python_callable=create_model_version,
#     op_args=[
#         os.getenv("AIRFLOW_VAR_PROJECT_ID"),
#         os.getenv("AIRFLOW_VAR_REGION"),
#         os.getenv("AIRFLOW_VAR_MODEL_NAME"),
#         os.getenv("AIRFLOW_VAR_GCS_BUCKET"),
#         "model.joblib",
#     ],
#     provide_context=True,
#     dag=dag,
# )

# deploy_model_version_task = PythonOperator(
#     task_id="deploy_model_version",
#     python_callable=deploy_model_version,
#     op_args=[
#         os.getenv("AIRFLOW_VAR_PROJECT_ID"),
#         os.getenv("AIRFLOW_VAR_REGION"),
#         os.getenv("AIRFLOW_VAR_MODEL_NAME"),
#     ],
#     provide_context=True,
#     dag=dag,
# )

# get_gcp_predictions_task = PythonOperator(
#     task_id="get_gcp_predictions",
#     python_callable=get_gcp_predictions,
#     op_args=[os.getenv("AIRFLOW_VAR_MODEL_NAME"), "{{ ds }}"],
#     provide_context=True,
#     dag=dag,
# )

(get_all_merged_data_task >> train_model_task)
