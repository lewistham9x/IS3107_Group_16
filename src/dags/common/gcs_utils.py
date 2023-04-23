from datetime import datetime, timedelta
import pandas as pd
import os

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage
from google.oauth2 import service_account
from airflow.models import Variable
from dotenv import load_dotenv

load_dotenv()

def store_in_gcs(file_name):
    credentials = service_account.Credentials.from_service_account_info(
        Variable.get("google", deserialize_json=True)
    )
    # Upload file to GCS
    client = storage.Client(credentials=credentials)
    bucket_name = os.environ["AIRFLOW_VAR_GCS_BUCKET"]
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_filename(file_name)
    #  delete the file from the local filesystem
    os.remove(file_name)
    return f"gs://{bucket_name}/{file_name}"


def store_in_gcs_as_parquet(data, file_name):
    # Convert data to Pandas DataFrame
    df = pd.DataFrame().from_dict(data)

    # Save DataFrame to parquet format
    df.to_parquet(file_name)
    return store_in_gcs(file_name)

def download_from_gcs(bucket_name, file_name):
    # Download file from GCS
    credentials = service_account.Credentials.from_service_account_info(
        Variable.get("google", deserialize_json=True)
    )
    # Upload file to GCS
    client = storage.Client(credentials=credentials)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.download_to_filename(file_name)

    # Read parquet file into Pandas DataFrame
    df = pd.read_parquet(file_name)

    return df