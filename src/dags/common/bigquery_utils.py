# Not needed as a DAG, only need functions

import os
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.cloud import bigquery
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
import logging

# Load environment variables
load_dotenv()

# credentials = service_account.Credentials.from_service_account_file("credentials.json")


def read_bq(
    table, where=None, unique_id="id", manual=None, order_by="created_at", limit=None
):
    """
    Function to read data from a BigQuery table with optional filtering.

    Args:
        table (str): Table name.
        where (str, optional): WHERE clause for filtering the data. Defaults to None.
        unique_id (str, optional): Column name for the unique identifier. Defaults to "id".
        manual (str, optional): Manually specified SQL query. Defaults to None.
        order_by (str, optional): Column name for ordering the data. Defaults to "created_at".

    Returns:
        pd.DataFrame: DataFrame containing the data from BigQuery.
    """
    try:
        project_id = os.environ["AIRFLOW_VAR_PROJECT_ID"]
        dataset_id = os.environ["AIRFLOW_VAR_DATASET_NAME"]
        table_id = f"{project_id}.{dataset_id}.{table}"

        if manual is None:
            if where is None:
                query = f"""
                SELECT event.*
                FROM (
                    SELECT ARRAY_AGG(
                        t ORDER BY t.{order_by} DESC LIMIT 1
                    )[OFFSET(0)] event
                    FROM `{table_id}` t
                    GROUP BY t.{unique_id}
                )"""
            else:
                query = f"""
                SELECT event.*
                FROM (
                    SELECT ARRAY_AGG(
                        t ORDER BY t.{order_by} DESC LIMIT 1
                    )[OFFSET(0)] event
                    FROM `{table_id}` t
                    WHERE t.{where}
                    GROUP BY t.{unique_id}
                )"""

        else:
            query = manual

        if limit is not None:
            query = f"{query} LIMIT {limit}"

        print("query:" + query)

        credentials = service_account.Credentials.from_service_account_info(
            Variable.get("google", deserialize_json=True)
        )

        # Execute the query to get the DataFrame
        bq_results = pd.read_gbq(
            query,
            project_id=project_id,
            credentials=credentials,
        )

        print("results:", bq_results)

        return bq_results
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()


def write_bq(df, table, no_dupes=None):
    # if df is list, convert to dataframe
    if isinstance(df, list):
        df = pd.DataFrame().from_dict(df)

    credentials = service_account.Credentials.from_service_account_info(
        Variable.get("google", deserialize_json=True)
    )

    # Ensure there is a created_at column
    if "created_at" not in df.columns:
        df["created_at"] = datetime.now()

    project_id = os.environ["AIRFLOW_VAR_PROJECT_ID"]
    dataset_id = os.environ["AIRFLOW_VAR_DATASET_NAME"]
    table_id = f"{project_id}.{dataset_id}.{table}"

    # try catch to handle if table does not exist
    try:
        if no_dupes is not None:
            # Create a string representation of unique values in the no_dupes column
            unique_values = ", ".join([f"'{value}'" for value in df[no_dupes].unique()])

            # Get the existing rows from the table that match the unique values
            query = f"""
                SELECT {no_dupes}
                FROM `{table_id}`
                WHERE {no_dupes} IN ({unique_values})
            """
            existing_values = pd.read_gbq(
                query,
                project_id=project_id,
                credentials=credentials,
            )

            # Find the new rows that are not in the table yet
            new_rows = df[~df[no_dupes].isin(existing_values[no_dupes])]
        else:
            new_rows = df
    except Exception as e:
        print("Error: ", e)
        new_rows = df

    if new_rows.empty:
        return

    try:
        # Use this method of loading as it doesn't take up imports from the bq side
        client = bigquery.Client(credentials=credentials)
        table_ref = client.dataset(dataset_id).table(table)
        table_obj = client.get_table(table_ref)
        result = client.insert_rows_from_dataframe(table_obj, new_rows)
        print("result", result)
    except Exception as e:
        # Usually used if schema doesn't exist already
        print("e", e)
        result = new_rows.to_gbq(
            table_id,
            project_id=os.environ["AIRFLOW_VAR_PROJECT_ID"],
            if_exists="append",
            credentials=credentials,
            chunksize=500,
        )
        print("result", result)
