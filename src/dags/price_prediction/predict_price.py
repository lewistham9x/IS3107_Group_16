from datetime import datetime, timedelta
from google.oauth2 import service_account
from google.cloud import bigquery
from sklearn.model_selection import train_test_split
import pandas as pd
import os
from google.cloud import aiplatform

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from common.bigquery_utils import read_bq, write_bq
from sklearn.metrics import mean_absolute_error
from airflow.models.baseoperator import chain

from price_prediction.model import (
    get_gcp_predictions,
    get_merged_data,
    transform_data,
    predictors,
    market_cap_columns,
    predicted_column,
    test_interval,
)


# this prepares it for prediction of the execution date
def preprocess_data_for_inference(
    predictors,
    predicted_column,
    window_size=test_interval,
    execution_date="2022-01-01",
    **kwargs,
):
    execution_date = datetime.strptime(execution_date, "%Y-%m-%d")

    X, y = transform_data(
        predictors,
        predicted_column,
        window_size=window_size,
        start_date=execution_date,
        ti=kwargs["ti"],
    )
    print("x", X)
    print("y", y)

    # Separate the data (< execution date) for prediction
    X_before_execution_date = X[X.index < execution_date]
    y_before_execution_date = y[y.index < execution_date]

    # Set the execution date's one as actual result
    X_execution_date = X[X.index == execution_date]
    y_execution_date = y[y.index == execution_date]

    return (
        X_before_execution_date,
        X_execution_date,
        y_before_execution_date,
        y_execution_date,
    )


def get_single_merged_data(nft_id, execution_date):
    df = get_merged_data(nft_id)  # .to_json(orient="split", index=False)

    X, y = transform_data(
        predictors,
        predicted_column,
        window_size="60D",
        start_date=execution_date,
        df=df,
    )

    # Separate the data (< execution date) for prediction
    X_before_execution_date = X[X.index < execution_date]
    y_before_execution_date = y[y.index < execution_date]

    # Set the execution date's one as actual result
    X_execution_date = X[X.index == execution_date]
    y_execution_date = y[y.index == execution_date]

    return (
        X_before_execution_date,
        X_execution_date,
        y_before_execution_date,
        y_execution_date,
    )
    return json_df


def predict_and_save_nft_price(**kwargs):
    execution_date = kwargs["execution_date"]
    nft_list = read_bq(os.getenv("AIRFLOW_VAR_NFT_TABLE"))

    for _, nft_row in nft_list.iterrows():
        nft_id = nft_row["id"]

        ti = kwargs["ti"]

        if ti is not None:
            X_train, X_test, y_train, y_test = ti.xcom_pull(key="return_value")
        else:
            return

        if len(X_test) >= 2:
            model_endpoint = read_bq(
                os.environ["AIRFLOW_VAR_MODEL_TABLE"], unique_id="model"
            )
            model_endpoint = model_endpoint["endpoint_url"][0]

            instances_execution_date = [X_test.iloc[-1]]
            instances_day_before = [X_test.iloc[-2]]        

            predictions_execution_date = get_gcp_predictions(
                project_id=os.environ["AIRFLOW_VAR_PROJECT_ID"],
                region=os.environ["AIRFLOW_VAR_REGION"],
                endpoint_url=model_endpoint,
                instances=instances_execution_date,
            )

            predictions_day_before = get_gcp_predictions(
                project_id=os.environ["AIRFLOW_VAR_PROJECT_ID"],
                region=os.environ["AIRFLOW_VAR_REGION"],
                endpoint_url=model_endpoint,
                instances=instances_day_before,
            )

            accuracy = None
            if not y_test.empty:
                accuracy = mean_absolute_error(y_test.iloc[-1:], predictions_day_before)

            prediction_data = []

            # Add prediction for the execution date
            prediction_data.append(
                {
                    "nft_id": nft_id,
                    "price": predictions_execution_date[0],
                    "accuracy": accuracy,
                    "created_at": instances_execution_date,
                    "actual_price": None if y_test.empty else y_test.iloc[-1],
                }
            )

            # Add prediction for the day before's actual value now that it's known
            prediction_data.append(
                {
                    "nft_id": nft_id,
                    "price": predictions_day_before[0],
                    "accuracy": accuracy,
                    "created_at": instances_execution_date - timedelta(days=1),
                    "actual_price": None if y_test.empty else y_test.iloc[-2],
                }
            )

            write_bq(prediction_data, os.getenv("AIRFLOW_VAR_PREDICTION_TABLE"))
        else:
            print("skipping due to insufficient data")


# Define your DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2023, 3, 1),
}

dag = DAG(
    "nft_price_prediction",
    default_args=default_args,
    description="Predict NFT prices for past day and the current day",  # past day is to update the actual value
    schedule_interval=timedelta(days=1),
    catchup=False,
)

nft_list = read_bq(os.getenv("AIRFLOW_VAR_NFT_TABLE"))

for _, nft_row in nft_list.iterrows():
    nft_id = nft_row["id"]

    get_single_merged_data_task = PythonOperator(
        task_id=f"get_single_merged_data_{nft_id}",
        python_callable=get_single_merged_data,
        op_args=[nft_id],
        op_kwargs={"execution_date": "{{ ds }}"},
        provide_context=True,
        dag=dag,
    )

    predict_nft_prices_task = PythonOperator(
        task_id=f"predict_nft_prices_{nft_id}",
        python_callable=predict_and_save_nft_price,
        op_kwargs={"nft_id": nft_id, "execution_date": "{{ ds }}"},
        provide_context=True,
        dag=dag,
    )

    (get_single_merged_data_task >> predict_nft_prices_task)

    # tasks.append(
    #     (
    #         get_single_merged_data_task,
    #         preprocess_data_for_inference_task,
    #         predict_nft_prices_task,
    #     )
    # )


# # Set task dependencies
# for task_group in tasks:
#     chain(*task_group)
#     branch_task >> task_group[0]
