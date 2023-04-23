import joblib
from google.cloud import storage
from google.cloud import aiplatform
from datetime import datetime
from datetime import datetime
import pandas as pd

from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.ensemble import StackingRegressor
from sklearn.metrics import classification_report, accuracy_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from imblearn.pipeline import make_pipeline as make_imb_pipeline

from sklearn.ensemble import RandomForestRegressor
from sklearn.svm import SVR
from sklearn.linear_model import LinearRegression
import joblib
from google.cloud import storage
from google.cloud import aiplatform

import os
import pandas as pd
import numpy as np

from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.ensemble import RandomForestClassifier, StackingClassifier

# from sklearn.metrics import classification_report, accuracy_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from xgboost import XGBClassifier
from lightgbm import LGBMClassifier
from imblearn.over_sampling import SMOTE
from imblearn.under_sampling import EditedNearestNeighbours
from imblearn.combine import SMOTEENN
from imblearn.pipeline import make_pipeline as make_imb_pipeline
from google.oauth2 import service_account

from airflow.models import Variable

from common.bigquery_utils import read_bq, write_bq

from io import BytesIO

credentials = service_account.Credentials.from_service_account_info(
    Variable.get("google", deserialize_json=True)
)

# credentials = service_account.Credentials.from_service_account_file("credentials.json")

predictors = [
    "avg_sentiment",
    "total_num_tweets",
    "avg_sentiment_no_bot",
    "avg_sentiment_with_bot",
    "marketCap_lag1",
    "marketCapEth_lag1",
]
market_cap_columns = ["marketCap", "marketCapEth"]
predicted_column = "marketCap"
test_interval = "60D"  # Set the interval for the test data (approx. 2 months)

model_path = "model.joblib"
# credentials = service_account.Credentials.from_service_account_file("credentials.json")


def create_lagged_df(df, lag_columns, lag=1):
    """
    Create a DataFrame with lagged values for the specified columns.

    Parameters:
    df (pd.DataFrame): Input DataFrame
    lag_columns (list): List of column names for which to create lagged values
    lag (int): Number of periods to lag, defaults to 1

    Returns:
    pd.DataFrame: DataFrame with lagged values for the specified columns
    """
    # Make a copy of the input DataFrame to avoid modifying the original data
    lagged_df = df.copy()
    print(lagged_df.head())

    # Loop through each column specified in the lag_columns list
    for column in lag_columns:
        # Create a new column name for the lagged values
        lagged_column_name = f"{column}_lag{lag}"
        print(f"Creating column {lagged_column_name}")

        # Shift the column values by the specified lag and store them in the new column
        lagged_df[lagged_column_name] = lagged_df[column].shift(lag)
        print(f"Created column {lagged_column_name}")

    # Return the DataFrame with the new lagged columns
    return lagged_df


def exponential_smoothing(series, alpha):
    result = [series[0]]
    for n in range(1, len(series)):
        result.append(alpha * series[n] + (1 - alpha) * result[n - 1])
    return result


def aggregate_by_interval(
    df,
    interval,
    date_col,
    nft_id_col="nft_id",
    value_cols=[
        "avg_sentiment",
        "total_num_tweets",
        "avg_sentiment_no_bot",
        "avg_sentiment_with_bot",
        "marketCap_lag1",
        "marketCapEth_lag1",
        "marketCap",
    ],
    alpha=0.9,
):
    # Ensure date_col is datetime
    df[date_col] = pd.to_datetime(df[date_col])

    # Set date_col as index and sort
    df = df.set_index(date_col).sort_index()

    # Create an empty DataFrame to store the aggregated values
    aggregated_data = []

    # Group the dataframe by nft_id
    grouped_df = df.groupby(nft_id_col)

    # Iterate through each group and apply exponential smoothing
    for nft_id, group in grouped_df:
        aggregated_group = pd.DataFrame()
        for value_col in value_cols:
            resampled_df = group[value_col].resample(interval).mean()
            smoothed_values = exponential_smoothing(resampled_df, alpha)
            aggregated_group[value_col] = smoothed_values

        # Set the index of the aggregated group DataFrame as the resampled index
        aggregated_group.index = resampled_df.index
        aggregated_group[nft_id_col] = nft_id
        aggregated_data.append(aggregated_group)

    # Concatenate all aggregated groups into a single DataFrame
    aggregated_df = pd.concat(aggregated_data)
    aggregated_df.reset_index(inplace=True)
    aggregated_df.rename(columns={"index": date_col}, inplace=True)

    return aggregated_df



def transform_data(
    predictors, predicted_column, window_size="60D", start_date=datetime.now(), df=None
):
    print("df,", df)

    # df = pd.read_json(
    #     '{"columns":["date","marketCap","volume","marketCapEth","volumeEth","nft_id","created_at","keyword","platform","avg_sentiment","total_num_tweets","avg_sentiment_no_bot","avg_sentiment_with_bot"],"data":[[1618617600000,0.0,0.0,0.0,0.0,"6113f83c3126bb611b9b119d",1682202001833,"@boredapeyc","twitter",0.1518429993,453,0.0999287047,0.0519142946],[1618704000000,0.0,0.0,0.0,0.0,"6113f83c3126bb611b9b119d",1682202001833,"@boredapeyc","twitter",0.260106322,23,0.1302239799,0.1298823421],[1618790400000,0.0,0.0,0.0,0.0,"6113f83c3126bb611b9b119d",1682202001833,"@boredapeyc","twitter",0.3779742611,15,0.2830117717,0.0949624894],[1618876800000,0.0,0.0,0.0,0.0,"6113f83c3126bb611b9b119d",1682202001833,"@boredapeyc","twitter",-0.1778774604,5,-0.1768368691,-0.0010405913],[1618963200000,0.0,0.0,0.0,0.0,"6113f83c3126bb611b9b119d",1682202001833,"@boredapeyc","twitter",0.2308797388,17,0.1472076198,0.083672119],[1619049600000,0.0,0.0,0.0,0.0,"6113f83c3126bb611b9b119d",1682202001833,"@boredapeyc","twitter",0.2156003094,32,0.2331629851,-0.0175626757],[1619136000000,0.0,0.0,0.0,0.0,"6113f83c3126bb611b9b119d",1682202001833,"@boredapeyc","twitter",0.2300169197,49,0.2277702741,0.0022466457],[1619222400000,0.0,0.0,0.0,0.0,"6113f83c3126bb611b9b119d",1682202001833,"@boredapeyc","twitter",0.2522113139,106,0.1877036169,0.0645076971],[1619308800000,0.0,0.0,0.0,0.0,"6113f83c3126bb611b9b119d",1682202001833,"@boredapeyc","twitter",0.0897506369,113,0.0782041928,0.0115464441],[1619395200000,0.0,0.0,0.0,0.0,"6113f83c3126bb611b9b119d",1682202001833,"@boredapeyc","twitter",0.219461366,147,0.1022332408,0.1172281251],[1619481600000,0.0,0.0,0.0,0.0,"6113f83c3126bb611b9b119d",1682202001833,"@boredapeyc","twitter",0.0626587576,155,0.0116623309,0.0509964267],[1619568000000,0.0,0.0,0.0,0.0,"6113f83c3126bb611b9b119d",1682202001833,"@boredapeyc","twitter",0.0968399418,142,0.0468671702,0.0499727716],[1619654400000,0.0,0.0,0.0,0.0,"6113f83c3126bb611b9b119d",1682202001833,"@boredapeyc","twitter",0.0101738915,183,0.0118610917,-0.0016872002],[1619740800000,438.652189737,438.652189737,0.1601191519,0.1601191519,"6113f83c3126bb611b9b119d",1682202001833,"@boredapeyc","twitter",0.0970027962,301,0.0752514494,0.0217513468],[1619827200000,1543475.6498200034,1627238.8878790177,534.4583746188,563.5434420326,"6113f83c3126bb611b9b119d",1682202001833,"@boredapeyc","twitter",0.1647501097,1500,0.1312505324,0.0334995773],[1621036800000,9526965.3141291123,174291.5000542765,3003.6487306551,45.3555607788,"6113f83c3126bb611b9b119d",1682202001833,"@boredapeyc","twitter",0.1738584463,784,0.1182019056,0.0556565408],[1622332800000,11439906.5454105549,1242630.7701521679,3818.5266515845,515.2898814706,"6113f83c3126bb611b9b119d",1682202001833,"@boredapeyc","twitter",0.1588746397,500,0.114582789,0.0442918507],[1624924800000,39647472.2760692313,649912.3465364774,16578.2229629098,298.8445835575,"6113f83c3126bb611b9b119d",1682202001833,"@boredapeyc","twitter",0.1336956348,500,0.0809996035,0.0526960314],[1630368000000,261701047.0914402604,8988697.9381213188,89089.4112914983,2689.699576034,"6113f83c3126bb611b9b119d",1682202001833,"@boredapeyc","twitter",0.1036533098,500,0.0598938267,0.0437594831],[1632960000000,336694174.6745361686,5684366.6524797576,109462.0135157184,1920.2794586952,"6113f83c3126bb611b9b119d",1682202001833,"@boredapeyc","twitter",0.2983253209,500,0.1199375442,0.1783877766],[1638230400000,2230800049.7036013603,3520903.8293070439,490264.414984767,773.0601531478,"6113f83c3126bb611b9b119d",1682202001833,"@boredapeyc","twitter",0.0690063724,500,0.0357366441,0.0332697283],[1643587200000,3238919697.7875909805,26554867.1934234165,1199429.4250827567,10340.820406086,"6113f83c3126bb611b9b119d",1682202001833,"@boredapeyc","twitter",0.1390369649,1000,0.0953538593,0.0436831056],[1646006400000,2641323350.778459549,1940710.4119344736,902530.3666999672,710.3117799045,"6113f83c3126bb611b9b119d",1682202001833,"@boredapeyc","twitter",0.1442177952,500,0.0882013942,0.0560164011],[1648684800000,3694972836.0718340874,7039812.8893973632,1127548.4500362054,2095.8214440344,"6113f83c3126bb611b9b119d",1682202001833,"@boredapeyc","twitter",0.1417511541,500,0.0960509762,0.045700178],[1653955200000,2187064402.1017427444,7982960.185772107,970509.5452663744,4052.0001737411,"6113f83c3126bb611b9b119d",1682202001833,"@boredapeyc","twitter",0.1885321223,500,0.1219595519,0.0665725703],[1656547200000,1623237300.523737669,1142598.1147145075,929443.7310316489,1095.5114012441,"6113f83c3126bb611b9b119d",1682202001833,"@boredapeyc","twitter",0.1415490842,500,0.1001098639,0.0414392203],[1659225600000,1949274329.6836357117,294855.6552148609,942560.1549046612,172.3436150912,"6113f83c3126bb611b9b119d",1682202001833,"@boredapeyc","twitter",0.121862814,500,0.0890709232,0.0327918908],[1661904000000,1724407645.2597670555,1801393.2828786217,827389.9607803387,1156.76,"6113f83c3126bb611b9b119d",1682202001833,"@boredapeyc","twitter",0.1457854718,500,0.0948938014,0.0508916704],[1665100800000,1672265072.0161113739,323834.3561111641,868014.6387092912,241.4839803496,"6113f83c3126bb611b9b119d",1682202001833,"@boredapeyc","twitter",0.2103544803,1000,0.152897001,0.0574574793],[1618617600000,0.0,0.0,0.0,0.0,"6130c104e355227cf5b463a6",1682202002333,"@boredapeyc","twitter",0.1518429993,453,0.0999287047,0.0519142946],[1618704000000,0.0,0.0,0.0,0.0,"6130c104e355227cf5b463a6",1682202002333,"@boredapeyc","twitter",0.260106322,23,0.1302239799,0.1298823421],[1618790400000,0.0,0.0,0.0,0.0,"6130c104e355227cf5b463a6",1682202002333,"@boredapeyc","twitter",0.3779742611,15,0.2830117717,0.0949624894],[1618876800000,0.0,0.0,0.0,0.0,"6130c104e355227cf5b463a6",1682202002333,"@boredapeyc","twitter",-0.1778774604,5,-0.1768368691,-0.0010405913],[1618963200000,0.0,0.0,0.0,0.0,"6130c104e355227cf5b463a6",1682202002333,"@boredapeyc","twitter",0.2308797388,17,0.1472076198,0.083672119],[1619049600000,0.0,0.0,0.0,0.0,"6130c104e355227cf5b463a6",1682202002333,"@boredapeyc","twitter",0.2156003094,32,0.2331629851,-0.0175626757],[1619136000000,0.0,0.0,0.0,0.0,"6130c104e355227cf5b463a6",1682202002333,"@boredapeyc","twitter",0.2300169197,49,0.2277702741,0.0022466457],[1619222400000,0.0,0.0,0.0,0.0,"6130c104e355227cf5b463a6",1682202002333,"@boredapeyc","twitter",0.2522113139,106,0.1877036169,0.0645076971],[1619308800000,0.0,0.0,0.0,0.0,"6130c104e355227cf5b463a6",1682202002333,"@boredapeyc","twitter",0.0897506369,113,0.0782041928,0.0115464441],[1619395200000,0.0,0.0,0.0,0.0,"6130c104e355227cf5b463a6",1682202002333,"@boredapeyc","twitter",0.219461366,147,0.1022332408,0.1172281251],[1619481600000,0.0,0.0,0.0,0.0,"6130c104e355227cf5b463a6",1682202002333,"@boredapeyc","twitter",0.0626587576,155,0.0116623309,0.0509964267],[1619568000000,0.0,0.0,0.0,0.0,"6130c104e355227cf5b463a6",1682202002333,"@boredapeyc","twitter",0.0968399418,142,0.0468671702,0.0499727716],[1619654400000,0.0,0.0,0.0,0.0,"6130c104e355227cf5b463a6",1682202002333,"@boredapeyc","twitter",0.0101738915,183,0.0118610917,-0.0016872002],[1619740800000,0.0,0.0,0.0,0.0,"6130c104e355227cf5b463a6",1682202002333,"@boredapeyc","twitter",0.0970027962,301,0.0752514494,0.0217513468],[1619827200000,0.0,0.0,0.0,0.0,"6130c104e355227cf5b463a6",1682202002333,"@boredapeyc","twitter",0.1647501097,1500,0.1312505324,0.0334995773],[1621036800000,0.0,0.0,0.0,0.0,"6130c104e355227cf5b463a6",1682202002333,"@boredapeyc","twitter",0.1738584463,784,0.1182019056,0.0556565408],[1622332800000,0.0,0.0,0.0,0.0,"6130c104e355227cf5b463a6",1682202002333,"@boredapeyc","twitter",0.1588746397,500,0.114582789,0.0442918507],[1624924800000,0.0,0.0,0.0,0.0,"6130c104e355227cf5b463a6",1682202002333,"@boredapeyc","twitter",0.1336956348,500,0.0809996035,0.0526960314],[1630368000000,144977512.3975797594,15973303.2099680491,44890.1072249653,4800.4583531663,"6130c104e355227cf5b463a6",1682202002333,"@boredapeyc","twitter",0.1036533098,500,0.0598938267,0.0437594831],[1632960000000,189795423.0649631917,1325982.852752591,57287.2418424239,445.354514029,"6130c104e355227cf5b463a6",1682202002333,"@boredapeyc","twitter",0.2983253209,500,0.1199375442,0.1783877766],[1638230400000,531303635.5789166689,1951169.7350186436,122290.4598051556,431.0815750916,"6130c104e355227cf5b463a6",1682202002333,"@boredapeyc","twitter",0.0690063724,500,0.0357366441,0.0332697283],[1643587200000,1320380826.1219134331,15457273.6666843742,488728.315907797,5939.1926862742,"6130c104e355227cf5b463a6",1682202002333,"@boredapeyc","twitter",0.1390369649,1000,0.0953538593,0.0436831056],[1646006400000,984190794.4290839434,1040343.2254105384,334682.2629961887,386.7253745334,"6130c104e355227cf5b463a6",1682202002333,"@boredapeyc","twitter",0.1442177952,500,0.0882013942,0.0560164011],[1648684800000,1453258859.6819441319,4914129.1038171854,444649.767763346,1469.0576860197,"6130c104e355227cf5b463a6",1682202002333,"@boredapeyc","twitter",0.1417511541,500,0.0960509762,0.045700178],[1653955200000,960489609.1139942408,1718653.2416880594,403819.9959430323,871.6084296721,"6130c104e355227cf5b463a6",1682202002333,"@boredapeyc","twitter",0.1885321223,500,0.1219595519,0.0665725703],[1656547200000,774276385.7907397747,606776.5416474023,392693.3473557798,585.6844730385,"6130c104e355227cf5b463a6",1682202002333,"@boredapeyc","twitter",0.1415490842,500,0.1001098639,0.0414392203],[1659225600000,834953415.5775455236,546891.3332196593,370654.4350682285,320.9206619072,"6130c104e355227cf5b463a6",1682202002333,"@boredapeyc","twitter",0.121862814,500,0.0890709232,0.0327918908],[1661904000000,759884786.063010931,394642.9419950576,331744.1973499564,250.0286390395,"6130c104e355227cf5b463a6",1682202002333,"@boredapeyc","twitter",0.1457854718,500,0.0948938014,0.0508916704],[1665100800000,747671963.1277396679,335238.0104166683,353005.4434849478,249.8778682705,"6130c104e355227cf5b463a6",1682202002333,"@boredapeyc","twitter",0.2103544803,1000,0.152897001,0.0574574793],[1661731200000,832898977.3278476,391873.7403490744,383588.5187353772,263.2760461893,"626dc37925a8ca6a57530d25",1682202001132,"@othersidemeta","twitter",0.0530434671,500,0.0221853678,0.0308580992],[1663977600000,802699612.0582592487,307359.6479125646,383737.1705201023,230.6147406643,"626dc37925a8ca6a57530d25",1682202001132,"@othersidemeta","twitter",0.1649816521,500,0.0917618788,0.0732197733],[1664841600000,798683806.179553628,324659.6081833579,378916.4942287061,241.278115966,"626dc37925a8ca6a57530d25",1682202001132,"@othersidemeta","twitter",0.1575971578,529,0.0976570524,0.0599401054],[1664928000000,799865541.4704090357,348766.6725138794,380740.9884604528,258.7455748024,"626dc37925a8ca6a57530d25",1682202001132,"@othersidemeta","twitter",0.0919803,1415,0.055930983,0.0360493169],[1665014400000,794428494.8253350258,362228.9606532917,376922.0602940461,264.8865844796,"626dc37925a8ca6a57530d25",1682202001132,"@othersidemeta","twitter",0.0358210523,1000,0.0200678085,0.0157532438],[1665100800000,800532928.5485084057,374722.3615184603,383208.4479898324,280.216699232,"626dc37925a8ca6a57530d25",1682202001132,"@othersidemeta","twitter",0.1212964673,500,0.0532223074,0.0680741599],[1665705600000,794238566.7701665163,504733.9365043085,383228.3880468294,384.4025246591,"626dc37925a8ca6a57530d25",1682202001132,"@othersidemeta","twitter",0.1484259275,500,0.0865240273,0.0619019002]]}',
    #     orient="split",
    # )

    lagged_df1 = create_lagged_df(df, market_cap_columns)
    print("lagged_df1", lagged_df1)

    lagged_df2 = aggregate_by_interval(lagged_df1, window_size, "date")
    print("lagged_df2", lagged_df2)

    lagged_df2.index = pd.to_datetime(lagged_df2["date"])

    print("lagged_df2.index", lagged_df2.index)

    lagged_df3 = lagged_df2.loc[lagged_df2.index < start_date]

    print("lagged_df3", lagged_df3)

    X = lagged_df3.loc[:, predictors]
    y = lagged_df3[predicted_column]

    return X, y


def train_model(X_train_resampled=None, y_train_resampled=None, **kwargs):
    ti = kwargs["ti"]

    if ti is not None:
        X_train_resampled, X_test, y_train_resampled, y_test = ti.xcom_pull(
            key="return_value"
        )
    elif X_train_resampled is None or y_train_resampled is None:
        return

    # Define the regressors
    base_regressors = [
        ("rf", RandomForestRegressor(random_state=42)),
        ("svm", make_pipeline(StandardScaler(), SVR())),
        ("lr", LinearRegression()),
    ]

    stacked_regressor = StackingRegressor(
        estimators=base_regressors, final_estimator=LinearRegression()
    )

    # Define the hyperparameters for GridSearch
    param_grid = [
        {
            "rf__max_depth": [3, 5, 10],
            "rf__min_samples_leaf": [2, 5, 10],
            "rf__n_estimators": [10, 50, 100],
        },
        {
            "svm__svr__C": [0.1, 1, 10],
            "svm__svr__kernel": ["linear", "rbf"],
        },
        {
            "lr__fit_intercept": [True, False],
        },
    ]

    # Perform GridSearch
    grid_search = GridSearchCV(
        estimator=stacked_regressor,
        param_grid=param_grid,
        scoring="neg_mean_squared_error",
        cv=5,
    )
    grid_search.fit(X_train_resampled, y_train_resampled)

    model = grid_search.best_estimator_

    model_blob = save_model(model)

    url = upload_model_to_gcs(
        model_blob, os.getenv("AIRFLOW_VAR_GCS_BUCKET"), "model.joblib"
    )

    # return url


def predict(model, X_test):
    return model.predict(X_test)


def evaluate_predictions(y_test, y_pred):
    print("Mean Absolute Error:", mean_absolute_error(y_test, y_pred))
    print("Mean Squared Error:", mean_squared_error(y_test, y_pred))
    print("R2 Score:", r2_score(y_test, y_pred))


def save_model(model):
    model_binary_stream = BytesIO()
    joblib.dump(model, model_binary_stream)
    model_binary_stream.seek(0)  # Reset the stream position
    return model_binary_stream


def upload_model_to_gcs(model_blob, bucket_name, blob_name):
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    print("uploading to gcs")
    blob.upload_from_file(model_blob)
    blob_path = f"gs://{bucket_name}/{blob_name}"

    current_date = datetime.now().date()
    current_date_string = current_date.strftime("%Y-%m-%d")

    region = "asia-southeast1"
    print("Creating model version")
    resource_name = create_model_version(
        os.getenv("AIRFLOW_VAR_PROJECT_ID"),
        region,
        model_path,
        current_date_string,
        blob_path.replace(f"/model.joblib", ""),
    )
    url = deploy_model_version(
        os.getenv("AIRFLOW_VAR_PROJECT_ID"), region, resource_name
    )


def create_model_version(
    project_id,
    region,
    model_name,
    model_version,
    model_uri,
    serving_container_image_uri=None,
):
    aiplatform.init(project=project_id, location=region, credentials=credentials)
    if serving_container_image_uri is None:
        serving_container_image_uri = (
            "us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.0-24:latest"
        )

    model = aiplatform.Model.upload(
        display_name=f"{model_name}_v{model_version}",
        artifact_uri=model_uri,
        serving_container_image_uri=serving_container_image_uri,
    )
    return model.resource_name


def deploy_model_version(project_id, region, model_resource_name):
    aiplatform.init(project=project_id, location=region)

    endpoint = aiplatform.Endpoint.create(display_name="nft_price_prediction")

    # Insert endpoint URL into BigQuery table
    write_bq(
        df=pd.DataFrame(
            {
                "model": "nft_predict",
                "endpoint_url": [endpoint.resource_name],
                "created_at": [datetime.now()],
            }
        ),
        table=os.environ["AIRFLOW_VAR_MODEL_TABLE"],
        no_dupes=None,
    )

    deployed_model = endpoint.deploy(
        model=model_resource_name,
        deployed_model_display_name="nft_price_prediction_model",
        traffic_percentage=100,
    )

    return endpoint


def get_gcp_predictions(project_id, region, endpoint_url, instances):
    aiplatform.init(project=project_id, location=region, credentials=credentials)
    endpoint = aiplatform.Endpoint(endpoint_url)
    predictions = endpoint.predict(instances=instances)
    return predictions


def get_merged_data(nft_id):
    # Read NFT data
    nft_df = read_bq(os.getenv("AIRFLOW_VAR_NFT_TABLE"), f"id = '{nft_id}'")

    print("nft data ", nft_df.head())
    # Read NFT price data
    price_df = read_bq(
        os.getenv("AIRFLOW_VAR_NFT_PRICE_TABLE"),
        f"nft_id = '{nft_id}'",
        unique_id="date",
    )
    print("nft prices ", price_df.head())
    project_id = os.environ["AIRFLOW_VAR_PROJECT_ID"]
    dataset_id = os.environ["AIRFLOW_VAR_DATASET_NAME"]
    table_id = f"{project_id}.{dataset_id}.{os.getenv('AIRFLOW_VAR_SENTIMENT_TABLE')}"

    # Get Twitter data for the NFT
    twitter_id = nft_df["twitter"].iloc[0]
    sentiment_df = read_bq(
        os.getenv("AIRFLOW_VAR_SENTIMENT_TABLE"),
        manual=f"""
        WITH aggregated_data AS (
          SELECT
            DATE(date_hour) as date,
            keyword,
            platform,
            AVG(sentiment) AS avg_sentiment,
            SUM(num_tweets) AS total_num_tweets,
            AVG(sentiment_no_bot) AS avg_sentiment_no_bot,
            AVG(sentiment_with_bot) AS avg_sentiment_with_bot
          FROM
            `{table_id}`
          WHERE
            keyword = '@{twitter_id.split("twitter.com/")[1].replace("/", "")}'
          GROUP BY
            date, keyword, platform
        )

        SELECT * FROM aggregated_data
        ORDER BY date;
    """,
    )
    price_df["date"] = pd.to_datetime(price_df["date"]).dt.date
    sentiment_df["date"] = pd.to_datetime(sentiment_df["date"]).dt.date

    # Merge the data into a single DataFrame
    merged_df = price_df.merge(sentiment_df, on="date", how="right")

    return merged_df


def model_pipeline():
    # Set predictors and predicted column

    X_train, X_test, y_train, y_test = preprocess_data(
        predictors, predicted_column, test_interval
    )

    model = train_model(X_train, y_train)

    y_pred = predict(model, X_test)
    evaluate_predictions(y_test, y_pred)

    model_path = save_model(model, model_path)

    url = upload_model_to_gcs(
        model_path, os.getenv("AIRFLOW_VAR_GCS_BUCKET"), "model.joblib"
    )

    return url


def model_inference_test():
    df = get_merged_data(
        "6130c104e355227cf5b463a6"
    )  # .to_json(orient="split", index=False)

    X, y = transform_data(
        predictors,
        predicted_column,
        window_size="60D",
        start_date=datetime.now(),
        df=df,
    )
    print("x", X)
    print("y", y)

    # # Separate the data (< execution date) for prediction
    # X_before_execution_date = X[X.index < execution_date]
    # y_before_execution_date = y[y.index < execution_date]

    # # Set the execution date's one as actual result
    # X_execution_date = X[X.index == execution_date]
    # y_execution_date = y[y.index == execution_date]

    # return (
    #     X_before_execution_date,
    #     X_execution_date,
    #     y_before_execution_date,
    #     y_execution_date,
    # )


# model_inference_test()
