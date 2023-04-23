# README

This README document provides instructions for setting up Poetry and Airflow configuration path.

## Setup Poetry

To setup Poetry, follow the steps below:

1. Install Poetry if you haven't already. You can find the installation instructions for Poetry [here](https://python-poetry.org/docs/#installation).

2. For M1 Mac, some additional setup is required for grpcio and pyarrow. Run the following commands to set up the required environment variables:

   ```
   export GRPC_PYTHON_BUILD_SYSTEM_OPENSSL=1
   export GRPC_PYTHON_BUILD_SYSTEM_ZLIB=1
   export OPENSSL_ROOT_DIR="/opt/homebrew/Cellar/openssl@3/3.0.1"
   ```

   This will ensure that grpcio and pyarrow work correctly on your M1 Mac.

## Set up Airflow configuration path

To set up the Airflow configuration path, follow the steps below:

1. Open your terminal and run the following command:

   ```
   export AIRFLOW_HOME=~/Projects/akee-predict/src
   ```

   This sets the `AIRFLOW_HOME` environment variable to the path where you want to store your Airflow configuration files. Replace `~/Projects/akee-predict/src` with the path where you want to store your configuration files.

2. Once you have set the `AIRFLOW_HOME` environment variable, you can initialize your Airflow database and start the webserver by running the following commands:

   ```
   airflow db init
   airflow webserver
   ```

   This will initialize your Airflow database and start the webserver. You can now access the Airflow web UI by opening your web browser and going to `http://localhost:8080`.

That's it! You have successfully set up Poetry and Airflow configuration path.
