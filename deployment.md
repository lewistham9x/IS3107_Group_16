# Readme: Setting up Airflow on Kubernetes with K3s

This README document provides an overview of how to set up Airflow on Kubernetes using K3s. It includes installation instructions for K3s and Airflow, git sync for DAG management, building and storing a Docker image, storing credentials, and backfill loading.

## K3s Installation

To install K3s on your machine, follow the steps outlined in the [K3s quick start guide](https://docs.k3s.io/quick-start).

## Airflow Installation

After installing K3s, you can install Airflow using the [Helm chart](https://airflow.apache.org/docs/helm-chart/stable/index.html).

```bash
helm repo add apache-airflow https://airflow.apache.org
helm install airflow apache-airflow/airflow
```

To customize the installation, you can create an override values file (e.g. `override-values.yaml`) and include it when running the installation command.

```bash
helm upgrade --install airflow apache-airflow/airflow -f kubernetes/override-values.yaml
```

## DAG Management with Git Sync

To manage DAGs in Airflow, you can use Git Sync, which allows you to automatically sync DAG files from a Git repository to a Kubernetes cluster.

Follow the [Git Sync documentation](https://airflow.apache.org/docs/helm-chart/stable/manage-dags-files.html) to set up Git Sync.

## Building and Storing a Docker Image

To build and store a Docker image that includes all necessary dependencies, run the following command:

```bash
gcloud builds submit --region=global --tag us-west2-docker.pkg.dev/akee-376111/akee-docker/akee-predict:latest --machine-type=E2_HIGHCPU_32
```

## Storing Credentials

To store credentials, create a Kubernetes secret using the following command:

```bash
kubectl create secret generic credentials-json --from-file=credentials.json=./credentials.json -n airflow
```

Then, in the Airflow admin panel's variable section, add a new variable with the key "google" and the value of the contents of `credentials.json`.

## Backfill Loading

To backfill load data for your DAGs, use the following commands:

```bash
airflow dags backfill nft_dag --start-date 2021-01-01 --end-date 2023-01-01
airflow dags backfill twitter_extract --start-date 2021-01-01 --end-date 2023-01-01
```

Replace `nft_dag` and `twitter_extract` with the names of your own DAGs.

## Generating Fernet Key

To generate a Fernet key, you need to run the `fernet.py` script. You can do this by running the following command:

```bash
python fernet.py
```

This will generate a Fernet key which will be used to encrypt and decrypt passwords and other sensitive information in your Airflow installation.

## Storing Fernet Key

To store the Fernet key securely, you need to create a Kubernetes secret. You can do this by running the following command:

```bash
kubectl create secret generic fernet-key --from-literal=fernet-key=<YOUR_FERNET_KEY> -n airflow
```

Replace `<YOUR_FERNET_KEY>` with the Fernet key generated in the previous step.

## Adding Credentials to Airflow

To add your Google Cloud credentials to Airflow, you need to add them to two places: Variables and Connections.

### Variables

To add the credentials to variables, go to the Airflow admin panel and navigate to the "Variables" section. Click the "Create" button and add a new variable with the key "google" and the value of the contents of your `credentials.json` file.

### Connections

To add the credentials to connections, go to the Airflow admin panel and navigate to the "Connections" section. Click the "Create" button and fill in the form with the following details:

- Conn Id: `google_cloud`
- Conn Type: `Google Cloud Platform`
- Project Id: `<YOUR_PROJECT_ID>`
- Keyfile JSON: Contents of your `credentials.json` file

Replace `<YOUR_PROJECT_ID>` with your actual Google Cloud project ID.

## Using Kubernetes Executor

It is recommended to use the Kubernetes Executor with Airflow on Kubernetes. This can be done by adding the following configuration to your `airflow.cfg` file:

```ini
[core]
executor = KubernetesExecutor
```

This will ensure that your DAGs are run in a Kubernetes Pod rather than on the Airflow server.

Congratulations, you have completed the setup of Airflow on Kubernetes with K3s and can now run your DAGs in a scalable and fault-tolerant manner!
