FROM apache/airflow:slim-2.5.1-python3.9

USER root

RUN apt-get update && apt-get install git libgomp1 -y
USER airflow

RUN pip install poetry
RUN python -m poetry config virtualenvs.create false

COPY poetry.lock pyproject.toml /app/

# switch directory
WORKDIR /app

USER root
RUN chmod -R 777 /app

USER airflow

RUN umask 0002; python -m poetry export -f requirements.txt > requirements2.txt

RUN umask 0002; \
    pip install --no-cache-dir -r requirements2.txt

RUN umask 0002; \
    pip install celery apache-airflow-providers-google


RUN umask 0002; \
    pip install psycopg2-binary

RUN umask 0002; \
    pip install 'apache-airflow[kubernetes]' 'apache-airflow[dask]'
