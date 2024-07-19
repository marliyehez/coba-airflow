from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator
from google.oauth2 import service_account
import pandas as pd
import logging

from etl_mysql_bq_bigquery_daily import MySQLToBigQueryPipeline

# MySQLToBigQueryPipeline 'Dim_Date' arguments
pipeline_kwargs = {
    'project_id' :'golden-union-392713',
    'source_table': 'shop_dataset.orders',
    'dest_table': 'shop_dwh_marli.Dim_Date',
    'credentials_src': '/tmp/bq-admin.json',
    'credentials_dest': '/tmp/bq-admin.json'
}

# Initialize and configure logging
logging.basicConfig(filename='ingestion.log',
                    filemode='a',
                    format='%(asctime)s - %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')

# Initialize the pipeline (outside of tasks for illustration purposes)
pipeline = MySQLToBigQueryPipeline(**pipeline_kwargs)

# Set default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'dim_date',
    default_args=default_args,
    description='Dim Date',
    schedule_interval='0 2 * * *',
    start_date=datetime(2024, 7, 18),
    catchup=False,
) as dag:

    @task
    def extract(**kwargs):
        logging.info(f"Extracting the data from {pipeline.source_table}")
        df = pipeline.extract()
        return df

    @task
    def transform(**kwargs):
        extracted_data = kwargs['ti'].xcom_pull('extract')
        df = pd.DataFrame.from_dict(extracted_data)
        logging.info("Transforming")
        transformed_df = pipeline.transform(df)
        return transformed_df

    @task
    def load(**kwargs):
        transformed_data = kwargs['ti'].xcom_pull('transform')
        transformed_df = pd.DataFrame.from_dict(transformed_data)
        logging.info(f"Load data to {pipeline.dest_table}")
        pipeline.load(transformed_df)


    extract() >> transform() >> load()
