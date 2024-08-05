"""
Customers processing pipeline
"""
import datetime as dt

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from table_defs.customers_csv import customers_csv

DEFAULT_ARGS = {
    'depends_on_past': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': 5,
}

dag = DAG(
    dag_id="customers_pipeline",
    description="Ingest and process customers data",
    schedule_interval='0 7 * * *',
    start_date=dt.datetime(2022, 8, 1),
    end_date=dt.datetime(2022, 8, 6),
    catchup=True,
    tags=['customers'],
    default_args=DEFAULT_ARGS,
)

dag.doc_md = __doc__

transfer_from_data_lake_to_raw = BigQueryInsertJobOperator(
    task_id='transfer_from_data_lake_to_raw',
    dag=dag,
    location='us-east1',
    project_id='de-07-maksym-ostapenko',
    configuration={
        "query": {
            "query": "{% include 'sql/transfer_customers_from_data_lake_raw_to_bronze.sql' %}",
            "useLegacySql": False,
            "tableDefinitions": {
                "customers_csv": customers_csv,
            },
        }
    },
    params={
        'data_lake_raw_bucket': "raw-bucket-ostapenko",
        'project_id': "de-07-maksym-ostapenko"
    }
)

transfer_from_bronze_to_silver = BigQueryInsertJobOperator(
    task_id='transfer_from_bronze_to_silver',
    dag=dag,
    location='us-east1',
    project_id='de-07-maksym-ostapenko',
    configuration={
        "query": {
            "query": "{% include 'sql/transfer_customers_from_bronze_to_silver.sql' %}",
            "useLegacySql": False,
        }
    },
    params={
        'project_id': "de-07-maksym-ostapenko"
    }

)

transfer_from_data_lake_to_raw >> transfer_from_bronze_to_silver