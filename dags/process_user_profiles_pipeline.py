"""
User profiles processing pipeline
"""
import datetime as dt

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

DEFAULT_ARGS = {
    'depends_on_past': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': 5,
}

dag = DAG(
    dag_id="user_profiles_pipeline",
    description="Ingest user profiles data",
    schedule_interval=None,
    start_date=dt.datetime(2022, 8, 1),
    catchup=True,
    tags=['user profiles'],
    default_args=DEFAULT_ARGS,
)

dag.doc_md = __doc__

transfer_from_data_lake_to_silver = GCSToBigQueryOperator(
    task_id='transfer_from_data_lake_to_silver',
    dag=dag,
    bucket="raw-bucket-ostapenko",
    source_objects=['user_profiles/user_profiles.json'],
    destination_project_dataset_table=f'de-07-maksym-ostapenko.silver.user_profiles',
    schema_fields=[
        {"name": "email", "type": "STRING", "mode": "NULLABLE"},
        {"name": "full_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "state", "type": "STRING", "mode": "NULLABLE"},
        {"name": "birth_date", "type": "STRING", "mode": "NULLABLE"},
        {"name": "phone_number", "type": "STRING", "mode": "NULLABLE"},
    ],
    source_format="NEWLINE_DELIMITED_JSON",

)

transfer_from_data_lake_to_silver