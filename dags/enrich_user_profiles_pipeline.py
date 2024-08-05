"""
User profiles enrichment pipeline
"""
import datetime as dt

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

DEFAULT_ARGS = {
    'depends_on_past': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': 5,
}

dag = DAG(
    dag_id="user_profiles_enrichment_pipeline",
    description="Enrich user profiles data",
    schedule_interval=None,
    start_date=dt.datetime(2022, 8, 1),
    catchup=True,
    tags=['user profiles'],
    default_args=DEFAULT_ARGS,
)

dag.doc_md = __doc__

enrich_user_profiles_to_gold =  BigQueryInsertJobOperator(
    task_id='enrich_user_profiles_to_gold',
    dag=dag,
    location='us-east1',
    project_id='de-07-maksym-ostapenko',
    configuration={
        "query": {
            "query": "{% include 'sql/enrich_user_profiles_to_gold.sql' %}",
            "useLegacySql": False,
        }
    },
    params={
        'project_id': "de-07-maksym-ostapenko"
    }
)

enrich_user_profiles_to_gold