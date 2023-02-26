import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from DDL.manage_tables import init_bq_client
from ETL_ELT.elt_pipeline import (load_files_from_bucket_to_raw_tables,
                                  load_invalid_records_from_raw_to_error,
                                  load_records_from_stage_to_dm,
                                  load_valid_records_from_raw_to_stage,
                                  processing_fact_table)
from utils.bq_utils import move_files_from_source_to_destination_bucket

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"
] = '/opt/airflow/dags/json_key/data-n-analytics-edu-345714-658a4f6e1c6d.json'

DATASET_ID = 'job_4_star'
BUCKET_NAME = "raw_files_job_2"
DESTINATION_BUCKET = "processed_files_for_job_4"


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'ELT_PIPELINE',
    default_args=default_args,
    description='ELT_pipeline_to_process_data_from_GCS_to_star_schema_DWH',
    schedule_interval=timedelta(hours=3),
)

t1 = PythonOperator(
    task_id='load_files_from_bucket_to_raw_tables',
    python_callable=load_files_from_bucket_to_raw_tables,
    op_kwargs={'client': init_bq_client()},
    dag=dag
)

t2 = PythonOperator(
    task_id="load_invalid_records_from_raw_to_error",
    python_callable=load_invalid_records_from_raw_to_error,
    op_kwargs={'client': init_bq_client()},
    dag=dag
)

t3 = PythonOperator(
    task_id="load_valid_records_from_raw_to_stage",
    python_callable=load_valid_records_from_raw_to_stage,
    op_kwargs={'client': init_bq_client()},
    dag=dag
)

t4 = PythonOperator(
    task_id="load_records_from_stage_to_dm",
    python_callable=load_records_from_stage_to_dm,
    op_kwargs={'client': init_bq_client()},
    dag=dag
)

t5 = PythonOperator(
    task_id="processing_fact_table",
    python_callable=processing_fact_table,
    op_kwargs={'client': init_bq_client()},
    dag=dag
)

t6 = PythonOperator(
    task_id="move_files_from_source_to_destination_bucket",
    python_callable=move_files_from_source_to_destination_bucket,
    op_kwargs={
            'bucket_name': BUCKET_NAME,
            "destination_bucket": DESTINATION_BUCKET
    },
    dag=dag
)

t1 >> t2 >> t3 >> t4 >> t5 >> t6
