#Absolute imports
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

#Custom imports
from extract_openaq_data import main

default_args={
    'owner': 'ericsig',
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}

etl_dag = DAG(
    default_args=default_args,
    dag_id='OpenAQ_etl',
    description= 'Extract, Transform and Load data from OpenAQ API to Redshift DB',
    start_date=datetime(2023,9,28),
    schedule_interval='@daily',
    catchup=True
)

etl_pipeline = PythonOperator(
    task_id='etl_pipeline',
    python_callable=main,
    dag=etl_dag
)

etl_pipeline