import os
from datetime import timedelta, datetime

import pandas as pd
from airflow.hooks.filesystem import FSHook
from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from dotenv import load_dotenv
from airflow.providers.mongo.hooks.mongo import MongoHook


parsed = '333.csv'
filename = 'tiktok_google_play_reviews.csv'
hook = FSHook('file_sensor')
filepath = os.path.join(hook.get_path(), filename)
filepath_parsed = os.path.join(hook.get_path(), parsed)
args = {
    'owner': 'lg45h',
    'start_date': days_ago(1)
}


def db_upload():
    load_dotenv()
    container_name = 'df151e0e8057'
    port_number = 27017
    connection_string = "mongodb://admin:password@239b2a458473:27017/?authMechanism=DEFAULT"
    client = MongoClient(connection_string)
    df = pd.read_csv(filepath_parsed)
    data = df.to_dict('records')
    db = client['task7']
    collection = db['tiktokdata']
    result = collection.delete_many({})
    collection.insert_many(data)
    client.close()
    return True


with DAG(
    dag_id='mongoupload',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    description='SnowFlake ETL',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['ETL']
) as dag:
    db_upload = PythonOperator(
        task_id='db_upload',
        python_callable=db_upload,
        dag=dag
    )
