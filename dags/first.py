import csv
import os.path
from datetime import timedelta, datetime
from airflow import DAG
from airflow.hooks.filesystem import FSHook
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
import pandas as pd
import re

parsed = '333.csv'
filename = 'tiktok_google_play_reviews.csv'
hook = FSHook('file_sensor')
filepath = os.path.join(hook.get_path(), filename)
filepath_parsed = os.path.join(hook.get_path(), parsed)
df = pd.read_csv(filepath, sep=',', on_bad_lines='skip', index_col=False, dtype='unicode')

args = {
    'owner': 'lg45h',
    'start_date': days_ago(1)
}


def sample_python_task(**context):
    print('hewwo >< i found your baka data file ~~~')


def replace_null_values():
    global df
    df = df.fillna('-')
    df.to_csv(filepath, index=False)
    return True


def sort_by_date():
    global df
    df = df.sort_values(by='at')
    return True


def remove_non_text_chars(text):
    pattern = r'[^a-zA-Z\s!,.?]'
    cleaned_text = re.sub(pattern, '', str(text))
    return cleaned_text


def filter_content():
    global df
    df['content'] = df['content'].apply(remove_non_text_chars)
    return True


def output_file_contents():
    global df
    first_20_rows = df[['content', 'at']].head(20)
    print(first_20_rows)

    # reusability moment
    if os.path.exists(filepath_parsed):
        os.remove(filepath_parsed)
    df.to_csv(filepath_parsed, sep=',', index=False, quoting=csv.QUOTE_ALL)
    return True


with DAG(
    dag_id='first_sensor',
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
    sensor_task = FileSensor(
        task_id='sensor_task',
        fs_conn_id='file_sensor',
        filepath='tiktok_google_play_reviews.csv',
        poke_interval=10
    )
    with TaskGroup(group_id='task_group', dag=dag) as task_group:
        crucial_logic = PythonOperator(
            task_id='yes_its_here',
            python_callable=sample_python_task,
            retries=10,
            retry_delay=timedelta(1),
            dag=dag
        )
        replace_null_values = PythonOperator(
            task_id='replace_null_values',
            python_callable=replace_null_values,
            dag=dag
        )
        sort_by_date = PythonOperator(
            task_id="sort_by_date",
            python_callable=sort_by_date
        )
        filter_content = PythonOperator(
            task_id='filter_content',
            python_callable=filter_content
        )
        crucial_logic >> replace_null_values >> sort_by_date >> filter_content

    output_file_contents = PythonOperator(
        task_id='output_file_contents',
        python_callable=output_file_contents,
        provide_context=True,
        dag=dag
    )
    trigger = TriggerDagRunOperator(
        task_id='test_trigger_dagrun',
        trigger_dag_id="mongoupload",
        dag=dag
    )
    sensor_task >> task_group >> output_file_contents >> trigger
