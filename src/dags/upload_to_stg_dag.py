import pendulum
import boto3
import logging
import os
import csv

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.vertica.hooks.vertica import VerticaHook


log = logging.getLogger(__name__)

AWS_ACCESS_KEY_ID = Variable.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY')
YANDEX_LOGIN = Variable.get('YANDEX_LOGIN')

vertica_connection_id = 'VERTICA_CONNECTION'


def get_s3_file(file_key, target_filename):
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket = 'sprint6',
        Key = file_key,
        Filename = f"/data/{target_filename}"
    )

def print_10_lines():
    directory = '/data'
    for filename in os.listdir(directory):
        file_with_path = '/data/' + filename
        file_extension = os.path.splitext(file_with_path)[1]
        if file_extension == '.csv':
            log.info(f"{file_with_path}, {file_extension}")
            with open(file_with_path, 'r') as f:
                file_reader = csv.reader(f)
                for _ in range(10):
                    log.info(next(file_reader))

def upload_to_vertica_stg(source_filepath, sql_filepath, target_table, column_list):
    vertica_hook = VerticaHook(vertica_connection_id, schema=f'{YANDEX_LOGIN}__STAGING')

    with open(f'{sql_filepath}') as f:
        sql_command = f.read()

    for r in (("$login", YANDEX_LOGIN), ("$table_name", target_table), ("$columns", ", ".join(column_list)), ("$filename", f"/data/{source_filepath}")):
        sql_command = sql_command.replace(*r)
    
    log.info(sql_command)

    vertica_hook.run(sql_command)


with DAG(
    dag_id = "upload_to_stg",
    schedule_interval = None,
    start_date = pendulum.datetime(2021, 9, 20)
) as dag:
    fetch_group_log = PythonOperator(
        task_id = "fetch_group_log",
        python_callable=get_s3_file,
        op_kwargs={
            "file_key": "group_log.csv",
            "target_filename": "group_log.csv"
        }
    )
    print_10_lines_group_log = PythonOperator(
        task_id = "print_10_lines",
        python_callable=print_10_lines
    )
    upload_group_log_to_stg = PythonOperator(
        task_id = "upload_group_log_to_stg",
        python_callable=upload_to_vertica_stg,
        op_kwargs={
            "source_filepath": "group_log.csv",
            "sql_filepath": "/lessons/sql/load_data/load_to_stg/load_group_log.sql",
            "target_table": "group_log",
            "column_list": ["group_id", "user_id", "user_id_from", "event", "event_dt"]
        }
    )

    fetch_group_log >> print_10_lines_group_log >> upload_group_log_to_stg