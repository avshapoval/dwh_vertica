import pendulum
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.vertica.hooks.vertica import VerticaHook


log = logging.getLogger(__name__)

YANDEX_LOGIN = Variable.get('YANDEX_LOGIN')

vertica_connection_id = 'VERTICA_CONNECTION'


def upload_to_dds(sql_filepath, target_table, column_list):
    vertica_hook = VerticaHook(vertica_connection_id)

    with open(f'{sql_filepath}') as f:
        sql_command = f.read()

    for r in (("$login", YANDEX_LOGIN), ("$table_name", target_table), ("$columns", ", ".join(column_list))):
        sql_command = sql_command.replace(*r)
    
    log.info(sql_command)

    vertica_hook.run(sql_command)


with DAG(
    dag_id = "stg_to_dds",
    schedule_interval = None,
    start_date = pendulum.datetime(2021, 9, 20)
) as dag:
    load_l_user_group_activity = PythonOperator(
        task_id = "load_l_user_group_activity",
        python_callable=upload_to_dds,
        op_kwargs={
            "sql_filepath": "/lessons/sql/load_data/load_to_dds/load_l_user_group_activity.sql",
            "target_table": "l_user_group_activity",
            "column_list": ["hk_l_user_group_activity", "hk_user_id", "hk_group_id", "load_dt", "load_src"]
        }
    )
    load_s_auth_history = PythonOperator(
        task_id = "load_s_auth_history",
        python_callable=upload_to_dds,
        op_kwargs={
            "sql_filepath": "/lessons/sql/load_data/load_to_dds/load_s_auth_history.sql",
            "target_table": "s_auth_history",
            "column_list": ["hk_l_user_group_activity", "user_id_from", "event", "event_dt", "load_dt", "load_src"]
        }
    )
    load_l_user_group_activity >> load_s_auth_history