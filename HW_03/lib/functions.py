import os
import pandas as pd
from deco import python_operator
from airflow.operators.bash import BashOperator
# from airflow.operators.postgres_operator import PostgresOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable

pivot_name = Variable.get("pivot_name")
mean_fare_name = Variable.get("mean_fare_name")


def get_path(file_name):
    return os.path.join(os.path.expanduser('~'), file_name)


@python_operator()
def download_titanic_dataset(**context):
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    df = pd.read_csv(url)
    context['task_instance'].xcom_push('HW3_titanic', df.to_json())


@python_operator()
def pivot_dataset(**context):
    pivot_value = context['task_instance'].xcom_pull(task_ids="download_titanic_dataset", key='HW3_titanic')
    df = pd.read_json(pivot_value).pivot_table(index=['Sex'],
                                               columns=['Pclass'],
                                               values='Name',
                                               aggfunc='count').reset_index()
    # context['task_instance'].xcom_push(key="pivot", value=df.to_json())
    hook = BaseHook.get_hook(conn_id='titanic_bd')
    df.to_sql(pivot_name, hook.get_sqlalchemy_engine())


@python_operator()
def mean_fare_per_class(**context):
    mean_fare_value = context['task_instance'].xcom_pull(task_ids="download_titanic_dataset", key='HW3_titanic')
    df = pd.read_json(mean_fare_value)[['Pclass', 'Fare']].groupby('Pclass').mean()
    # context['task_instance'].xcom_push(key="mean_fare", value=df.to_json())
    hook = BaseHook.get_hook(conn_id='titanic_bd')
    df.to_sql(mean_fare_name, hook.get_sqlalchemy_engine())


def first_task(dag):
    first_task = BashOperator(
        task_id='first_task',
        bash_command='echo "Here we start! Info: run_id={{ run_id }} | dag_run={{ dag_run }}"',
        dag=dag,
    )
    return first_task


def last_task(dag):
    last_task = BashOperator(
        task_id="last_task",
        bash_command="echo Pipeline finished! Execution date is {{ ds }}",
        dag=dag
    )
    return last_task
