import os
import datetime as dt
import pandas as pd
from util.deco import python_operator
from airflow.operators.bash import BashOperator
import logging

def get_path(file_name):
    return os.path.join(os.path.expanduser('~'), file_name)
# @python_operator()
# def pushes_something_to_xcom(**context):
#     context['task_instance'].xcom_push('my_xcom_key', 'my_xcom_value')
#
#
# @python_operator()
# def pulls_something_from_xcom(**context):
#     xcom_value = context['task_instance'].xcom_pull(task_ids="pushes_something_to_xcom", key='my_xcom_key')
#     logging.info('XCom value: "%s"', xcom_value)
@python_operator()
def download_titanic_dataset(**context):
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    df = pd.read_csv(url)
    # import pdb; pdb.set_trace()
    df.to_csv(get_path('titanic.csv'), encoding='utf-8')
    # context['task_instance'].xcom_push('my_xcom_key', value = df)

@python_operator()
def pivot_dataset():
    titanic_df = pd.read_csv(get_path('titanic.csv'))
    df = titanic_df.pivot_table(index=['Sex'],
                                columns=['Pclass'],
                                values='Name',
                                aggfunc='count').reset_index()
    df.to_csv(get_path('titanic_pivot.csv'))

@python_operator()
def mean_fare_per_class():
    titanic_df = pd.read_csv(get_path("titanic.csv"))
    df = titanic_df[['Pclass', 'Fare']].groupby('Pclass').mean()
    df.to_csv(get_path('titanic_mean_fares.csv'))

def first_task(dag):
    first_task = BashOperator(
        task_id='first_task',
        bash_command='echo "Here we start! Info: run_id={{ run_id }} | dag_run={{ dag_run }}"',
        dag=dag,
    )
    return first_task

# последний таск
def last_task(dag):
    last_task = BashOperator(
        task_id="last_task",
        bash_command="echo Pipeline finished! Execution date is {{ ds }}",
        dag=dag
    )
    return last_task