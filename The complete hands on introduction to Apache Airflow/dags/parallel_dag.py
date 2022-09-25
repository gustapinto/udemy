from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG('parallel_dag', start_date=datetime(2022, 1, 1),
         schedule_interval='@daily', catchup=False) as dag:
    task_extract_a = BashOperator(task_id='extract_a',
                                  bash_command='sleep 10')
    task_extract_b = BashOperator(task_id='extract_b',
                                  bash_command='sleep 10')
    task_load_a = BashOperator(task_id='load_a', bash_command='sleep 10')
    task_load_b = BashOperator(task_id='load_b', bash_command='sleep 10')
    '''
    queue -> Define em qual fila a task será executada quando estivermos
             utilizanfo o CeleryExecutor
    '''
    task_transform = BashOperator(task_id='transform',
                                  queue='high_cpu',
                                  bash_command='sleep 30')

    task_extract_a >> task_load_a
    task_extract_b >> task_load_b
    [task_load_a, task_load_b] >> task_transform