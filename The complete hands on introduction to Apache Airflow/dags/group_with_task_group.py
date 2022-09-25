from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from groups.group_downloads import download_tasks
from groups.group_transforms import transform_tasks


with DAG('group_with_task_group', start_date=datetime(2022, 1, 1),
         schedule_interval='@daily', catchup=False) as dag:

    task_downloads = download_tasks()
    task_check_files = BashOperator(task_id='check_files',
                                    bash_command='sleep 10')
    task_transforms = transform_tasks()

    task_downloads >> task_check_files >> task_transforms
