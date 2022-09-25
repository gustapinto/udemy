from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


def download_tasks():
    with TaskGroup('downloads', tooltip='Download tasks') as group:
        task_download_a = BashOperator(task_id='download_a',
                                       bash_command='sleep 10')
        task_download_b = BashOperator(task_id='download_b',
                                       bash_command='sleep 10')
        task_download_c = BashOperator(task_id='download_c',
                                       bash_command='sleep 10')

        return group
