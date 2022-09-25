from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator

from subdags.subdag_downloads import subdag_downloads
from subdags.subdag_transforms import subdag_transforms


'''
OBS: SubDags não são mais suportadas desde a versão 2.2. ao invés delas
utilize TaskGroups
'''
with DAG('group_with_subdags', start_date=datetime(2022, 1, 1),
         schedule_interval='@daily', catchup=False) as dag:
    args = {
        'start_date': dag.start_date,
        'schedule_interval': dag.schedule_interval,
        'catchup': dag.catchup,
    }

    task_downloads = SubDagOperator(task_id='downloads',
                                    subdag=subdag_downloads(dag.dag_id,
                                                            'downloads',
                                                            args))
    task_check_files = BashOperator(task_id='check_files',
                                    bash_command='sleep 10')
    task_transforms = SubDagOperator(task_id='transforms',
                                     subdag=subdag_transforms(dag.dag_id,
                                                              'transforms',
                                                              args))


    task_downloads >> task_check_files >> task_transforms
