from airflow import DAG
from airflow.operators.bash import BashOperator


'''
Uma subdag é definada como uma função que retorna uma DAg
'''
def subdag_downloads(parent_dag_id, child_dag_id, args):
    with DAG(f'{parent_dag_id}.{child_dag_id}',
             start_date=args['start_date'],
             schedule_interval=args['schedule_interval'],
             catchup=args['catchup']) as dag:
        task_download_a = BashOperator(task_id='download_a',
                                       bash_command='sleep 10')
        task_download_b = BashOperator(task_id='download_b',
                                       bash_command='sleep 10')
        task_download_c = BashOperator(task_id='download_c',
                                       bash_command='sleep 10')

        return dag
