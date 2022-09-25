from airflow import DAG
from airflow.operators.bash import BashOperator


def subdag_transforms(parent_dag_id, child_dag_id, args):
    with DAG(f'{parent_dag_id}.{child_dag_id}', **args) as dag:
        task_transform_a = BashOperator(task_id='transform_a',
                                        bash_command='sleep 10')
        task_transform_b = BashOperator(task_id='transform_b',
                                        bash_command='sleep 10')
        task_transform_c = BashOperator(task_id='transform_c',
                                        bash_command='sleep 10')

        return dag
