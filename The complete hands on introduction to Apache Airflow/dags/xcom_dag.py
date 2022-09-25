from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator



def _t1(ti):
    # Salva um valor explicitamente no XCom, com esse valor podendo
    # ocupar até 64kb
    ti.xcom_push(key='my_key', value=42)

    # Outra forma de adicionarmos valores no XCom é retornando esses
    # valores com um return, essa XCom terá como chave "return_value"
    return 42


def _t2(ti):
    # Retira valores do Xcom, especificando qual a chave do valor
    # e qual task produziou (xcom_push) esse valor
    value = ti.xcom_pull(key='my_key', task_ids='t1')
    return_value = ti.xcom_pull(key='return_value', task_ids='t1')

    print(value)
    print(return_value)


def _branch(ti):
    value = ti.xcom_pull(key='my_key', task_ids='t1')

    '''
    Branch operators permitem que a ordem e a excução das tasks seja
    controlado, com a função de "branch" retornando o task_id da task
    que deverá ser utilizada
    '''

    if value == 42:
        return 't2'

    return 't3'


with DAG('xcom_dag', start_date=datetime(2022, 1, 1),
         schedule_interval='@daily', catchup=False) as dag:
    task_t1 = PythonOperator(task_id='t1', python_callable=_t1)
    task_branch = BranchPythonOperator(task_id='branch',
                                       python_callable=_branch)
    task_t2 = PythonOperator(task_id='t2', python_callable=_t2)
    task_t3 = BashOperator(task_id='t3', bash_command="echo ''")
    task_t4 = BashOperator(task_id='t4', bash_command="echo ''",
                           trigger_rule='none_failed_min_one_success')

    task_t1 >> task_branch >> [task_t2, task_t3] >> task_t4
