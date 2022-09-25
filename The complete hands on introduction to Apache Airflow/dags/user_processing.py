import json
from datetime import datetime

import pandas
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


def _process_user(ti):
    # Utiliza os resultados da task extract_user para obter os usuários
    user_response = ti.xcom_pull(key='return_value', task_ids='extract_user')
    user = user_response['results'][0]

    processed_user = pandas.json_normalize({
        'firstname': user['name']['first'],
        'lastname': user['name']['last'],
        'country': user['location']['country'],
        'username': user['login']['username'],
        'password': user['login']['password'],
        'email': user['email'],
    })

    # Salva o usuário processa em um csv para ser posteriormente lido, esse
    # padrão de salvar os dados processados em arquivos para serem consumidos
    # por outras dags é muito comum em tasks Airflow
    processed_user.to_csv('/tmp/processed_user.csv', index=None, header=False)


def _store_user():
    hook = PostgresHook(postgres_conn_id='postgres')

    # Copia os dados do arquivo csv para a tabela users do banco de dados
    hook.copy_expert(sql="COPY users FROM stdin WITH DELIMITER as ','",
                     filename='/tmp/processed_user.csv')


'''
start_date -> Define a data inicial em que as dag runs vâo começar a ser
              agendadas
catchup -> Define se as dag runs que já passaram desde a start_date deven
           ser executadas quando a dag for ativa, é recomendado deixar
           como False para evitar uma sobrecarga de execuções. OBS: o padrão
           do Airflow é Trues

OBS: Podemos testar tasks apartir da interface do CLI do airflow usando a sintaxe:
     airflow tasks test <nome da dag> <nome da task> <start_date>
'''
with DAG('user_processing',
         start_date=datetime(2022, 1, 1),
         schedule_interval='@daily',
         catchup=False) as dag:
    '''
    Define uma task utilizando um operator de ação, ou seja, é uma task
    que realiza uma operação independente de fatores externos

    postgres_conn_id -> É o id da conexão definida a partir da UI do Airflow
    '''
    task_create_table = PostgresOperator(task_id='create_table',
                                         postgres_conn_id='postgres',
                                         sql='''
                                             CREATE TABLE IF NOT EXISTS users (
                                                  firstname TEXT NOT NULL,
                                                  lastname TEXT NOT NULL,
                                                  country TEXT NOT NULL,
                                                  username TEXT NOT NULL,
                                                  password TEXT NOT NULL,
                                                  email TEXT NOT NULL
                                             )
                                         ''')
    '''
    Define uma task utilizando sensors, que são operador que emitem ações
    quando uma conexão ou evento é disparada

    http_conn_id -> É o id da conexão http definida a partir da UI do Airflow
    '''
    task_is_api_available = HttpSensor(task_id='is_api_available',
                                       http_conn_id='user_api',
                                       endpoint='api/')
    '''
    response_filter -> Função que define como a resposta da requisição
                       será tratada
    log_response -> Determina se a resposta deverá aparecer nos logs
    '''
    task_extract_user = SimpleHttpOperator(
        task_id='extract_user',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET',
        response_filter=lambda r: json.loads(r.text),
        log_response=True)

    task_process_user = PythonOperator(task_id='process_user',
                                       python_callable=_process_user)
    task_store_user = PythonOperator(task_id='store_user',
                                     python_callable=_store_user)

    task_create_table >> task_is_api_available >> task_extract_user >> task_process_user >> task_store_user
