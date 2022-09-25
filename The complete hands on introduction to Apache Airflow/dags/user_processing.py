from datetime import datetime

from airflow import DAG

'''
start_date -> Define a data inicial em que as dag runs vâo começar a ser
              agendadas
catchup -> Define se as dag runs que já passaram desde a start_date deven
           ser executadas quando a dag for ativa, é recomendado deixar
           como False para evitar uma sobrecarga de execuções. OBS: o padrão
           do Airflow é Trues
'''
with DAG('user_processing', start_date=datetime(2022, 1, 1),
         schedule_interval='@daily', catchup=False) as dag:
    pass
