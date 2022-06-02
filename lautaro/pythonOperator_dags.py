from airflow import DAG
from datetime import  timedelta, datetime
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
from scripts.extraccionSQL import sqlUMoron
from scripts.extraccionSQL import sqlUNRC
from scripts.funcionPandas import pandasProcesamiento


logger = logging.getLogger('logger')
handlerConsola = logging.StreamHandler() 
logger.addHandler(handlerConsola) 
logger.setLevel(logging.DEBUG)
logging.basicConfig(format='%(asctime)s - %(name)s - %(message)s', datefmt='%Y-%m-%d')
logger.info('Mensaje del log')

default_args_dag={
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'universidadesF_dags.py',
    description='Dag Universidades grupo F',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 5, 25),
    default_args=default_args_dag
) as dag:
    consultaSQL_UNMoron = PythonOperator(
        task_id='consultaSQL_UNMoron',
        python_callable=sqlUMoron,
        dag=dag
        )
    consultaSQL_UNRC = PythonOperator(
        task_id='consultaSQL_UNRC',
        python_callable=sqlUNRC,
        dag=dag
        )
    procesamientoPandas = PythonOperator(
        task_id='procesamientoPandas',
        python_callable=pandasProcesamiento,
        dag=dag
        )
    cargaS3 = DummyOperator(task_id='cargaS3')
     

    [consultaSQL_UNMoron, consultaSQL_UNRC ] >> procesamientoPandas >> cargaS3



    

    


