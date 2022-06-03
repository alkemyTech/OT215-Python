from airflow import DAG
from datetime import  timedelta, datetime
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
from scripts.extraccionSQL import sqlUMoron
from scripts.extraccionSQL import sqlUNRC
from scripts.funcionPandas import pandasProcesamiento
from scripts.cargaS3 import s3CargaUMoron
from scripts.cargaS3 import s3CargaUNRC


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
    cargaS3_UMoron = PythonOperator(
        task_id='cargaS3_UMoron',
        python_callable=s3CargaUMoron,
        op_kwargs={
            'filename': '/home/lautaror/airflow/dags/files/fileUMoron.txt',
            'key': 'fileUMoron.txt',
            'bucket_name': 'cohorte-mayo-2820e45d'
        }
        )
    cargaS3_UNRC = PythonOperator(
        task_id='cargaS3_UNRC',
        python_callable=s3CargaUNRC,
        op_kwargs={
            'filename': '/home/lautaror/airflow/dags/files/fileUNRC.txt',
            'key': 'fileUNRC.txt',
            'bucket_name': 'cohorte-mayo-2820e45d'
        }
        )     

    [consultaSQL_UNMoron, consultaSQL_UNRC ] >> procesamientoPandas >> [cargaS3_UMoron, cargaS3_UNRC]