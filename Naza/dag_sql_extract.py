import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

from scripts.db_extraction import data_extract
from scripts.log_config import LOG_CFG
from scripts.uai_unlpam_cleaning import uai_unlpam_cleaner

logging.config.dictConfig(LOG_CFG)
logger = logging.getLogger('DAG')

default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG("unlpam_uai_etl",
         description="""
         Consultas SQL para las universidades,
         procesamiento de los datos con Pandas,
         cargado de datos en S3.
         """,
         start_date=datetime(2022, 5, 23),
         schedule_interval='@hourly',
         default_args=default_args
         ) as dag:

    # Consulta para traer los datos de la primer universidad (UNLPam)
    unlpam_query = PythonOperator(
        task_id='query_unlpam',
        python_callable=data_extract,
        op_args=['query_unlpam.sql']
    )

    # Consulta para traer los datos de la segunda universidad (UAI)
    uai_query = PythonOperator(
        task_id='query_uai',
        python_callable=data_extract,
        op_args=['query_uai.sql']
    )

    # Procesamiento de datos
    data_process = DummyOperator(task_id='data_process')

    # Cargado de datos en S3
    data_load = DummyOperator(task_id='data_load')

    [unlpam_query, uai_query] >> data_process >> data_load
