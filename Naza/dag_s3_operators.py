import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.db_extraction import data_extract
from scripts.log_config import LOG_CFG
from scripts.s3_load import file_to_s3
from scripts.uai_unlpam_cleaning import uai_unlpam_cleaner

logging.config.dictConfig(LOG_CFG)
logger = logging.getLogger('DAG')

default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG("unlpam_uai",
         description="""
         Consultas SQL para las universidades,
         procesamiento de los datos con Pandas,
         cargado de datos en S3.
         """,
         start_date=datetime(2022, 5, 23),
         schedule_interval='@once',
         # default_args=default_args
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
    data_process = PythonOperator(
        task_id='uai_unlpam_cleaning',
        python_callable=uai_unlpam_cleaner
    )

    uai_to_s3 = PythonOperator(
        task_id='uai_to_s3',
        python_callable=file_to_s3,
        op_kwargs={
            'filename': 'files/uai_clean.txt',
            'key': 'uai_clean',
            'bucket_name': 'cohorte-mayo-2820e45d'
        }
    )

    unlpam_to_s3 = PythonOperator(
        task_id='unlpam_to_s3',
        python_callable=file_to_s3,
        op_kwargs={
            'filename': 'files/unlpam_clean.txt',
            'key': 'unlpam_clean',
            'bucket_name': 'cohorte-mayo-2820e45d'
        }
    )

    [unlpam_query, uai_query] >> data_process >> [unlpam_to_s3, uai_to_s3]
