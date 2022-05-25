from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator

with DAG("universities_etl",
         description="""
         Consultas SQL para las universidades,
         procesamiento de los datos con Pandas,
         cargado de datos en S3.
         """,
         start_date=datetime(2022, 5, 23),
         schedule_interval='@hourly'
         ) as dag:

    # Consulta para traer los datos de la primer universidad (UNLPam)
    fst_query = DummyOperator(task_id='query_unlpam', retries=5)

    # Consulta para traer los datos de la segunda universidad (UAI)
    scnd_query = DummyOperator(task_id='query_uai', retries=5)

    # Procesamiento de datos
    data_process = DummyOperator(task_id='data_process', retries=5)

    # Cargado de datos en S3
    data_load = DummyOperator(task_id='data_load', retries=5)

    [fst_query, scnd_query] >> data_process >> data_load
