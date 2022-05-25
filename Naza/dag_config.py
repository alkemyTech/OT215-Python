from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator

with DAG("extract_uai_unlpam",
         description="""
         Consultas SQL para las universidades UAI y UNLPam,
         procesamiento de los datos con Pandas,
         cargado de datos en S3.
         """,
         start_date=datetime(2022, 5, 24),
         schedule_interval='@hourly'
         ) as dag:

    # Consulta para traer los datos de la primer universidad (UNLPam)
    # -> PostgresOperator
    fst_query = DummyOperator(task_id='query_unlpam')

    # Consulta para traer los datos de la segunda universidad -(UAI)
    # -> PostgresOperator
    scnd_query = DummyOperator(task_id='query_uai')

    # Procesamiento de datos
    # -> PythonOperator
    data_process = DummyOperator(task_id='data_process')

    # Cargado de datos en S3
    # -> PythonOperator
    data_load = DummyOperator(task_id='data_load')

    [fst_query, scnd_query] >> data_process >> data_load
