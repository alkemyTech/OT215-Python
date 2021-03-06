import logging
import os
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

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

    # Dado un .sql y una conexion previamente configurada desde la UI de
    # Airflow, nos conectamos a la base de datos, se ejecuta la query
    # y el resultado se guarda en .csv en el directorio files
    # (Subdirectorio del directorio donde se levanta el server).
    def data_extract(sql: str):
        pg_hook = PostgresHook(postgres_conn_id="unlpam_uai")
        pg_engine = pg_hook.get_sqlalchemy_engine()
        conn = pg_engine.connect()
        with open('scripts/' + sql, 'r') as query:
            df = pd.read_sql_query(query.read(), conn)
            os.makedirs('files', exist_ok=True)
            df.to_csv('files/' + sql.split('.')[0] + '.csv')

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

    # Cargado de datos en S3
    data_load = DummyOperator(task_id='data_load')

    [unlpam_query, uai_query] >> data_process >> data_load
