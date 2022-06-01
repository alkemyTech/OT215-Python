"""OT215-62 Sprint 2 C
Configurar un Python Operators, para que extraiga información de la base de datos 
utilizando el .sql disponible en el repositorio base.
"""
from datetime import datetime
from datetime import timedelta
from logging import raiseExceptions
import pandas as pd 
from pathlib import Path
import os
import logging

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

#Logger configuration
logger = logging.getLogger('dag_logger')
logging.basicConfig(format='%(asctime)s %(logger)s %(message)s', datefmt='%Y-%m-%d', filename='log_dag.log', 
encoding='utf-8', level=logging.DEBUG)

#parent project path and files, sql directory
path = Path(__file__).resolve().parent.parent

# Retries configuration 
DEFAULT_ARGS = {
    'owner': 'university_dag',
    'email': ['university_dag@example.com'],
    'retries':1,
    'retry_delay': timedelta(minutes=5)
}

# Extraction data for universities
def extract_data(query1, query2): 
    """  
    config and connection postgres DB training  
    PostgresHook provided connection
    save fie into csv
    """
    queries= [query1, query2]

    try:
        logging.info(f"connection to postgrest database.")
        pg_hook = PostgresHook(
            postgres_conn_id='postgrest',
            schema='training'
        )
        pg_conn = pg_hook.get_conn()
        cursor = pg_conn.cursor()
        logging.info(f"connect to db succesfull.")

    except Exception as e:
        logging.error(f"Error connection to database." + e)

    for query in queries:
        #open sql file
        fd = open(query)
        sql_uni_file = fd.read()
        fd.close()
        #execute queries
        cursor.execute(sql_uni_file)
        sql_query = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(data=sql_query, columns=col_names)

        #create directory if not exists
        if not os.path.exists(str(path) + 'dags/files' ):
           os.makedirs(str(path) + 'dags/files/')
        #save file university name
        file_name= str(''.join(list(query))).replace(str(path) + "/dags/sql","").replace(".sql","")
        df.to_csv(os.path.join(path,r'dags/files/'+ file_name+'.csv'))
        logging.info(f"Cvs {file_name} created with success.")

 
with DAG(
    dag_id='sql_to_csv_dag',
    description='''
                Extraction data
                for universities  
                and save into csv
                ''',
    default_args=DEFAULT_ARGS,
    schedule_interval = timedelta(hours=1),
    start_date=datetime(year=2022, month=5, day=24),
    catchup=False
) as dag:
    #config Python operator UNJ
    extraction_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        op_kwargs={
                    "query1":str(path) + '/dags/sql/uni_jujuy.sql',
                    "query2":str(path) + '/dags/sql/uni_palermo.sql'}
    )

extraction_data