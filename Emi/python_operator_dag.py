"""OT215-70 Sprint 2 C 
Configurar el Python Operator para que ejecute las dos funciones 
que procese los datos para las siguientes universidades:
Universidad Nacional De Jujuy
Universidad De Palermo
"""
from datetime import datetime
from datetime import timedelta
from pathlib import Path

from airflow.models import DAG
from airflow.operators.python import PythonOperator

#parent project path and files, sql directory
path = Path(__file__).resolve().parent.parent

# Retries configuration 
DEFAULT_ARGS = {
    'owner': 'university_dag',
    'email': ['university_dag@example.com'],
    'retries':1,
    'retry_delay': timedelta(minutes=5)
}

# Extraction .sql data function
def extract_data(query1, query2):
    pass

with DAG(
    dag_id='python_operator_sql_dag',
    description='''
                Configuration 
                Python Operators
                for SQL data UNJ and UP 
                ''',
    default_args=DEFAULT_ARGS,
    schedule_interval = timedelta(hours=1),
    start_date=datetime(year=2022, month=5, day=24),
    catchup=False
) as dag:

    # Config Python operator extration .sql data queries
    extraction_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        op_kwargs={
                    "query1":str(path) + '/dags/sql/uni_jujuy.sql',
                    "query2":str(path) + '/dags/sql/uni_palermo.sql'
        }
    )

extraction_data