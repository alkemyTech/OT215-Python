"""
OT215 Sprint 1 C
Configurar los retries con la conexión a la base de datos
Configurar el retry para las tareas del DAG de las siguientes universidades:
Universidad Nacional De Jujuy
Universidad De Palermo
"""
from datetime import datetime
from datetime import timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

# Retries configuration for task university of Jujuy and Palermo
DEFAULT_ARGS = {
    """
    config:
    retries: the number of retries 
    that should be performed before failing the task 
    retry_delay : delay between retries
    """
    
    'owner': 'retries_university_dag',
    'depends_on_past': False,
    'email': ['my_university_dag@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries':1,
    'retry_delay': timedelta(minutes=5)
}

# Connection to postgres
def connection_postgres(): 
    """  
    config and connection postgres DB training  
    PostgresHook provided connection
    """
    pg_hook = PostgresHook(
        postgres_conn_id='postgres',
        schema='training'
    )
    pg_conn = pg_hook.get_conn()

    return None


with DAG(
    dag_id='retries_university_dag',
    default_args=DEFAULT_ARGS,
    schedule_interval = timedelta(hours=1),
    start_date=datetime(year=2022, month=5, day=24),
    catchup=False
) as dag:
    # task connection Postgres database
    task_connection = PythonOperator(
        task_id='connection_postgres',
        python_callable=connection_postgres,
    )

task_connection