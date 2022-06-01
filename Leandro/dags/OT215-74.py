from distutils.debug import DEBUG
import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
#from airflow.operators.bash_operator import BashOperator
#from airflow.operators.python_operator import PythonOperator
from datetime import timedelta,datetime
import logging
from OT215-82 import normalize_fun

logging.basicConfig(
    format = '%(asctime)s %(name)s %(message)s',
    datefmt='%Y-%m-%d' ,
    level  = logging.DEBUG 
)

#--------------------DAG-----------------
default_args = {
    'start_date': datetime(2022,5,25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    }

dag=DAG(
    'OT215-42-50',
    default_args=default_args,
    description='Para primer sprint',
    schedule_interval=timedelta(minutes=5)
)

t0 = DummyOperator(task_id="start")

#---------------------operadores--------------
connection_pstg="" #conexion a la base de datos postgresql
#---------------------SQL---------------------
kennedy_query = PostgresOperator(
        task_id="kennedy_query",
        sql='./sql/OT215-18_ubaKennedy.sql'
        postgres_conn_id=connection_pstg
    )

sociales_query = PostgresOperator(
        task_id="sociales_query",
        sql='./sql/OT215-18_latSociales.sql'
        postgres_conn_id=connection_pstg
    )

#------------python operator-----------
normalize_data_kennedy=PythonOperator(
    task_id="normalize_data_kennedy",
    python_callable=normalize_fun,
    op_kwargs = {"path_file" : "./files/kennedy.csv"},
    dag=dag
)

normalize_data_sociales=PythonOperator(
    task_id="normalize_data_sociales",
    python_callable=normalize_fun,
    op_kwargs = {"path_file" : "./files/sociales.csv"},
    dag=dag
)

t0 >> kennedy_query >> normalize_data_kennedy
t0 >> sociales_query >> normalize_data_sociales