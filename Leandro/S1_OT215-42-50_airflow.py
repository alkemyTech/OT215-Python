import airflow
from airflow import DAG
#from airflow.operators.bash_operator import BashOperator
#from airflow.operators.python_operator import PythonOperator
from datetime import timedelta,datetime


#--------------------DAG-----------------
default_args = {
    'start_date': datetime(2022,5,25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    }

dag=DAG(
    'OT215-42-50',
    default_args=default_args,
    description='Para primer sprint',
    schedule_interval=timedelta(minutes=5)
)

#---------------------operadores--------------
#Conexion a las bases de datos
