"""
OT215 Sprint 1 C
Configurar logs para Universidad Nacional De Jujuy
Configurar logs para Universidad De Palermo
Utilizar la librería de logging de python: https://docs.python.org/3/howto/logging.html
Deben dejar la configuración lista para que se pueda incluir dentro de las funciones futuras.
No es necesario empezar a escribir logs.
"""
from datetime import datetime
from datetime import timedelta
import logging
from airflow.models import DAG
from airflow.operators.python import PythonOperator

def log_dag ():
    '''
    logs configuration %Y-%m-%d-name_logger-message
    '''
    logger = logging.getLogger('university_logger')
    logging.basicConfig(format='%(asctime)s %(logger)s %(message)s', datefmt='%Y-%m-%d', filename='log_dag.log', encoding='utf-8', level=logging.DEBUG)
    logging.info("Logs start...")
    return None

def log_university_jujuy():
    '''
    default handler log for university of Jujuy
    '''
    pass

def log_university_palermo():
    '''
    deafult handler log for university of Palermo
    '''
    pass

with DAG(
    dag_id='logs_UNJ_UP_dag',
    description='''
                logs settings for DAG 
                and functions log 
                for UNJ and UP
                ''',
    schedule_interval = timedelta(hours=1),
    start_date=datetime(year=2022, month=5, day=24),
    catchup=False
) as dag:
    log_dag_task = PythonOperator(
        task_id = "log_config",
        python_callable = log_dag
    )
    log_jujuy = PythonOperator(
        task_id = "log_uni_jujuy",
        python_callable = log_university_jujuy
    )
    log_palermo = PythonOperator(
        task_id = "log_uni_palermo",
        python_callable = log_university_palermo
    )

log_dag_task >> log_jujuy >> log_palermo